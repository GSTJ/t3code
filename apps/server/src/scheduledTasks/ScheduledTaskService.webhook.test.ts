// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no createHmac.
import * as NodeCrypto from "node:crypto";

import * as NodePlatformCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ScheduledTaskUpsertInput, SecretRequestError } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Queue from "effect/Queue";
import * as EffectScheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";

import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ScheduledTaskService from "./ScheduledTaskService.ts";

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);

it.effect(
  "preserves omitted dedupe settings and releases replay/coalesce relay claims without using rate slots",
  () =>
    withService(({ service, sql, launches }) =>
      Effect.gen(function* () {
        const dedupe = {
          replayKey: "{{headers.delivery-id}}",
          coalesceKey: "{{body.group}}",
          coalesceWindowSeconds: 10,
        };
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({ schedule: { type: "webhook", dedupe } }),
        );
        const updated = yield* service.upsert(yield* webhookTaskInput());
        assert.deepEqual(
          updated.task.schedule.type === "webhook" && updated.task.schedule.dedupe,
          dedupe,
        );
        const request = (id: string, bodyText = "{}", relayDeliveryId?: string) =>
          requestFor(task, {
            headers: { "delivery-id": id },
            bodyText,
            body: new TextEncoder().encode(bodyText),
            ...(relayDeliveryId === undefined ? {} : { relayDeliveryId }),
          });
        yield* service.triggerWebhook(request("A"));
        yield* Queue.take(launches);
        yield* service.triggerWebhook(request("B1", '{"group":"B"}'));
        for (let i = 0; i < 65; i++) {
          const duplicate = yield* service.triggerWebhook(request("A", "{}", `duplicate-${i}`));
          assert.equal(duplicate._tag === "accepted" && duplicate.outcome, "duplicate");
          const coalesced = yield* service.triggerWebhook(
            request(`B${i + 2}`, '{"group":"B"}', `coalesced-${i}`),
          );
          assert.equal(coalesced._tag === "accepted" && coalesced.outcome, "coalesced");
        }
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_relay_deliveries`).length, 0);
        assert.isTrue(
          (yield* service.listWebhookDeliveries({ id: task.id })).deliveries.every(
            (delivery) => delivery.outcome !== "duplicate" && delivery.outcome !== "coalesced",
          ),
        );
        const fresh = yield* service.triggerWebhook(request("C", "{}", "duplicate-0"));
        assert.equal(fresh._tag === "accepted" && fresh.outcome, "accepted");
        yield* Queue.take(launches);
        const cleared = yield* service.upsert(
          yield* webhookTaskInput({ schedule: { type: "webhook", dedupe: {} } }),
        );
        assert.deepEqual(
          cleared.task.schedule.type === "webhook" && cleared.task.schedule.dedupe,
          {},
        );
      }),
    ),
);

type LaunchInput = ThreadLaunchService.ThreadLaunchInput;

const webhookTaskInput = (overrides: Record<string, unknown> = {}) =>
  decodeUpsertInput({
    id: "scheduled-task:hook",
    title: "Review PRs",
    prompt: "Review this PR: {{body.pull_request.url}}",
    enabled: true,
    schedule: { type: "webhook" },
    projectId: "project-webhook",
    workspaceStrategy: { type: "root" },
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    ...overrides,
  });

const pullRequestBody = new TextEncoder().encode(
  JSON.stringify({ pull_request: { url: "https://github.com/org/repo/pull/45" } }),
);

const requestFor = (
  task: { readonly id: string; readonly webhook?: { readonly path: string } | undefined },
  overrides: Partial<ScheduledTaskService.WebhookTriggerRequest> = {},
): ScheduledTaskService.WebhookTriggerRequest => ({
  hookId: task.id,
  token: task.webhook?.path.split("/").at(-1) ?? "",
  method: "POST",
  path: `/api/hooks/${task.id}`,
  query: "",
  headers: { "content-type": "application/json" },
  body: pullRequestBody,
  bodyText: new TextDecoder().decode(pullRequestBody),
  ...overrides,
});

const withService = <A, E>(
  body: (input: {
    readonly service: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly launches: Queue.Queue<LaunchInput>;
    readonly sql: SqlClient.SqlClient;
    readonly secretsByRef: Map<string, string>;
  }) => Effect.Effect<A, E, never>,
  options: {
    readonly gate?: Deferred.Deferred<void>;
    readonly relayHookBaseUrl?: string;
    readonly sql?: SqlClient.SqlClient;
    readonly workers?: Map<string, Effect.Effect<void>>;
  } = {},
) =>
  Effect.gen(function* () {
    const launches = yield* Queue.unbounded<LaunchInput>();
    const secretsByRef = new Map<string, string>();
    const layerDependencies = Layer.mergeAll(
      NodePlatformCrypto.layer,
      options.workers === undefined
        ? Scheduler.layer
        : Layer.succeed(Scheduler.Scheduler, {
            register: <E, R>(name: string, work: Effect.Effect<void, E, R>) =>
              Effect.gen(function* () {
                const context = yield* Effect.context<R>();
                options.workers!.set(name, work.pipe(Effect.provideContext(context), Effect.orDie));
              }),
          }),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({
        launch: (input) =>
          Queue.offer(launches, input).pipe(
            Effect.andThen(options.gate ? Deferred.await(options.gate) : Effect.void),
            Effect.as({ threadId: "thread-1", resumed: false } as never),
          ),
      }),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
      }),
      Layer.mock(ThreadManagementService.ThreadManagementService)({}),
      Layer.mock(SecretRequests.SecretRequests)({
        consume: ({ ref }) => {
          const value = secretsByRef.get(ref);
          secretsByRef.delete(ref);
          return value === undefined
            ? Effect.fail(new SecretRequestError({ reason: "ref_unavailable" }))
            : Effect.succeed(value);
        },
      }),
      Layer.succeed(
        ScheduledTaskService.ScheduledTaskWebhookOrigin,
        Effect.succeed({ relayHookBaseUrl: options.relayHookBaseUrl ?? null }),
      ),
    );
    return yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const sql = yield* SqlClient.SqlClient;
      return yield* body({ service, launches, sql, secretsByRef });
    }).pipe(
      Effect.provide(
        Layer.fresh(ScheduledTaskService.layer.pipe(Layer.provide(layerDependencies))),
      ),
    );
  }).pipe(
    Effect.provide(
      options.sql === undefined
        ? SqlitePersistence.layerMemory
        : Layer.succeed(SqlClient.SqlClient, options.sql),
    ),
  );

it.effect("dispatches exactly the rendered prompt and logs the delivery", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      assert.equal(task.nextRunAt, null);
      assert.isDefined(task.webhook);
      assert.isTrue(task.webhook!.path.startsWith("/api/hooks/scheduled-task%3Ahook/"));
      assert.equal(task.webhook!.url, null);

      const result = yield* service.triggerWebhook(requestFor(task));
      assert.equal(result._tag, "accepted");
      const launched = yield* Queue.take(launches);
      const framed =
        /^Review this PR: <(webhook-data-[0-9a-f]{12})>https:\/\/github\.com\/org\/repo\/pull\/45<\/\1>\n\nText between <\1> and <\/\1> was copied from the webhook request\./;
      const frame = framed.exec(launched.initialMessage?.text ?? "")?.[1];
      assert.isDefined(frame);
      assert.equal(
        launched.commandId,
        `scheduled-task:${task.id}:webhook:${result._tag === "accepted" ? result.deliveryId : ""}`,
      );

      const { deliveries } = yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      });
      assert.equal(deliveries.length, 1);
      assert.equal(deliveries[0]?.outcome, "accepted");
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: deliveries[0]!.id,
      });
      assert.equal(delivery.body, new TextDecoder().decode(pullRequestBody));
      assert.equal(delivery.renderedPrompt, launched.initialMessage?.text);

      // Each delivery gets its own frame, so a request can't predict the tag that closes it.
      yield* service.triggerWebhook(requestFor(task));
      const next = framed.exec((yield* Queue.take(launches)).initialMessage?.text ?? "")?.[1];
      assert.isDefined(next);
      assert.notEqual(next, frame);
    }),
  ),
);

it("builds the relay hook URL from the managed tunnel's key, never the environment id", () => {
  const relayUrl = "https://relay.example.com/";
  assert.equal(
    ScheduledTaskService.relayHookBaseUrl({
      relayUrl,
      tunnelName: "t3coderelay-managedendpoint-dev-julius-0123456789abcdef",
    }),
    "https://relay.example.com/v1/hooks/0123456789abcdef",
  );
  for (const tunnelName of [undefined, "t3coderelay-managedendpoint", "x-0123456789ABCDEF"]) {
    assert.isNull(ScheduledTaskService.relayHookBaseUrl({ relayUrl, tunnelName }));
  }
});

it.effect("gives webhook tasks a relay URL when the environment has a managed tunnel", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* webhookTaskInput());
        const token = task.webhook!.path.split("/").at(-1);
        assert.equal(
          task.webhook?.url,
          `https://relay.example.com/v1/hooks/0123456789abcdef/scheduled-task%3Ahook/${token}`,
        );
      }),
    { relayHookBaseUrl: "https://relay.example.com/v1/hooks/0123456789abcdef" },
  ),
);

it.effect("answers not found for a wrong token or unknown hook without logging", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const wrongToken = yield* service.triggerWebhook(requestFor(task, { token: "nope" }));
      assert.equal(wrongToken._tag, "not_found");
      const unknown = yield* service.triggerWebhook(requestFor(task, { hookId: "missing" }));
      assert.equal(unknown._tag, "not_found");
      assert.equal(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries
          .length,
        0,
      );
    }),
  ),
);

it.effect("rotating the token retires the old URL and saving keeps it", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const input = yield* webhookTaskInput();
      const { task } = yield* service.upsert(input);
      const saved = yield* service.upsert(yield* webhookTaskInput({ title: "Renamed" }));
      assert.equal(saved.task.webhook?.path, task.webhook?.path);

      const rotated = yield* service.rotateWebhookToken({ id: task.id });
      assert.notEqual(rotated.task.webhook?.path, task.webhook?.path);
      assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "not_found");
      assert.equal((yield* service.triggerWebhook(requestFor(rotated.task)))._tag, "accepted");
    }),
  ),
);

it.effect("checks the configured signature and keeps the secret write-only", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: {
              header: "X-Hub-Signature-256",
              encoding: "hex",
              prefix: "sha256=",
              secret: "s3cret",
            },
          },
        }),
      );
      assert.deepEqual(task.schedule, {
        type: "webhook",
        signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" },
        maxDeliveryAgeMinutes: null,
      });
      assert.isTrue(task.webhook?.hasSecret);

      const unsigned = yield* service.triggerWebhook(requestFor(task));
      assert.equal(unsigned._tag, "rejected_signature");

      const signature = `sha256=${NodeCrypto.createHmac("sha256", "s3cret").update(pullRequestBody).digest("hex")}`;
      const signed = yield* service.triggerWebhook(
        requestFor(task, {
          headers: { "content-type": "application/json", "x-hub-signature-256": signature },
        }),
      );
      assert.equal(signed._tag, "accepted");

      const resaved = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" },
          },
        }),
      );
      assert.isTrue(resaved.task.webhook?.hasSecret);

      const outcomes = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries.map((delivery) => [delivery.outcome, delivery.signatureVerified]);
      assert.deepEqual(outcomes.toSorted(), [
        ["accepted", true],
        ["rejected_signature", false],
      ]);
    }),
  ),
);

it.effect("logs but does not run deliveries to a disabled task, and refuses run now", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "disabled");
      assert.equal(yield* Queue.size(launches), 0);
      const runNow = yield* service.runNow({ id: task.id }).pipe(Effect.flip);
      assert.equal(runNow.message, "Webhook tasks run when their URL receives a request.");
    }),
  ),
);

it.effect("queues a burst of deliveries instead of dropping them", () =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    yield* withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* webhookTaskInput());
          const results = yield* Effect.forEach([1, 2, 3], () =>
            service.triggerWebhook(requestFor(task)),
          );
          assert.deepEqual(
            results.map((result) => result._tag),
            ["accepted", "accepted", "accepted"],
          );
          yield* Queue.take(launches);
          yield* Deferred.succeed(gate, undefined);
          const rest = yield* Effect.all([Queue.take(launches), Queue.take(launches)]);
          assert.equal(new Set(rest.map((launch) => launch.commandId)).size, 2);
        }),
      { gate },
    );
  }),
);

it.effect("rate limits a hook past 60 deliveries a minute", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const results = yield* Effect.forEach(Array.from({ length: 61 }), () =>
        service
          .triggerWebhook(requestFor(task))
          .pipe(
            Effect.tap((result) =>
              result._tag === "accepted" ? Queue.take(launches) : Effect.void,
            ),
          ),
      );
      assert.equal(results.at(-2)?._tag, "accepted");
      assert.equal(results.at(-1)?._tag, "rate_limited");
      yield* Effect.forEach([1, 2, 3], () => service.triggerWebhook(requestFor(task)));
      const outcomes = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries.map((delivery) => delivery.outcome);
      assert.equal(outcomes.filter((outcome) => outcome === "rate_limited").length, 1);
    }),
  ),
);

it.effect("a save carrying a stale token cannot undo a rotation", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const rotated = yield* service.rotateWebhookToken({ id: task.id });
      const saved = yield* service.upsert(yield* webhookTaskInput({ title: "Edited" }));
      assert.equal(saved.task.webhook?.path, rotated.task.webhook?.path);
      assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "not_found");
    }),
  ),
);

it.effect("keeps the newest 50 deliveries when they share a timestamp", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      const ids = yield* Effect.forEach(Array.from({ length: 55 }), (_, index) =>
        service.triggerWebhook(requestFor(task, { query: `n=${index}` })).pipe(Effect.as(index)),
      );
      const { deliveries } = yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      });
      assert.equal(deliveries.length, 50);
      const first = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: deliveries[0]!.id,
      });
      const last = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: deliveries.at(-1)!.id,
      });
      assert.equal(first.delivery.query, `n=${ids.at(-1)}`);
      assert.equal(last.delivery.query, "n=5");
    }),
  ),
);

it.effect("keeps credential headers and query values out of the delivery log", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      yield* service.triggerWebhook(
        requestFor(task, {
          query: "page=2&api_key=k",
          headers: {
            "content-type": "application/json",
            authorization: "Bearer sender-token",
            "x-webhook-key": "k",
            "x-github-event": "push",
          },
        }),
      );
      const [summary] = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: summary!.id,
      });
      assert.equal(delivery.query, "page=2&api_key=[redacted]");
      assert.equal(delivery.headers.authorization, "[redacted]");
      assert.equal(delivery.headers["x-webhook-key"], "[redacted]");
      assert.equal(delivery.headers["x-github-event"], "push");
    }),
  ),
);

const queuedDeliveryCases = [
  {
    change: "paused",
    reason: "The task was paused before this delivery ran.",
    apply: (service: ScheduledTaskService.ScheduledTaskService["Service"], id: string) =>
      service.setEnabled({ id: id as never, enabled: false }),
  },
  {
    change: "switched to an interval trigger",
    reason: "The task's trigger changed before this delivery ran.",
    apply: (service: ScheduledTaskService.ScheduledTaskService["Service"]) =>
      webhookTaskInput({ schedule: { type: "interval", everyMs: 3_600_000 } }).pipe(
        Effect.flatMap(service.upsert),
      ),
  },
] as const;

it.effect.each(queuedDeliveryCases)(
  "a delivery queued behind a run does not start once the task is $change",
  ({ reason, apply }) =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      yield* withService(
        ({ service, launches }) =>
          Effect.gen(function* () {
            const { task } = yield* service.upsert(yield* webhookTaskInput());
            yield* service.triggerWebhook(requestFor(task));
            const queued = yield* service.triggerWebhook(requestFor(task));
            yield* Queue.take(launches);
            yield* apply(service, task.id);
            yield* Deferred.succeed(gate, undefined);
            const deliveryId = queued._tag === "accepted" ? queued.deliveryId : undefined;
            let delivery = (yield* service.getWebhookDelivery({
              id: task.id,
              deliveryId: deliveryId!,
            })).delivery;
            while (delivery.outcome === "accepted") {
              yield* Effect.yieldNow;
              delivery = (yield* service.getWebhookDelivery({
                id: task.id,
                deliveryId: deliveryId!,
              })).delivery;
            }
            assert.equal(delivery.outcome, "dispatch_failed");
            assert.equal(delivery.error, reason);
            assert.equal(yield* Queue.size(launches), 0);
          }),
        { gate },
      );
    }),
);

it.effect("a save without a secret keeps a secret changed after it was read", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const signature = { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" };
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: { type: "webhook", signature: { ...signature, secret: "old" } },
        }),
      );
      yield* service.upsert(
        yield* webhookTaskInput({
          schedule: { type: "webhook", signature: { ...signature, secret: "new" } },
        }),
      );
      yield* service.upsert(
        yield* webhookTaskInput({ title: "Edited", schedule: { type: "webhook", signature } }),
      );
      const sign = (secret: string) =>
        `sha256=${NodeCrypto.createHmac("sha256", secret).update(pullRequestBody).digest("hex")}`;
      const withSignature = (secret: string) =>
        requestFor(task, {
          headers: { "content-type": "application/json", "x-hub-signature-256": sign(secret) },
        });
      assert.equal(
        (yield* service.triggerWebhook(withSignature("old")))._tag,
        "rejected_signature",
      );
      assert.equal((yield* service.triggerWebhook(withSignature("new")))._tag, "accepted");
    }),
  ),
);

it.effect("caps deliveries waiting behind a stuck run", () =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    yield* withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* webhookTaskInput());
          yield* service.triggerWebhook(requestFor(task));
          yield* Queue.take(launches);
          const waiting = yield* Effect.forEach(Array.from({ length: 20 }), () =>
            service.triggerWebhook(requestFor(task)),
          );
          assert.equal(waiting.filter((result) => result._tag === "accepted").length, 19);
          assert.equal(waiting.at(-1)?._tag, "rate_limited");
          const logged = (yield* service.listWebhookDeliveries({
            id: task.id,
            acceptAnyOutcome: true,
          })).deliveries;
          assert.equal(logged.length, 20);
          yield* Deferred.succeed(gate, undefined);
        }),
      { gate },
    );
  }),
);

it.effect("logs a body's first 64 KiB by bytes, not characters", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      const text = "界".repeat(30_000);
      const body = new TextEncoder().encode(text);
      yield* service.triggerWebhook(requestFor(task, { body, bodyText: text }));
      const [summary] = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: summary!.id,
      });
      assert.isTrue(delivery.bodyTruncated);
      assert.isAtMost(new TextEncoder().encode(delivery.body).byteLength, 64 * 1024 + 3);
    }),
  ),
);

it.effect("counts the prompt as the provider does, without surrounding whitespace", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      // Values are framed, so only an empty placeholder can leave padding at the edge.
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({ prompt: `{{body.none}}${" ".repeat(2_000)}{{body.text}}` }),
      );
      const text = JSON.stringify({ text: "x".repeat(119_000) });
      const result = yield* service.triggerWebhook(
        requestFor(task, { body: new TextEncoder().encode(text), bodyText: text }),
      );
      assert.equal(result._tag, "accepted");
      yield* Queue.take(launches);
    }),
  ),
);

it.effect("does not start a run when the filled-in prompt is too long", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ prompt: "{{body}}" }));
      const text = "x".repeat(200_000);
      const body = new TextEncoder().encode(text);
      const result = yield* service.triggerWebhook(requestFor(task, { body, bodyText: text }));
      assert.equal(result._tag, "accepted");
      assert.equal(yield* Queue.size(launches), 0);
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: result._tag === "accepted" ? result.deliveryId : ("" as never),
      });
      assert.equal(delivery.outcome, "dispatch_failed");
      assert.equal(delivery.error, "The filled-in prompt is too long.");
      assert.equal(delivery.renderedPrompt?.length, 64 * 1024);
      const ok = yield* service.triggerWebhook(requestFor(task));
      assert.equal(ok._tag, "accepted");
      yield* Queue.take(launches);
    }),
  ),
);

it.effect("a held request already delivered directly runs only once", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const direct = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "relay-1" }),
      );
      const replayed = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "relay-1", receivedAt: "2026-10-04T10:00:00.000Z" }),
      );
      assert.equal(direct._tag, "accepted");
      assert.deepEqual(replayed, { ...direct, outcome: "duplicate" } as typeof replayed);
      yield* Queue.take(launches);
      const logged = (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true }))
        .deliveries;
      assert.equal(logged.length, 1);
    }),
  ),
);

it.effect("relay retries recover failed acceptance without releasing another request's claim", () =>
  withService(({ service, launches, sql }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const request = requestFor(task, { relayDeliveryId: "retry-after-log-failure" });
      yield* sql`
        CREATE TRIGGER fail_webhook_delivery_log
        BEFORE INSERT ON scheduled_task_webhook_deliveries
        BEGIN
          SELECT RAISE(FAIL, 'injected delivery log failure');
        END
      `;
      const error = yield* service.triggerWebhook(request).pipe(Effect.flip);
      assert.equal(error.message, "Could not record webhook delivery.");
      assert.equal(yield* Queue.size(launches), 0);
      assert.isEmpty(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries,
      );
      yield* sql`DROP TRIGGER fail_webhook_delivery_log`;

      const retried = yield* Effect.all(
        [service.triggerWebhook(request), service.triggerWebhook(request)],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        retried.map((result) => (result._tag === "accepted" ? result.outcome : result._tag)).sort(),
        ["accepted", "duplicate"],
      );
      assert.include((yield* Queue.take(launches)).commandId, request.relayDeliveryId!);

      yield* sql`
        CREATE TRIGGER fail_webhook_claim
        BEFORE INSERT ON scheduled_task_webhook_relay_deliveries
        BEGIN
          SELECT RAISE(FAIL, 'injected claim failure');
        END
      `;
      const retryError = yield* service.triggerWebhook(request).pipe(Effect.flip);
      assert.equal(retryError.message, "Could not record webhook delivery.");
      yield* sql`DROP TRIGGER fail_webhook_claim`;
      const replayed = yield* service.triggerWebhook(request);
      assert.equal(replayed._tag === "accepted" ? replayed.outcome : replayed._tag, "duplicate");
      assert.equal(yield* Queue.size(launches), 0);
      assert.equal(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries
          .length,
        1,
      );
    }),
  ),
);

it.effect("a held request whose sender hung up mid-request still runs", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      for (let step = 0; step < 60; step++) {
        const request = requestFor(task, { relayDeliveryId: `hung-up-${step}` });
        const fiber = yield* service
          .triggerWebhook(request)
          .pipe(Effect.provideService(EffectScheduler.MaxOpsBeforeYield, 8), Effect.forkChild);
        for (let yields = 0; yields < step; yields++) yield* Effect.yieldNow;
        yield* Fiber.interrupt(fiber);
        const retried = yield* service.triggerWebhook(request);
        assert.equal(retried._tag, "accepted");
        const deliveryId = retried._tag === "accepted" ? retried.deliveryId : undefined;
        yield* service.getWebhookDelivery({ id: task.id, deliveryId: deliveryId! });
        const launch = yield* Queue.take(launches);
        assert.include(launch.commandId, `hung-up-${step}`);
        yield* TestClock.adjust("61 seconds");
      }
      assert.equal(yield* Queue.size(launches), 0);
    }),
  ),
);

it.effect("a held request runs once even after the log has trimmed it", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const first = yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "kept" }));
      assert.equal(first._tag, "accepted");
      yield* Queue.take(launches);
      const paused = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      yield* Effect.forEach(Array.from({ length: 55 }), () =>
        service.triggerWebhook(requestFor(paused.task)),
      );
      yield* service.upsert(yield* webhookTaskInput());
      const replay = yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "kept" }));
      assert.equal(replay._tag, "accepted");
      assert.equal(yield* Queue.size(launches), 0);
    }),
  ),
);

it.effect("a rate-limited held request can run on a later pass", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      yield* Effect.forEach(Array.from({ length: 60 }), () =>
        service.triggerWebhook(requestFor(task)).pipe(Effect.tap(() => Queue.take(launches))),
      );
      const limited = yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "later" }));
      assert.equal(limited._tag, "rate_limited");
      yield* service.upsert(yield* webhookTaskInput());
      yield* TestClock.adjust("61 seconds");
      const retried = yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "later" }));
      assert.equal(retried._tag, "accepted");
      yield* Queue.take(launches);
    }),
  ),
);

it.effect("logs a held request at the time the relay received it", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      const receivedAt = DateTime.formatIso(DateTime.subtract(yield* DateTime.now, { minutes: 5 }));
      yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "relay-2", receivedAt }));
      const [delivery] = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      assert.equal(delivery?.receivedAt, receivedAt);
    }),
  ),
);

it.effect("a receive time in the future counts as now", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({ schedule: { type: "webhook", maxDeliveryAgeMinutes: 30 } }),
      );
      const now = yield* DateTime.now;
      const result = yield* service.triggerWebhook(
        requestFor(task, {
          relayDeliveryId: "future",
          receivedAt: DateTime.formatIso(DateTime.add(now, { days: 365 })),
        }),
      );
      assert.equal(result._tag, "accepted");
      yield* Queue.take(launches);
      const [delivery] = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      assert.equal(delivery?.receivedAt, DateTime.formatIso(now));
    }),
  ),
);

it.effect("skips a held request older than the task's max age", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({ schedule: { type: "webhook", maxDeliveryAgeMinutes: 30 } }),
      );
      const now = yield* DateTime.now;
      const old = DateTime.formatIso(DateTime.subtract(now, { minutes: 31 }));
      const fresh = DateTime.formatIso(DateTime.subtract(now, { minutes: 5 }));
      const tooOld = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "old", receivedAt: old }),
      );
      assert.equal(tooOld._tag, "expired");
      assert.equal(yield* Queue.size(launches), 0);
      const ok = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "fresh", receivedAt: fresh }),
      );
      assert.equal(ok._tag, "accepted");
      const outcomes = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries.map((delivery) => delivery.outcome);
      assert.includeMembers(outcomes, ["expired", "accepted"]);
    }),
  ),
);

it.effect("runs a held request of any age when no max age is set", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const now = yield* DateTime.now;
      const result = yield* service.triggerWebhook(
        requestFor(task, {
          relayDeliveryId: "ancient",
          receivedAt: DateTime.formatIso(DateTime.subtract(now, { hours: 23 })),
        }),
      );
      assert.equal(result._tag, "accepted");
    }),
  ),
);

it.effect("deleting a task removes its delivery log", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      yield* service.triggerWebhook(requestFor(task));
      yield* service.delete({ id: task.id });
      assert.equal(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries
          .length,
        0,
      );
    }),
  ),
);

const githubSignature = (secret: string) =>
  `sha256=${NodeCrypto.createHmac("sha256", secret).update(pullRequestBody).digest("hex")}`;

it.effect("a signature can take the user's secret by ref, which works only once", () =>
  withService(({ service, launches, secretsByRef }) =>
    Effect.gen(function* () {
      secretsByRef.set("secret-ref:00000000000000000000000000000001", "github-secret");
      const githubSchedule = (secretRef: string) => ({
        type: "webhook",
        signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=", secretRef },
      });
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: githubSchedule("secret-ref:00000000000000000000000000000001"),
        }),
      );
      assert.isTrue(task.webhook!.hasSecret);
      const signed = yield* service.triggerWebhook(
        requestFor(task, {
          headers: {
            "content-type": "application/json",
            "x-hub-signature-256": githubSignature("github-secret"),
          },
        }),
      );
      assert.equal(signed._tag, "accepted");
      yield* Queue.take(launches);

      const reused = yield* service
        .upsert(
          yield* webhookTaskInput({
            id: "scheduled-task:other",
            schedule: githubSchedule("secret-ref:00000000000000000000000000000001"),
          }),
        )
        .pipe(Effect.flip);
      assert.include(reused.message, "already used");
    }),
  ),
);

it.effect("a retried save with an already used secretRef keeps the stored secret", () =>
  withService(({ service, launches, secretsByRef }) =>
    Effect.gen(function* () {
      secretsByRef.set("secret-ref:00000000000000000000000000000002", "github-secret");
      const save = webhookTaskInput({
        id: undefined,
        commandId: "command:mcp:schedule-task:release-hook",
        schedule: {
          type: "webhook",
          signature: {
            header: "x-hub-signature-256",
            encoding: "hex",
            prefix: "sha256=",
            secretRef: "secret-ref:00000000000000000000000000000002",
          },
        },
      });
      const first = yield* service.upsert(yield* save);
      const retried = yield* service.upsert(
        yield* webhookTaskInput({
          id: undefined,
          commandId: "command:mcp:schedule-task:release-hook",
          schedule: {
            type: "webhook",
            signature: {
              header: "x-hub-signature-256",
              encoding: "hex",
              prefix: "sha256=",
              secretRef: "secret-ref:00000000000000000000000000000002",
              secret: "made-up-secret",
            },
          },
        }),
      );
      assert.equal(retried.task.id, first.task.id);
      const signed = yield* service.triggerWebhook(
        requestFor(retried.task, {
          headers: {
            "content-type": "application/json",
            "x-hub-signature-256": githubSignature("github-secret"),
          },
        }),
      );
      assert.equal(signed._tag, "accepted");
      yield* Queue.take(launches);
    }),
  ),
);

it.effect("a signature without any secret is refused", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const failure = yield* service
        .upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" },
            },
          }),
        )
        .pipe(Effect.flip);
      assert.include(failure.message, "needs a signing secret");
    }),
  ),
);

const signatureFor = (secret: string) =>
  `sha256=${NodeCrypto.createHmac("sha256", secret).update(pullRequestBody).digest("hex")}`;

const deliveriesCounted = (outcome: string, source: "relay" | "direct") =>
  Metric.snapshot.pipe(
    Effect.map((snapshots) => {
      const found = snapshots.find(
        (snapshot) =>
          snapshot.id === "t3_webhook_deliveries_total" &&
          snapshot.attributes?.outcome === outcome &&
          snapshot.attributes?.source === source,
      );
      return found?.type === "Counter" ? Number(found.state.count) : 0;
    }),
  );

it.effect("keeps delivery attributes on the webhook operation span", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      const spans: Tracer.NativeSpan[] = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const receivedAt = DateTime.formatIso(DateTime.subtract(yield* DateTime.now, { seconds: 1 }));
      yield* service
        .triggerWebhook(requestFor(task, { receivedAt, relayDeliveryId: "traced-delivery" }))
        .pipe(Effect.withTracer(tracer));

      const span = spans.find((entry) => entry.name === "ScheduledTaskService.triggerWebhook");
      assert.isDefined(span);
      assert.equal(span?.attributes.get("scheduled_task.id"), task.id);
      assert.equal(span?.attributes.get("scheduled_task.webhook.outcome"), "disabled");
      assert.equal(span?.attributes.get("scheduled_task.webhook.source"), "relay");
      assert.equal(span?.attributes.get("scheduled_task.webhook.held_ms"), 1_000);
    }),
  ),
);

it.effect("counts each handled request by what happened to it", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: {
              header: "x-hub-signature-256",
              encoding: "hex",
              prefix: "sha256=",
              secret: "github-secret",
            },
          },
        }),
      );
      const before = {
        accepted: yield* deliveriesCounted("accepted", "direct"),
        rejected: yield* deliveriesCounted("rejected_signature", "direct"),
        notFound: yield* deliveriesCounted("not_found", "direct"),
        relayAccepted: yield* deliveriesCounted("accepted", "relay"),
        duplicate: yield* deliveriesCounted("duplicate", "relay"),
      };
      const signed = {
        "content-type": "application/json",
        "x-hub-signature-256": signatureFor("github-secret"),
      };
      yield* service.triggerWebhook(requestFor(task, { headers: signed }));
      yield* Queue.take(launches);
      yield* service.triggerWebhook(requestFor(task));
      yield* service.triggerWebhook(requestFor(task, { token: "wrong" }));
      const relayed = requestFor(task, { headers: signed, relayDeliveryId: "relay-1" });
      yield* service.triggerWebhook(relayed);
      yield* Queue.take(launches);
      yield* service.triggerWebhook(relayed);

      assert.equal((yield* deliveriesCounted("accepted", "direct")) - before.accepted, 1);
      assert.equal((yield* deliveriesCounted("rejected_signature", "direct")) - before.rejected, 1);
      assert.equal((yield* deliveriesCounted("not_found", "direct")) - before.notFound, 1);
      assert.equal((yield* deliveriesCounted("accepted", "relay")) - before.relayAccepted, 1);
      assert.equal((yield* deliveriesCounted("duplicate", "relay")) - before.duplicate, 1);
    }),
  ),
);

it.effect("dedupes replay keys per task, persists them, and expires them after seven days", () =>
  withService(
    ({ service, launches, sql }) =>
      Effect.gen(function* () {
        const schedule = { type: "webhook", dedupe: { replayKey: "{{headers.delivery-id}}" } };
        const { task } = yield* service.upsert(yield* webhookTaskInput({ schedule }));
        const request = requestFor(task, { headers: { "delivery-id": "A" } });
        const first = yield* service.triggerWebhook(request);
        assert.equal(first._tag === "accepted" && first.outcome, "accepted");
        yield* Queue.take(launches);
        const replay = yield* service.triggerWebhook(request);
        assert.equal(replay._tag === "accepted" && replay.outcome, "duplicate");
        assert.equal(yield* Queue.size(launches), 0);
        const other = yield* service.upsert(
          yield* webhookTaskInput({ id: "scheduled-task:other", schedule }),
        );
        yield* service.triggerWebhook(requestFor(other.task, { headers: request.headers }));
        yield* Queue.take(launches);
        yield* withService(
          ({ service: restarted }) =>
            Effect.gen(function* () {
              const replay = yield* restarted.triggerWebhook(request);
              assert.equal(replay._tag === "accepted" && replay.outcome, "duplicate");
            }),
          { sql, workers: new Map() },
        );
        yield* TestClock.adjust("7 days");
        const expired = yield* service.triggerWebhook(request);
        assert.equal(expired._tag === "accepted" && expired.outcome, "accepted");
        yield* Queue.take(launches);
        assert.include(
          (yield* service.listWebhookDeliveries({
            id: task.id,
            acceptAnyOutcome: true,
          })).deliveries.map((d) => d.outcome),
          "duplicate",
        );
      }),
    { workers: new Map() },
  ),
);

it.effect.each([1, 3])(
  "coalesces %i deliveries into one run with the latest body, headers and query",
  (count) => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              prompt: "{{body.value}} {{headers.version}} {{query.version}}",
              schedule: {
                type: "webhook",
                dedupe: { coalesceKey: "{{body.key}}", coalesceWindowSeconds: 10 },
              },
            }),
          );
          for (let value = 1; value <= count; value++) {
            const bodyText = JSON.stringify({ key: "B", value });
            yield* service.triggerWebhook(
              requestFor(task, {
                bodyText,
                body: new TextEncoder().encode(bodyText),
                headers: { version: String(value) },
                query: `version=${value}`,
              }),
            );
            if (value < count) yield* TestClock.adjust("2 seconds");
          }
          assert.equal(yield* Queue.size(launches), 0);
          yield* workers.get("webhook-coalescing")!;
          assert.equal(yield* Queue.size(launches), 0);
          yield* TestClock.adjust(count === 1 ? "10 seconds" : "6 seconds");
          yield* workers.get("webhook-coalescing")!;
          const launched = yield* Queue.take(launches);
          assert.equal(
            (launched.initialMessage!.text.match(new RegExp(`>${count}<`, "g")) ?? []).length,
            3,
          );
          yield* workers.get("webhook-coalescing")!;
          assert.equal(yield* Queue.size(launches), 0);
          const outcomes = (yield* service.listWebhookDeliveries({
            id: task.id,
            acceptAnyOutcome: true,
          })).deliveries.map((d) => d.outcome);
          assert.equal(outcomes.filter((o) => o === "accepted").length, 1);
          assert.equal(outcomes.filter((o) => o === "coalesced").length, count - 1);
        }),
      { workers },
    );
  },
);

it.effect("recovers a persisted coalescing window after restarting the service", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: { type: "webhook", dedupe: { coalesceKey: "B", coalesceWindowSeconds: 10 } },
          }),
        );
        yield* service.triggerWebhook(requestFor(task));
        yield* TestClock.adjust("4 seconds");
        const restartedWorkers = new Map<string, Effect.Effect<void>>();
        yield* withService(
          ({ launches }) =>
            Effect.gen(function* () {
              yield* restartedWorkers.get("webhook-coalescing")!;
              assert.equal(yield* Queue.size(launches), 0);
              yield* TestClock.adjust("6 seconds");
              yield* restartedWorkers.get("webhook-coalescing")!;
              yield* Queue.take(launches);
              yield* restartedWorkers.get("webhook-coalescing")!;
              assert.equal(yield* Queue.size(launches), 0);
              assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
            }),
          { sql, workers: restartedWorkers },
        );
      }),
    { workers },
  );
});

it.effect("empty dedupe key renders run normally and malformed templates fail at save time", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            dedupe: { replayKey: "{{headers.absent}}", coalesceKey: "{{body.absent}}" },
          },
        }),
      );
      for (let i = 0; i < 2; i++) {
        const result = yield* service.triggerWebhook(requestFor(task));
        assert.equal(result._tag === "accepted" && result.outcome, "accepted");
        yield* Queue.take(launches);
      }
      const failure = yield* service
        .upsert(
          yield* webhookTaskInput({
            schedule: { type: "webhook", dedupe: { replayKey: "{{body.bad" } },
          }),
        )
        .pipe(Effect.flip);
      assert.include(failure.message, "Invalid webhook dedupe template");
      for (const coalesceWindowSeconds of [9, 3601, 10.5]) {
        const result = yield* webhookTaskInput({
          schedule: { type: "webhook", dedupe: { coalesceWindowSeconds } },
        }).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
      }
    }),
  ),
);

it.effect("applies replay before coalescing and defaults the fixed window to 120 seconds", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            prompt: "{{body.value}}",
            schedule: {
              type: "webhook",
              dedupe: { replayKey: "{{headers.delivery-id}}", coalesceKey: "{{body.group}}" },
            },
          }),
        );
        const request = (id: string, value: string) => {
          const bodyText = JSON.stringify({ group: "B", value });
          return requestFor(task, {
            bodyText,
            body: new TextEncoder().encode(bodyText),
            headers: { "delivery-id": id },
          });
        };
        yield* service.triggerWebhook(request("B1", "first"));
        yield* service.triggerWebhook(request("B2", "latest"));
        const replay = yield* service.triggerWebhook(
          request("B2", "replay must not replace latest"),
        );
        assert.equal(replay._tag === "accepted" && replay.outcome, "duplicate");
        yield* TestClock.adjust("119 seconds");
        yield* workers.get("webhook-coalescing")!;
        assert.equal(yield* Queue.size(launches), 0);
        yield* TestClock.adjust("1 second");
        yield* workers.get("webhook-coalescing")!;
        assert.include((yield* Queue.take(launches)).initialMessage!.text, ">latest<");
      }),
    { workers },
  );
});

it.effect("rolls back replay and pending state when recording a delivery fails", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: { replayKey: "A", coalesceKey: "B", coalesceWindowSeconds: 10 },
            },
          }),
        );
        yield* sql`CREATE TRIGGER fail_dedupe_log BEFORE INSERT ON scheduled_task_webhook_deliveries BEGIN SELECT RAISE(FAIL, 'injected'); END`;
        yield* service.triggerWebhook(requestFor(task)).pipe(Effect.flip);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_replay_keys`).length, 0);
        yield* sql`DROP TRIGGER fail_dedupe_log`;
        const retry = yield* service.triggerWebhook(requestFor(task));
        assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
        yield* TestClock.adjust("10 seconds");
        yield* workers.get("webhook-coalescing")!;
        yield* Queue.take(launches);
      }),
    { workers },
  );
});

it.effect("starts a new fixed window at the deadline without replacing the expired payload", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            prompt: "{{body}}",
            schedule: { type: "webhook", dedupe: { coalesceKey: "B", coalesceWindowSeconds: 10 } },
          }),
        );
        const request = (bodyText: string) =>
          requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) });
        yield* service.triggerWebhook(request("first"));
        yield* TestClock.adjust("10 seconds");
        yield* service.triggerWebhook(request("second"));
        yield* workers.get("webhook-coalescing")!;
        assert.include((yield* Queue.take(launches)).initialMessage!.text, ">first<");
        assert.equal(yield* Queue.size(launches), 0);
        yield* TestClock.adjust("10 seconds");
        yield* workers.get("webhook-coalescing")!;
        assert.include((yield* Queue.take(launches)).initialMessage!.text, ">second<");
      }),
    { workers },
  );
});

it.effect("pausing a pending task prevents dispatch and deletion clears its dedupe state", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, launches, sql }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: { replayKey: "A", coalesceKey: "B", coalesceWindowSeconds: 10 },
            },
          }),
        );
        yield* service.triggerWebhook(requestFor(task));
        yield* service.setEnabled({ id: task.id, enabled: false });
        yield* TestClock.adjust("10 seconds");
        yield* workers.get("webhook-coalescing")!;
        assert.equal(yield* Queue.size(launches), 0);
        assert.equal(
          (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true }))
            .deliveries[0]?.outcome,
          "dispatch_failed",
        );
        yield* service.delete({ id: task.id });
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_replay_keys`).length, 0);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
      }),
    { workers },
  );
});
