// @effect-diagnostics-next-line nodeBuiltinImport:off -- Keys need a deterministic digest, not random bytes.
import * as NodeCrypto from "node:crypto";
import { isWebhookDedupeTemplate, ScheduledTask, ScheduledTaskError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { renderWebhookPrompt, type WebhookRequest } from "./webhookTemplate.ts";

type Dedupe = Extract<ScheduledTask["schedule"], { type: "webhook" }>["dedupe"];

export function validateWebhookDedupe(dedupe: Dedupe): string | undefined {
  if (
    dedupe?.coalesceWindowSeconds !== undefined &&
    (!Number.isInteger(dedupe.coalesceWindowSeconds) ||
      dedupe.coalesceWindowSeconds < 10 ||
      dedupe.coalesceWindowSeconds > 3600)
  )
    return "Coalesce window must be an integer from 10 to 3600 seconds.";
  if (
    [dedupe?.replayKey, dedupe?.coalesceKey].some(
      (template) => template !== undefined && !isWebhookDedupeTemplate(template),
    )
  )
    return "Invalid webhook dedupe template: include at least one request placeholder.";
}

function key(template: string | undefined, request: WebhookRequest): string {
  if (template === undefined) return "";
  const rendered = renderWebhookPrompt(template, request, "", true);
  return rendered.missing.length > 0 || rendered.prompt === ""
    ? ""
    : NodeCrypto.createHash("sha256").update(rendered.prompt).digest("hex");
}

export const releaseWebhookReplay = Effect.fn("ScheduledTaskService.releaseWebhookReplay")(
  function* (deliveryId: string) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE delivery_id = ${deliveryId}`;
  },
);

export const acceptWebhookDedupe = Effect.fn("ScheduledTaskService.acceptWebhookDedupe")(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected">,
  reserveCoalescedWrite: Effect.Effect<boolean>,
) {
  const sql = yield* SqlClient.SqlClient;
  const dedupe = task.schedule.type === "webhook" ? task.schedule.dedupe : undefined;
  if (dedupe === undefined) return "run" as const;
  const replayKey = key(dedupe.replayKey, request);
  if (replayKey !== "") {
    const seen = yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys
      WHERE task_id = ${task.id} AND replay_key = ${replayKey} AND expires_at > ${now}`;
    if (seen.length > 0) return "duplicate" as const;
  }
  const coalesceKey = key(dedupe.coalesceKey, request);
  if (coalesceKey === "") return "run" as const;
  const pending =
    coalesceKey === ""
      ? []
      : yield* sql<{ delivery_id: string }>`SELECT delivery_id FROM scheduled_task_webhook_pending
    WHERE task_id = ${task.id} AND coalesce_key = ${coalesceKey} AND state = 'pending' AND due_at > ${now}`;
  if (pending.length > 0) {
    if (!(yield* reserveCoalescedWrite)) return yield* Effect.fail("coalesce_limited" as const);
  } else yield* reserve;
  if (replayKey !== "") {
    yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE expires_at <= ${now}`;
    yield* sql`INSERT INTO scheduled_task_webhook_replay_keys (task_id, replay_key, delivery_id, expires_at)
      VALUES (${task.id}, ${replayKey}, ${pending[0]?.delivery_id ?? deliveryId}, ${now + 7 * 24 * 60 * 60 * 1000})`;
  }
  const requestJson = yield* encodePendingRequest({
    method: request.method,
    path: request.path,
    query: request.query,
    headers: request.headers,
    bodyText: request.bodyText,
  });
  if (pending.length > 0) {
    yield* sql`UPDATE scheduled_task_webhook_pending SET request_json = ${requestJson} WHERE delivery_id = ${pending[0]!.delivery_id}`;
    return "coalesced" as const;
  }
  // Expired windows are immutable, even if the scheduler has not dispatched them yet.
  yield* sql`UPDATE scheduled_task_webhook_pending SET state = 'dispatching'
    WHERE task_id = ${task.id} AND coalesce_key = ${coalesceKey} AND state = 'pending' AND due_at <= ${now}`;
  yield* sql`INSERT INTO scheduled_task_webhook_pending (delivery_id, task_id, coalesce_key, due_at, task_json, request_json)
    VALUES (${deliveryId}, ${task.id}, ${coalesceKey}, ${now + (dedupe.coalesceWindowSeconds ?? 120) * 1000}, ${yield* encodePendingTask(task)}, ${requestJson})`;
  return "pending" as const;
});

export const acceptImmediateWebhookDedupe = Effect.fn(
  "ScheduledTaskService.acceptImmediateWebhookDedupe",
)(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected">,
) {
  const sql = yield* SqlClient.SqlClient;
  const replayKey = key(
    task.schedule.type === "webhook" ? task.schedule.dedupe?.replayKey : undefined,
    request,
  );
  if (
    replayKey !== "" &&
    (yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys WHERE task_id = ${task.id} AND replay_key = ${replayKey} AND expires_at > ${now}`)
      .length > 0
  )
    return "duplicate" as const;
  yield* reserve;
  if (replayKey !== "") {
    yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE expires_at <= ${now}`;
    yield* sql`INSERT INTO scheduled_task_webhook_replay_keys (task_id, replay_key, delivery_id, expires_at)
      VALUES (${task.id}, ${replayKey}, ${deliveryId}, ${now + 7 * 24 * 60 * 60 * 1000})`;
  }
  return "run" as const;
});

const PendingTaskJson = Schema.fromJsonString(ScheduledTask);
const encodePendingTask = Schema.encodeEffect(PendingTaskJson);
export const decodePendingTask = Schema.decodeUnknownEffect(PendingTaskJson);
const PendingRequestJson = Schema.fromJsonString(
  Schema.Struct({
    method: Schema.String,
    path: Schema.String,
    query: Schema.String,
    headers: Schema.Record(Schema.String, Schema.String),
    bodyText: Schema.String,
  }),
);
const encodePendingRequest = Schema.encodeEffect(PendingRequestJson);
export const decodePendingRequest = Schema.decodeUnknownEffect(PendingRequestJson);
export const dedupeError = (cause: unknown) =>
  new ScheduledTaskError({ message: "Could not persist webhook dedupe state.", cause });
