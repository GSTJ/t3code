import { ScheduledTask, ScheduledTaskError } from "@t3tools/contracts";
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
  ) {
    return "Coalesce window must be an integer from 10 to 3600 seconds.";
  }
  for (const template of [dedupe?.replayKey, dedupe?.coalesceKey]) {
    if (template === undefined) continue;
    const remainder = template.replace(
      /\{\{\s*(?:(?:body|headers|query)(?:\.[^{}\s]+)*|request)\s*\}\}/g,
      "",
    );
    if (remainder.includes("{{") || remainder.includes("}}"))
      return "Invalid webhook dedupe template.";
  }
}

export const acceptWebhookDedupe = Effect.fn("ScheduledTaskService.acceptWebhookDedupe")(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected"> = Effect.void,
) {
  const sql = yield* SqlClient.SqlClient;
  const dedupe = task.schedule.type === "webhook" ? task.schedule.dedupe : undefined;
  if (dedupe === undefined) return "run" as const;
  const key = (template: string | undefined) =>
    template === undefined ? "" : renderWebhookPrompt(template, request, "", true).prompt;
  const replayKey = key(dedupe?.replayKey);
  if (replayKey !== "") {
    yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE expires_at <= ${now}`;
    const claimed =
      yield* sql`INSERT INTO scheduled_task_webhook_replay_keys (task_id, replay_key, expires_at)
      VALUES (${task.id}, ${replayKey}, ${now + 7 * 24 * 60 * 60 * 1000})
      ON CONFLICT DO NOTHING RETURNING replay_key`;
    if (claimed.length === 0) return "duplicate" as const;
  }
  const coalesceKey = key(dedupe?.coalesceKey);
  if (coalesceKey === "") {
    yield* reserve;
    return "run" as const;
  }
  const requestJson = yield* encodePendingRequest({
    method: request.method,
    path: request.path,
    query: request.query,
    headers: request.headers,
    bodyText: request.bodyText,
  });
  // Expired windows are immutable, even if the scheduler has not dispatched them yet.
  yield* sql`UPDATE scheduled_task_webhook_pending SET state = 'dispatching'
    WHERE task_id = ${task.id} AND coalesce_key = ${coalesceKey} AND due_at <= ${now}`;
  const replaced = yield* sql`UPDATE scheduled_task_webhook_pending
    SET request_json = ${requestJson}
    WHERE task_id = ${task.id} AND coalesce_key = ${coalesceKey} AND state = 'pending'
    RETURNING delivery_id`;
  if (replaced.length > 0) return "coalesced" as const;
  yield* reserve;
  yield* sql`INSERT INTO scheduled_task_webhook_pending
    (delivery_id, task_id, coalesce_key, due_at, task_json, request_json)
    VALUES (${deliveryId}, ${task.id}, ${coalesceKey}, ${now + (dedupe?.coalesceWindowSeconds ?? 120) * 1000},
      ${yield* encodePendingTask(task)}, ${requestJson})`;
  return "pending" as const;
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
