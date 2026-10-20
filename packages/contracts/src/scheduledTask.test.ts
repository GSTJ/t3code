import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  MIN_SCHEDULED_TASK_INTERVAL_MS,
  ScheduledTaskSchedule,
  ScheduledTaskUpsertSchedule,
} from "./scheduledTask.ts";

const decodeSchedule = Schema.decodeUnknownSync(ScheduledTaskSchedule);
const decodeUpsertSchedule = Schema.decodeUnknownSync(ScheduledTaskUpsertSchedule);

describe("ScheduledTaskSchedule", () => {
  it("keeps legacy sub-minute persisted schedules readable", () => {
    expect(
      decodeSchedule({
        type: "interval",
        everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS - 1,
      }),
    ).toEqual({
      type: "interval",
      everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS - 1,
    });
  });

  it("still rejects corrupt non-positive persisted intervals", () => {
    expect(() => decodeSchedule({ type: "interval", everyMs: 0 })).toThrow();
  });
});

describe("ScheduledTaskUpsertSchedule", () => {
  it("accepts interval schedules at the one-minute minimum", () => {
    expect(
      decodeUpsertSchedule({
        type: "interval",
        everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS,
      }),
    ).toEqual({
      type: "interval",
      everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS,
    });
  });

  it("rejects interval schedules more frequent than once per minute", () => {
    expect(() =>
      decodeUpsertSchedule({
        type: "interval",
        everyMs: MIN_SCHEDULED_TASK_INTERVAL_MS - 1,
      }),
    ).toThrow();
  });
});

describe("webhook filters", () => {
  const accepts = (filters: unknown) => {
    try {
      decodeUpsertSchedule({ type: "webhook", filters });
      return true;
    } catch {
      return false;
    }
  };

  it("keeps valid filters", () => {
    const filters = [
      { path: "headers.x-github-event", op: "equals", value: "pull_request" },
      { path: "body.action", op: "in", value: ["opened", "labeled"] },
      { path: "body.label.name", op: "matches", value: "^agent:(review|fix)$" },
      { path: "query.source", op: "exists" },
    ];
    expect(decodeUpsertSchedule({ type: "webhook", filters })).toEqual({
      type: "webhook",
      filters,
    });
    expect(decodeSchedule({ type: "webhook", signature: null, filters })).toMatchObject({
      filters,
    });
  });

  it("rejects invalid or oversized patterns at save time", () => {
    expect(accepts([{ path: "body.action", op: "matches", value: "(unclosed" }])).toBe(false);
    expect(accepts([{ path: "body.action", op: "matches", value: "a".repeat(257) }])).toBe(false);
  });

  it("rejects empty or unknown paths and values that do not fit the op", () => {
    expect(accepts([{ path: "", op: "exists" }])).toBe(false);
    expect(accepts([{ path: "  ", op: "exists" }])).toBe(false);
    expect(accepts([{ path: "request", op: "exists" }])).toBe(false);
    expect(accepts([{ path: "header.x-github-event", op: "exists" }])).toBe(false);
    expect(accepts([{ path: "body.action", op: "in", value: "opened" }])).toBe(false);
    expect(accepts([{ path: "body.action", op: "equals" }])).toBe(false);
    expect(accepts([{ path: "body.action", op: "contains", value: "x" }])).toBe(false);
  });

  it("allows at most 16 filters", () => {
    const sixteen = Array.from({ length: 16 }, () => ({ path: "body.action", op: "exists" }));
    expect(accepts(sixteen)).toBe(true);
    expect(accepts([...sixteen, { path: "body.action", op: "exists" }])).toBe(false);
  });
});
