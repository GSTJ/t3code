import { describe, expect, it } from "vite-plus/test";

import {
  parseMaxDeliveryAge,
  parseWebhookFilterDrafts,
  webhookFilterDrafts,
} from "./scheduledTaskWebhook.ts";

describe("parseMaxDeliveryAge", () => {
  it("treats blank as no limit and rejects values the server would not accept", () => {
    expect(parseMaxDeliveryAge("")).toBeNull();
    expect(parseMaxDeliveryAge(" 45 ")).toBe(45);
    for (const invalid of ["0", "1.5", "-3", "abc", "1441"]) {
      expect(parseMaxDeliveryAge(invalid)).toBeUndefined();
    }
  });
});

describe("webhook filter drafts", () => {
  it("round-trips saved filters through the form rows", () => {
    const filters = [
      { path: "headers.x-github-event", op: "equals", value: "pull_request" },
      { path: "body.action", op: "in", value: ["opened", "reopened"] },
      { path: "body.label.name", op: "in", value: ["needs review", "a-b"] },
      { path: "query.source", op: "exists" },
    ] as const;
    const drafts = webhookFilterDrafts(filters);
    expect(drafts[1]).toEqual({ path: "body.action", op: "in", value: "opened, reopened" });
    expect(parseWebhookFilterDrafts(drafts)).toEqual({ filters });
  });

  it("names the first row the server would refuse", () => {
    expect(
      parseWebhookFilterDrafts([
        { path: "body.action", op: "equals", value: "opened" },
        { path: "body.action", op: "in", value: " , " },
        { path: "", op: "exists", value: "" },
      ]),
    ).toEqual({
      error: "Filter 2 needs a path under body, headers or query, and at least one value.",
    });
    expect(parseWebhookFilterDrafts([{ path: " ", op: "exists", value: "" }])).toEqual({
      error: "Filter 1 needs a path under body, headers or query.",
    });
    const seventeen = Array.from({ length: 17 }, () => ({
      path: "body.action",
      op: "exists" as const,
      value: "",
    }));
    expect(parseWebhookFilterDrafts(seventeen)).toEqual({ error: "Use at most 16 filters." });
  });
});
