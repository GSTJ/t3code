import {
  MAX_WEBHOOK_DELIVERY_AGE_MINUTES,
  MAX_WEBHOOK_FILTERS,
  ScheduledTaskWebhookFilter,
  type ScheduledTaskWebhookFilterOp,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const DEFAULT_WEBHOOK_PROMPT = "Handle this webhook:\n{{body}}";

/** Blank means "no limit"; undefined means the input is not a valid limit, which blocks saving. */
export function parseMaxDeliveryAge(value: string): number | null | undefined {
  if (value.trim() === "") return null;
  const minutes = Number(value.trim());
  return Number.isInteger(minutes) && minutes > 0 && minutes <= MAX_WEBHOOK_DELIVERY_AGE_MINUTES
    ? minutes
    : undefined;
}

/** One editable filter row. `in` takes a comma-separated list; `exists` ignores the value. */
export interface WebhookFilterDraft {
  readonly path: string;
  readonly op: ScheduledTaskWebhookFilterOp;
  readonly value: string;
}

export const WEBHOOK_FILTER_OPS: ReadonlyArray<{
  readonly op: ScheduledTaskWebhookFilterOp;
  readonly label: string;
}> = [
  { op: "equals", label: "equals" },
  { op: "notEquals", label: "does not equal" },
  { op: "in", label: "is one of" },
  { op: "startsWith", label: "starts with" },
  { op: "matches", label: "matches regex" },
  { op: "exists", label: "exists" },
];

export const EMPTY_WEBHOOK_FILTER_DRAFT: WebhookFilterDraft = {
  path: "",
  op: "equals",
  value: "",
};

export function webhookFilterDrafts(
  filters: ReadonlyArray<ScheduledTaskWebhookFilter> | undefined,
): ReadonlyArray<WebhookFilterDraft> {
  return (filters ?? []).map((filter) => ({
    path: filter.path,
    op: filter.op,
    value:
      filter.op === "exists" ? "" : filter.op === "in" ? filter.value.join(", ") : filter.value,
  }));
}

const decodeFilter = Schema.decodeUnknownOption(ScheduledTaskWebhookFilter);

/**
 * Checks each row with the server's own schema, so a draft that parses here
 * also saves. `invalidIndex` names the first row that would not.
 */
export function parseWebhookFilterDrafts(
  drafts: ReadonlyArray<WebhookFilterDraft>,
):
  | { readonly filters: ReadonlyArray<ScheduledTaskWebhookFilter> }
  | { readonly invalidIndex: number } {
  const filters: ScheduledTaskWebhookFilter[] = [];
  for (const [index, draft] of drafts.entries()) {
    const path = draft.path.trim();
    const filter = decodeFilter(
      draft.op === "exists"
        ? { path, op: draft.op }
        : draft.op === "in"
          ? {
              path,
              op: draft.op,
              value: draft.value
                .split(",")
                .map((item) => item.trim())
                .filter((item) => item !== ""),
            }
          : { path, op: draft.op, value: draft.value },
    );
    if (Option.isNone(filter)) return { invalidIndex: index };
    filters.push(filter.value);
  }
  return filters.length > MAX_WEBHOOK_FILTERS ? { invalidIndex: MAX_WEBHOOK_FILTERS } : { filters };
}
