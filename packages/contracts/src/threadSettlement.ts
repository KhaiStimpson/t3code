import * as Schema from "effect/Schema";

import { IsoDateTime, RunId, ThreadId } from "./baseSchemas.ts";

export const ThreadSettlementInactivityMs = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 90 * 24 * 60 * 60 * 1_000 }),
);

export const ConditionalThreadSettlementInput = Schema.Struct({
  threadId: ThreadId,
  expectedRunId: RunId,
  minInactiveMs: ThreadSettlementInactivityMs,
});
export type ConditionalThreadSettlementInput = typeof ConditionalThreadSettlementInput.Type;

export const ThreadSettlementRefusal = Schema.Literals([
  "run_changed",
  "not_completed",
  "active",
  "queued_work",
  "pending_input",
  "background_work",
  "watching",
  "recovery_armed",
  "user_override",
  "snoozed",
  "archived",
  "unsupported_thread",
  "too_recent",
  "state_changed",
]);
export type ThreadSettlementRefusal = typeof ThreadSettlementRefusal.Type;

const resultIdentity = { threadId: ThreadId, runId: RunId };
export const ConditionalThreadSettlementResult = Schema.Union([
  Schema.Struct({
    ...resultIdentity,
    outcome: Schema.Literals(["settled", "already_settled"]),
    settledAt: IsoDateTime,
  }),
  Schema.Struct({
    ...resultIdentity,
    outcome: Schema.Literal("skipped"),
    reason: ThreadSettlementRefusal,
    retryAfterMs: Schema.optional(ThreadSettlementInactivityMs),
  }),
]);
export type ConditionalThreadSettlementResult = typeof ConditionalThreadSettlementResult.Type;
