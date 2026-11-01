import {
  isOrchestrationV2WorkActive,
  isProviderNativeSubagentThread,
  pendingGuestMessageCount,
  type ConditionalThreadSettlementInput,
  type ConditionalThreadSettlementResult,
  type OrchestrationV2ProviderThread,
  type ThreadSettlementRefusal,
} from "@t3tools/contracts";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";

import type { ProjectionRecords } from "./ProjectionStore.ts";
import { isUndeliveredMailboxSteer } from "./NotificationMailbox.ts";

export const settlementRecordFields = [
  "runs",
  "runtimeRequests",
  "messages",
  "subagents",
  "providerThreads",
  "providerTurns",
  "turnItems",
] as const;

export function conditionalSettlementDecision(
  input: ConditionalThreadSettlementInput,
  projection: ProjectionRecords<(typeof settlementRecordFields)[number]>,
  nowMs: number,
  backgroundActivityMs: number,
): ConditionalThreadSettlementResult | null {
  const { thread, runs } = projection;
  const skip = (reason: ThreadSettlementRefusal) => ({
    threadId: input.threadId,
    runId: input.expectedRunId,
    outcome: "skipped" as const,
    reason,
  });
  if (thread.archivedAt !== null) return skip("archived");
  if (isProviderNativeSubagentThread(thread)) return skip("unsupported_thread");
  const latest = runs.reduce<(typeof runs)[number] | undefined>(
    (current, run) => (current === undefined || run.ordinal > current.ordinal ? run : current),
    undefined,
  );
  if (latest?.id !== input.expectedRunId || latest.threadId !== input.threadId)
    return skip("run_changed");
  if (latest.status !== "completed" || latest.completedAt === null) return skip("not_completed");
  if (thread.settledOverride === "settled" && thread.settledAt !== null)
    return {
      threadId: input.threadId,
      runId: input.expectedRunId,
      outcome: "already_settled",
      settledAt: DateTime.formatIso(thread.settledAt),
    };
  if (runs.some((run) => ["preparing", "starting", "running", "waiting"].includes(run.status)))
    return skip("active");
  if (runs.some((run) => run.status === "queued")) return skip("queued_work");
  if (
    projection.runtimeRequests.some((request) => request.status === "pending") ||
    pendingGuestMessageCount(thread.guestShare) > 0
  )
    return skip("pending_input");
  if (
    projection.providerThreads.some((provider) => provider.status === "active") ||
    projection.turnItems.some(
      (item) => item.type === "dynamic_tool" && isOrchestrationV2WorkActive(item.status),
    ) ||
    projection.subagents.some(
      (task) =>
        isOrchestrationV2WorkActive(task.status) ||
        task.completionDelivery?.state === "pending" ||
        task.completionDelivery?.state === "claimed",
    ) ||
    derivePendingBackgroundWork({
      latestRun: latest,
      runs,
      providerThreads: projection.providerThreads,
      turnItems: projection.turnItems,
    }).length > 0 ||
    runs.some(
      (run) =>
        run.delegatedCompletion?.disposition === "open" &&
        run.delegatedCompletion.delivery !== null,
    ) ||
    projection.messages.some((message) => isUndeliveredMailboxSteer(projection, message.id))
  )
    return skip("background_work");
  if (thread.pullRequests?.some((link) => link.watch !== undefined)) return skip("watching");
  if (thread.limitRecovery?.autoResume === true || thread.limitRecovery?.snooze === true)
    return skip("recovery_armed");
  if (
    thread.pinnedAt != null ||
    thread.autoSettleDisabledAt != null ||
    thread.settledOverride === "active"
  )
    return skip("user_override");
  if (thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs)
    return skip("snoozed");

  let activityMs = Math.max(DateTime.toEpochMillis(latest.completedAt), backgroundActivityMs);
  for (const message of projection.messages)
    activityMs = Math.max(activityMs, DateTime.toEpochMillis(message.updatedAt));
  for (const task of projection.subagents)
    activityMs = Math.max(activityMs, DateTime.toEpochMillis(task.updatedAt));
  for (const item of projection.turnItems)
    activityMs = Math.max(activityMs, DateTime.toEpochMillis(item.updatedAt));
  const remaining = input.minInactiveMs - (nowMs - activityMs);
  if (remaining > 0)
    return {
      ...skip("too_recent"),
      retryAfterMs: Math.min(remaining, 90 * 24 * 60 * 60 * 1_000),
    };
  return null;
}

export function backgroundRosterKey(provider: OrchestrationV2ProviderThread): string {
  return JSON.stringify(
    (provider.pendingBackgroundTasks ?? [])
      .map(({ taskId, kind, awaited }) => [taskId, kind, awaited ?? false])
      .sort((left, right) => String(left[0]).localeCompare(String(right[0]))),
  );
}
