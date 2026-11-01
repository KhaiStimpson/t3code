import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  RuntimeRequestId,
  ProviderThreadId,
  TurnItemId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("These runs never reach a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const layer = ProviderReplayHarness.layerWithRegistry(
  { name: "conditional-settlement" },
  ProviderAdapterRegistry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
).pipe(Layer.provideMerge(ProjectionStore.layer.pipe(Layer.provide(database))));

const completedThread = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threadId = ThreadId.make("thread:conditional");
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("conditional:create"),
    threadId,
    projectId: ProjectId.make("project:conditional-settlement"),
    title: "Completed automation",
    modelSelection: { instanceId, model: "test" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("conditional:send"),
    threadId,
    messageId: MessageId.make("conditional:message"),
    text: "Run maintenance",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const projection = yield* orchestrator.getThreadProjection(threadId);
  const run = projection.runs[0]!;
  yield* projections.apply({
    type: "run.updated",
    id: EventId.make("conditional:completed"),
    threadId,
    occurredAt: yield* DateTime.now,
    payload: { ...run, status: "completed", completedAt: yield* DateTime.now },
  });
  return {
    orchestrator,
    projections,
    threadId,
    run,
    input: { threadId, expectedRunId: run.id, minInactiveMs: 600_000 },
  };
});

it.effect("settles only at the inactivity boundary and preserves the result on retry", () =>
  Effect.gen(function* () {
    const { orchestrator, input, threadId } = yield* completedThread;
    const settle = (id: string) =>
      orchestrator.settleThreadIf({ ...input, commandId: CommandId.make(id) });
    yield* TestClock.adjust(599_999);
    assert.deepStrictEqual(yield* settle("conditional:early"), {
      threadId,
      runId: input.expectedRunId,
      outcome: "skipped",
      reason: "too_recent",
      retryAfterMs: 1,
    });
    assert.equal((yield* orchestrator.getThreadProjection(threadId)).thread.settledAt, null);
    yield* TestClock.adjust(1);
    const settled = yield* settle("conditional:eligible");
    assert.equal(settled.outcome, "settled");
    yield* TestClock.adjust(10_000);
    assert.deepStrictEqual(yield* settle("conditional:retry"), {
      ...settled,
      outcome: "already_settled",
    });
  }).pipe(Effect.provide(layer)),
);

it.effect("cannot settle a newer run with an old request even within the same clock tick", () =>
  Effect.gen(function* () {
    const { orchestrator, input, threadId } = yield* completedThread;
    yield* TestClock.adjust(600_000);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("conditional:followup"),
      threadId,
      messageId: MessageId.make("conditional:followup"),
      text: "Follow up",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    assert.deepStrictEqual(
      yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:old"),
      }),
      { threadId, runId: input.expectedRunId, outcome: "skipped", reason: "run_changed" },
    );
    assert.equal((yield* orchestrator.getThreadProjection(threadId)).thread.settledAt, null);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["pin", "user_override"],
  ["unsettle", "user_override"],
  ["snooze", "snoozed"],
  ["archive", "archived"],
] as const)("respects %s without committing any settlement effects", ([action, reason]) =>
  Effect.gen(function* () {
    const { orchestrator, input, threadId } = yield* completedThread;
    yield* TestClock.adjust(600_000);
    const commandId = CommandId.make("conditional:preference");
    yield* orchestrator.dispatch(
      action === "snooze"
        ? {
            type: "thread.snooze",
            commandId,
            threadId,
            snoozedUntil: DateTime.formatIso(DateTime.makeUnsafe(3_600_000)),
          }
        : action === "unsettle"
          ? { type: "thread.unsettle", commandId, threadId, reason: "user" }
          : { type: `thread.${action}`, commandId, threadId },
    );
    const sequence = yield* orchestrator.getThreadEventSequence(threadId);
    assert.deepStrictEqual(
      yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:refused"),
      }),
      { threadId, runId: input.expectedRunId, outcome: "skipped", reason },
    );
    assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
  }).pipe(Effect.provide(layer)),
);

it.effect("refuses a persistent tool that ordinary background summaries omit", () =>
  Effect.gen(function* () {
    const { orchestrator, projections, input, threadId } = yield* completedThread;
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make("conditional:persistent"),
      type: "turn-item.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: TurnItemId.make("conditional:persistent"),
        threadId,
        runId: input.expectedRunId,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 10,
        type: "dynamic_tool",
        toolName: "background-command",
        input: { persistent: true },
        status: "running",
        title: "Background work",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    });
    yield* TestClock.adjust(600_000);
    const sequence = yield* orchestrator.getThreadEventSequence(threadId);
    assert.deepStrictEqual(
      yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:persistent-refused"),
      }),
      { threadId, runId: input.expectedRunId, outcome: "skipped", reason: "background_work" },
    );
    assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "failed",
  "interrupted",
  "cancelled",
  "rolled_back",
  "missing-completion",
  "question",
] as const)("preserves visibility and pending questions for %s", (scenario) =>
  Effect.gen(function* () {
    const { orchestrator, projections, input, threadId, run } = yield* completedThread;
    const occurredAt = yield* DateTime.now;
    const event: OrchestrationV2DomainEvent =
      scenario === "question"
        ? {
            id: EventId.make("conditional:question"),
            type: "runtime-request.updated",
            threadId,
            occurredAt,
            payload: {
              id: RuntimeRequestId.make("conditional:question"),
              nodeId: run.rootNodeId!,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: occurredAt,
              resolvedAt: null,
            },
          }
        : {
            id: EventId.make("conditional:outcome"),
            type: "run.updated",
            threadId,
            occurredAt,
            payload: {
              ...run,
              status: scenario === "missing-completion" ? "completed" : scenario,
              completedAt: scenario === "missing-completion" ? null : occurredAt,
            },
          };
    yield* projections.apply(event);
    yield* TestClock.adjust(600_000);
    const sequence = yield* orchestrator.getThreadEventSequence(threadId);
    assert.deepStrictEqual(
      yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:refused"),
      }),
      {
        threadId,
        runId: input.expectedRunId,
        outcome: "skipped",
        reason: scenario === "question" ? "pending_input" : "not_completed",
      },
    );
    assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
    const saved = yield* orchestrator.getThreadProjection(threadId);
    assert.equal(saved.thread.settledAt, null);
    if (scenario === "question") assert.equal(saved.runtimeRequests[0]?.status, "pending");
  }).pipe(Effect.provide(layer)),
);

it.effect("concurrent callers commit only one settlement generation", () =>
  Effect.gen(function* () {
    const { orchestrator, input, threadId } = yield* completedThread;
    yield* TestClock.adjust(600_000);
    const before = yield* orchestrator.getThreadEventSequence(threadId);
    const results = yield* Effect.forEach(
      ["first", "second"],
      (id) =>
        orchestrator.settleThreadIf({ ...input, commandId: CommandId.make(`conditional:${id}`) }),
      { concurrency: "unbounded" },
    );
    assert.deepStrictEqual(results.map((result) => result.outcome).sort(), [
      "already_settled",
      "settled",
    ]);
    assert.equal(yield* orchestrator.getThreadEventSequence(threadId), before + 1);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "the commit transaction rejects a provider event arriving after inspection in the same millisecond",
  () =>
    Effect.gen(function* () {
      const { orchestrator, threadId, run } = yield* completedThread;
      const sink = yield* EventSink.EventSinkV2;
      yield* TestClock.adjust(600_000);
      const snapshot = yield* orchestrator.getThreadEventSequence(threadId);
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("conditional:racing-question"),
            type: "runtime-request.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: RuntimeRequestId.make("conditional:racing-question"),
              nodeId: run.rootNodeId!,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: now,
              resolvedAt: null,
            },
          },
        ],
      });
      const commandId = CommandId.make("conditional:stale-commit");
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const result = yield* sink
        .commitCommand({
          commandId,
          threadId,
          commandType: "thread.settle",
          acceptedAt: now,
          expectedThreadSequence: snapshot,
          events: [
            {
              id: EventId.make("conditional:stale-event"),
              type: "thread.settled",
              threadId,
              occurredAt: now,
              payload: { ...projection.thread, settledOverride: "settled", settledAt: now },
            },
          ],
          effects: [],
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.equal(result.failure._tag, "EventSinkThreadChangedError");
      assert.deepStrictEqual(
        yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect),
        [],
      );
      const saved = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(saved.thread.settledAt, null);
      assert.equal(saved.runtimeRequests[0]?.status, "pending");
    }).pipe(Effect.provide(layer)),
);

it.effect("background completion resets inactivity but provider metadata refreshes do not", () =>
  Effect.gen(function* () {
    const { orchestrator, input, threadId } = yield* completedThread;
    const sink = yield* EventSink.EventSinkV2;
    const createdAt = yield* DateTime.now;
    const provider: OrchestrationV2ProviderThread = {
      id: ProviderThreadId.make("conditional:provider"),
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: instanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [{ taskId: "task-1", kind: "background_task" }],
      createdAt,
      updatedAt: createdAt,
    };
    const writeProvider = (id: string, payload: OrchestrationV2ProviderThread) =>
      sink.write({
        events: [
          {
            id: EventId.make(id),
            threadId,
            type: "provider-thread.updated",
            occurredAt: payload.updatedAt,
            payload,
          },
        ],
      });
    yield* writeProvider("conditional:background-start", provider);
    yield* TestClock.adjust(600_000);
    assert.equal(
      (yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:background-refused"),
      })).outcome,
      "skipped",
    );
    const completedAt = yield* DateTime.now;
    const finished = { ...provider, pendingBackgroundTasks: [], updatedAt: completedAt };
    yield* writeProvider("conditional:background-ended", finished);
    assert.deepStrictEqual(
      yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:background-cooling"),
      }),
      {
        threadId,
        runId: input.expectedRunId,
        outcome: "skipped",
        reason: "too_recent",
        retryAfterMs: 600_000,
      },
    );
    yield* TestClock.adjust(500_000);
    yield* writeProvider("conditional:metadata-refresh", {
      ...finished,
      updatedAt: yield* DateTime.now,
    });
    yield* TestClock.adjust(100_000);
    assert.equal(
      (yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:background-ready"),
      })).outcome,
      "settled",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["opt-out", "user_override"],
  ["recovery", "recovery_armed"],
  ["native-child", "unsupported_thread"],
  ["watch", "watching"],
] as const)("preserves %s policy rather than forcing settlement", ([scenario, reason]) =>
  Effect.gen(function* () {
    const { orchestrator, projections, input, threadId } = yield* completedThread;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const now = yield* DateTime.now;
    if (scenario === "watch") {
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("conditional:watch"),
        threadId,
        host: "github.com",
        repository: "acme/app",
        number: 1,
        watching: true,
        link: { url: "https://github.com/acme/app/pull/1", source: "agent" },
      });
    } else {
      yield* projections.apply({
        id: EventId.make("conditional:policy"),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: now,
        payload: {
          ...projection.thread,
          ...(scenario === "opt-out" ? { autoSettleDisabledAt: now } : {}),
          ...(scenario === "recovery"
            ? {
                limitRecovery: {
                  runId: input.expectedRunId,
                  resetAt: DateTime.formatIso(now),
                  autoResume: true,
                },
              }
            : {}),
          ...(scenario === "native-child"
            ? {
                creationSource: "provider" as const,
                lineage: {
                  ...projection.thread.lineage,
                  relationshipToParent: "subagent" as const,
                  parentThreadId: ThreadId.make("conditional:parent"),
                },
              }
            : {}),
        },
      });
    }
    yield* TestClock.adjust(600_000);
    const sequence = yield* orchestrator.getThreadEventSequence(threadId);
    assert.deepStrictEqual(
      yield* orchestrator.settleThreadIf({
        ...input,
        commandId: CommandId.make("conditional:policy-refused"),
      }),
      { threadId, runId: input.expectedRunId, outcome: "skipped", reason },
    );
    assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
    assert.equal((yield* orchestrator.getThreadProjection(threadId)).thread.settledAt, null);
  }).pipe(Effect.provide(layer)),
);
