import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { ConditionalThreadSettlementInput } from "./threadSettlement.ts";

it("conditional settlement requires an exact run and a bounded positive integer inactivity", () => {
  const decode = Schema.decodeUnknownSync(ConditionalThreadSettlementInput);
  const input = { threadId: "thread-1", expectedRunId: "run-1", minInactiveMs: 600_000 };
  assert.deepStrictEqual(decode(input), input);
  for (const minInactiveMs of [0, -1, 0.5, NaN, Infinity, 7_776_000_001])
    assert.throws(() => decode({ ...input, minInactiveMs }));
  assert.throws(() => decode({ threadId: input.threadId, minInactiveMs: 600_000 }));
});
