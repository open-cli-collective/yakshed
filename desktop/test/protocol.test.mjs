import assert from "node:assert/strict";
import test from "node:test";

import { safeErrorMessage, validateProtocolMessage, validateRpcRequest } from "../protocol.mjs";

test("product RPC boundary accepts only named operations and plain params", () => {
  assert.deepEqual(validateRpcRequest("snapshot", {}), {});
  assert.deepEqual(validateRpcRequest("stats", {}), {});
  assert.throws(() => validateRpcRequest("shell.exec", {}), /unsupported product operation/);
  assert.throws(() => validateRpcRequest("snapshot", []), /parameters must be an object/);
  assert.throws(() => validateRpcRequest("snapshot", { nested: { value: "x".repeat(65_537) } }), /request is too large|string is too large/);
});

test("error text redacts credential-shaped values and local paths", () => {
  const value = safeErrorMessage(new Error("api_key=sk-test-123456789012 /Users/rianjs/private.txt"));
  assert.match(value, /api_key=\[REDACTED\]/);
  assert.doesNotMatch(value, /sk-test/);
  assert.doesNotMatch(value, /\/Users\/rianjs/);
});

test("preserves an authoritative active-run hint without widening the event shape", () => {
  assert.deepEqual(validateProtocolMessage({ event: "changed", revision: 4, active_count: 2 }), {
    type: "event",
    value: { event: "changed", revision: 4, active_count: 2 },
  });
  assert.throws(() => validateProtocolMessage({ event: "changed", revision: 4, active_count: -1 }), /active run count/);
});
