export const PRODUCT_METHODS = new Set([
  "snapshot",
  "stats",
  "task.detail",
  "task.create",
  "task.update",
  "task.archive",
  "note.save",
  "todo.create",
  "todo.update",
  "todo.delete",
  "run.start",
  "run.interrupt",
  "approval.respond",
  "settings.update",
  "connection.create",
  "adapter.status",
  "adapter.login",
]);

export const MAX_RPC_BYTES = 256 * 1024;
export const MAX_VALUE_DEPTH = 8;
export const MAX_STRING_LENGTH = 64 * 1024;
export const MAX_ARRAY_LENGTH = 1_000;
export const MAX_OBJECT_KEYS = 256;

export function isPlainObject(value) {
  if (value === null || typeof value !== "object") return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function validateValue(value, depth = 0) {
  if (depth > MAX_VALUE_DEPTH) throw new TypeError("request is too deeply nested");
  if (value === null || typeof value === "boolean" || typeof value === "number") {
    if (typeof value === "number" && !Number.isFinite(value)) throw new TypeError("request contains an invalid number");
    return;
  }
  if (typeof value === "string") {
    if (value.length > MAX_STRING_LENGTH) throw new TypeError("request string is too large");
    return;
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_ARRAY_LENGTH) throw new TypeError("request array is too large");
    for (const item of value) validateValue(item, depth + 1);
    return;
  }
  if (!isPlainObject(value)) throw new TypeError("request contains an unsupported value");
  const keys = Object.keys(value);
  if (keys.length > MAX_OBJECT_KEYS) throw new TypeError("request object is too large");
  for (const key of keys) {
    if (key.length > 256) throw new TypeError("request key is too large");
    validateValue(value[key], depth + 1);
  }
}

export function validateRpcRequest(method, params) {
  if (typeof method !== "string" || !PRODUCT_METHODS.has(method)) throw new TypeError("unsupported product operation");
  if (params !== undefined && !isPlainObject(params)) throw new TypeError("operation parameters must be an object");
  validateValue(params ?? {});
  const encoded = JSON.stringify({ method, params: params ?? {} });
  if (Buffer.byteLength(encoded, "utf8") > MAX_RPC_BYTES) throw new TypeError("request is too large");
  return params ?? {};
}

export function validateProtocolMessage(message) {
  if (!isPlainObject(message)) throw new TypeError("service message must be an object");
  if (Object.prototype.hasOwnProperty.call(message, "event")) {
    if (message.event !== "changed" || !Number.isSafeInteger(message.revision) || message.revision < 0) {
      throw new TypeError("invalid service event");
    }
    const activeCount = message.active_count;
    if (activeCount !== undefined && (!Number.isSafeInteger(activeCount) || activeCount < 0)) {
      throw new TypeError("invalid active run count");
    }
    return {
      type: "event",
      value: { event: "changed", revision: message.revision, ...(activeCount === undefined ? {} : { active_count: activeCount }) },
    };
  }
  if ((typeof message.id !== "string" && typeof message.id !== "number") ||
      (!Object.prototype.hasOwnProperty.call(message, "result") && !Object.prototype.hasOwnProperty.call(message, "error"))) {
    throw new TypeError("invalid service response");
  }
  if (Object.prototype.hasOwnProperty.call(message, "error")) {
    if (!isPlainObject(message.error) || typeof message.error.message !== "string") throw new TypeError("invalid service error");
    return { type: "response", value: { id: String(message.id), error: { message: message.error.message.slice(0, 1_024) } } };
  }
  return { type: "response", value: { id: String(message.id), result: message.result } };
}

export function safeErrorMessage(error, fallback = "YakShed backend request failed") {
  const raw = error instanceof Error ? error.message : String(error ?? "");
  if (!raw) return fallback;
  return raw
    .replace(/(api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|password|client[_-]?secret|private[_-]?key|secret|bearer)\s*[:=]\s*[^\s,;]+/gi, "$1=[REDACTED]")
    .replace(/\b(sk|sess|token|key)-[A-Za-z0-9_-]{12,}\b/g, "[REDACTED]")
    .replace(/(?:\/Users\/|\/home\/|[A-Za-z]:\\)[^\s]+/g, "[path]")
    .slice(0, 512);
}
