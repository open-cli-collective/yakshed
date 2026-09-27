import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, stat, rm } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { BackendService } from "../desktop/service.mjs";
import { MAX_RPC_BYTES, safeErrorMessage, validateRpcRequest } from "../desktop/protocol.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const staticRoot = path.resolve(process.env.YAKSHED_PREVIEW_DIR || path.join(root, "desktop", "frontend"));
const demo = process.argv.includes("--demo");
const port = Number(process.env.YAKSHED_PREVIEW_PORT || valueAfter("--port") || 4173);
const token = randomBytes(24).toString("hex");
const clients = new Set();
let dataDir;
let service;
let server;
let stopping = false;
let previewWorkspace;
const demoConnectionIds = new Set();

if (!demo) {
  console.error("browser preview is test-only; pass --demo to create its isolated service");
  process.exit(2);
}
if (!Number.isInteger(port) || port < 1 || port > 65_535) throw new Error("invalid preview port");

function valueAfter(flag) {
  const index = process.argv.indexOf(flag);
  return index === -1 ? undefined : process.argv[index + 1];
}

function sendJson(response, status, value) {
  const body = JSON.stringify(value);
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(body) });
  response.end(body);
}

function authorized(request, url) {
  return request.headers["x-yakshed-preview-token"] === token;
}

function sameOrigin(request) {
  const expected = `http://127.0.0.1:${port}`;
  const host = request.headers.host;
  const origin = request.headers.origin;
  const fetchSite = request.headers["sec-fetch-site"];
  return (host === `127.0.0.1:${port}` || host === `localhost:${port}`)
    && (!origin || origin === expected || origin === `http://localhost:${port}`)
    && fetchSite !== "cross-site" && fetchSite !== "cross-origin";
}

function bridgeScript() {
  return `(() => {
  const token = ${JSON.stringify(token)};
  const listeners = new Set();
  const events = new EventSource('/__yakshed_events?token=' + encodeURIComponent(token));
  events.onmessage = (message) => { try { const value = JSON.parse(message.data); listeners.forEach((listener) => listener(value)); } catch {} };
  window.yakshed = Object.freeze({
    request(method, params = {}) {
      return fetch('/__yakshed_rpc', { method: 'POST', headers: { 'content-type': 'application/json', 'x-yakshed-preview-token': token }, body: JSON.stringify({ method, params }) })
        .then(async (response) => { const value = await response.json(); if (!response.ok) throw new Error(value.error?.message || 'preview request failed'); return value.result; });
    },
    onEvent(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    chooseWorkspace() {
      return fetch('/__yakshed_workspace', { headers: { 'x-yakshed-preview-token': token } })
        .then(async (response) => { const value = await response.json(); if (!response.ok) throw new Error(value.error?.message || 'preview workspace unavailable'); return value.result; });
    },
  });
})();`;
}

async function readBody(request) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of request) {
    bytes += chunk.length;
    if (bytes > MAX_RPC_BYTES) throw new Error("preview request is too large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function staticFile(url) {
  const decoded = decodeURIComponent(url.pathname);
  const relative = decoded === "/" ? "index.html" : decoded.replace(/^\/+/, "");
  const candidate = path.resolve(staticRoot, relative);
  if (candidate !== staticRoot && !candidate.startsWith(`${staticRoot}${path.sep}`)) return null;
  try {
    const details = await stat(candidate);
    if (!details.isFile()) return null;
    return candidate;
  } catch {
    return null;
  }
}

function contentType(file) {
  return { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".woff2": "font/woff2" }[path.extname(file).toLowerCase()] || "application/octet-stream";
}

async function handle(request, response) {
  const url = new URL(request.url || "/", "http://127.0.0.1");
  if (url.pathname === "/__yakshed_health") return sendJson(response, 200, { ok: true, demo: true });
  if (url.pathname === "/__yakshed_bridge.js") {
    if (!sameOrigin(request) || url.searchParams.get("token") !== token) return sendJson(response, 403, { error: { message: "preview authorization required" } });
    const body = bridgeScript();
    response.writeHead(200, { "content-type": "text/javascript; charset=utf-8", "cache-control": "no-store", "cross-origin-resource-policy": "same-origin", "x-content-type-options": "nosniff", "content-length": Buffer.byteLength(body) });
    return response.end(body);
  }
  if (url.pathname === "/__yakshed_workspace") {
    if (request.method !== "GET" || !sameOrigin(request) || !authorized(request, url)) return sendJson(response, 403, { error: { message: "preview authorization required" } });
    return sendJson(response, 200, { result: previewWorkspace });
  }
  if (url.pathname === "/__yakshed_rpc") {
    if (request.method !== "POST" || !sameOrigin(request) || !authorized(request, url)) return sendJson(response, 403, { error: { message: "preview authorization required" } });
    try {
      const payload = JSON.parse(await readBody(request));
      validateRpcRequest(payload.method, payload.params);
      validatePreviewOperation(payload);
      const result = await service.request(payload.method, payload.params);
      if (payload.method === "connection.create" && result && typeof result.id === "string") demoConnectionIds.add(result.id);
      return sendJson(response, 200, { result });
    } catch (error) {
      return sendJson(response, 400, { error: { message: safeErrorMessage(error) } });
    }
  }
  if (url.pathname === "/__yakshed_events") {
    if (request.method !== "GET" || !sameOrigin(request) || url.searchParams.get("token") !== token) return sendJson(response, 403, { error: { message: "preview authorization required" } });
    response.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-store", connection: "keep-alive" });
    response.write(": connected\n\n");
    clients.add(response);
    request.on("close", () => clients.delete(response));
    return;
  }
  if (request.method !== "GET" && request.method !== "HEAD") return sendJson(response, 405, { error: { message: "method not allowed" } });
  const file = await staticFile(url);
  if (!file) return sendJson(response, 404, { error: { message: "not found" } });
  let body = await readFile(file);
  if (path.extname(file) === ".html") {
    body = Buffer.from(body.toString("utf8").replace(/<\/head>/i, `<script src="/__yakshed_bridge.js?token=${encodeURIComponent(token)}"></script></head>`));
  }
  response.writeHead(200, { "content-type": contentType(file), "cache-control": "no-store", "content-security-policy": "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'", "x-frame-options": "DENY", "content-length": body.length });
  response.end(request.method === "HEAD" ? undefined : body);
}

function validatePreviewOperation(payload) {
  const params = payload.params || {};
  if (payload.method === "adapter.login") throw new Error("provider login is disabled in browser preview");
  if (payload.method === "connection.create" && params.adapter !== "demo") throw new Error("browser preview only permits the demo adapter");
  if (payload.method === "adapter.status" && params.adapter !== "demo") throw new Error("browser preview only permits the demo adapter");
  if (payload.method === "run.start") {
    if (typeof params.connection_id !== "string" || !demoConnectionIds.has(params.connection_id)) throw new Error("browser preview only permits demo runs");
  }
}

async function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const client of clients) client.end();
  clients.clear();
  if (server) await new Promise((resolve) => server.close(() => resolve()));
  await service?.stop();
  if (dataDir) await rm(dataDir, { recursive: true, force: true });
  process.exitCode = code;
}

async function main() {
  await stat(staticRoot);
  dataDir = await mkdtemp(path.join(os.tmpdir(), "yakshed-preview-"));
  previewWorkspace = path.join(dataDir, "workspace");
  await mkdir(previewWorkspace);
  service = new BackendService({ dataDir, demo: true, packaged: false });
  service.on("event", (event) => {
    const message = `data: ${JSON.stringify(event)}\n\n`;
    for (const client of clients) client.write(message);
  });
  service.on("error", (error) => {
    const message = `data: ${JSON.stringify({ event: "backend_error", message: safeErrorMessage(error) })}\n\n`;
    for (const client of clients) client.write(message);
  });
  service.start();
  server = createServer((request, response) => void handle(request, response).catch((error) => sendJson(response, 500, { error: { message: safeErrorMessage(error) } })));
  server.listen(port, "127.0.0.1", () => {
    console.log(`YAKSHED_PREVIEW_URL=http://127.0.0.1:${port}`);
  });
  process.once("SIGINT", () => void stop(130));
  process.once("SIGTERM", () => void stop(143));
}

main().catch(async (error) => {
  console.error(safeErrorMessage(error, "browser preview failed"));
  await stop(1);
});
