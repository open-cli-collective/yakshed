import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import test from "node:test";
import os from "node:os";
import path from "node:path";

import { BackendService } from "../service.mjs";

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));
const nextExit = (service) => new Promise((resolve) => service.once("exit", resolve));

async function waitForFile(file, timeout = 2_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try {
      return await readFile(file, "utf8");
    } catch {
      await delay(25);
    }
  }
  throw new Error(`timed out waiting for ${file}`);
}

async function waitForProcessGone(pid, timeout = 2_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0);
    } catch {
      return;
    }
    await delay(25);
  }
  throw new Error(`process ${pid} is still alive`);
}

test("spawn failures settle and stop remains bounded", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "yakshed-service-error-"));
  try {
    const errors = [];
    const service = new BackendService({ dataDir: path.join(root, "data"), python: path.join(root, "missing-python") });
    service.on("error", (error) => errors.push(error));
    const exited = nextExit(service);
    service.start();
    const details = await Promise.race([
      exited,
      delay(2_000).then(() => { throw new Error("spawn failure did not settle"); }),
    ]);
    assert.equal(details.expected, false);
    await Promise.race([
      service.stop(),
      delay(1_000).then(() => { throw new Error("stop hung after spawn failure"); }),
    ]);
    assert.match(errors[0]?.message || "", /failed to start|spawn/i);
    await assert.rejects(service.request("snapshot"), /unavailable/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("service supplies a valid temp directory when the parent omits TMPDIR", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "yakshed-service-tmpdir-"));
  const helper = path.join(root, "helper.cjs");
  const tempDir = path.join(root, "native-temp");
  const previous = process.env.TMPDIR;
  const source = `#!/usr/bin/env node
const { mkdirSync, writeFileSync } = require("node:fs");
const path = require("node:path");
const index = process.argv.indexOf("--data-dir");
const dataDir = process.argv[index + 1];
mkdirSync(dataDir, { recursive: true });
writeFileSync(path.join(dataDir, "tmpdir"), process.env.TMPDIR || "");
process.stdin.resume();
`;
  try {
    await writeFile(helper, source, { mode: 0o755 });
    await chmod(helper, 0o755);
    await mkdir(tempDir);
    delete process.env.TMPDIR;
    const dataDir = path.join(root, "data");
    const service = new BackendService({ dataDir, packaged: true, serviceBinary: helper, tempDir });
    service.on("error", () => {});
    service.start();
    const observed = (await waitForFile(path.join(dataDir, "tmpdir"))).trim();
    assert.equal(observed, tempDir);
    await service.stop();
  } finally {
    if (previous === undefined) delete process.env.TMPDIR;
    else process.env.TMPDIR = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test("leader exit cleanup reaps a detached descendant", { skip: process.platform === "win32" }, async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "yakshed-service-group-"));
  const helper = path.join(root, "leader.cjs");
  const source = `#!/usr/bin/env node
const { spawn } = require("node:child_process");
const { mkdirSync, writeFileSync } = require("node:fs");
const path = require("node:path");
const index = process.argv.indexOf("--data-dir");
const dataDir = process.argv[index + 1];
mkdirSync(dataDir, { recursive: true });
const child = spawn(process.execPath, ["-e", "process.on('SIGTERM', () => {}); setInterval(() => {}, 1000);"], { stdio: "ignore" });
writeFileSync(path.join(dataDir, "child.pid"), String(child.pid));
process.exit(0);
`;
  try {
    await writeFile(helper, source, { mode: 0o755 });
    await chmod(helper, 0o755);
    const dataDir = path.join(root, "data");
    const service = new BackendService({ dataDir, packaged: true, serviceBinary: helper });
    service.on("error", () => {});
    const exited = nextExit(service);
    service.start();
    const childPid = Number(await waitForFile(path.join(dataDir, "child.pid")));
    assert.ok(Number.isSafeInteger(childPid) && childPid > 0);
    await Promise.race([
      exited,
      delay(2_000).then(() => { throw new Error("leader did not exit"); }),
    ]);
    await Promise.race([
      service.stop(),
      delay(2_000).then(() => { throw new Error("stop hung with a detached descendant"); }),
    ]);
    await waitForProcessGone(childPid);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
