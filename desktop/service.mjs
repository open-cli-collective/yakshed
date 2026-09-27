import { spawn } from "node:child_process";
import { existsSync, mkdirSync, statSync } from "node:fs";
import { EventEmitter } from "node:events";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  MAX_RPC_BYTES,
  safeErrorMessage,
  validateProtocolMessage,
  validateRpcRequest,
} from "./protocol.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MAX_LINE_BYTES = 4 * 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;

function asAbsoluteDirectory(value, label) {
  if (typeof value !== "string" || !path.isAbsolute(value) || value.length > 4_096) {
    throw new TypeError(`${label} must be an absolute path`);
  }
  return path.normalize(value);
}

function hasValidTempDirectory(value) {
  if (typeof value !== "string" || !value || !path.isAbsolute(value)) return false;
  try {
    return statSync(value).isDirectory();
  } catch {
    return false;
  }
}

function childEnvironment(tempDir) {
  const environment = { ...process.env, PYTHONUNBUFFERED: "1", YAKSHED_DESKTOP: "1" };
  if (!hasValidTempDirectory(environment.TMPDIR)) environment.TMPDIR = hasValidTempDirectory(tempDir) ? tempDir : os.tmpdir();
  return environment;
}

function commandFor({ packaged, dataDir, demo, serviceBinary, python }) {
  if (packaged) {
    const binary = serviceBinary || path.join(process.resourcesPath, "bin", "yakshed-service");
    if (!existsSync(binary)) throw new Error("YakShed service binary is not installed");
    return { command: binary, args: ["--data-dir", dataDir, ...(demo ? ["--demo"] : [])], cwd: dataDir };
  }
  return {
    command: python || process.env.YAKSHED_PYTHON || "python3",
    args: ["-m", "backend.yakshed", "--data-dir", dataDir, ...(demo ? ["--demo"] : [])],
    cwd: process.env.YAKSHED_REPO_ROOT || ROOT,
  };
}

export class BackendService extends EventEmitter {
  constructor({ dataDir, demo = false, packaged = false, serviceBinary, python, tempDir, requestTimeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
    super();
    this.dataDir = asAbsoluteDirectory(dataDir, "data directory");
    this.demo = Boolean(demo);
    this.packaged = Boolean(packaged);
    this.serviceBinary = serviceBinary;
    this.python = python;
    this.tempDir = tempDir;
    this.requestTimeoutMs = requestTimeoutMs;
    this.child = null;
    this.stdout = "";
    this.sequence = 0;
    this.pending = new Map();
    this.stopping = false;
    this.lastError = null;
    this.groupPid = null;
  }

  start() {
    if (this.child) return;
    if (this.packaged) mkdirSync(this.dataDir, { recursive: true });
    const command = commandFor({
      packaged: this.packaged,
      dataDir: this.dataDir,
      demo: this.demo,
      serviceBinary: this.serviceBinary,
      python: this.python,
    });
    const child = spawn(command.command, command.args, {
      cwd: command.cwd,
      env: childEnvironment(this.tempDir),
      stdio: ["pipe", "pipe", "pipe"],
      detached: process.platform !== "win32",
      windowsHide: true,
    });
    this.child = child;
    this.groupPid = child.pid ?? null;
    let finalized = false;
    const finalize = (code, signal, spawnFailure = false) => {
      if (finalized) return;
      finalized = true;
      const expected = this.stopping;
      this.child = null;
      this.#reapProcessGroup(child.pid);
      this.#failAll(new Error(expected ? "YakShed service stopped" : "YakShed service stopped unexpectedly"));
      if (!expected && !spawnFailure && this.listenerCount("error") > 0) this.emit("error", new Error("YakShed service stopped unexpectedly"));
      this.emit("exit", { code, signal, expected });
    };
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => this.#consumeStdout(chunk));
    // Keep stderr private. Provider/runtime errors can include credentials or paths.
    child.stderr.on("data", (chunk) => {
      this.lastError = safeErrorMessage(chunk, "YakShed service reported an error");
    });
    child.stdin.on("error", (error) => this.#failAll(error));
    child.on("error", (error) => {
      this.lastError = safeErrorMessage(error, "YakShed service failed to start");
      this.#failAll(error);
      if (child.pid === undefined) {
        if (this.listenerCount("error") > 0) this.emit("error", new Error(this.lastError));
        finalize(null, null, true);
      }
    });
    child.on("exit", (code, signal) => finalize(code, signal));
    child.on("close", (code, signal) => finalize(code, signal));
  }

  request(method, params = {}) {
    validateRpcRequest(method, params);
    if (!this.child || this.stopping || !this.child.stdin.writable) {
      return Promise.reject(new Error("YakShed backend is unavailable"));
    }
    const id = `desktop_${++this.sequence}`;
    const line = JSON.stringify({ id, method, params });
    if (Buffer.byteLength(line, "utf8") > MAX_RPC_BYTES) return Promise.reject(new Error("request is too large"));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error("YakShed backend request timed out"));
      }, this.requestTimeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      try {
        this.child.stdin.write(`${line}\n`);
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error);
      }
    });
  }

  async stop() {
    const child = this.child;
    const pid = this.groupPid;
    if (!child && !pid) return;
    this.stopping = true;
    this.#failAll(new Error("YakShed backend is shutting down"));
    if (child?.stdin.writable) child.stdin.end();
    await new Promise((resolve) => {
      let done = false;
      let graceTimer;
      let killTimer;
      const groupAlive = () => {
        if (process.platform === "win32" || !pid) return child ? child.exitCode === null && child.signalCode === null : false;
        try {
          process.kill(-pid, 0);
          return true;
        } catch {
          return false;
        }
      };
      const signalGroup = (signal) => {
        if (!pid) return;
        try {
          if (process.platform === "win32") child?.kill(signal);
          else process.kill(-pid, signal);
        } catch {
          try { child?.kill(signal); } catch { /* already exited */ }
        }
      };
      const finish = () => {
        if (done) return;
        done = true;
        clearTimeout(graceTimer);
        clearTimeout(killTimer);
        resolve();
      };
      const terminate = (signal) => {
        if (!groupAlive()) return;
        signalGroup(signal);
      };
      const parentExited = () => {
        // A detached provider process can outlive its Python parent. Reap the
        // process group even when the leader emitted `exit` first.
        if (!groupAlive()) {
          finish();
          return;
        }
        clearTimeout(killTimer);
        terminate("SIGTERM");
        killTimer = setTimeout(() => {
          terminate("SIGKILL");
          finish();
        }, 500);
      };
      child?.once("exit", parentExited);
      if (!child || child.exitCode !== null || child.signalCode !== null) return parentExited();
      graceTimer = setTimeout(() => terminate("SIGTERM"), 1_500);
      killTimer = setTimeout(() => terminate("SIGKILL"), 3_500);
    });
    this.child = null;
    this.groupPid = null;
    this.stopping = false;
  }

  #reapProcessGroup(pid) {
    if (process.platform === "win32" || !pid) return;
    try {
      process.kill(-pid, 0);
      process.kill(-pid, "SIGTERM");
    } catch {
      return;
    }
    setTimeout(() => {
      try { process.kill(-pid, "SIGKILL"); } catch { /* group already exited */ }
    }, 500);
  }

  #consumeStdout(chunk) {
    this.stdout += chunk;
    if (Buffer.byteLength(this.stdout, "utf8") > MAX_LINE_BYTES) {
      this.#failAll(new Error("YakShed service response is too large"));
      this.child?.kill("SIGKILL");
      return;
    }
    let newline;
    while ((newline = this.stdout.indexOf("\n")) !== -1) {
      const line = this.stdout.slice(0, newline).trim();
      this.stdout = this.stdout.slice(newline + 1);
      if (!line) continue;
      try {
        const message = validateProtocolMessage(JSON.parse(line));
        if (message.type === "event") {
          this.emit("event", message.value);
          continue;
        }
        const pending = this.pending.get(message.value.id);
        if (!pending) continue;
        this.pending.delete(message.value.id);
        clearTimeout(pending.timer);
        if (message.value.error) pending.reject(new Error(safeErrorMessage(message.value.error.message)));
        else pending.resolve(message.value.result);
      } catch (error) {
        this.lastError = safeErrorMessage(error, "YakShed service sent an invalid response");
        this.emit("error", new Error("YakShed service sent an invalid response"));
      }
    }
  }

  #failAll(error) {
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(error);
      this.pending.delete(id);
    }
  }
}

export function resolveDataDirectory(appDataPath, { demo = false, override } = {}) {
  if (override) return asAbsoluteDirectory(override, "data directory");
  return path.join(appDataPath, demo ? "demo-data" : "data");
}
