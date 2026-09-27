import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const demo = process.argv.includes("--demo");
const smoke = process.argv.includes("--smoke");
const port = process.env.YAKSHED_DEV_PORT || "5173";
const bin = (name) => path.join(root, "node_modules", ".bin", process.platform === "win32" ? `${name}.cmd` : name);
const children = [];

function run(args, env = {}) {
  const child = spawn(bin(args.shift()), args, { cwd: root, env: { ...process.env, ...env }, stdio: "inherit" });
  children.push(child);
  return child;
}

async function waitForRenderer() {
  const url = `http://127.0.0.1:${port}`;
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) return url;
    } catch {
      // Vite is still starting.
    }
    await delay(250);
  }
  throw new Error(`renderer did not start at ${url}`);
}

async function main() {
  const vite = run(["vite", "--host", "127.0.0.1", "--port", port]);
  const url = await waitForRenderer();
  const electron = spawn(bin("electron"), [".", "--dev", ...(demo ? ["--demo"] : []), ...(smoke ? ["--smoke"] : [])], {
    cwd: root,
    env: { ...process.env, YAKSHED_RENDERER_URL: url },
    stdio: "inherit",
  });
  children.push(electron);
  electron.once("exit", (code, signal) => shutdown(code ?? (signal ? 1 : 0)));
  process.once("SIGINT", () => shutdown(130));
  process.once("SIGTERM", () => shutdown(143));
}

let stopping = false;
function shutdown(code) {
  if (stopping) return;
  stopping = true;
  for (const child of children) {
    if (!child.killed) child.kill("SIGTERM");
  }
  setTimeout(() => process.exit(code), 250).unref();
}

main().catch((error) => {
  console.error(error.message);
  shutdown(1);
});
