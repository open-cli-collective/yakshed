import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const explicit = process.argv[2];
const candidates = explicit
  ? [explicit]
  : [
      path.join(root, "release", "YakShed.app", "Contents", "MacOS", "YakShed"),
      path.join(root, "dist", "mac", "YakShed.app", "Contents", "MacOS", "YakShed"),
      path.join(root, "dist", "mac-arm64", "YakShed.app", "Contents", "MacOS", "YakShed"),
      path.join(root, "dist", "mac-x64", "YakShed.app", "Contents", "MacOS", "YakShed"),
    ];

async function findExecutable() {
  for (const candidate of candidates) {
    try {
      if ((await stat(candidate)).isFile()) return candidate;
    } catch {
      // Try the next builder target.
    }
  }
  throw new Error(`packaged YakShed executable not found; pass its path or run npm run package first`);
}

async function main() {
  const executable = await findExecutable();
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "yakshed-package-smoke-"));
  try {
    const child = spawn(executable, ["--smoke", "--data-dir", dataDir], { cwd: root, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { output += chunk; });
    child.stderr.on("data", (chunk) => { output += chunk; });
    const timer = setTimeout(() => child.kill("SIGTERM"), 15_000);
    const code = await new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (exitCode, signal) => resolve(exitCode ?? (signal ? 1 : 0)));
    });
    clearTimeout(timer);
    if (code !== 0) throw new Error(`packaged app exited with ${code}${output ? `: ${output.slice(-512)}` : ""}`);
    const entries = await readdir(dataDir);
    if (!entries.length) throw new Error("packaged app did not initialize its isolated data directory");
    console.log(`package smoke passed: ${path.basename(path.dirname(path.dirname(path.dirname(executable))))}`);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
