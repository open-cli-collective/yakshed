import { access, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const binary = path.join(root, "backend", "dist", "yakshed-service");

try {
  const details = await stat(binary);
  await access(binary);
  if (!details.isFile() || (process.platform !== "win32" && (details.mode & 0o111) === 0)) throw new Error("not executable");
} catch {
  console.error(`missing executable backend sidecar: ${binary}`);
  console.error("run the pinned backend freeze step before packaging YakShed");
  process.exit(1);
}
