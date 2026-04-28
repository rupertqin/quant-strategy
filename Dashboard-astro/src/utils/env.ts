import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

const PROJECT_ROOT = resolve("..");

export function loadEnv(
  key: string,
  defaultValue?: string,
): string | undefined {
  const envPath = resolve(PROJECT_ROOT, ".env");
  if (!existsSync(envPath)) return defaultValue;

  const content = readFileSync(envPath, "utf-8");
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) continue;
    const k = trimmed.slice(0, eqIdx).trim();
    const v = trimmed.slice(eqIdx + 1).trim();
    if (k === key) return v;
  }
  return defaultValue;
}

export function getStorageDir(): string {
  const dir = loadEnv("QUANT_STORAGE_DIR");
  return resolve(PROJECT_ROOT, dir || "");
}
