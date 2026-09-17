import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadConfig, requireR2 } from "../config.js";
import { createPipeline } from "../pipeline/pipeline.js";
import { R2Storage } from "../storage/r2.js";

const serviceDirectory = resolve(fileURLToPath(new URL("../..", import.meta.url)));
export const defaultEnvPath = resolve(serviceDirectory, ".env");

function isMissingFile(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

/** Load the service-local environment without replacing values supplied by the shell or launchd. */
export function loadWorkerEnv(path = defaultEnvPath): void {
  const existing = new Map(Object.entries(process.env));
  try {
    process.loadEnvFile(path);
  } catch (error) {
    if (!isMissingFile(error)) throw error;
  }
  for (const [key, value] of existing) {
    if (value !== undefined) process.env[key] = value;
  }
}

function usage(): string {
  return "Usage: trend-worker <discover|rank|download|upload|run> [--url <public-url>]\n";
}

function parse(argv: readonly string[]): { readonly command: string; readonly urls: readonly string[]; readonly json: boolean } {
  const [command, ...rest] = argv;
  if (command === undefined || command === "--help" || command === "help") throw new Error(usage());
  const urls: string[] = [];
  let json = false;
  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index]!;
    if (arg === "--") continue;
    if (arg === "--json") { json = true; continue; }
    if (arg === "--url") {
      const value = rest[++index];
      if (value === undefined) throw new Error("--url requires a value");
      urls.push(value);
      continue;
    }
    throw new Error(`unknown option ${arg}\n${usage()}`);
  }
  return { command, urls, json };
}

export async function runCli(argv = process.argv.slice(2)): Promise<void> {
  const { command, urls, json } = parse(argv);
  loadWorkerEnv();
  const config = loadConfig(process.env, { manualUrls: urls });
  const storageNeeded = command === "upload" || command === "run";
  let storage: R2Storage | undefined;
  if (storageNeeded) {
    requireR2(config);
    storage = new R2Storage(config, { maxAttempts: config.maxRetries, baseDelayMs: config.retryBaseDelayMs });
  }
  const { pipeline, db } = createPipeline(config, storage);
  try {
    const result = command === "discover" ? await pipeline.discover()
      : command === "rank" ? pipeline.rank()
        : command === "download" ? await pipeline.download()
          : command === "upload" ? await pipeline.upload()
            : command === "run" ? await pipeline.run()
              : undefined;
    if (result === undefined) throw new Error(`unknown command ${command}\n${usage()}`);
    process.stdout.write(`${json ? JSON.stringify(result, null, 2) : `${JSON.stringify(result)}\n`}`);
  } finally {
    db.close();
  }
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runCli().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  });
}
