import { loadConfig, requireR2 } from "../config.js";
import { createPipeline } from "../pipeline/pipeline.js";
import { R2Storage } from "../storage/r2.js";

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

async function main(): Promise<void> {
  const { command, urls, json } = parse(process.argv.slice(2));
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

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
