import type { Logger } from "./types.js";

function write(level: string, event: string, fields: Record<string, unknown> = {}): void {
  process.stderr.write(`${JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    event,
    ...fields,
  })}\n`);
}

export const logger: Logger = {
  info: (event, fields) => write("info", event, fields),
  warn: (event, fields) => write("warn", event, fields),
  error: (event, fields) => write("error", event, fields),
};
