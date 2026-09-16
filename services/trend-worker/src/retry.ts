export type RetryOptions = {
  readonly maxAttempts: number;
  readonly baseDelayMs: number;
  readonly shouldRetry?: (error: unknown) => boolean;
  readonly onRetry?: (error: unknown, attempt: number, delayMs: number) => void;
};

function statusCode(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null || !("$metadata" in error)) return undefined;
  const metadata = error.$metadata;
  if (typeof metadata !== "object" || metadata === null || !("httpStatusCode" in metadata)) return undefined;
  return typeof metadata.httpStatusCode === "number" ? metadata.httpStatusCode : undefined;
}

export function isRetryable(error: unknown): boolean {
  const status = statusCode(error);
  return status === 408 || status === 429 || status === 500 || status === 502 || status === 503 || status === 504;
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

export async function withRetry<T>(operation: (attempt: number) => Promise<T>, options: RetryOptions): Promise<T> {
  const shouldRetry = options.shouldRetry ?? isRetryable;
  let lastError: unknown;
  for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
    try {
      return await operation(attempt);
    } catch (error) {
      lastError = error;
      if (attempt >= options.maxAttempts || !shouldRetry(error)) throw error;
      const delayMs = options.baseDelayMs * 2 ** (attempt - 1);
      options.onRetry?.(error, attempt, delayMs);
      await sleep(delayMs);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

export class RateLimiter {
  #nextAllowedAt = 0;

  constructor(private readonly delayMs: number) {}

  async wait(): Promise<void> {
    const now = Date.now();
    const delay = Math.max(0, this.#nextAllowedAt - now);
    this.#nextAllowedAt = Math.max(now, this.#nextAllowedAt) + this.delayMs;
    if (delay > 0) await sleep(delay);
  }
}
