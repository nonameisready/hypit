import { downloadVideo } from "@hypit/yt-dlp";

import { withRetry, type RateLimiter, type RetryOptions } from "../retry.js";

export interface VideoDownloader {
  download(url: string, target: string, timeoutMs: number): Promise<void>;
}

export class HypitVideoDownloader implements VideoDownloader {
  constructor(private readonly retry: RetryOptions, private readonly limiter: RateLimiter) {}

  async download(url: string, target: string, timeoutMs: number): Promise<void> {
    await withRetry(async () => {
      await this.limiter.wait();
      await downloadVideo(url, target, { timeoutMs });
    }, this.retry);
  }
}
