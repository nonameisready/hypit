import type { Logger, NormalizedVideo, VideoMetrics } from "../types.js";

export type ProviderContext = {
  readonly discoveredAt: string;
  readonly signal?: AbortSignal;
  readonly fetch: typeof globalThis.fetch;
  readonly waitForRateLimit: () => Promise<void>;
  readonly logger: Logger;
};

export interface TrendProvider {
  readonly name: string;
  fetchTrendingVideos(context: ProviderContext): Promise<readonly unknown[]>;
  normalizeVideo(raw: unknown, discoveredAt: string): NormalizedVideo;
  getMetrics(video: NormalizedVideo, raw: unknown, context: ProviderContext): Promise<VideoMetrics>;
}
