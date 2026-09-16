import type { WorkerConfig } from "../config.js";
import type { TrendProvider } from "./provider.js";
import { ManualUrlProvider } from "./manual.js";
import { YouTubeProvider } from "./youtube.js";

export function createProviders(config: WorkerConfig): readonly TrendProvider[] {
  return config.providers.map((name) => {
    if (name === "manual") return new ManualUrlProvider(config.trendUrlsFile, config.manualUrls);
    if (name === "youtube") {
      if (config.youtubeApiKey === undefined) throw new Error("TREND_PROVIDERS includes youtube but YOUTUBE_API_KEY is missing");
      return new YouTubeProvider(config.youtubeApiKey, config.youtubeQuery, config.youtubeRegionCode);
    }
    throw new Error(`unknown trend provider ${name}`);
  });
}
