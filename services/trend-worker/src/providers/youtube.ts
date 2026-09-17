import { parseDuration } from "../youtube-duration.js";
import type { NormalizedVideo, VideoMetrics } from "../types.js";
import type { ProviderContext, TrendProvider } from "./provider.js";

type YouTubeItem = {
  readonly id?: { readonly videoId?: string };
  readonly snippet?: { readonly channelTitle?: string; readonly title?: string; readonly description?: string; readonly publishedAt?: string };
  readonly contentDetails?: { readonly duration?: string };
  readonly statistics?: { readonly viewCount?: string; readonly likeCount?: string; readonly commentCount?: string; readonly favoriteCount?: string };
};

function integer(value: string | undefined): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

export class YouTubeProvider implements TrendProvider {
  readonly name = "youtube";
  private readonly query: string | undefined;
  private readonly maxVideoAgeHours: number;

  constructor(
    private readonly apiKey: string,
    query: string | undefined,
    private readonly regionCode: string,
    maxVideoAgeHours: number,
  ) {
    if (!Number.isFinite(maxVideoAgeHours) || maxVideoAgeHours < 0) {
      throw new Error(`YouTube max video age must be a non-negative number, got ${maxVideoAgeHours}`);
    }
    this.query = query?.trim() || undefined;
    this.maxVideoAgeHours = maxVideoAgeHours;
  }

  async fetchTrendingVideos(context: ProviderContext): Promise<readonly unknown[]> {
    const publishedAfter = new Date(Date.parse(context.discoveredAt) - this.maxVideoAgeHours * 3_600_000).toISOString();
    const search = await this.request("search", {
      part: "snippet",
      type: "video",
      videoDuration: "short",
      order: "viewCount",
      maxResults: "50",
      publishedAfter,
      regionCode: this.regionCode,
      ...(this.query === undefined ? {} : { q: this.query }),
    }, context);
    const ids = Array.isArray(search.items)
      ? search.items.map((item) => (item as YouTubeItem).id?.videoId).filter((id): id is string => id !== undefined)
      : [];
    if (ids.length === 0) return [];
    const details = await this.request("videos", { part: "snippet,contentDetails,statistics", id: ids.join(",") }, context);
    return Array.isArray(details.items) ? details.items : [];
  }

  normalizeVideo(raw: unknown, discoveredAt: string): NormalizedVideo {
    const item = raw as YouTubeItem;
    const videoId = item.id?.videoId ?? (raw as { id?: string }).id;
    if (videoId === undefined) throw new Error("YouTube API item has no video id");
    return {
      platform: "youtube",
      video_id: videoId,
      url: `https://www.youtube.com/shorts/${videoId}`,
      creator: item.snippet?.channelTitle ?? null,
      caption: item.snippet?.title ?? item.snippet?.description ?? null,
      published_at: item.snippet?.publishedAt ?? null,
      duration: parseDuration(item.contentDetails?.duration),
      discovered_at: discoveredAt,
    };
  }

  async getMetrics(_video: NormalizedVideo, raw: unknown, _context: ProviderContext): Promise<VideoMetrics> {
    const stats = (raw as YouTubeItem).statistics;
    return {
      views: integer(stats?.viewCount),
      likes: integer(stats?.likeCount),
      comments: integer(stats?.commentCount),
      shares: 0,
    };
  }

  private async request(path: string, params: Record<string, string>, context: ProviderContext): Promise<{ readonly items?: readonly unknown[] }> {
    const url = new URL(`https://www.googleapis.com/youtube/v3/${path}`);
    for (const [key, value] of Object.entries({ ...params, key: this.apiKey })) url.searchParams.set(key, value);
    await context.waitForRateLimit();
    const response = await context.fetch(url, context.signal === undefined ? {} : { signal: context.signal });
    if (!response.ok) throw new Error(`YouTube API ${path} failed with HTTP ${response.status}: ${(await response.text()).slice(-1000)}`);
    return await response.json() as { readonly items?: readonly unknown[] };
  }
}
