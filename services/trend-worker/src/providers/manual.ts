import { readFile } from "node:fs/promises";

import { supportedPlatform } from "../config.js";
import type { NormalizedVideo, Platform, VideoMetrics } from "../types.js";
import type { ProviderContext, TrendProvider } from "./provider.js";

type ManualRecord = Partial<NormalizedVideo> & {
  readonly url: string;
  readonly metrics?: Partial<VideoMetrics>;
};

const emptyMetrics: VideoMetrics = { views: 0, likes: 0, comments: 0, shares: 0 };

function asRecord(raw: unknown): ManualRecord {
  if (typeof raw === "string") return { url: raw };
  if (typeof raw !== "object" || raw === null || typeof (raw as { url?: unknown }).url !== "string") {
    throw new Error("manual trend input must be a URL or an object containing url");
  }
  return raw as ManualRecord;
}

function platformFromUrl(url: string): Platform {
  const parsed = new URL(url);
  const host = parsed.hostname.toLowerCase();
  if (host.endsWith("tiktok.com")) return "tiktok";
  if (host.endsWith("instagram.com")) return "instagram";
  if (host === "youtu.be" || host.endsWith("youtube.com")) return "youtube";
  throw new Error(`unsupported trend URL host: ${parsed.hostname}`);
}

function videoIdFromUrl(url: string, platform: Platform): string {
  const parsed = new URL(url);
  const candidates = platform === "tiktok"
    ? [parsed.pathname.match(/\/video\/(\d+)/u)?.[1], parsed.pathname.match(/\/v\/(\w+)/u)?.[1]]
    : platform === "instagram"
      ? [parsed.pathname.match(/\/(?:reel|p)\/([^/?#]+)/u)?.[1]]
      : [parsed.hostname === "youtu.be" ? parsed.pathname.slice(1) : parsed.pathname.match(/\/shorts\/([^/?#]+)/u)?.[1], parsed.searchParams.get("v") ?? undefined];
  const id = candidates.find((item) => item !== undefined && item.length > 0);
  if (id === undefined) throw new Error(`could not determine ${platform} video id from ${url}`);
  return id;
}

function metricValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : 0;
}

export class ManualUrlProvider implements TrendProvider {
  readonly name = "manual";

  constructor(private readonly filePath: string, private readonly urls: readonly string[] = []) {}

  async fetchTrendingVideos(_context: ProviderContext): Promise<readonly unknown[]> {
    const records: unknown[] = this.urls.map((url) => ({ url }));
    try {
      const text = await readFile(this.filePath, "utf8");
      for (const [index, line] of text.split(/\r?\n/u).entries()) {
        const trimmed = line.trim();
        if (trimmed.length === 0 || trimmed.startsWith("#")) continue;
        try {
          records.push(trimmed.startsWith("{") ? JSON.parse(trimmed) : trimmed);
        } catch (error) {
          throw new Error(`invalid manual trend input at ${this.filePath}:${index + 1}: ${String(error)}`);
        }
      }
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
    return records;
  }

  normalizeVideo(raw: unknown, discoveredAt: string): NormalizedVideo {
    const record = asRecord(raw);
    const url = new URL(record.url).toString();
    const platform = record.platform ?? platformFromUrl(url);
    if (!supportedPlatform(platform)) throw new Error(`unsupported platform ${String(platform)}`);
    return {
      platform,
      video_id: record.video_id ?? videoIdFromUrl(url, platform),
      url,
      creator: record.creator ?? null,
      caption: record.caption ?? null,
      published_at: record.published_at ?? null,
      duration: record.duration ?? null,
      discovered_at: record.discovered_at ?? discoveredAt,
    };
  }

  async getMetrics(_video: NormalizedVideo, raw: unknown, _context: ProviderContext): Promise<VideoMetrics> {
    const metrics = asRecord(raw).metrics;
    return {
      views: metricValue(metrics?.views ?? emptyMetrics.views),
      likes: metricValue(metrics?.likes ?? emptyMetrics.likes),
      comments: metricValue(metrics?.comments ?? emptyMetrics.comments),
      shares: metricValue(metrics?.shares ?? emptyMetrics.shares),
    };
  }
}
