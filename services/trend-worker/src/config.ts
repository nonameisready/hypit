import { resolve } from "node:path";

import type { Platform } from "./types.js";

export type ViralWeights = {
  readonly view_velocity: number;
  readonly share_velocity: number;
  readonly comment_velocity: number;
  readonly like_velocity: number;
  readonly acceleration: number;
};

export type WorkerConfig = {
  readonly dataDir: string;
  readonly databasePath: string;
  readonly trendUrlsFile: string;
  readonly providers: readonly string[];
  readonly manualUrls: readonly string[];
  readonly youtubeApiKey: string | undefined;
  readonly youtubeRegionCode: string;
  readonly youtubeQuery: string | undefined;
  readonly topN: number;
  readonly minViralScore: number | undefined;
  readonly maxVideoAgeHours: number;
  readonly minSnapshotsForDownload: number;
  readonly maxNewDownloadsPerRun: number;
  readonly maxNewDownloadsPerDay: number;
  readonly hotRetentionDays: number;
  readonly r2SoftLimitGb: number;
  readonly maxClassicVideos: number;
  readonly deleteLocalAfterUpload: boolean;
  readonly downloadTimeoutMs: number;
  readonly uploadTimeoutMs: number;
  readonly maxRetries: number;
  readonly retryBaseDelayMs: number;
  readonly rateLimitMs: number;
  readonly freshnessHalfLifeHours: number;
  readonly weights: ViralWeights;
  readonly r2: {
    readonly accountId: string | undefined;
    readonly accessKeyId: string | undefined;
    readonly secretAccessKey: string | undefined;
    readonly bucket: string | undefined;
    readonly endpoint: string | undefined;
    readonly publicBaseUrl: string | undefined;
  };
};

export type ConfigOverrides = { readonly manualUrls?: readonly string[] };

function text(env: NodeJS.ProcessEnv, name: string, fallback?: string): string | undefined {
  const value = env[name]?.trim();
  return value === undefined || value.length === 0 ? fallback : value;
}

function integer(env: NodeJS.ProcessEnv, name: string, fallback: number, minimum: number): number {
  const value = Number(text(env, name, String(fallback)));
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be an integer >= ${minimum}`);
  return value;
}

function number(env: NodeJS.ProcessEnv, name: string, fallback: number, minimum: number): number {
  const value = Number(text(env, name, String(fallback)));
  if (!Number.isFinite(value) || value < minimum) throw new Error(`${name} must be a number >= ${minimum}`);
  return value;
}

function optionalNumber(env: NodeJS.ProcessEnv, name: string): number | undefined {
  const raw = text(env, name);
  if (raw === undefined) return undefined;
  const value = Number(raw);
  if (!Number.isFinite(value)) throw new Error(`${name} must be a number`);
  return value;
}

function boolean(env: NodeJS.ProcessEnv, name: string, fallback: boolean): boolean {
  const raw = text(env, name, String(fallback))!.toLowerCase();
  if (raw === "true" || raw === "1" || raw === "yes") return true;
  if (raw === "false" || raw === "0" || raw === "no") return false;
  throw new Error(`${name} must be true or false`);
}

function weight(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const value = number(env, name, fallback, 0);
  if (value > 1) throw new Error(`${name} must be between 0 and 1`);
  return value;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env, overrides: ConfigOverrides = {}): WorkerConfig {
  const dataDir = resolve(text(env, "DATA_DIR", "data")!);
  const databasePath = resolve(text(env, "DATABASE_PATH", `${dataDir}/trend-worker.sqlite`)!);
  const fileUrls = text(env, "TREND_URLS_FILE", `${dataDir}/trend-urls.jsonl`)!;
  const providers = (text(env, "TREND_PROVIDERS", "manual") ?? "manual").split(",").map((item) => item.trim()).filter(Boolean);
  const manualUrls = overrides.manualUrls ?? [];
  const weights = {
    view_velocity: weight(env, "VIRAL_WEIGHT_VIEW_VELOCITY", 0.35),
    share_velocity: weight(env, "VIRAL_WEIGHT_SHARE_VELOCITY", 0.30),
    comment_velocity: weight(env, "VIRAL_WEIGHT_COMMENT_VELOCITY", 0.15),
    like_velocity: weight(env, "VIRAL_WEIGHT_LIKE_VELOCITY", 0.10),
    acceleration: weight(env, "VIRAL_WEIGHT_ACCELERATION", 0.10),
  } satisfies ViralWeights;
  const weightTotal = Object.values(weights).reduce((sum, value) => sum + value, 0);
  if (weightTotal <= 0) throw new Error("viral score weights must add up to more than zero");
  return {
    dataDir,
    databasePath,
    trendUrlsFile: resolve(fileUrls),
    providers,
    manualUrls,
    youtubeApiKey: text(env, "YOUTUBE_API_KEY"),
    youtubeRegionCode: text(env, "YOUTUBE_REGION_CODE", "US")!,
    youtubeQuery: text(env, "YOUTUBE_QUERY"),
    topN: integer(env, "TOP_N", 20, 1),
    minViralScore: optionalNumber(env, "MIN_VIRAL_SCORE"),
    maxVideoAgeHours: number(env, "MAX_VIDEO_AGE_HOURS", 72, 0),
    minSnapshotsForDownload: integer(env, "MIN_SNAPSHOTS_FOR_DOWNLOAD", 2, 1),
    maxNewDownloadsPerRun: integer(env, "MAX_NEW_DOWNLOADS_PER_RUN", 3, 1),
    maxNewDownloadsPerDay: integer(env, "MAX_NEW_DOWNLOADS_PER_DAY", 30, 1),
    hotRetentionDays: integer(env, "HOT_RETENTION_DAYS", 14, 1),
    r2SoftLimitGb: number(env, "R2_SOFT_LIMIT_GB", 8, Number.MIN_VALUE),
    maxClassicVideos: integer(env, "MAX_CLASSIC_VIDEOS", 400, 0),
    deleteLocalAfterUpload: boolean(env, "DELETE_LOCAL_AFTER_UPLOAD", true),
    downloadTimeoutMs: integer(env, "DOWNLOAD_TIMEOUT_MS", 900_000, 1),
    uploadTimeoutMs: integer(env, "UPLOAD_TIMEOUT_MS", 300_000, 1),
    maxRetries: integer(env, "MAX_RETRIES", 3, 1),
    retryBaseDelayMs: integer(env, "RETRY_BASE_DELAY_MS", 500, 0),
    rateLimitMs: integer(env, "RATE_LIMIT_MS", 500, 0),
    freshnessHalfLifeHours: number(env, "FRESHNESS_HALF_LIFE_HOURS", 48, 0.001),
    weights,
    r2: {
      accountId: text(env, "CF_ACCOUNT_ID"),
      accessKeyId: text(env, "R2_ACCESS_KEY_ID"),
      secretAccessKey: text(env, "R2_SECRET_ACCESS_KEY"),
      bucket: text(env, "R2_BUCKET"),
      endpoint: text(env, "R2_ENDPOINT"),
      publicBaseUrl: text(env, "R2_PUBLIC_BASE_URL"),
    },
  };
}

export function requireR2(config: WorkerConfig): asserts config is WorkerConfig & {
  readonly r2: {
    readonly accessKeyId: string;
    readonly secretAccessKey: string;
    readonly bucket: string;
    readonly endpoint: string;
    readonly accountId: string | undefined;
    readonly publicBaseUrl: string | undefined;
  };
} {
  const missing = ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET", "R2_ENDPOINT"]
    .filter((name) => config.r2[name === "R2_ACCESS_KEY_ID" ? "accessKeyId" : name === "R2_SECRET_ACCESS_KEY" ? "secretAccessKey" : name === "R2_BUCKET" ? "bucket" : "endpoint"] === undefined);
  if (missing.length > 0) throw new Error(`R2 configuration is incomplete: missing ${missing.join(", ")}`);
}

export function supportedPlatform(value: string): value is Platform {
  return value === "tiktok" || value === "instagram" || value === "youtube";
}
