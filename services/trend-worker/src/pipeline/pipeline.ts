import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rm, stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { dirname, join } from "node:path";

import type { WorkerConfig } from "../config.js";
import { TrendDatabase, type StoredVideo } from "../db/database.js";
import { HypitVideoDownloader } from "../downloader/hypit-downloader.js";
import type { VideoDownloader } from "../downloader/hypit-downloader.js";
import { logger as defaultLogger } from "../logger.js";
import { RateLimiter, type RetryOptions } from "../retry.js";
import { scoreCandidates } from "../scoring/score.js";
import { metadataObjectKey, rawObjectKey } from "../storage/object-keys.js";
import { makeMetadata, metadataBytes } from "../storage/metadata.js";
import type { ObjectStorage } from "../storage/r2.js";
import type { Logger, NormalizedVideo, ScoredVideo, VideoMetrics } from "../types.js";
import { createProviders } from "../providers/registry.js";

async function hashFile(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }

export type PipelineDependencies = {
  readonly db: TrendDatabase;
  readonly config: WorkerConfig;
  readonly downloader: VideoDownloader;
  readonly storage?: ObjectStorage;
  readonly logger?: Logger;
  readonly now?: () => string;
};

export class TrendPipeline {
  readonly #db: TrendDatabase;
  readonly #config: WorkerConfig;
  readonly #downloader: VideoDownloader;
  readonly #storage: ObjectStorage | undefined;
  readonly #logger: Logger;
  readonly #now: () => string;

  constructor(dependencies: PipelineDependencies) {
    this.#db = dependencies.db;
    this.#config = dependencies.config;
    this.#downloader = dependencies.downloader;
    this.#storage = dependencies.storage;
    this.#logger = dependencies.logger ?? defaultLogger;
    this.#now = dependencies.now ?? (() => new Date().toISOString());
  }

  async discover(): Promise<{ readonly discovered: number; readonly failed: number }> {
    const providers = createProviders(this.#config);
    const providerLimiter = new RateLimiter(this.#config.rateLimitMs);
    let discovered = 0;
    let failed = 0;
    for (const provider of providers) {
      const discoveredAt = this.#now();
      try {
        const context = { discoveredAt, fetch: globalThis.fetch, logger: this.#logger, waitForRateLimit: () => providerLimiter.wait() };
        const rawVideos = await provider.fetchTrendingVideos(context);
        for (const raw of rawVideos) {
          try {
            const video = provider.normalizeVideo(raw, discoveredAt);
            const metrics = await provider.getMetrics(video, raw, context);
            const videoDbId = this.#db.upsertVideo(video, discoveredAt);
            this.#db.insertSnapshot(videoDbId, { captured_at: discoveredAt, ...metrics });
            discovered += 1;
          } catch (error) {
            failed += 1;
            this.#logger.error("trend_item_failed", { provider: provider.name, error: errorMessage(error) });
          }
        }
        this.#logger.info("provider_discovered", { provider: provider.name, count: rawVideos.length });
      } catch (error) {
        failed += 1;
        this.#logger.error("provider_failed", { provider: provider.name, error: errorMessage(error) });
      }
    }
    return { discovered, failed };
  }

  rank(): readonly ScoredVideo[] {
    const now = this.#now();
    const candidates = this.#db.listScoringCandidates(this.#config.maxVideoAgeHours, now);
    const scored = scoreCandidates(candidates.map(({ video, snapshots }) => ({ video, snapshots })), now, {
      weights: this.#config.weights,
      freshnessHalfLifeHours: this.#config.freshnessHalfLifeHours,
    });
    const byId = new Map(candidates.map(({ video }) => [`${video.platform}:${video.video_id}`, video.db_id]));
    for (const item of scored) this.#db.updateScore(byId.get(`${item.video.platform}:${item.video.video_id}`)!, item.viral_score, now);
    this.#logger.info("videos_ranked", { count: scored.length, top: scored.slice(0, this.#config.topN).map((item) => ({ platform: item.video.platform, video_id: item.video.video_id, viral_score: item.viral_score })) });
    return scored;
  }

  async download(): Promise<{ readonly downloaded: number; readonly failed: number }> {
    const videos = this.#db.listTopVideos(this.#config.topN, this.#config.minViralScore);
    let downloaded = 0;
    let failed = 0;
    for (const video of videos) {
      const existing = this.#db.getDownloadJob(video.db_id);
      if (existing?.status === "succeeded" && existing.local_path !== null) continue;
      const now = this.#now();
      this.#db.beginDownload(video.db_id, now);
      let directory: string | undefined;
      try {
        await mkdir(join(this.#config.dataDir, "tmp"), { recursive: true });
        directory = await mkdtemp(join(this.#config.dataDir, "tmp", `${video.platform}-${video.video_id}-`));
        const target = join(directory, "video.mp4");
        await this.#downloader.download(video.url, target, this.#config.downloadTimeoutMs);
        const fileSize = (await stat(target)).size;
        const sha256 = await hashFile(target);
        this.#db.completeDownload(video.db_id, target, sha256, fileSize, this.#now());
        downloaded += 1;
        this.#logger.info("video_downloaded", { platform: video.platform, video_id: video.video_id, file_size: fileSize, sha256 });
      } catch (error) {
        failed += 1;
        this.#db.failDownload(video.db_id, errorMessage(error), this.#now());
        if (directory !== undefined) await rm(directory, { recursive: true, force: true });
        this.#logger.error("video_download_failed", { platform: video.platform, video_id: video.video_id, error: errorMessage(error) });
      }
    }
    return { downloaded, failed };
  }

  async upload(): Promise<{ readonly uploaded: number; readonly failed: number }> {
    if (this.#storage === undefined) throw new Error("upload requires R2 storage configuration");
    const videos = this.#db.listTopVideos(this.#config.topN, this.#config.minViralScore);
    let uploaded = 0;
    let failed = 0;
    for (const video of videos) {
      const job = this.#db.getDownloadJob(video.db_id);
      if (job?.status !== "succeeded" || job.local_path === null || job.sha256 === null || job.file_size === null) continue;
      if (this.#db.getUpload(video.db_id)?.status === "uploaded") continue;
      try {
        const date = video.discovered_at;
        const rawKey = rawObjectKey(video.platform, video.video_id, date);
        const metadataKey = metadataObjectKey(video.platform, video.video_id, date);
        const existing = this.#db.findUploadedBySha256(job.sha256);
        const scored = this.scoredForVideo(video);
        this.#db.beginUpload(video.db_id, job.sha256, existing?.r2_object_key ?? rawKey, job.file_size, this.#now());
        if (existing === undefined) await this.#storage.putFile(rawKey, job.local_path, "video/mp4", this.#config.uploadTimeoutMs);
        const metadata = makeMetadata(scored, existing?.r2_object_key ?? rawKey, job.file_size, job.sha256, "uploaded");
        await this.#storage.putBytes(metadataKey, metadataBytes(metadata), "application/json", this.#config.uploadTimeoutMs);
        this.#db.completeUpload(video.db_id, metadataKey, this.#now());
        uploaded += 1;
        if (this.#config.deleteLocalAfterUpload) await rm(dirname(job.local_path), { recursive: true, force: true });
        this.#logger.info("video_uploaded", { platform: video.platform, video_id: video.video_id, r2_object_key: existing?.r2_object_key ?? rawKey, metadata_object_key: metadataKey });
      } catch (error) {
        failed += 1;
        this.#db.failUpload(video.db_id, errorMessage(error), this.#now());
        this.#logger.error("video_upload_failed", { platform: video.platform, video_id: video.video_id, error: errorMessage(error) });
      }
    }
    return { uploaded, failed };
  }

  async run(): Promise<Record<string, unknown>> {
    const discovery = await this.discover();
    this.rank();
    const download = await this.download();
    const upload = await this.upload();
    return { discovery, download, upload };
  }

  private scoredForVideo(video: StoredVideo): ScoredVideo {
    const now = this.#now();
    const scored = scoreCandidates([{ video, snapshots: this.#db.getSnapshots(video.db_id) }], now, {
      weights: this.#config.weights,
      freshnessHalfLifeHours: this.#config.freshnessHalfLifeHours,
    })[0];
    if (scored === undefined) throw new Error(`could not score ${video.platform}:${video.video_id}`);
    return scored;
  }
}

export function createPipeline(config: WorkerConfig, storage?: ObjectStorage): { readonly pipeline: TrendPipeline; readonly db: TrendDatabase } {
  const db = new TrendDatabase(config.databasePath);
  const retry = { maxAttempts: config.maxRetries, baseDelayMs: config.retryBaseDelayMs };
  const downloader = new HypitVideoDownloader(retry, new RateLimiter(config.rateLimitMs));
  return { db, pipeline: new TrendPipeline({ db, config, downloader, ...(storage === undefined ? {} : { storage }) }) };
}
