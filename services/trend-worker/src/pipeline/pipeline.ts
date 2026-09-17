import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rm, stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { dirname, join } from "node:path";

import type { WorkerConfig } from "../config.js";
import { TrendDatabase, type LibraryRecord, type StoredVideo } from "../db/database.js";
import { HypitVideoDownloader } from "../downloader/hypit-downloader.js";
import type { VideoDownloader } from "../downloader/hypit-downloader.js";
import { logger as defaultLogger } from "../logger.js";
import { RateLimiter, type RetryOptions } from "../retry.js";
import { scoreCandidates } from "../scoring/score.js";
import { archiveMetadataObjectKey, metadataObjectKey, rawObjectKey } from "../storage/object-keys.js";
import { makeMetadata, metadataBytes } from "../storage/metadata.js";
import type { ObjectInfo, ObjectStorage } from "../storage/r2.js";
import type { Logger, ScoredVideo } from "../types.js";
import { createProviders } from "../providers/registry.js";

async function hashFile(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function videoKey(video: { readonly platform: string; readonly video_id: string }): string { return `${video.platform}:${video.video_id}`; }
function isDeletableKey(key: string): boolean { return key.startsWith("raw/") || key.startsWith("metadata/"); }
function isExpectedRawKey(key: string): boolean { return /^raw\/(?:tiktok|instagram|youtube)\/\d{4}\/\d{2}\/\d{2}\/[^/]+\.mp4$/u.test(key); }
function isExpectedMetadataKey(key: string): boolean { return /^metadata\/(?:tiktok|instagram|youtube)\/\d{4}\/\d{2}\/\d{2}\/[^/]+\.json$/u.test(key); }
function utcDayStart(now: string): string {
  const date = new Date(now);
  date.setUTCHours(0, 0, 0, 0);
  return date.toISOString();
}
function rawTargetBytes(limitBytes: number): number { return Math.floor(limitBytes * 15 / 16); }

export type MaintenanceSummary = {
  readonly hot_count: number;
  readonly classic_count: number;
  readonly archived_count: number;
  readonly archived_this_run: number;
  readonly deleted_raw_objects: number;
  readonly freed_bytes: number;
  readonly managed_r2_bytes: number;
  readonly storage_limit_bytes: number;
  readonly raw_object_count: number;
  readonly raw_bytes: number;
  readonly raw_gb: number;
  readonly raw_storage_target_bytes: number;
};

type ArchivePlan = {
  readonly record: LibraryRecord;
  readonly reason: string;
};

type MaintenancePlan = {
  readonly raw_object_count: number;
  readonly raw_bytes: number;
  readonly hot_count: number;
  readonly classic_count: number;
  readonly archived_count: number;
  readonly candidates_to_archive: readonly { readonly platform: string; readonly video_id: string; readonly reason: string }[];
  readonly raw_objects_to_delete: readonly { readonly key: string; readonly reason: string; readonly size: number }[];
  readonly bytes_expected_to_free: number;
  readonly projected_raw_bytes_after_prune: number;
  readonly projected_raw_gb_after_prune: number;
  readonly storage_limit_bytes: number;
  readonly raw_storage_target_bytes: number;
};

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
    const byId = new Map(candidates.map(({ video }) => [videoKey(video), video.db_id]));
    for (const item of scored) this.#db.updateScore(byId.get(videoKey(item.video))!, item.viral_score, now);
    this.#logger.info("videos_ranked", { count: scored.length, top: scored.slice(0, this.#config.topN).map((item) => ({ platform: item.video.platform, video_id: item.video.video_id, viral_score: item.viral_score })) });
    return scored;
  }

  async download(): Promise<{ readonly downloaded: number; readonly failed: number }> {
    const now = this.#now();
    const since = utcDayStart(now);
    const usedToday = this.#db.countSuccessfulDownloadsSince(since);
    const remainingToday = Math.max(0, this.#config.maxNewDownloadsPerDay - usedToday);
    const candidates = this.#db.listDownloadCandidates(this.#config.topN, this.#config.minViralScore, this.#config.minSnapshotsForDownload, this.#config.maxVideoAgeHours, now);
    let downloaded = 0;
    let failed = 0;
    for (const video of candidates) {
      if (downloaded >= Math.min(this.#config.maxNewDownloadsPerRun, remainingToday)) break;
      const current = this.#db.getDownloadJob(video.db_id);
      if (current?.status === "succeeded" && current.local_path !== null) continue;
      const started = this.#now();
      this.#db.beginDownload(video.db_id, started);
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
    const rawObjects = await this.#storage.listObjects("raw/");
    const rawKeys = new Set(rawObjects.map((object) => object.key));
    let uploaded = 0;
    let failed = 0;
    for (const video of this.#db.listPendingUploads()) {
      const job = this.#db.getDownloadJob(video.db_id);
      if (job?.status !== "succeeded" || job.local_path === null || job.sha256 === null || job.file_size === null) continue;
      try {
        const date = video.discovered_at;
        const rawKey = rawObjectKey(video.platform, video.video_id, date);
        const metadataKey = metadataObjectKey(video.platform, video.video_id, date);
        const existing = this.#db.findUploadedBySha256(job.sha256);
        const reusedKey = existing !== undefined && rawKeys.has(existing.r2_object_key) ? existing.r2_object_key : undefined;
        const r2Key = reusedKey ?? rawKey;
        const scored = this.scoredForVideo(video);
        this.#db.beginUpload(video.db_id, job.sha256, r2Key, job.file_size, this.#now());
        if (reusedKey === undefined) {
          await this.#storage.putFile(rawKey, job.local_path, "video/mp4", this.#config.uploadTimeoutMs);
          rawKeys.add(rawKey);
        }
        const current = this.#db.getVideo(video.db_id) ?? video;
        const metadata = makeMetadata(scored, r2Key, job.file_size, job.sha256, "uploaded", { library_tier: "hot", raw_available: true, peak_viral_score: current.peak_viral_score });
        await this.#storage.putBytes(metadataKey, metadataBytes(metadata), "application/json", this.#config.uploadTimeoutMs);
        this.#db.completeUpload(video.db_id, metadataKey, this.#now());
        uploaded += 1;
        if (this.#config.deleteLocalAfterUpload) await rm(dirname(job.local_path), { recursive: true, force: true });
        this.#logger.info("video_uploaded", { platform: video.platform, video_id: video.video_id, r2_object_key: r2Key, metadata_object_key: metadataKey });
      } catch (error) {
        failed += 1;
        this.#db.failUpload(video.db_id, errorMessage(error), this.#now());
        this.#logger.error("video_upload_failed", { platform: video.platform, video_id: video.video_id, error: errorMessage(error) });
      }
    }
    return { uploaded, failed };
  }

  async recalculateLibraryTiers(): Promise<{ readonly hot_count: number; readonly classic_count: number; readonly archived_count: number }> {
    if (this.#storage === undefined) throw new Error("library maintenance requires R2 storage configuration");
    const rawKeys = new Set((await this.#storage.listObjects("raw/")).map((object) => object.key));
    const records = this.#db.listActiveUploadedRecords();
    const eligible = records.filter((record) => record.snapshotCount >= this.#config.minSnapshotsForDownload && rawKeys.has(record.upload.r2_object_key));
    eligible.sort((a, b) => this.compareClassicRank(a, b));
    const classicIds = new Set(eligible.slice(0, this.#config.maxClassicVideos).map(({ video }) => video.db_id));
    const now = this.#now();
    for (const record of records) this.#db.setLibraryTier(record.video.db_id, classicIds.has(record.video.db_id) ? "classic" : "hot", now);
    const stats = this.#db.stats(utcDayStart(this.#now()));
    this.#logger.info("library_tiers_recalculated", { hot_count: stats.hotVideos, classic_count: stats.classicVideos, archived_count: stats.archivedVideos });
    return { hot_count: stats.hotVideos, classic_count: stats.classicVideos, archived_count: stats.archivedVideos };
  }

  async refreshActiveMetadata(scored: readonly ScoredVideo[] = []): Promise<number> {
    if (this.#storage === undefined) throw new Error("metadata refresh requires R2 storage configuration");
    const scoreByKey = new Map(scored.map((item) => [videoKey(item.video), item]));
    let refreshed = 0;
    for (const record of this.#db.listActiveUploadedRecords()) {
      const item = scoreByKey.get(videoKey(record.video));
      if (item === undefined) continue;
      try {
        const metadataKey = record.upload.metadata_object_key ?? metadataObjectKey(record.video.platform, record.video.video_id, record.video.discovered_at);
        const metadata = makeMetadata(item, record.upload.r2_object_key, record.upload.file_size, record.upload.sha256, "uploaded", {
          library_tier: record.upload.library_tier === "archived" ? "hot" : record.upload.library_tier,
          raw_available: true,
          peak_viral_score: record.video.peak_viral_score,
        });
        await this.storagePutBytes(metadataKey, metadataBytes(metadata));
        this.#db.markMetadataRefreshed(record.video.db_id, this.#now());
        refreshed += 1;
      } catch (error) {
        this.#logger.error("metadata_refresh_failed", { platform: record.video.platform, video_id: record.video.video_id, error: errorMessage(error) });
      }
    }
    return refreshed;
  }

  async maintain(): Promise<MaintenanceSummary> {
    if (this.#storage === undefined) throw new Error("maintenance requires R2 storage configuration");
    await this.recalculateLibraryTiers();
    let archivedThisRun = 0;
    let deletedRawObjects = 0;
    let freedBytes = 0;
    let objects = await this.rawObjects();

    for (const object of this.orphanRawObjects(objects)) {
      try {
        await this.deleteManagedObject(object.key);
        deletedRawObjects += 1;
        freedBytes += object.size;
        this.#logger.info("orphan_raw_deleted", { key: object.key, size: object.size });
      } catch (error) {
        this.#logger.error("orphan_raw_delete_failed", { key: object.key, error: errorMessage(error) });
      }
    }
    objects = await this.rawObjects();

    const expireBefore = Date.parse(this.#now()) - this.#config.hotRetentionDays * 24 * 3_600_000;
    const expired = this.#db.listActiveUploadedRecords().filter((record) => record.upload.library_tier !== "classic" && Date.parse(record.upload.uploaded_at ?? this.#now()) <= expireBefore);
    expired.sort((a, b) => this.compareOldest(a, b));
    for (const record of expired) {
      const result = await this.archiveRecord(record, "hot_retention_expired", objects);
      archivedThisRun += result.archived ? 1 : 0;
      deletedRawObjects += result.deletedRaw ? 1 : 0;
      freedBytes += result.freedBytes;
      if (result.archived) objects = await this.rawObjects();
    }

    let usage = this.totalBytes(objects);
    const limit = this.#config.r2SoftLimitGb * 1024 ** 3;
    const target = rawTargetBytes(limit);
    let guard = this.#db.listActiveUploadedRecords().length + 1;
    while (usage > target && guard-- > 0) {
      const records = this.#db.listActiveUploadedRecords();
      const rawKeys = new Set(objects.filter((object) => object.key.startsWith("raw/")).map((object) => object.key));
      const nonClassics = records.filter((record) => record.upload.library_tier !== "classic" && (rawKeys.has(record.upload.r2_object_key) || record.upload.metadata_object_key !== null));
      nonClassics.sort((a, b) => this.compareOldest(a, b));
      const classics = records.filter((record) => record.upload.library_tier === "classic");
      classics.sort((a, b) => this.compareClassicRank(b, a));
      const candidate = nonClassics[0] ?? classics[0];
      if (candidate === undefined) break;
      const result = await this.archiveRecord(candidate, candidate.upload.library_tier === "classic" ? "storage_pressure_classic" : "storage_pressure_hot", objects);
      if (!result.archived) break;
      archivedThisRun += 1;
      deletedRawObjects += result.deletedRaw ? 1 : 0;
      freedBytes += result.freedBytes;
      objects = await this.rawObjects();
      usage = this.totalBytes(objects);
    }
    await this.recalculateLibraryTiers();
    const finalObjects = await this.rawObjects();
    const stats = this.#db.stats(utcDayStart(this.#now()));
    const rawBytes = this.totalBytes(finalObjects);
    const summary = { hot_count: stats.hotVideos, classic_count: stats.classicVideos, archived_count: stats.archivedVideos, archived_this_run: archivedThisRun, deleted_raw_objects: deletedRawObjects, freed_bytes: freedBytes, managed_r2_bytes: rawBytes, storage_limit_bytes: limit, raw_object_count: finalObjects.length, raw_bytes: rawBytes, raw_gb: rawBytes / 1024 ** 3, raw_storage_target_bytes: target };
    this.#logger.info("storage_maintenance_complete", summary);
    return summary;
  }

  async stats(): Promise<Record<string, unknown>> {
    if (this.#storage === undefined) throw new Error("stats requires R2 storage configuration");
    const now = this.#now();
    const stats = this.#db.stats(utcDayStart(now));
    const rawObjects = await this.rawObjects();
    const rawBytes = this.totalBytes(rawObjects);
    const softLimitBytes = this.#config.r2SoftLimitGb * 1024 ** 3;
    return {
      videos_total: stats.totalDiscoveredVideos,
      snapshots_total: stats.snapshotsTotal,
      uploaded_total: stats.uploadedTotal,
      active_uploaded: stats.activeUploadedVideos,
      active_count: stats.activeUploadedVideos,
      hot_count: stats.hotVideos,
      classic_count: stats.classicVideos,
      archived_count: stats.archivedVideos,
      downloads_today: stats.downloadsToday,
      raw_object_count: rawObjects.length,
      raw_bytes: rawBytes,
      raw_gb: rawBytes / 1024 ** 3,
      raw_storage_target_bytes: rawTargetBytes(softLimitBytes),
      soft_limit_gb: this.#config.r2SoftLimitGb,
      classic_limit: this.#config.maxClassicVideos,
      managed_r2_bytes: rawBytes,
      managed_r2_gb: rawBytes / 1024 ** 3,
      configured_soft_limit_gb: this.#config.r2SoftLimitGb,
      consistency: await this.consistencyReport(rawObjects),
    };
  }

  async prune(options: { readonly dryRun?: boolean } = {}): Promise<Record<string, unknown>> {
    const rawObjects = await this.rawObjects();
    if (options.dryRun === true) return this.maintenancePlan(rawObjects);
    await this.recalculateLibraryTiers();
    const refreshed = await this.refreshActiveMetadata(this.rank());
    const maintenance = await this.maintain();
    const finalRefresh = await this.refreshActiveMetadata(this.rank());
    return { ...maintenance, metadata_refreshed: refreshed + finalRefresh };
  }

  async run(): Promise<Record<string, unknown>> {
    const discovery = await this.discover();
    const ranked = this.rank();
    await this.recalculateLibraryTiers();
    const preMaintenance = await this.maintain();
    const download = await this.download();
    const upload = await this.upload();
    let metadataRefreshed = await this.refreshActiveMetadata(ranked);
    await this.recalculateLibraryTiers();
    metadataRefreshed += await this.refreshActiveMetadata(ranked);
    const postMaintenance = await this.maintain();
    metadataRefreshed += await this.refreshActiveMetadata(ranked);
    const finalStats = await this.stats();
    return {
      discovered: discovery.discovered,
      ranked: ranked.length,
      downloaded: download.downloaded,
      uploaded: upload.uploaded,
      metadata_refreshed: metadataRefreshed,
      hot_count: postMaintenance.hot_count,
      classic_count: postMaintenance.classic_count,
      archived_count: postMaintenance.archived_count,
      archived_this_run: preMaintenance.archived_this_run + postMaintenance.archived_this_run,
      deleted_raw_objects: preMaintenance.deleted_raw_objects + postMaintenance.deleted_raw_objects,
      freed_bytes: preMaintenance.freed_bytes + postMaintenance.freed_bytes,
      managed_r2_bytes: finalStats.raw_bytes,
      storage_limit_bytes: postMaintenance.storage_limit_bytes,
      discovery,
      download,
      upload,
    };
  }

  private prospectiveTiers(records: readonly LibraryRecord[], rawObjects: readonly ObjectInfo[]): { readonly tiers: ReadonlyMap<number, "hot" | "classic">; readonly classicIds: ReadonlySet<number> } {
    const rawKeys = new Set(rawObjects.map((object) => object.key));
    const eligible = records.filter((record) => record.snapshotCount >= this.#config.minSnapshotsForDownload && rawKeys.has(record.upload.r2_object_key));
    eligible.sort((a, b) => this.compareClassicRank(a, b));
    const classicIds = new Set(eligible.slice(0, this.#config.maxClassicVideos).map(({ video }) => video.db_id));
    const tiers = new Map(records.map(({ video }) => [video.db_id, classicIds.has(video.db_id) ? "classic" as const : "hot" as const]));
    return { tiers, classicIds };
  }

  private maintenancePlan(rawObjects: readonly ObjectInfo[]): MaintenancePlan {
    const records = [...this.#db.listActiveUploadedRecords()];
    const { tiers } = this.prospectiveTiers(records, rawObjects);
    const limit = this.#config.r2SoftLimitGb * 1024 ** 3;
    const target = rawTargetBytes(limit);
    const rawByKey = new Map(rawObjects.map((object) => [object.key, object]));
    const rawObjectsToDelete = new Map<string, { readonly key: string; readonly reason: string; readonly size: number }>();
    const candidates: ArchivePlan[] = [];
    const activeRecords = new Set(records.map(({ video }) => video.db_id));
    const activeReferences = new Map<string, number>();
    for (const record of records) activeReferences.set(record.upload.r2_object_key, (activeReferences.get(record.upload.r2_object_key) ?? 0) + 1);
    const markRawForDelete = (key: string, reason: string): void => {
      const object = rawByKey.get(key);
      if (object !== undefined) rawObjectsToDelete.set(key, { key, reason, size: object.size });
    };
    const archive = (record: LibraryRecord, reason: string): void => {
      if (!activeRecords.has(record.video.db_id)) return;
      candidates.push({ record, reason });
      activeRecords.delete(record.video.db_id);
      const key = record.upload.r2_object_key;
      const references = (activeReferences.get(key) ?? 1) - 1;
      activeReferences.set(key, references);
      if (references <= 0) markRawForDelete(key, reason);
    };

    const references = this.#db.listRawReferences();
    const protectedKeys = new Set(references.filter((reference) => reference.status !== "uploaded" || reference.storage_status === "active").map((reference) => reference.r2_object_key));
    for (const object of rawObjects) if (isExpectedRawKey(object.key) && !protectedKeys.has(object.key)) markRawForDelete(object.key, "orphan_raw");

    const expireBefore = Date.parse(this.#now()) - this.#config.hotRetentionDays * 24 * 3_600_000;
    const expired = records.filter((record) => tiers.get(record.video.db_id) !== "classic" && Date.parse(record.upload.uploaded_at ?? "") <= expireBefore);
    expired.sort((a, b) => this.compareOldest(a, b));
    for (const record of expired) archive(record, "hot_retention_expired");

    const projected = () => rawObjects.reduce((sum, object) => sum + object.size, 0) - [...rawObjectsToDelete.values()].reduce((sum, object) => sum + object.size, 0);
    while (projected() > target) {
      const remaining = records.filter((record) => activeRecords.has(record.video.db_id));
      const nonClassics = remaining.filter((record) => tiers.get(record.video.db_id) !== "classic");
      nonClassics.sort((a, b) => this.compareOldest(a, b));
      const classics = remaining.filter((record) => tiers.get(record.video.db_id) === "classic");
      classics.sort((a, b) => this.compareClassicRank(b, a));
      const candidate = nonClassics[0] ?? classics[0];
      if (candidate === undefined) break;
      archive(candidate, tiers.get(candidate.video.db_id) === "classic" ? "storage_pressure_classic" : "storage_pressure_hot");
    }

    const initialHot = records.filter((record) => tiers.get(record.video.db_id) === "hot").length;
    const initialClassic = records.length - initialHot;
    const archivedFromHot = candidates.filter(({ record }) => tiers.get(record.video.db_id) === "hot").length;
    const archivedFromClassic = candidates.filter(({ record }) => tiers.get(record.video.db_id) === "classic").length;
    const dbStats = this.#db.stats();
    const rawBytes = this.totalBytes(rawObjects);
    const expectedFree = [...rawObjectsToDelete.values()].reduce((sum, object) => sum + object.size, 0);
    return {
      raw_object_count: rawObjects.length,
      raw_bytes: rawBytes,
      hot_count: initialHot - archivedFromHot,
      classic_count: initialClassic - archivedFromClassic,
      archived_count: dbStats.archivedVideos + candidates.length,
      candidates_to_archive: candidates.map(({ record, reason }) => ({ platform: record.video.platform, video_id: record.video.video_id, reason })),
      raw_objects_to_delete: [...rawObjectsToDelete.values()].sort((a, b) => a.key.localeCompare(b.key)),
      bytes_expected_to_free: expectedFree,
      projected_raw_bytes_after_prune: rawBytes - expectedFree,
      projected_raw_gb_after_prune: (rawBytes - expectedFree) / 1024 ** 3,
      storage_limit_bytes: limit,
      raw_storage_target_bytes: target,
    };
  }

  private async consistencyReport(rawObjects: readonly ObjectInfo[]): Promise<Record<string, unknown>> {
    if (this.#storage === undefined) return {};
    const metadataObjects = await this.#storage.listObjects("metadata/");
    const archiveObjects = await this.#storage.listObjects("archive/metadata/");
    const rawKeys = new Set(rawObjects.map((object) => object.key));
    const activeRecords = this.#db.listActiveUploadedRecords();
    const activeMetadataKeys = new Set(activeRecords.map((record) => record.upload.metadata_object_key).filter((key): key is string => key !== null));
    const references = this.#db.listRawReferences();
    const protectedKeys = new Set(references.filter((reference) => reference.status !== "uploaded" || reference.storage_status === "active").map((reference) => reference.r2_object_key));
    return {
      active_uploads_missing_raw: activeRecords.filter((record) => !rawKeys.has(record.upload.r2_object_key)).map((record) => `${record.video.platform}:${record.video.video_id}`),
      active_metadata_missing_raw: activeRecords.filter((record) => !rawKeys.has(record.upload.r2_object_key) && activeMetadataKeys.has(record.upload.metadata_object_key ?? "")).map((record) => `${record.video.platform}:${record.video.video_id}`),
      orphan_raw_objects: rawObjects.filter((object) => isExpectedRawKey(object.key) && !protectedKeys.has(object.key)).map((object) => object.key),
      unknown_raw_objects: rawObjects.filter((object) => !isExpectedRawKey(object.key)).map((object) => object.key),
      metadata_objects_without_active_db_reference: metadataObjects.filter((object) => !activeMetadataKeys.has(object.key)).map((object) => object.key),
      archive_metadata_objects: archiveObjects.length,
    };
  }

  private async archiveRecord(record: LibraryRecord, reason: string, objects: readonly ObjectInfo[]): Promise<{ readonly archived: boolean; readonly deletedRaw: boolean; readonly freedBytes: number }> {
    if (this.#storage === undefined) return { archived: false, deletedRaw: false, freedBytes: 0 };
    const now = this.#now();
    const scored = this.scoredForVideo(record.video);
    const archiveKey = record.upload.archive_metadata_object_key ?? archiveMetadataObjectKey(record.video.platform, record.video.video_id, record.video.discovered_at);
    const archiveMetadata = makeMetadata(scored, record.upload.r2_object_key, record.upload.file_size, record.upload.sha256, "uploaded", {
      library_tier: "archived",
      raw_available: false,
      peak_viral_score: record.video.peak_viral_score,
      archived_at: now,
      archive_reason: reason,
      prior_tier: record.upload.library_tier === "archived" ? "hot" : record.upload.library_tier,
    });
    try {
      await this.storagePutBytes(archiveKey, metadataBytes(archiveMetadata));
      let deletedRaw = false;
      let freedBytes = 0;
      if (isExpectedRawKey(record.upload.r2_object_key) && this.#db.countActiveRawReferences(record.upload.r2_object_key, record.video.db_id) === 0) {
        const rawObject = objects.find((object) => object.key === record.upload.r2_object_key);
        if (rawObject !== undefined) {
          await this.deleteManagedObject(record.upload.r2_object_key);
          deletedRaw = true;
          freedBytes += rawObject.size;
        }
      }
      const activeMetadataKey = record.upload.metadata_object_key ?? metadataObjectKey(record.video.platform, record.video.video_id, record.video.discovered_at);
      if (activeMetadataKey.startsWith("metadata/")) await this.deleteManagedObject(activeMetadataKey);
      this.#db.markArchived(record.video.db_id, archiveKey, reason, now);
      this.#logger.info("video_archived", { platform: record.video.platform, video_id: record.video.video_id, archive_key: archiveKey, reason, deleted_raw: deletedRaw });
      return { archived: true, deletedRaw, freedBytes };
    } catch (error) {
      this.#logger.error("video_archive_failed", { platform: record.video.platform, video_id: record.video.video_id, reason, error: errorMessage(error) });
      return { archived: false, deletedRaw: false, freedBytes: 0 };
    }
  }

  private async rawObjects(): Promise<readonly ObjectInfo[]> {
    if (this.#storage === undefined) throw new Error("storage is required");
    return (await this.#storage.listObjects("raw/")).filter((object) => object.key.startsWith("raw/"));
  }

  private orphanRawObjects(objects: readonly ObjectInfo[]): readonly ObjectInfo[] {
    const references = this.#db.listRawReferences();
    const protectedKeys = new Set(references.filter((reference) => reference.status !== "uploaded" || reference.storage_status === "active").map((reference) => reference.r2_object_key));
    return objects.filter((object) => isExpectedRawKey(object.key) && !protectedKeys.has(object.key));
  }

  private async storagePutBytes(key: string, bytes: Uint8Array): Promise<void> {
    if (this.#storage === undefined) throw new Error("storage is required");
    await this.#storage.putBytes(key, bytes, "application/json", this.#config.uploadTimeoutMs);
  }

  private async deleteManagedObject(key: string): Promise<void> {
    if (!isDeletableKey(key) || (key.startsWith("raw/") ? !isExpectedRawKey(key) : !isExpectedMetadataKey(key))) throw new Error(`refusing to delete unmanaged object ${key}`);
    if (this.#storage === undefined) throw new Error("storage is required");
    await this.#storage.deleteObject(key);
  }

  private totalBytes(objects: readonly ObjectInfo[]): number { return objects.reduce((sum, object) => sum + object.size, 0); }

  private compareClassicRank(a: LibraryRecord, b: LibraryRecord): number {
    return b.video.peak_viral_score - a.video.peak_viral_score
      || b.video.viral_score - a.video.viral_score
      || (b.latestMetrics?.views ?? 0) - (a.latestMetrics?.views ?? 0)
      || (b.latestMetrics?.likes ?? 0) - (a.latestMetrics?.likes ?? 0)
      || (b.latestMetrics?.comments ?? 0) - (a.latestMetrics?.comments ?? 0)
      || (b.latestMetrics?.shares ?? 0) - (a.latestMetrics?.shares ?? 0)
      || (b.video.peak_score_at ?? "").localeCompare(a.video.peak_score_at ?? "")
      || videoKey(a.video).localeCompare(videoKey(b.video));
  }

  private compareOldest(a: LibraryRecord, b: LibraryRecord): number {
    const aTime = Date.parse(a.upload.uploaded_at ?? "");
    const bTime = Date.parse(b.upload.uploaded_at ?? "");
    return (Number.isFinite(aTime) ? aTime : Number.MAX_SAFE_INTEGER) - (Number.isFinite(bTime) ? bTime : Number.MAX_SAFE_INTEGER)
      || a.video.peak_viral_score - b.video.peak_viral_score
      || videoKey(a.video).localeCompare(videoKey(b.video));
  }

  private scoredForVideo(video: StoredVideo): ScoredVideo {
    const scored = scoreCandidates([{ video, snapshots: this.#db.getSnapshots(video.db_id) }], this.#now(), {
      weights: this.#config.weights,
      freshnessHalfLifeHours: this.#config.freshnessHalfLifeHours,
    })[0];
    if (scored === undefined) throw new Error(`could not score ${video.platform}:${video.video_id}`);
    return scored;
  }
}

export function createPipeline(config: WorkerConfig, storage?: ObjectStorage): { readonly pipeline: TrendPipeline; readonly db: TrendDatabase } {
  const db = new TrendDatabase(config.databasePath);
  const retry: RetryOptions = { maxAttempts: config.maxRetries, baseDelayMs: config.retryBaseDelayMs };
  const downloader = new HypitVideoDownloader(retry, new RateLimiter(config.rateLimitMs));
  return { db, pipeline: new TrendPipeline({ db, config, downloader, ...(storage === undefined ? {} : { storage }) }) };
}
