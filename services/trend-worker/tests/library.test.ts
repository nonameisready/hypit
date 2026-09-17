import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { loadConfig } from "../src/config.js";
import { TrendDatabase } from "../src/db/database.js";
import { TrendPipeline } from "../src/pipeline/pipeline.js";
import { metadataObjectKey, rawObjectKey } from "../src/storage/object-keys.js";
import type { ObjectInfo, ObjectStorage } from "../src/storage/r2.js";
import type { VideoDownloader } from "../src/downloader/hypit-downloader.js";

const NOW = "2026-09-16T12:00:00.000Z";
const weights = { view_velocity: 0.35, share_velocity: 0.30, comment_velocity: 0.15, like_velocity: 0.10, acceleration: 0.10 } as const;

class FakeStorage implements ObjectStorage {
  readonly objects = new Map<string, Uint8Array>();
  readonly deleted: string[] = [];
  readonly operations: string[] = [];

  async putFile(key: string, path: string): Promise<number> {
    const bytes = new Uint8Array(await readFile(path));
    this.objects.set(key, bytes);
    this.operations.push(`put:${key}`);
    return bytes.byteLength;
  }

  async putBytes(key: string, bytes: Uint8Array): Promise<number> {
    this.objects.set(key, new Uint8Array(bytes));
    this.operations.push(`put:${key}`);
    return bytes.byteLength;
  }

  async listObjects(prefix: string): Promise<readonly ObjectInfo[]> {
    return [...this.objects.entries()].filter(([key]) => key.startsWith(prefix)).map(([key, bytes]) => ({ key, size: bytes.byteLength }));
  }

  async deleteObject(key: string): Promise<void> {
    this.deleted.push(key);
    this.operations.push(`delete:${key}`);
    this.objects.delete(key);
  }

  putSized(key: string, size: number): void { this.objects.set(key, new Uint8Array(size)); }
  json(key: string): Record<string, unknown> { return JSON.parse(new TextDecoder().decode(this.objects.get(key))) as Record<string, unknown>; }
}

const downloader: VideoDownloader = {
  async download(_url, target) { await writeFile(target, "video-bytes"); },
};

function video(id: string, discoveredAt = NOW, publishedAt: string | null = discoveredAt) {
  return { platform: "youtube" as const, video_id: id, url: `https://youtu.be/${id}`, creator: null, caption: null, published_at: publishedAt, duration: 10, discovered_at: discoveredAt };
}

function config(directory: string, values: Record<string, string> = {}) {
  return loadConfig({ ...process.env, DATA_DIR: directory, DATABASE_PATH: join(directory, "worker.sqlite"), TREND_PROVIDERS: "manual", RATE_LIMIT_MS: "0", ...values });
}

function makePipeline(directory: string, db: TrendDatabase, storage: FakeStorage, values: Record<string, string> = {}, now = NOW): TrendPipeline {
  return new TrendPipeline({ db, storage, downloader, config: config(directory, values), now: () => now });
}

function seed(db: TrendDatabase, id: string, score = 0.5, discoveredAt = NOW, publishedAt: string | null = discoveredAt, snapshots = 2): number {
  const dbId = db.upsertVideo(video(id, discoveredAt, publishedAt), NOW);
  for (let index = 0; index < snapshots; index += 1) db.insertSnapshot(dbId, { captured_at: new Date(Date.parse(NOW) - (snapshots - index) * 3_600_000).toISOString(), views: index * 100 + 100, likes: index * 10 + 10, comments: index * 2 + 2, shares: index + 1 });
  db.updateScore(dbId, score, NOW);
  return dbId;
}

function uploaded(db: TrendDatabase, id: number, key: string, uploadedAt = NOW, size = 100, metadataKey = `metadata/youtube/2026/09/16/video-${id}.json`): void {
  db.beginUpload(id, `sha-${id}`, key, size, uploadedAt);
  db.completeUpload(id, metadataKey, uploadedAt);
}

async function withDb<T>(callback: (directory: string, db: TrendDatabase) => Promise<T> | T): Promise<T> {
  const directory = await mkdtemp(join(tmpdir(), "trend-worker-library-"));
  const db = new TrendDatabase(join(directory, "worker.sqlite"));
  try { return await callback(directory, db); } finally { db.close(); await rm(directory, { recursive: true, force: true }); }
}

test("one snapshot is not downloadable when minimum is two", async () => withDb(async (directory, db) => {
  seed(db, "one", 1, NOW, NOW, 1);
  const pipeline = makePipeline(directory, db, new FakeStorage());
  assert.deepEqual(await pipeline.download(), { downloaded: 0, failed: 0 });
}));

test("two snapshots make a candidate eligible", async () => withDb(async (directory, db) => {
  seed(db, "two", 1);
  const pipeline = makePipeline(directory, db, new FakeStorage());
  assert.deepEqual(await pipeline.download(), { downloaded: 1, failed: 0 });
}));

test("expired candidates are excluded using published_at", async () => withDb(async (directory, db) => {
  seed(db, "old", 1, NOW, "2026-09-01T00:00:00.000Z");
  const pipeline = makePipeline(directory, db, new FakeStorage(), { MAX_VIDEO_AGE_HOURS: "72" });
  assert.deepEqual(await pipeline.download(), { downloaded: 0, failed: 0 });
}));

test("successfully uploaded videos are never downloaded again", async () => withDb(async (directory, db) => {
  const id = seed(db, "uploaded", 1);
  uploaded(db, id, "raw/youtube/2026/09/16/uploaded.mp4");
  let calls = 0;
  const pipeline = new TrendPipeline({ db, config: config(directory), storage: new FakeStorage(), downloader: { async download() { calls += 1; } }, now: () => NOW });
  assert.deepEqual(await pipeline.download(), { downloaded: 0, failed: 0 });
  assert.equal(calls, 0);
}));

test("MAX_NEW_DOWNLOADS_PER_RUN limits new downloads while TOP_N remains a pool", async () => withDb(async (directory, db) => {
  for (let index = 0; index < 5; index += 1) seed(db, `run-${index}`, 1 - index / 10);
  const pipeline = makePipeline(directory, db, new FakeStorage(), { TOP_N: "5", MAX_NEW_DOWNLOADS_PER_RUN: "2" });
  assert.deepEqual(await pipeline.download(), { downloaded: 2, failed: 0 });
}));

test("rolling daily upload window limits new downloads", async () => withDb(async (directory, db) => {
  for (let index = 0; index < 2; index += 1) {
    const id = seed(db, `already-${index}`);
    db.beginDownload(id, "2026-09-16T01:00:00.000Z");
    db.completeDownload(id, `/tmp/already-${index}.mp4`, `sha-already-${index}`, 10, "2026-09-16T01:00:00.000Z");
  }
  seed(db, "blocked", 1);
  const pipeline = makePipeline(directory, db, new FakeStorage(), { MAX_NEW_DOWNLOADS_PER_DAY: "2" });
  assert.deepEqual(await pipeline.download(), { downloaded: 0, failed: 0 });
}));

test("pending downloaded jobs upload even when they are not in the current top-N", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "pending", 0.1);
  const path = join(directory, "pending.mp4");
  await writeFile(path, "pending-video");
  db.beginDownload(id, NOW);
  db.completeDownload(id, path, "pending-sha", (await stat(path)).size, NOW);
  const pipeline = makePipeline(directory, db, storage, { TOP_N: "1", DELETE_LOCAL_AFTER_UPLOAD: "false" });
  assert.deepEqual(await pipeline.upload(), { uploaded: 1, failed: 0 });
  assert.ok(storage.objects.has("raw/youtube/2026/09/16/pending.mp4"));
}));

test("peak viral score only increases", async () => withDb(async (_directory, db) => {
  const id = seed(db, "peak", 0.8);
  db.updateScore(id, 0.4, "2026-09-16T13:00:00.000Z");
  assert.equal(db.getVideo(id)?.peak_viral_score, 0.8);
  db.updateScore(id, 0.9, "2026-09-16T14:00:00.000Z");
  assert.equal(db.getVideo(id)?.peak_viral_score, 0.9);
  assert.equal(db.getVideo(id)?.peak_score_at, "2026-09-16T14:00:00.000Z");
}));

test("classic selection is capped and a higher peak displaces a lower classic", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const low = seed(db, "low", 0.4);
  const high = seed(db, "high", 0.9);
  storage.putSized("raw/youtube/2026/09/16/low.mp4", 10);
  storage.putSized("raw/youtube/2026/09/16/high.mp4", 10);
  uploaded(db, low, "raw/youtube/2026/09/16/low.mp4");
  uploaded(db, high, "raw/youtube/2026/09/16/high.mp4");
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "1" });
  await pipeline.recalculateLibraryTiers();
  assert.equal(db.getUpload(high)?.library_tier, "classic");
  assert.equal(db.getUpload(low)?.library_tier, "hot");
}));

test("a displaced classic younger than retention becomes hot", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const first = seed(db, "first", 0.8);
  const second = seed(db, "second", 0.9);
  storage.putSized("raw/youtube/2026/09/16/first.mp4", 10);
  storage.putSized("raw/youtube/2026/09/16/second.mp4", 10);
  uploaded(db, first, "raw/youtube/2026/09/16/first.mp4");
  uploaded(db, second, "raw/youtube/2026/09/16/second.mp4");
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "1" });
  await pipeline.recalculateLibraryTiers();
  assert.equal(db.getUpload(first)?.library_tier, "hot");
}));

test("a displaced classic older than retention is archived", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const old = seed(db, "oldclassic", 0.4, "2026-08-01T00:00:00.000Z", "2026-08-01T00:00:00.000Z");
  const high = seed(db, "newclassic", 0.9);
  storage.putSized("raw/youtube/2026/08/01/oldclassic.mp4", 10);
  storage.putSized("raw/youtube/2026/09/16/newclassic.mp4", 10);
  uploaded(db, old, "raw/youtube/2026/08/01/oldclassic.mp4", "2026-08-01T00:00:00.000Z");
  uploaded(db, high, "raw/youtube/2026/09/16/newclassic.mp4");
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "1" });
  await pipeline.maintain();
  assert.equal(db.getUpload(old)?.library_tier, "archived");
  assert.ok(storage.objects.has("archive/metadata/youtube/2026/08/01/oldclassic.json"));
}));

test("archive metadata is written before active raw and metadata deletion", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "expire", 0.4);
  const raw = "raw/youtube/2026/09/16/expire.mp4";
  const active = metadataObjectKey("youtube", "expire", NOW);
  storage.putSized(raw, 10);
  storage.putSized(active, 2);
  uploaded(db, id, raw, "2026-08-01T00:00:00.000Z");
  db.beginUpload(id, "sha-1", raw, 10, "2026-08-01T00:00:00.000Z");
  db.completeUpload(id, active, "2026-08-01T00:00:00.000Z");
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "0" });
  await pipeline.maintain();
  const archivePut = storage.operations.indexOf("put:archive/metadata/youtube/2026/09/16/expire.json");
  const rawDelete = storage.operations.indexOf(`delete:${raw}`);
  const metadataDelete = storage.operations.indexOf(`delete:${active}`);
  assert.ok(archivePut >= 0 && archivePut < rawDelete && rawDelete < metadataDelete);
  assert.ok(storage.objects.has("archive/metadata/youtube/2026/09/16/expire.json"));
  assert.equal(storage.json("archive/metadata/youtube/2026/09/16/expire.json").library_tier, "archived");
  assert.equal(storage.json("archive/metadata/youtube/2026/09/16/expire.json").raw_available, false);
}));

test("soft-limit pruning prioritizes expired hot videos", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const hot = seed(db, "hot", 0.2);
  const classic = seed(db, "classic", 0.9);
  storage.putSized("raw/youtube/2026/09/16/hot.mp4", 100);
  storage.putSized("raw/youtube/2026/09/16/classic.mp4", 100);
  uploaded(db, hot, "raw/youtube/2026/09/16/hot.mp4", "2026-08-01T00:00:00.000Z");
  uploaded(db, classic, "raw/youtube/2026/09/16/classic.mp4");
  const pipeline = makePipeline(directory, db, storage, { R2_SOFT_LIMIT_GB: String(10_000 / 1024 ** 3), MAX_CLASSIC_VIDEOS: "1" });
  await pipeline.maintain();
  assert.ok(storage.deleted[0]?.includes("hot.mp4"));
  assert.ok(storage.objects.has("raw/youtube/2026/09/16/classic.mp4"));
}));

test("soft cap overrides the configured classic maximum", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const strong = seed(db, "strong-classic", 0.9);
  const weak = seed(db, "weak-classic", 0.2);
  const strongRaw = "raw/youtube/2026/09/16/strong-classic.mp4";
  const weakRaw = "raw/youtube/2026/09/16/weak-classic.mp4";
  storage.putSized(strongRaw, 100);
  storage.putSized(weakRaw, 100);
  uploaded(db, strong, strongRaw);
  uploaded(db, weak, weakRaw);
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "2", R2_SOFT_LIMIT_GB: String(160 / 1024 ** 3) });
  await pipeline.maintain();
  assert.equal(db.getUpload(strong)?.library_tier, "classic");
  assert.equal(db.getUpload(weak)?.library_tier, "archived");
  assert.equal(storage.objects.has(weakRaw), false);
}));

test("shared raw objects are not deleted while another active upload references them", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const first = seed(db, "shared-a", 0.2);
  const second = seed(db, "shared-b", 0.3);
  const raw = "raw/youtube/2026/09/16/shared.mp4";
  storage.putSized(raw, 100);
  uploaded(db, first, raw, "2026-08-01T00:00:00.000Z");
  uploaded(db, second, raw, NOW);
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "0" });
  await pipeline.maintain();
  assert.ok(storage.objects.has(raw));
  assert.equal(db.getUpload(first)?.library_tier, "archived");
}));

test("maintenance never deletes objects outside managed namespaces", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  storage.putSized("adscream/unrelated.mp4", 1000);
  storage.putSized("raw/unrelated.bin", 1000);
  const id = seed(db, "managed", 0.2);
  storage.putSized("raw/youtube/2026/09/16/managed.mp4", 10);
  uploaded(db, id, "raw/youtube/2026/09/16/managed.mp4", "2026-08-01T00:00:00.000Z");
  await makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "0" }).maintain();
  assert.equal(storage.deleted.some((key) => key.startsWith("adscream/")), false);
  assert.ok(storage.objects.has("adscream/unrelated.mp4"));
  assert.ok(storage.objects.has("raw/unrelated.bin"));
}));

test("active metadata refresh updates current metrics without uploading MP4 again", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "refresh", 0.2);
  const raw = "raw/youtube/2026/09/16/refresh.mp4";
  const active = metadataObjectKey("youtube", "refresh", NOW);
  storage.putSized(raw, 10);
  uploaded(db, id, raw, NOW, 100, active);
  await storage.putBytes(active, new TextEncoder().encode("old"));
  let mediaUploads = 0;
  const originalPutFile = storage.putFile.bind(storage);
  storage.putFile = async (...args: Parameters<FakeStorage["putFile"]>) => { mediaUploads += 1; return originalPutFile(...args); };
  db.insertSnapshot(id, { captured_at: "2026-09-16T11:00:00.000Z", views: 100, likes: 10, comments: 2, shares: 1 });
  db.insertSnapshot(id, { captured_at: NOW, views: 1000, likes: 100, comments: 20, shares: 10 });
  const pipeline = makePipeline(directory, db, storage);
  const ranked = pipeline.rank();
  assert.equal(await pipeline.refreshActiveMetadata(ranked), 1);
  assert.equal(mediaUploads, 0);
  assert.equal((storage.json(active).metrics as { views: number }).views, 1000);
  assert.equal(db.getUpload(id)?.metadata_refreshed_at, NOW);
}));

test("classic videos keep active metadata under metadata/", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "classic-active", 0.9);
  const raw = "raw/youtube/2026/09/16/classic-active.mp4";
  const active = "metadata/youtube/2026/09/16/classic-active.json";
  storage.putSized(raw, 10);
  storage.putSized(active, 2);
  uploaded(db, id, raw, NOW, 10, active);
  await makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "1" }).recalculateLibraryTiers();
  assert.equal(db.getUpload(id)?.library_tier, "classic");
  assert.ok(storage.objects.has(active));
  assert.equal(storage.deleted.includes(active), false);
}));

test("prune dry-run reports a plan without deleting or mutating DB state", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "dry-run", 0.2);
  const raw = "raw/youtube/2026/09/16/dry-run.mp4";
  const active = "metadata/youtube/2026/09/16/dry-run.json";
  storage.putSized(raw, 10);
  storage.putSized(active, 2);
  uploaded(db, id, raw, "2026-08-01T00:00:00.000Z", 10, active);
  const result = await makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "0" }).prune({ dryRun: true });
  assert.equal(storage.deleted.length, 0);
  assert.equal(db.getUpload(id)?.library_tier, "hot");
  assert.deepEqual(result.candidates_to_archive, [{ platform: "youtube", video_id: "dry-run", reason: "hot_retention_expired" }]);
  assert.equal(result.bytes_expected_to_free, 10);
  assert.equal(result.projected_raw_bytes_after_prune, 0);
}));

test("prune is idempotent after a successful archive", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "idempotent", 0.2);
  const raw = "raw/youtube/2026/09/16/idempotent.mp4";
  storage.putSized(raw, 10);
  uploaded(db, id, raw, "2026-08-01T00:00:00.000Z");
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "0" });
  const first = await pipeline.maintain();
  const deleteCount = storage.deleted.length;
  const second = await pipeline.maintain();
  assert.equal(first.archived_this_run, 1);
  assert.equal(second.archived_this_run, 0);
  assert.equal(storage.deleted.length, deleteCount);
}));

test("interrupted prune can resume after archive metadata and raw deletion", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "resume", 0.2);
  const raw = "raw/youtube/2026/09/16/resume.mp4";
  const active = "metadata/youtube/2026/09/16/resume.json";
  storage.putSized(raw, 10);
  storage.putSized(active, 2);
  uploaded(db, id, raw, "2026-08-01T00:00:00.000Z", 10, active);
  const originalDelete = storage.deleteObject.bind(storage);
  let failOnce = true;
  storage.deleteObject = async (key) => {
    if (key === active && failOnce) { failOnce = false; throw new Error("simulated interruption"); }
    await originalDelete(key);
  };
  const pipeline = makePipeline(directory, db, storage, { MAX_CLASSIC_VIDEOS: "0" });
  await pipeline.maintain();
  assert.equal(db.getUpload(id)?.library_tier, "hot");
  assert.ok(storage.objects.has("archive/metadata/youtube/2026/09/16/resume.json"));
  assert.equal(storage.objects.has(raw), false);
  await pipeline.maintain();
  assert.equal(db.getUpload(id)?.library_tier, "archived");
  assert.equal(storage.objects.has(active), false);
}));

test("stats reports snapshot, upload, daily download, and raw storage counts", async () => withDb(async (directory, db) => {
  const storage = new FakeStorage();
  const id = seed(db, "stats", 0.8);
  const raw = "raw/youtube/2026/09/16/stats.mp4";
  storage.putSized(raw, 12);
  uploaded(db, id, raw);
  db.beginDownload(id, "2026-09-16T01:00:00.000Z");
  db.completeDownload(id, "/tmp/stats.mp4", "stats-sha", 12, "2026-09-16T01:00:00.000Z");
  const result = await makePipeline(directory, db, storage).stats();
  assert.equal(result.videos_total, 1);
  assert.equal(result.snapshots_total, 2);
  assert.equal(result.uploaded_total, 1);
  assert.equal(result.downloads_today, 1);
  assert.equal(result.raw_object_count, 1);
  assert.equal(result.raw_bytes, 12);
  assert.equal(result.classic_limit, 400);
}));

test("stats and prune dry-run include zero-state observability fields", async () => withDb(async (directory, db) => {
  const pipeline = makePipeline(directory, db, new FakeStorage());
  const stats = await pipeline.stats();
  const prune = await pipeline.prune({ dryRun: true });
  const statsFields = [
    "hot_count", "classic_count", "archived_count", "active_count", "videos_total",
    "snapshots_total", "uploaded_total", "downloads_today", "raw_object_count", "raw_bytes",
    "raw_gb", "raw_storage_target_bytes", "soft_limit_gb", "classic_limit",
  ];
  const pruneFields = [
    "hot_count", "classic_count", "archived_count", "candidates_to_archive",
    "raw_objects_to_delete", "bytes_expected_to_free", "projected_raw_bytes_after_prune",
    "raw_storage_target_bytes",
  ];
  for (const field of statsFields) assert.equal(Object.prototype.hasOwnProperty.call(stats, field), true, field);
  for (const field of pruneFields) assert.equal(Object.prototype.hasOwnProperty.call(prune, field), true, field);
  assert.equal(stats.active_count, 0);
  assert.equal(stats.raw_storage_target_bytes, Math.floor(8 * 1024 ** 3 * 15 / 16));
  assert.equal(prune.hot_count, 0);
  assert.equal(prune.classic_count, 0);
  assert.equal(prune.archived_count, 0);
  assert.deepEqual(prune.candidates_to_archive, []);
  assert.deepEqual(prune.raw_objects_to_delete, []);
  assert.equal(prune.bytes_expected_to_free, 0);
  assert.equal(prune.projected_raw_bytes_after_prune, 0);
}));

test("existing database migration preserves old records and uploads", async () => {
  const directory = await mkdtemp(join(tmpdir(), "trend-worker-migration-"));
  const path = join(directory, "old.sqlite");
  const old = new DatabaseSync(path);
  old.exec(`CREATE TABLE videos (id INTEGER PRIMARY KEY, platform TEXT NOT NULL, video_id TEXT NOT NULL, url TEXT NOT NULL, creator TEXT, caption TEXT, published_at TEXT, duration REAL, discovered_at TEXT NOT NULL, viral_score REAL NOT NULL DEFAULT 0, score_updated_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(platform, video_id)); CREATE TABLE metric_snapshots (id INTEGER PRIMARY KEY, video_db_id INTEGER NOT NULL, captured_at TEXT NOT NULL, views REAL NOT NULL, likes REAL NOT NULL, comments REAL NOT NULL, shares REAL NOT NULL); CREATE TABLE download_jobs (id INTEGER PRIMARY KEY, video_db_id INTEGER NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, local_path TEXT, sha256 TEXT, file_size INTEGER, error TEXT, updated_at TEXT NOT NULL); CREATE TABLE uploads (id INTEGER PRIMARY KEY, video_db_id INTEGER NOT NULL, sha256 TEXT NOT NULL, r2_object_key TEXT NOT NULL, metadata_object_key TEXT, status TEXT NOT NULL, file_size INTEGER NOT NULL, updated_at TEXT NOT NULL); INSERT INTO videos VALUES (1, 'youtube', 'legacy', 'https://youtu.be/legacy', NULL, NULL, NULL, NULL, '${NOW}', 0.7, NULL, '${NOW}', '${NOW}'); INSERT INTO uploads (video_db_id, sha256, r2_object_key, metadata_object_key, status, file_size, updated_at) VALUES (1, 'legacy-sha', 'raw/youtube/2026/09/16/legacy.mp4', 'metadata/youtube/2026/09/16/legacy.json', 'uploaded', 12, '${NOW}');`);
  old.close();
  try {
    const db = new TrendDatabase(path);
    assert.equal(db.getVideo(1)?.peak_viral_score, 0.7);
    assert.equal(db.getUpload(1)?.library_tier, "hot");
    assert.equal(db.getUpload(1)?.storage_status, "active");
    db.close();
  } finally { await rm(directory, { recursive: true, force: true }); }
});
