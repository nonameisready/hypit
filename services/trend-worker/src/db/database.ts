import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

import type { LibraryTier, MetricSnapshot, NormalizedVideo, Platform, VideoMetrics } from "../types.js";

type Row = Record<string, unknown>;

export type StoredVideo = NormalizedVideo & {
  readonly db_id: number;
  readonly viral_score: number;
  readonly peak_viral_score: number;
  readonly peak_score_at: string | null;
};

export type DownloadJob = {
  readonly video_db_id: number;
  readonly status: string;
  readonly attempts: number;
  readonly local_path: string | null;
  readonly sha256: string | null;
  readonly file_size: number | null;
  readonly error: string | null;
};

export type UploadRecord = {
  readonly video_db_id: number;
  readonly sha256: string;
  readonly r2_object_key: string;
  readonly metadata_object_key: string | null;
  readonly status: string;
  readonly file_size: number;
  readonly library_tier: LibraryTier;
  readonly storage_status: "active" | "archived";
  readonly archived_at: string | null;
  readonly archive_reason: string | null;
  readonly archive_metadata_object_key: string | null;
  readonly uploaded_at: string | null;
  readonly metadata_refreshed_at: string | null;
};

export type LibraryRecord = {
  readonly video: StoredVideo;
  readonly upload: UploadRecord;
  readonly snapshotCount: number;
  readonly latestMetrics: VideoMetrics | null;
};

export type DatabaseStats = {
  readonly totalDiscoveredVideos: number;
  readonly activeUploadedVideos: number;
  readonly hotVideos: number;
  readonly classicVideos: number;
  readonly archivedVideos: number;
  readonly snapshotsTotal: number;
  readonly uploadedTotal: number;
  readonly downloadsToday: number;
};

export type RawReference = {
  readonly r2_object_key: string;
  readonly status: string;
  readonly storage_status: string | null;
};

function requiredString(row: Row, name: string): string {
  if (typeof row[name] !== "string") throw new Error(`SQLite row has no ${name}`);
  return row[name] as string;
}

function nullableString(row: Row, name: string): string | null {
  return typeof row[name] === "string" ? row[name] as string : null;
}

function numberValue(row: Row, name: string, fallback = 0): number {
  return typeof row[name] === "number" ? row[name] as number : fallback;
}

function integerValue(row: Row, name: string, fallback = 0): number {
  return typeof row[name] === "number" ? Math.trunc(row[name] as number) : fallback;
}

export class TrendDatabase {
  readonly #db: DatabaseSync;

  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true });
    this.#db = new DatabaseSync(path);
    this.#db.exec(`
      PRAGMA busy_timeout = 5000;
      PRAGMA foreign_keys = ON;
      CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY,
        platform TEXT NOT NULL,
        video_id TEXT NOT NULL,
        url TEXT NOT NULL,
        creator TEXT,
        caption TEXT,
        published_at TEXT,
        duration REAL,
        discovered_at TEXT NOT NULL,
        viral_score REAL NOT NULL DEFAULT 0,
        peak_viral_score REAL NOT NULL DEFAULT 0,
        peak_score_at TEXT,
        score_updated_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(platform, video_id)
      );
      CREATE TABLE IF NOT EXISTS metric_snapshots (
        id INTEGER PRIMARY KEY,
        video_db_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
        captured_at TEXT NOT NULL,
        views REAL NOT NULL,
        likes REAL NOT NULL,
        comments REAL NOT NULL,
        shares REAL NOT NULL,
        UNIQUE(video_db_id, captured_at)
      );
      CREATE INDEX IF NOT EXISTS metric_snapshots_video_time ON metric_snapshots(video_db_id, captured_at);
      CREATE TABLE IF NOT EXISTS download_jobs (
        id INTEGER PRIMARY KEY,
        video_db_id INTEGER NOT NULL UNIQUE REFERENCES videos(id) ON DELETE CASCADE,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        local_path TEXT,
        sha256 TEXT,
        file_size INTEGER,
        error TEXT,
        started_at TEXT,
        completed_at TEXT,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS uploads (
        id INTEGER PRIMARY KEY,
        video_db_id INTEGER NOT NULL UNIQUE REFERENCES videos(id) ON DELETE CASCADE,
        sha256 TEXT NOT NULL,
        r2_object_key TEXT NOT NULL,
        metadata_object_key TEXT,
        status TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        error TEXT,
        uploaded_at TEXT,
        updated_at TEXT NOT NULL,
        library_tier TEXT NOT NULL DEFAULT 'hot',
        storage_status TEXT NOT NULL DEFAULT 'active',
        archived_at TEXT,
        archive_reason TEXT,
        archive_metadata_object_key TEXT,
        metadata_refreshed_at TEXT
      );
      CREATE INDEX IF NOT EXISTS uploads_sha256 ON uploads(sha256, status);
    `);
    this.migrateExistingSchema();
  }

  close(): void { this.#db.close(); }

  upsertVideo(video: NormalizedVideo, now: string): number {
    this.#db.prepare(`
      INSERT INTO videos (platform, video_id, url, creator, caption, published_at, duration, discovered_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(platform, video_id) DO UPDATE SET
        url = excluded.url,
        creator = COALESCE(excluded.creator, videos.creator),
        caption = COALESCE(excluded.caption, videos.caption),
        published_at = COALESCE(excluded.published_at, videos.published_at),
        duration = COALESCE(excluded.duration, videos.duration),
        updated_at = excluded.updated_at
    `).run(video.platform, video.video_id, video.url, video.creator, video.caption, video.published_at, video.duration, video.discovered_at, now, now);
    const row = this.#db.prepare("SELECT id FROM videos WHERE platform = ? AND video_id = ?").get(video.platform, video.video_id) as Row;
    return numberValue(row, "id");
  }

  insertSnapshot(videoDbId: number, snapshot: MetricSnapshot): void {
    this.#db.prepare(`
      INSERT INTO metric_snapshots (video_db_id, captured_at, views, likes, comments, shares)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(video_db_id, captured_at) DO UPDATE SET views = excluded.views, likes = excluded.likes, comments = excluded.comments, shares = excluded.shares
    `).run(videoDbId, snapshot.captured_at, snapshot.views, snapshot.likes, snapshot.comments, snapshot.shares);
  }

  listScoringCandidates(maxAgeHours: number, now: string): readonly { readonly video: StoredVideo; readonly snapshots: readonly MetricSnapshot[] }[] {
    const cutoff = new Date(Date.parse(now) - maxAgeHours * 3_600_000).toISOString();
    const rows = this.#db.prepare("SELECT * FROM videos WHERE COALESCE(published_at, discovered_at) >= ? ORDER BY id ASC").all(cutoff) as Row[];
    return rows.map((row) => {
      const dbId = numberValue(row, "id");
      return { video: this.parseVideo(row), snapshots: this.getSnapshots(dbId) };
    });
  }

  updateScore(videoDbId: number, score: number, now: string): void {
    this.#db.prepare(`
      UPDATE videos SET
        viral_score = ?,
        peak_viral_score = CASE WHEN ? > peak_viral_score THEN ? ELSE peak_viral_score END,
        peak_score_at = CASE WHEN ? > peak_viral_score THEN ? ELSE peak_score_at END,
        score_updated_at = ?,
        updated_at = ?
      WHERE id = ?
    `).run(score, score, score, score, now, now, now, videoDbId);
  }

  getVideo(videoDbId: number): StoredVideo | undefined {
    const row = this.#db.prepare("SELECT * FROM videos WHERE id = ?").get(videoDbId) as Row | undefined;
    return row === undefined ? undefined : this.parseVideo(row);
  }

  listTopVideos(topN: number, minScore: number | undefined): readonly StoredVideo[] {
    const rows = this.#db.prepare("SELECT * FROM videos WHERE viral_score >= ? ORDER BY viral_score DESC, id ASC LIMIT ?").all(minScore ?? -Infinity, topN) as Row[];
    return rows.map((row) => this.parseVideo(row));
  }

  getSnapshots(videoDbId: number): readonly MetricSnapshot[] {
    return (this.#db.prepare("SELECT captured_at, views, likes, comments, shares FROM metric_snapshots WHERE video_db_id = ? ORDER BY captured_at ASC").all(videoDbId) as Row[]).map((row) => ({
      captured_at: requiredString(row, "captured_at"), views: numberValue(row, "views"), likes: numberValue(row, "likes"), comments: numberValue(row, "comments"), shares: numberValue(row, "shares"),
    }));
  }

  beginDownload(videoDbId: number, now: string): void {
    this.#db.prepare(`
      INSERT INTO download_jobs (video_db_id, status, attempts, started_at, updated_at) VALUES (?, 'running', 1, ?, ?)
      ON CONFLICT(video_db_id) DO UPDATE SET status = 'running', attempts = download_jobs.attempts + 1, error = NULL, started_at = ?, updated_at = ?
    `).run(videoDbId, now, now, now, now);
  }

  completeDownload(videoDbId: number, path: string, sha256: string, fileSize: number, now: string): void {
    this.#db.prepare("UPDATE download_jobs SET status = 'succeeded', local_path = ?, sha256 = ?, file_size = ?, completed_at = ?, updated_at = ? WHERE video_db_id = ?").run(path, sha256, fileSize, now, now, videoDbId);
  }

  failDownload(videoDbId: number, error: string, now: string): void {
    this.#db.prepare("UPDATE download_jobs SET status = 'failed', error = ?, updated_at = ? WHERE video_db_id = ?").run(error, now, videoDbId);
  }

  listDownloadCandidates(topN: number, minScore: number | undefined, minSnapshots = 1, maxAgeHours = Number.POSITIVE_INFINITY, now = new Date().toISOString()): readonly StoredVideo[] {
    const cutoff = Number.isFinite(maxAgeHours) ? new Date(Date.parse(now) - maxAgeHours * 3_600_000).toISOString() : "0000-01-01T00:00:00.000Z";
    const rows = this.#db.prepare(`
      SELECT v.* FROM videos v
      WHERE COALESCE(v.published_at, v.discovered_at) >= ?
        AND v.viral_score >= ?
        AND (SELECT COUNT(*) FROM metric_snapshots s WHERE s.video_db_id = v.id) >= ?
        AND NOT EXISTS (SELECT 1 FROM uploads u WHERE u.video_db_id = v.id AND u.status = 'uploaded')
        AND NOT EXISTS (SELECT 1 FROM download_jobs j WHERE j.video_db_id = v.id AND j.status = 'succeeded')
      ORDER BY v.viral_score DESC, v.id ASC
      LIMIT ?
    `).all(cutoff, minScore ?? -Infinity, minSnapshots, topN) as Row[];
    return rows.map((row) => this.parseVideo(row));
  }

  listPendingUploads(): readonly StoredVideo[] {
    const rows = this.#db.prepare(`
      SELECT v.* FROM videos v
      JOIN download_jobs j ON j.video_db_id = v.id AND j.status = 'succeeded'
      WHERE NOT EXISTS (SELECT 1 FROM uploads u WHERE u.video_db_id = v.id AND u.status = 'uploaded')
      ORDER BY v.viral_score DESC, v.id ASC
    `).all() as Row[];
    return rows.map((row) => this.parseVideo(row));
  }

  countSuccessfulUploadsSince(since: string): number {
    const row = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded' AND uploaded_at >= ?").get(since) as Row;
    return integerValue(row, "count");
  }

  countSuccessfulDownloadsSince(since: string): number {
    const row = this.#db.prepare(`
      SELECT COUNT(DISTINCT v.id) AS count
      FROM videos v
      LEFT JOIN download_jobs j ON j.video_db_id = v.id
      LEFT JOIN uploads u ON u.video_db_id = v.id
      WHERE (j.status = 'succeeded' AND j.completed_at >= ?)
         OR (u.status = 'uploaded' AND u.uploaded_at >= ?)
    `).get(since, since) as Row;
    return integerValue(row, "count");
  }

  getDownloadJob(videoDbId: number): DownloadJob | undefined {
    const row = this.#db.prepare("SELECT video_db_id, status, attempts, local_path, sha256, file_size, error FROM download_jobs WHERE video_db_id = ?").get(videoDbId) as Row | undefined;
    return row === undefined ? undefined : {
      video_db_id: numberValue(row, "video_db_id"), status: requiredString(row, "status"), attempts: numberValue(row, "attempts"), local_path: nullableString(row, "local_path"), sha256: nullableString(row, "sha256"), file_size: row.file_size === null ? null : numberValue(row, "file_size"), error: nullableString(row, "error"),
    };
  }

  getUpload(videoDbId: number): UploadRecord | undefined {
    const row = this.#db.prepare("SELECT * FROM uploads WHERE video_db_id = ?").get(videoDbId) as Row | undefined;
    return row === undefined ? undefined : this.parseUpload(row);
  }

  findUploadedBySha256(sha256: string): UploadRecord | undefined {
    const row = this.#db.prepare("SELECT * FROM uploads WHERE sha256 = ? AND status = 'uploaded' ORDER BY id ASC LIMIT 1").get(sha256) as Row | undefined;
    return row === undefined ? undefined : this.parseUpload(row);
  }

  beginUpload(videoDbId: number, sha256: string, key: string, fileSize: number, now: string): void {
    this.#db.prepare(`
      INSERT INTO uploads (video_db_id, sha256, r2_object_key, status, file_size, updated_at, library_tier, storage_status) VALUES (?, ?, ?, 'uploading', ?, ?, 'hot', 'active')
      ON CONFLICT(video_db_id) DO UPDATE SET sha256 = excluded.sha256, r2_object_key = excluded.r2_object_key, status = 'uploading', file_size = excluded.file_size, error = NULL, updated_at = excluded.updated_at
    `).run(videoDbId, sha256, key, fileSize, now);
  }

  completeUpload(videoDbId: number, metadataKey: string, now: string): void {
    this.#db.prepare("UPDATE uploads SET status = 'uploaded', metadata_object_key = ?, uploaded_at = COALESCE(uploaded_at, ?), metadata_refreshed_at = ?, updated_at = ?, library_tier = 'hot', storage_status = 'active', archived_at = NULL, archive_reason = NULL, archive_metadata_object_key = NULL WHERE video_db_id = ?").run(metadataKey, now, now, now, videoDbId);
  }

  markMetadataRefreshed(videoDbId: number, now: string): void {
    this.#db.prepare("UPDATE uploads SET metadata_refreshed_at = ?, updated_at = ? WHERE video_db_id = ? AND status = 'uploaded'").run(now, now, videoDbId);
  }

  failUpload(videoDbId: number, error: string, now: string): void {
    this.#db.prepare("UPDATE uploads SET status = 'failed', error = ?, updated_at = ? WHERE video_db_id = ?").run(error, now, videoDbId);
  }

  listUploadedRecords(): readonly LibraryRecord[] {
    const rows = this.#db.prepare(`
      SELECT v.*, u.sha256 AS upload_sha256, u.r2_object_key AS upload_r2_object_key,
        u.metadata_object_key AS upload_metadata_object_key, u.status AS upload_status,
        u.file_size AS upload_file_size, u.library_tier AS upload_library_tier,
        u.storage_status AS upload_storage_status, u.archived_at AS upload_archived_at,
        u.archive_reason AS upload_archive_reason, u.archive_metadata_object_key AS upload_archive_metadata_object_key,
        u.uploaded_at AS upload_uploaded_at, u.metadata_refreshed_at AS upload_metadata_refreshed_at,
        (SELECT COUNT(*) FROM metric_snapshots s WHERE s.video_db_id = v.id) AS snapshot_count,
        (SELECT s.views FROM metric_snapshots s WHERE s.video_db_id = v.id ORDER BY s.captured_at DESC LIMIT 1) AS latest_views,
        (SELECT s.likes FROM metric_snapshots s WHERE s.video_db_id = v.id ORDER BY s.captured_at DESC LIMIT 1) AS latest_likes,
        (SELECT s.comments FROM metric_snapshots s WHERE s.video_db_id = v.id ORDER BY s.captured_at DESC LIMIT 1) AS latest_comments,
        (SELECT s.shares FROM metric_snapshots s WHERE s.video_db_id = v.id ORDER BY s.captured_at DESC LIMIT 1) AS latest_shares
      FROM videos v JOIN uploads u ON u.video_db_id = v.id
      WHERE u.status = 'uploaded'
      ORDER BY v.id ASC
    `).all() as Row[];
    return rows.map((row) => ({
      video: this.parseVideo(row),
      upload: this.parseUpload(row, "upload_"),
      snapshotCount: integerValue(row, "snapshot_count"),
      latestMetrics: row.latest_views === null || row.latest_views === undefined ? null : {
        views: numberValue(row, "latest_views"), likes: numberValue(row, "latest_likes"), comments: numberValue(row, "latest_comments"), shares: numberValue(row, "latest_shares"),
      },
    }));
  }

  listActiveUploadedRecords(): readonly LibraryRecord[] {
    return this.listUploadedRecords().filter(({ upload }) => upload.storage_status === "active");
  }

  setLibraryTier(videoDbId: number, tier: Exclude<LibraryTier, "archived">, now: string): void {
    this.#db.prepare("UPDATE uploads SET library_tier = ?, storage_status = 'active', updated_at = ? WHERE video_db_id = ? AND status = 'uploaded'").run(tier, now, videoDbId);
  }

  markArchived(videoDbId: number, archiveKey: string, reason: string, now: string): void {
    this.#db.prepare("UPDATE uploads SET library_tier = 'archived', storage_status = 'archived', archived_at = ?, archive_reason = ?, archive_metadata_object_key = ?, updated_at = ? WHERE video_db_id = ? AND status = 'uploaded'").run(now, reason, archiveKey, now, videoDbId);
  }

  countActiveRawReferences(key: string, excludingVideoDbId: number): number {
    const row = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded' AND storage_status = 'active' AND r2_object_key = ? AND video_db_id != ?").get(key, excludingVideoDbId) as Row;
    return integerValue(row, "count");
  }

  listRawReferences(): readonly RawReference[] {
    return (this.#db.prepare("SELECT r2_object_key, status, storage_status FROM uploads").all() as Row[]).map((row) => ({
      r2_object_key: requiredString(row, "r2_object_key"),
      status: requiredString(row, "status"),
      storage_status: nullableString(row, "storage_status"),
    }));
  }

  stats(since?: string): DatabaseStats {
    const total = this.#db.prepare("SELECT COUNT(*) AS count FROM videos").get() as Row;
    const active = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded' AND storage_status = 'active'").get() as Row;
    const hot = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded' AND library_tier = 'hot'").get() as Row;
    const classic = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded' AND library_tier = 'classic'").get() as Row;
    const archived = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded' AND library_tier = 'archived'").get() as Row;
    const snapshots = this.#db.prepare("SELECT COUNT(*) AS count FROM metric_snapshots").get() as Row;
    const uploaded = this.#db.prepare("SELECT COUNT(*) AS count FROM uploads WHERE status = 'uploaded'").get() as Row;
    const downloadsToday = since === undefined ? 0 : this.countSuccessfulDownloadsSince(since);
    return { totalDiscoveredVideos: integerValue(total, "count"), activeUploadedVideos: integerValue(active, "count"), hotVideos: integerValue(hot, "count"), classicVideos: integerValue(classic, "count"), archivedVideos: integerValue(archived, "count"), snapshotsTotal: integerValue(snapshots, "count"), uploadedTotal: integerValue(uploaded, "count"), downloadsToday };
  }

  private migrateExistingSchema(): void {
    this.addColumnIfMissing("videos", "peak_viral_score REAL NOT NULL DEFAULT 0");
    this.addColumnIfMissing("videos", "peak_score_at TEXT");
    this.addColumnIfMissing("download_jobs", "started_at TEXT");
    this.addColumnIfMissing("download_jobs", "completed_at TEXT");
    this.addColumnIfMissing("uploads", "error TEXT");
    this.addColumnIfMissing("uploads", "library_tier TEXT NOT NULL DEFAULT 'hot'");
    this.addColumnIfMissing("uploads", "storage_status TEXT NOT NULL DEFAULT 'active'");
    this.addColumnIfMissing("uploads", "archived_at TEXT");
    this.addColumnIfMissing("uploads", "archive_reason TEXT");
    this.addColumnIfMissing("uploads", "archive_metadata_object_key TEXT");
    this.addColumnIfMissing("uploads", "metadata_refreshed_at TEXT");
    this.#db.exec("UPDATE uploads SET library_tier = 'hot' WHERE library_tier IS NULL OR library_tier = '';");
    this.#db.exec("UPDATE uploads SET storage_status = 'active' WHERE storage_status IS NULL OR storage_status = '';");
    this.#db.exec("UPDATE videos SET peak_viral_score = viral_score WHERE peak_viral_score < viral_score;");
  }

  private addColumnIfMissing(table: string, definition: string): void {
    const column = definition.split(" ", 1)[0]!;
    const columns = this.#db.prepare(`PRAGMA table_info(${table})`).all() as Row[];
    if (!columns.some((row) => row.name === column)) this.#db.exec(`ALTER TABLE ${table} ADD COLUMN ${definition}`);
  }

  private parseVideo(row: Row): StoredVideo {
    return {
      db_id: numberValue(row, "id"),
      platform: requiredString(row, "platform") as Platform,
      video_id: requiredString(row, "video_id"),
      url: requiredString(row, "url"),
      creator: nullableString(row, "creator"),
      caption: nullableString(row, "caption"),
      published_at: nullableString(row, "published_at"),
      duration: row.duration === null ? null : numberValue(row, "duration"),
      discovered_at: requiredString(row, "discovered_at"),
      viral_score: numberValue(row, "viral_score"),
      peak_viral_score: numberValue(row, "peak_viral_score", numberValue(row, "viral_score")),
      peak_score_at: nullableString(row, "peak_score_at"),
    };
  }

  private parseUpload(row: Row, prefix = ""): UploadRecord {
    const name = (column: string): string => prefix === "" ? column : `${prefix}${column}`;
    return {
      video_db_id: numberValue(row, prefix === "" ? "video_db_id" : "id"),
      sha256: requiredString(row, name(prefix === "" ? "sha256" : "sha256")),
      r2_object_key: requiredString(row, name(prefix === "" ? "r2_object_key" : "r2_object_key")),
      metadata_object_key: nullableString(row, name(prefix === "" ? "metadata_object_key" : "metadata_object_key")),
      status: requiredString(row, name(prefix === "" ? "status" : "status")),
      file_size: numberValue(row, name(prefix === "" ? "file_size" : "file_size")),
      library_tier: (nullableString(row, name("library_tier")) ?? "hot") as LibraryTier,
      storage_status: (nullableString(row, name("storage_status")) ?? "active") as "active" | "archived",
      archived_at: nullableString(row, name("archived_at")),
      archive_reason: nullableString(row, name("archive_reason")),
      archive_metadata_object_key: nullableString(row, name("archive_metadata_object_key")),
      uploaded_at: nullableString(row, name("uploaded_at")),
      metadata_refreshed_at: nullableString(row, name("metadata_refreshed_at")),
    };
  }
}
