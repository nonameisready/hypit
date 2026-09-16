import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

import type { MetricSnapshot, NormalizedVideo, Platform, ScoredVideo, VideoMetrics } from "../types.js";

type Row = Record<string, unknown>;

export type StoredVideo = NormalizedVideo & {
  readonly db_id: number;
  readonly viral_score: number;
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
        updated_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS uploads_sha256 ON uploads(sha256, status);
    `);
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
      const snapshots = (this.#db.prepare("SELECT captured_at, views, likes, comments, shares FROM metric_snapshots WHERE video_db_id = ? ORDER BY captured_at ASC").all(dbId) as Row[]).map((item) => ({
        captured_at: requiredString(item, "captured_at"),
        views: numberValue(item, "views"), likes: numberValue(item, "likes"), comments: numberValue(item, "comments"), shares: numberValue(item, "shares"),
      }));
      return { video: this.parseVideo(row), snapshots };
    });
  }

  updateScore(videoDbId: number, score: number, now: string): void {
    this.#db.prepare("UPDATE videos SET viral_score = ?, score_updated_at = ?, updated_at = ? WHERE id = ?").run(score, now, now, videoDbId);
  }

  listTopVideos(topN: number, minScore: number | undefined): readonly StoredVideo[] {
    const rows = this.#db.prepare(`SELECT * FROM videos WHERE viral_score >= ? ORDER BY viral_score DESC, id ASC LIMIT ?`).all(minScore ?? -Infinity, topN) as Row[];
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

  listDownloadCandidates(topN: number, minScore: number | undefined): readonly StoredVideo[] {
    const rows = this.#db.prepare(`
      SELECT v.* FROM videos v JOIN download_jobs j ON j.video_db_id = v.id
      WHERE j.status = 'succeeded' AND NOT EXISTS (SELECT 1 FROM uploads u WHERE u.video_db_id = v.id AND u.status = 'uploaded')
      AND v.viral_score >= ? ORDER BY v.viral_score DESC, v.id ASC LIMIT ?
    `).all(minScore ?? -Infinity, topN) as Row[];
    return rows.map((row) => this.parseVideo(row));
  }

  getDownloadJob(videoDbId: number): DownloadJob | undefined {
    const row = this.#db.prepare("SELECT video_db_id, status, attempts, local_path, sha256, file_size, error FROM download_jobs WHERE video_db_id = ?").get(videoDbId) as Row | undefined;
    return row === undefined ? undefined : {
      video_db_id: numberValue(row, "video_db_id"), status: requiredString(row, "status"), attempts: numberValue(row, "attempts"), local_path: nullableString(row, "local_path"), sha256: nullableString(row, "sha256"), file_size: row.file_size === null ? null : numberValue(row, "file_size"), error: nullableString(row, "error"),
    };
  }

  getUpload(videoDbId: number): UploadRecord | undefined {
    const row = this.#db.prepare("SELECT video_db_id, sha256, r2_object_key, metadata_object_key, status, file_size FROM uploads WHERE video_db_id = ?").get(videoDbId) as Row | undefined;
    return row === undefined ? undefined : this.parseUpload(row);
  }

  findUploadedBySha256(sha256: string): UploadRecord | undefined {
    const row = this.#db.prepare("SELECT video_db_id, sha256, r2_object_key, metadata_object_key, status, file_size FROM uploads WHERE sha256 = ? AND status = 'uploaded' ORDER BY id ASC LIMIT 1").get(sha256) as Row | undefined;
    return row === undefined ? undefined : this.parseUpload(row);
  }

  beginUpload(videoDbId: number, sha256: string, key: string, fileSize: number, now: string): void {
    this.#db.prepare(`
      INSERT INTO uploads (video_db_id, sha256, r2_object_key, status, file_size, updated_at) VALUES (?, ?, ?, 'uploading', ?, ?)
      ON CONFLICT(video_db_id) DO UPDATE SET sha256 = excluded.sha256, r2_object_key = excluded.r2_object_key, status = 'uploading', file_size = excluded.file_size, error = NULL, updated_at = excluded.updated_at
    `).run(videoDbId, sha256, key, fileSize, now);
  }

  completeUpload(videoDbId: number, metadataKey: string, now: string): void {
    this.#db.prepare("UPDATE uploads SET status = 'uploaded', metadata_object_key = ?, uploaded_at = ?, updated_at = ? WHERE video_db_id = ?").run(metadataKey, now, now, videoDbId);
  }

  failUpload(videoDbId: number, error: string, now: string): void {
    this.#db.prepare("UPDATE uploads SET status = 'failed', error = ?, updated_at = ? WHERE video_db_id = ?").run(error, now, videoDbId);
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
    };
  }

  private parseUpload(row: Row): UploadRecord {
    return {
      video_db_id: numberValue(row, "video_db_id"), sha256: requiredString(row, "sha256"), r2_object_key: requiredString(row, "r2_object_key"), metadata_object_key: nullableString(row, "metadata_object_key"), status: requiredString(row, "status"), file_size: numberValue(row, "file_size"),
    };
  }
}
