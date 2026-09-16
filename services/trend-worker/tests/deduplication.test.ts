import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";

import { TrendDatabase } from "../src/db/database.js";

test("database deduplicates a video by platform and video id", async () => {
  const directory = await mkdtemp(join(tmpdir(), "trend-worker-dedupe-"));
  const db = new TrendDatabase(join(directory, "worker.sqlite"));
  try {
    const video = { platform: "tiktok" as const, video_id: "123", url: "https://www.tiktok.com/@creator/video/123", creator: "creator", caption: null, published_at: null, duration: null, discovered_at: "2026-09-16T00:00:00.000Z" };
    const first = db.upsertVideo(video, video.discovered_at);
    const second = db.upsertVideo({ ...video, caption: "updated" }, video.discovered_at);
    assert.equal(first, second);
    db.insertSnapshot(first, { captured_at: video.discovered_at, views: 1, likes: 2, comments: 3, shares: 4 });
    db.insertSnapshot(first, { captured_at: video.discovered_at, views: 9, likes: 8, comments: 7, shares: 6 });
    assert.equal(db.getSnapshots(first).length, 1);
    assert.equal(db.getSnapshots(first)[0]?.views, 9);
  } finally {
    db.close();
    await rm(directory, { recursive: true, force: true });
  }
});
