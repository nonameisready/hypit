import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { loadConfig } from "../src/config.js";
import { TrendDatabase } from "../src/db/database.js";
import type { VideoDownloader } from "../src/downloader/hypit-downloader.js";
import { TrendPipeline } from "../src/pipeline/pipeline.js";
import type { ObjectStorage } from "../src/storage/r2.js";

test("pipeline continues after one download failure and uses mocked R2 storage", async () => {
  const directory = await mkdtemp(join(tmpdir(), "trend-worker-pipeline-"));
  const db = new TrendDatabase(join(directory, "worker.sqlite"));
  const uploaded: string[] = [];
  const downloader: VideoDownloader = {
    async download(url, target) {
      if (url.includes("fail")) throw new Error("mock download failure");
      await writeFile(target, "mock video bytes");
    },
  };
  const storage: ObjectStorage = {
    async putFile(key) {
      uploaded.push(`media:${key}`);
      return 16;
    },
    async putBytes(key) {
      uploaded.push(`metadata:${key}`);
      return 2;
    },
  };
  const config = loadConfig({
    ...process.env,
    DATA_DIR: directory,
    DATABASE_PATH: join(directory, "worker.sqlite"),
    TREND_PROVIDERS: "manual",
    MAX_VIDEO_AGE_HOURS: "72",
    DELETE_LOCAL_AFTER_UPLOAD: "false",
  }, { manualUrls: [
    "https://www.tiktok.com/@creator/video/123456789",
    "https://www.tiktok.com/@creator/video/987654321?fail=1",
  ] });
  const pipeline = new TrendPipeline({ db, config, downloader, storage, now: () => "2026-09-16T03:00:00.000Z" });
  try {
    assert.deepEqual(await pipeline.discover(), { discovered: 2, failed: 0 });
    pipeline.rank();
    assert.deepEqual(await pipeline.download(), { downloaded: 1, failed: 1 });
    assert.deepEqual(await pipeline.upload(), { uploaded: 1, failed: 0 });
    assert.equal(uploaded.length, 2);
  } finally {
    db.close();
    await rm(directory, { recursive: true, force: true });
  }
});
