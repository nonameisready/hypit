import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { defaultEnvPath, loadWorkerEnv } from "../src/cli/main.js";
import { loadConfig } from "../src/config.js";

const key = "HYPIT_TREND_WORKER_ENV_TEST";

test("default env path is the service-local .env", () => {
  assert.equal(defaultEnvPath, fileURLToPath(new URL("../.env", import.meta.url)));
});

test("worker loads a local env file and preserves an existing shell value", async () => {
  const directory = await mkdtemp(join(tmpdir(), "trend-worker-env-"));
  const path = join(directory, ".env");
  const previous = process.env[key];
  try {
    await writeFile(path, `${key}=from-file\n`);
    delete process.env[key];
    loadWorkerEnv(path);
    assert.equal(process.env[key], "from-file");
    process.env[key] = "from-shell";
    loadWorkerEnv(path);
    assert.equal(process.env[key], "from-shell");
  } finally {
    if (previous === undefined) delete process.env[key];
    else process.env[key] = previous;
    await rm(directory, { recursive: true, force: true });
  }
});

test("missing local env file is optional", async () => {
  const directory = await mkdtemp(join(tmpdir(), "trend-worker-env-missing-"));
  try {
    assert.doesNotThrow(() => loadWorkerEnv(join(directory, ".env")));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("service env files remain ignored while the example remains visible", async () => {
  const gitignore = await readFile(new URL("../../../.gitignore", import.meta.url), "utf8");
  assert.match(gitignore, /^\.env$/mu);
  assert.match(gitignore, /^!\.env\.example$/mu);
});

test("library controls have conservative defaults and validate their bounds", () => {
  const config = loadConfig({});
  assert.equal(config.minSnapshotsForDownload, 2);
  assert.equal(config.maxNewDownloadsPerRun, 3);
  assert.equal(config.maxNewDownloadsPerDay, 30);
  assert.equal(config.hotRetentionDays, 14);
  assert.equal(config.r2SoftLimitGb, 8);
  assert.equal(config.maxClassicVideos, 400);
  for (const [name, value] of [
    ["MIN_SNAPSHOTS_FOR_DOWNLOAD", "0"],
    ["MAX_NEW_DOWNLOADS_PER_RUN", "0"],
    ["MAX_NEW_DOWNLOADS_PER_DAY", "0"],
    ["HOT_RETENTION_DAYS", "0"],
    ["R2_SOFT_LIMIT_GB", "0"],
    ["MAX_CLASSIC_VIDEOS", "-1"],
  ] as const) assert.throws(() => loadConfig({ [name]: value }), new RegExp(name));
});
