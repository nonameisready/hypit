import assert from "node:assert/strict";
import test from "node:test";

import type { S3Client } from "@aws-sdk/client-s3";

import { loadConfig } from "../src/config.js";
import { R2Storage } from "../src/storage/r2.js";

test("R2 listObjects follows ListObjectsV2 pagination and returns sizes", async () => {
  const requests: (string | undefined)[] = [];
  const client = {
    async send(command: { readonly input: { readonly ContinuationToken?: string } }) {
      const token = command.input.ContinuationToken;
      requests.push(token);
      return token === undefined
        ? { Contents: [{ Key: "raw/youtube/a.mp4", Size: 10, LastModified: new Date("2026-09-16T00:00:00.000Z") }], IsTruncated: true, NextContinuationToken: "next" }
        : { Contents: [{ Key: "raw/youtube/b.mp4", Size: 20 }], IsTruncated: false };
    },
  } as unknown as S3Client;
  const storage = new R2Storage(loadConfig({ R2_ACCESS_KEY_ID: "test", R2_SECRET_ACCESS_KEY: "test", R2_BUCKET: "test", R2_ENDPOINT: "https://example.invalid" }), { maxAttempts: 1, baseDelayMs: 0 }, client);
  const objects = await storage.listObjects("raw/");
  assert.deepEqual(requests, [undefined, "next"]);
  assert.deepEqual(objects.map(({ key, size }) => ({ key, size })), [
    { key: "raw/youtube/a.mp4", size: 10 },
    { key: "raw/youtube/b.mp4", size: 20 },
  ]);
});
