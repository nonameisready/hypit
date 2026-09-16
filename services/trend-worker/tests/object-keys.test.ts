import assert from "node:assert/strict";
import test from "node:test";

import { metadataObjectKey, rawObjectKey } from "../src/storage/object-keys.js";

test("R2 object keys use UTC YYYY/MM/DD layout", () => {
  const date = "2026-09-16T23:00:00.000Z";
  assert.equal(rawObjectKey("tiktok", "123", date), "raw/tiktok/2026/09/16/123.mp4");
  assert.equal(metadataObjectKey("instagram", "abc", date), "metadata/instagram/2026/09/16/abc.json");
});

test("R2 object keys reject path traversal segments", () => {
  assert.throws(() => rawObjectKey("youtube", "../escape", "2026-09-16T00:00:00.000Z"), /segment/u);
});
