import assert from "node:assert/strict";
import test from "node:test";

import { makeMetadata, metadataBytes } from "../src/storage/metadata.js";

test("metadata generation preserves source, score, hash, and derived metrics", () => {
  const scored = {
    video: { platform: "youtube" as const, video_id: "abc", url: "https://youtube.com/shorts/abc", creator: "creator", caption: "caption", published_at: "2026-09-16T00:00:00.000Z", duration: 12, discovered_at: "2026-09-16T01:00:00.000Z" },
    metrics: { views: 100, likes: 10, comments: 5, shares: 3 },
    features: { view_velocity: 50, like_velocity: 5, comment_velocity: 2, share_velocity: 1, acceleration: 4, video_age_hours: 2 },
    viral_score: 0.75,
  };
  const document = makeMetadata(scored, "raw/youtube/2026/09/16/abc.mp4", 42, "deadbeef", "uploaded");
  assert.equal(document.r2_object_key, "raw/youtube/2026/09/16/abc.mp4");
  assert.equal(document.sha256, "deadbeef");
  assert.equal(document.metrics.share_velocity, 1);
  assert.match(new TextDecoder().decode(metadataBytes(document)), /"viral_score": 0\.75/u);
});
