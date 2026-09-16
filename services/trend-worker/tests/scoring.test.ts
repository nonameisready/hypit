import assert from "node:assert/strict";
import test from "node:test";

import { scoreCandidates } from "../src/scoring/score.js";

const weights = { view_velocity: 0.35, share_velocity: 0.30, comment_velocity: 0.15, like_velocity: 0.10, acceleration: 0.10 } as const;
const video = (id: string, publishedAt = "2026-09-16T00:00:00.000Z") => ({
  platform: "youtube" as const, video_id: id, url: `https://youtu.be/${id}`, creator: null, caption: null, published_at: publishedAt, duration: 20, discovered_at: "2026-09-16T01:00:00.000Z",
});

test("viral scoring ranks metric velocity and applies freshness decay", () => {
  const now = "2026-09-16T03:00:00.000Z";
  const result = scoreCandidates([
    { video: video("fast"), snapshots: [
      { captured_at: "2026-09-16T01:00:00.000Z", views: 100, likes: 10, comments: 2, shares: 1 },
      { captured_at: "2026-09-16T03:00:00.000Z", views: 11_000, likes: 100, comments: 20, shares: 200 },
    ] },
    { video: video("old", "2026-09-10T00:00:00.000Z"), snapshots: [
      { captured_at: "2026-09-16T01:00:00.000Z", views: 100, likes: 10, comments: 2, shares: 1 },
      { captured_at: "2026-09-16T03:00:00.000Z", views: 11_000, likes: 100, comments: 20, shares: 200 },
    ] },
  ], now, { weights, freshnessHalfLifeHours: 48 });
  assert.equal(result[0]?.video.video_id, "fast");
  assert.ok((result[0]?.viral_score ?? 0) > (result[1]?.viral_score ?? 0));
  assert.ok((result[0]?.features.view_velocity ?? 0) > 0);
});
