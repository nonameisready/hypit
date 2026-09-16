import assert from "node:assert/strict";
import test from "node:test";

import { YouTubeProvider } from "../src/providers/youtube.js";

test("YouTube provider maps mocked public API responses into normalized videos and metrics", async () => {
  const calls: string[] = [];
  const provider = new YouTubeProvider("test-key", "shorts", "US");
  const context = {
    discoveredAt: "2026-09-16T00:00:00.000Z",
    logger: { info() {}, warn() {}, error() {} },
    waitForRateLimit: async () => {},
    fetch: (async (input: string | URL) => {
      const url = new URL(input);
      calls.push(url.pathname);
      if (url.pathname.endsWith("/search")) return Response.json({ items: [{ id: { videoId: "abc" } }] });
      return Response.json({ items: [{
        id: "abc",
        snippet: { channelTitle: "Creator", title: "A short", publishedAt: "2026-09-15T23:00:00.000Z" },
        contentDetails: { duration: "PT20S" },
        statistics: { viewCount: "1000", likeCount: "100", commentCount: "10" },
      }] });
    }) as typeof fetch,
  };
  const raw = await provider.fetchTrendingVideos(context);
  const video = provider.normalizeVideo(raw[0], context.discoveredAt);
  const metrics = await provider.getMetrics(video, raw[0], context);
  assert.deepEqual(calls, ["/youtube/v3/search", "/youtube/v3/videos"]);
  assert.equal(video.video_id, "abc");
  assert.equal(video.duration, 20);
  assert.deepEqual(metrics, { views: 1000, likes: 100, comments: 10, shares: 0 });
});
