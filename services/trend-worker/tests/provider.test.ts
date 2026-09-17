import assert from "node:assert/strict";
import test from "node:test";

import { YouTubeProvider } from "../src/providers/youtube.js";

test("YouTube provider maps mocked public API responses into normalized videos and metrics", async () => {
  const calls: URL[] = [];
  const provider = new YouTubeProvider("test-key", "shorts", "US", 72);
  const context = {
    discoveredAt: "2026-09-16T00:00:00.000Z",
    logger: { info() {}, warn() {}, error() {} },
    waitForRateLimit: async () => {},
    fetch: (async (input: string | URL) => {
      const url = new URL(input);
      calls.push(url);
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
  assert.deepEqual(calls.map((url) => url.pathname), ["/youtube/v3/search", "/youtube/v3/videos"]);
  assert.equal(calls[0]?.searchParams.get("publishedAfter"), "2026-09-13T00:00:00.000Z");
  assert.equal(calls[0]?.searchParams.get("q"), "shorts");
  assert.equal(calls[0]?.searchParams.get("type"), "video");
  assert.equal(calls[0]?.searchParams.get("videoDuration"), "short");
  assert.equal(calls[0]?.searchParams.get("order"), "viewCount");
  assert.equal(calls[0]?.searchParams.get("maxResults"), "50");
  assert.equal(calls[0]?.searchParams.get("regionCode"), "US");
  assert.equal(video.video_id, "abc");
  assert.equal(video.duration, 20);
  assert.deepEqual(metrics, { views: 1000, likes: 100, comments: 10, shares: 0 });
});

test("YouTube discovery uses only the configured age window and omits a blank query", async () => {
  const calls: URL[] = [];
  const provider = new YouTubeProvider("test-key", "  ", "CA", 24);
  const context = {
    discoveredAt: "2026-09-16T12:30:00.000Z",
    logger: { info() {}, warn() {}, error() {} },
    waitForRateLimit: async () => {},
    fetch: (async (input: string | URL) => {
      calls.push(new URL(input));
      return Response.json({ items: [] });
    }) as typeof fetch,
  };
  await provider.fetchTrendingVideos(context);
  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.searchParams.get("publishedAfter"), "2026-09-15T12:30:00.000Z");
  assert.equal(calls[0]?.searchParams.has("q"), false);
});
