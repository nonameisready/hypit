import type { ViralWeights } from "../config.js";
import type { MetricSnapshot, NormalizedVideo, ScoredVideo, ScoringFeatures, VideoMetrics } from "../types.js";

export type ScoringConfig = {
  readonly weights: ViralWeights;
  readonly freshnessHalfLifeHours: number;
};

export type ScoringCandidate = {
  readonly video: NormalizedVideo;
  readonly snapshots: readonly MetricSnapshot[];
};

function hoursBetween(start: string, end: string): number {
  return Math.max(1 / 3600, (Date.parse(end) - Date.parse(start)) / 3_600_000);
}

function metricDelta(current: number, previous: number | undefined): number {
  return previous === undefined ? 0 : Math.max(0, current - previous);
}

function rawFeatures(candidate: ScoringCandidate, now: string): { readonly metrics: VideoMetrics; readonly features: ScoringFeatures } {
  const snapshots = [...candidate.snapshots].sort((a, b) => Date.parse(a.captured_at) - Date.parse(b.captured_at));
  const current = snapshots.at(-1);
  if (current === undefined) {
    return { metrics: { views: 0, likes: 0, comments: 0, shares: 0 }, features: {
      view_velocity: 0, like_velocity: 0, comment_velocity: 0, share_velocity: 0, acceleration: 0, video_age_hours: ageHours(candidate.video, now),
    } };
  }
  const previous = snapshots.at(-2);
  const intervalHours = previous === undefined ? 0 : hoursBetween(previous.captured_at, current.captured_at);
  const velocity = (currentValue: number, previousValue: number | undefined): number => intervalHours === 0 ? 0 : metricDelta(currentValue, previousValue) / intervalHours;
  const currentVelocity = {
    view_velocity: velocity(current.views, previous?.views),
    like_velocity: velocity(current.likes, previous?.likes),
    comment_velocity: velocity(current.comments, previous?.comments),
    share_velocity: velocity(current.shares, previous?.shares),
  };
  const older = snapshots.at(-3);
  const olderHours = older === undefined || previous === undefined ? 0 : hoursBetween(older.captured_at, previous.captured_at);
  const previousViewVelocity = older === undefined || previous === undefined || olderHours === 0
    ? 0
    : metricDelta(previous.views, older.views) / olderHours;
  const acceleration = older === undefined || previous === undefined || olderHours === 0
    ? 0
    : Math.max(0, currentVelocity.view_velocity - previousViewVelocity);
  return {
    metrics: { views: current.views, likes: current.likes, comments: current.comments, shares: current.shares },
    features: {
      ...currentVelocity,
      acceleration,
      video_age_hours: ageHours(candidate.video, now),
    },
  };
}

function ageHours(video: NormalizedVideo, now: string): number {
  const reference = video.published_at ?? video.discovered_at;
  return Math.max(0, (Date.parse(now) - Date.parse(reference)) / 3_600_000);
}

function normalize(value: number, maximum: number): number {
  return maximum <= 0 ? 0 : Math.log1p(value) / Math.log1p(maximum);
}

export function scoreCandidates(candidates: readonly ScoringCandidate[], now: string, config: ScoringConfig): readonly ScoredVideo[] {
  const raw = candidates.map((candidate) => ({ candidate, ...rawFeatures(candidate, now) }));
  const maxima = {
    view_velocity: Math.max(...raw.map((item) => item.features.view_velocity), 0),
    like_velocity: Math.max(...raw.map((item) => item.features.like_velocity), 0),
    comment_velocity: Math.max(...raw.map((item) => item.features.comment_velocity), 0),
    share_velocity: Math.max(...raw.map((item) => item.features.share_velocity), 0),
    acceleration: Math.max(...raw.map((item) => item.features.acceleration), 0),
  } satisfies Omit<ScoringFeatures, "video_age_hours">;
  return raw.map(({ candidate, metrics, features }) => {
    const normalized = {
      view_velocity: normalize(features.view_velocity, maxima.view_velocity),
      like_velocity: normalize(features.like_velocity, maxima.like_velocity),
      comment_velocity: normalize(features.comment_velocity, maxima.comment_velocity),
      share_velocity: normalize(features.share_velocity, maxima.share_velocity),
      acceleration: normalize(features.acceleration, maxima.acceleration),
    };
    const weighted = config.weights.view_velocity * normalized.view_velocity
      + config.weights.share_velocity * normalized.share_velocity
      + config.weights.comment_velocity * normalized.comment_velocity
      + config.weights.like_velocity * normalized.like_velocity
      + config.weights.acceleration * normalized.acceleration;
    const freshness = Math.exp(-Math.log(2) * features.video_age_hours / config.freshnessHalfLifeHours);
    return { video: candidate.video, metrics, features, viral_score: weighted * freshness };
  }).sort((a, b) => b.viral_score - a.viral_score);
}
