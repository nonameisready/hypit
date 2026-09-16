import type { MetadataDocument, NormalizedVideo, ScoredVideo } from "../types.js";

export function makeMetadata(
  scored: ScoredVideo,
  r2ObjectKey: string,
  fileSize: number,
  sha256: string,
  ingestionStatus: MetadataDocument["ingestion_status"],
): MetadataDocument {
  return {
    source_url: scored.video.url,
    platform: scored.video.platform,
    video_id: scored.video.video_id,
    creator: scored.video.creator,
    caption: scored.video.caption,
    published_at: scored.video.published_at,
    discovered_at: scored.video.discovered_at,
    metrics: { ...scored.metrics, ...scored.features },
    viral_score: scored.viral_score,
    r2_object_key: r2ObjectKey,
    file_size: fileSize,
    sha256,
    ingestion_status: ingestionStatus,
  };
}

export function metadataBytes(document: MetadataDocument): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(document, null, 2)}\n`);
}

export function asScoredVideo(video: NormalizedVideo, metrics: ScoredVideo["metrics"], features: ScoredVideo["features"], viralScore: number): ScoredVideo {
  return { video, metrics, features, viral_score: viralScore };
}
