export type Platform = "tiktok" | "instagram" | "youtube";

export type VideoMetrics = {
  readonly views: number;
  readonly likes: number;
  readonly comments: number;
  readonly shares: number;
};

export type NormalizedVideo = {
  readonly platform: Platform;
  readonly video_id: string;
  readonly url: string;
  readonly creator: string | null;
  readonly caption: string | null;
  readonly published_at: string | null;
  readonly duration: number | null;
  readonly discovered_at: string;
};

export type MetricSnapshot = VideoMetrics & {
  readonly captured_at: string;
};

export type ScoringFeatures = {
  readonly view_velocity: number;
  readonly like_velocity: number;
  readonly comment_velocity: number;
  readonly share_velocity: number;
  readonly acceleration: number;
  readonly video_age_hours: number;
};

export type ScoredVideo = {
  readonly video: NormalizedVideo;
  readonly metrics: VideoMetrics;
  readonly features: ScoringFeatures;
  readonly viral_score: number;
};

export type IngestionStatus = "discovered" | "ranked" | "downloaded" | "uploaded" | "failed";

export type MetadataDocument = {
  readonly source_url: string;
  readonly platform: Platform;
  readonly video_id: string;
  readonly creator: string | null;
  readonly caption: string | null;
  readonly published_at: string | null;
  readonly discovered_at: string;
  readonly metrics: VideoMetrics & ScoringFeatures;
  readonly viral_score: number;
  readonly r2_object_key: string;
  readonly file_size: number;
  readonly sha256: string;
  readonly ingestion_status: IngestionStatus;
};

export type Logger = {
  info(event: string, fields?: Record<string, unknown>): void;
  warn(event: string, fields?: Record<string, unknown>): void;
  error(event: string, fields?: Record<string, unknown>): void;
};
