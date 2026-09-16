import type { Platform } from "../types.js";

function segment(value: string): string {
  const normalized = value.trim();
  if (normalized.length === 0 || normalized === "." || normalized === ".." || normalized.includes("/")) throw new Error("R2 object key segment is invalid");
  return normalized;
}

function dateParts(date: string | Date): readonly [string, string, string] {
  const value = date instanceof Date ? date : new Date(date);
  if (Number.isNaN(value.getTime())) throw new Error(`invalid object key date ${String(date)}`);
  return [String(value.getUTCFullYear()).padStart(4, "0"), String(value.getUTCMonth() + 1).padStart(2, "0"), String(value.getUTCDate()).padStart(2, "0")];
}

export function rawObjectKey(platform: Platform, videoId: string, date: string | Date): string {
  const [year, month, day] = dateParts(date);
  return `raw/${segment(platform)}/${year}/${month}/${day}/${segment(videoId)}.mp4`;
}

export function metadataObjectKey(platform: Platform, videoId: string, date: string | Date): string {
  const [year, month, day] = dateParts(date);
  return `metadata/${segment(platform)}/${year}/${month}/${day}/${segment(videoId)}.json`;
}
