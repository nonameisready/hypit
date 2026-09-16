export function parseDuration(value: string | undefined): number | null {
  if (value === undefined) return null;
  const match = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/u.exec(value);
  if (match === null) return null;
  return Number(match[1] ?? 0) * 3600 + Number(match[2] ?? 0) * 60 + Number(match[3] ?? 0);
}
