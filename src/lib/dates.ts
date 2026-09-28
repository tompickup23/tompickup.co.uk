/* Publication dates for machine-readable output.

   Frontmatter can give a date with a time (2026-07-18T21:30:00) or a bare date
   (2025-10-06). A bare date parses as midnight UTC, and toISOString() would then
   claim the piece went up at 00:00, which is false. Archive-dated and
   retrospective pieces have no real time of day, so for those we say only the
   date: schema.org, Open Graph and sitemaps all accept YYYY-MM-DD. */
export function isDateOnly(date: Date): boolean {
  return (
    date.getUTCHours() === 0 &&
    date.getUTCMinutes() === 0 &&
    date.getUTCSeconds() === 0 &&
    date.getUTCMilliseconds() === 0
  );
}

export function isoDate(date: Date): string {
  return isDateOnly(date) ? date.toISOString().slice(0, 10) : date.toISOString();
}
