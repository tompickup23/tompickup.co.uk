// I060. The next page is already there. Builds the JSON for a
// <script type="speculationrules"> block: same-site pages the reader points
// at are fetched before the click.
//
// "prefetch" fetches the document only and runs none of its scripts, so an
// analytics beacon cannot count a page nobody opened. It is the default.
// "prerender" also renders the page in the background, which is faster still
// but runs its scripts: use it only where every script that counts a view
// waits for document.prerendering to end.
//
// Chromium prerenders and prefetches; Safari 26.2 prefetches; other browsers
// ignore the block.

/** Addresses never worth fetching ahead: downloads, feeds, linked images, anything with a query. */
export const DEFAULT_EXCLUDE = ["/*\\?*", "/*.pdf", "/*.xml", "/*.csv", "/*.json", "/*.zip", "/*.xlsx", "/*.ods", "/*.mp3", "/*.mp4", "/*.ics", "/*.txt", "/*.webp", "/*.png", "/*.jpg", "/*.jpeg", "/*.gif", "/*.avif", "/*.svg"];

/** Links never followed ahead, whatever their address. */
export const DEFAULT_SKIP = "[rel~=nofollow], [rel~=external], [download], [target=_blank], [data-no-speculate], [data-no-speculate] a";

/**
 * @param {object} [options]
 * @param {"prefetch"|"prerender"} [options.mode]
 * @param {"conservative"|"moderate"|"eager"} [options.eagerness] moderate acts on a 200 ms hover or on pointer down
 * @param {string[]} [options.include] address patterns to consider
 * @param {string[]} [options.exclude] address patterns added to DEFAULT_EXCLUDE
 * @param {string} [options.skip] selector for links to leave alone, replacing DEFAULT_SKIP
 */
export function speculationRules({ mode = "prefetch", eagerness = "moderate", include = ["/*"], exclude = [], skip = DEFAULT_SKIP } = {}) {
  if (mode !== "prefetch" && mode !== "prerender") throw new Error(`speculationRules: unknown mode ${mode}`);
  if (!["conservative", "moderate", "eager"].includes(eagerness)) throw new Error(`speculationRules: unknown eagerness ${eagerness}`);
  for (const p of [...include, ...exclude]) {
    if (!p.startsWith("/")) throw new Error(`speculationRules: ${p} is not a same-site path pattern`);
  }
  const where = {
    and: [
      { href_matches: include.length === 1 ? include[0] : include },
      { not: { href_matches: [...DEFAULT_EXCLUDE, ...exclude] } },
      { not: { selector_matches: skip } }
    ]
  };
  const rule = { where, eagerness };
  // A browser that cannot prerender still reads the prefetch list.
  return mode === "prerender" ? { prerender: [rule], prefetch: [rule] } : { prefetch: [rule] };
}
