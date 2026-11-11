// I071. On a missing page, the closest pages that do exist. Pure functions,
// no dependencies: NotFound.astro runs them in the browser against a list of
// the site's pages, and the tests run them in Node.

const STOP = new Set(["the", "a", "an", "of", "and", "in", "on", "for", "to", "is", "at", "by", "with", "html", "index", "www"]);

/** Words in an address or a title, lower case, without filler. */
export function words(s) {
  return String(s).toLowerCase().normalize("NFKD").split(/[^a-z0-9]+/).filter((w) => w.length > 1 && !STOP.has(w));
}

/** Edit distance for a mistyped slug; two letters swapped count as one slip. */
export function distance(a, b) {
  const d = Array.from({ length: a.length + 1 }, (_, i) => [i, ...Array(b.length).fill(0)]);
  for (let j = 1; j <= b.length; j += 1) d[0][j] = j;
  for (let i = 1; i <= a.length; i += 1) {
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + 1);
    }
  }
  return d[a.length][b.length];
}

const lastSegment = (path) => String(path).split(/[?#]/)[0].split("/").filter(Boolean).pop() ?? "";

/**
 * The pages closest to an address that does not exist.
 * @param {string} path the address asked for
 * @param {{url: string, title: string}[]} pages the pages that exist
 * @param {number} [limit]
 * @returns {{url: string, title: string, score: number}[]} best first; empty when nothing is close
 */
export function suggest(path, pages, limit = 3) {
  const asked = words(path);
  const slug = lastSegment(path).toLowerCase();
  if (!asked.length && !slug) return [];
  const scored = pages.map((page) => {
    const theirs = new Set([...words(page.url), ...words(page.title)]);
    const shared = asked.filter((w) => theirs.has(w)).length;
    // Share of the asked-for words found, so a long address is not punished.
    let score = asked.length ? shared / asked.length : 0;
    // A mistyped slug: close in letters to this page's own slug.
    const theirSlug = lastSegment(page.url).toLowerCase();
    if (slug && theirSlug) {
      const far = distance(slug, theirSlug) / Math.max(slug.length, theirSlug.length);
      if (far <= 0.34) score = Math.max(score, 1 - far);
    }
    // The same section counts for a little.
    const section = String(path).split("/").filter(Boolean)[0];
    if (section && page.url.split("/").filter(Boolean)[0] === section) score += 0.1;
    return { url: page.url, title: page.title, score };
  });
  return scored.filter((p) => p.score >= 0.5).sort((a, b) => b.score - a.score || a.url.length - b.url.length).slice(0, limit);
}

/**
 * The first retired section an address falls under.
 * @param {string} path
 * @param {{prefix: string}[]} retired
 */
export function retiredFor(path, retired) {
  return retired.find((r) => path === r.prefix || path.startsWith(r.prefix.endsWith("/") ? r.prefix : `${r.prefix}/`)) ?? null;
}
