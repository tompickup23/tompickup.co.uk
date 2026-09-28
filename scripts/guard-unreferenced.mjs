/* Fails the build if media ships that no published page uses.
 *
 * Everything in public/ is deployed, linked or not. On 28 September 2026 that
 * turned out to include 159 files from archived drafts (100 MB), among them a
 * video naming councillors beside attendance figures. They now live in
 * archive/public/. This check keeps the next orphan from going out: any file
 * under dist/videos, dist/images or dist/data must be referenced by something
 * else in dist/ (a page, feed, stylesheet or script).
 */
import fs from 'node:fs';
import path from 'node:path';

const DIST = 'dist';
const WATCHED = ['videos', 'images', 'data'];
/* Used by crawlers or browsers by convention rather than by a link. */
const ALLOW = new Set(['/images/logo-192.png', '/images/logo-512.png']);
const TEXT = /\.(html|xml|css|js|mjs|json|txt|webmanifest|svg)$/i;

function walk(dir, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full, out);
    else out.push(full);
  }
  return out;
}

const all = walk(DIST);
const corpus = all
  .filter((f) => TEXT.test(f) && fs.statSync(f).size < 20_000_000)
  .map((f) => fs.readFileSync(f, 'utf-8'))
  .join('\n');

const orphans = WATCHED.flatMap((dir) => walk(path.join(DIST, dir)))
  .map((f) => '/' + path.relative(DIST, f).split(path.sep).join('/'))
  .filter((url) => !ALLOW.has(url))
  .filter((url) => !corpus.includes(url) && !corpus.includes(encodeURI(url)));

if (orphans.length) {
  console.error('\n  Build blocked: files in dist/ that no page, feed or script uses:');
  for (const url of orphans) console.error(`    - ${url}`);
  console.error('\n  Link them from a page, or move them to archive/public/ (see archive/README.md).\n');
  process.exit(1);
}
