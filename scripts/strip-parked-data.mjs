/* Removes parked sections' data from the published build.
 *
 * The JSON stays in public/ so `npm run dev:observatory` and `npm run dev:doge`
 * still work, but it must not ship. No live page reads any of it, it is several
 * megabytes of every deploy, and the 1,046 Observatory company dossiers name
 * company officers while the notice explaining the lawful basis for publishing
 * those names, /lancs/business/method/, is no longer published. Serving the data
 * without the notice is the wrong half to keep.
 *
 * Runs at postbuild, before scripts/guard-parked.mjs asserts the result.
 * See docs/parked-sections.md.
 */
import fs from 'node:fs';
import path from 'node:path';

const DATA = 'dist/data';

/* Matched as exact name or prefix. Keep in step with PARKED_DATA in the guard. */
const PARKED_DATA = ['company', 'biz-', 'burnley-spending-'];

if (!fs.existsSync(DATA)) process.exit(0);

let files = 0;
let bytes = 0;

const remove = (target) => {
  const stat = fs.statSync(target);
  if (stat.isDirectory()) {
    for (const entry of fs.readdirSync(target)) remove(path.join(target, entry));
    fs.rmdirSync(target);
    return;
  }
  files += 1;
  bytes += stat.size;
  fs.unlinkSync(target);
};

for (const entry of fs.readdirSync(DATA)) {
  if (PARKED_DATA.some((prefix) => entry === prefix || entry.startsWith(prefix))) {
    remove(path.join(DATA, entry));
  }
}

console.log(
  `Parked section data stripped from dist: ${files} file(s), ` +
    `${(bytes / 1024 / 1024).toFixed(1)} MB`
);
