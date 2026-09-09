/* Removes the parked Observatory's data from the published build.
 *
 * The JSON stays in public/ so `npm run dev:observatory` still works, but it must
 * not ship: no live page references any of it, it is ~6.9 MB of every deploy, and
 * the 1,046 company dossiers name company officers. The notice explaining the
 * lawful basis for publishing those names, and the corrections route for anyone
 * named, both lived on /lancs/business/method/ — which is now unpublished. Serving
 * the data without the notice is the wrong half to keep.
 *
 * Runs at postbuild, before scripts/guard-parked.mjs asserts the result.
 */
import fs from 'node:fs';
import path from 'node:path';

const DATA = 'dist/data';
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
  if (entry === 'company' || entry.startsWith('biz-')) remove(path.join(DATA, entry));
}

console.log(
  `Parked Observatory data stripped from dist: ${files} file(s), ` +
    `${(bytes / 1024 / 1024).toFixed(1)} MB`
);
