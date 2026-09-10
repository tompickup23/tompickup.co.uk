/* Guards that parked sections do not reach production.
 *
 * Two sections are parked, for different reasons, both recorded in
 * docs/parked-sections.md:
 *
 *   _lancs  The Lancashire Business Observatory. 1,074 of the site's 1,112 pages,
 *           1,046 of them near-identical Companies House profiles, which is the
 *           shape Google treats as scaled thin content on a domain that carries a
 *           councillor's name and journalism.
 *   _doge   The Burnley spending explorer. Retired, not deleted: the same payments
 *           are on aidoge.co.uk/councils/burnley/, checked by hand against the
 *           council's own files, so a second copy here only split the record.
 *
 * `pre`  runs before a build: refuses to build while a section is in the routed
 *        tree, which is how scripts/parked-dev.sh leaves it mid-session.
 * `post` runs after: checks the actual output, so a future change that puts these
 *        URLs back some other way still fails the build rather than shipping.
 */
import fs from 'node:fs';
import path from 'node:path';

const mode = process.argv[2] ?? 'pre';
const DIST = 'dist';

/* Routed name -> parked name. Astro does not route a path segment starting `_`. */
const PARKED = [
  { live: 'src/pages/lancs', parked: 'src/pages/_lancs', out: 'lancs', dev: 'observatory' },
  { live: 'src/pages/doge', parked: 'src/pages/_doge', out: 'doge', dev: 'doge' },
];

/* Data that only the parked pages read. Prefix match inside dist/data. */
const PARKED_DATA = ['company', 'biz-', 'burnley-spending-'];

function fail(lines) {
  console.error('');
  for (const line of lines) console.error(`  ${line}`);
  console.error('');
  process.exit(1);
}

if (mode === 'pre') {
  const live = PARKED.filter((s) => fs.existsSync(s.live));
  if (live.length) {
    fail([
      'Build blocked: a parked section is in the routed tree.',
      ...live.map((s) => `  - ${s.live}/`),
      '',
      'Parked sections must not be published. Move them back:',
      ...live.map((s) => `  git mv ${s.live} ${s.parked}`),
      '',
      'To view one locally instead:',
      ...live.map((s) => `  npm run dev:${s.dev}`),
      '',
      'Why each is parked: docs/parked-sections.md',
    ]);
  }
} else {
  if (!fs.existsSync(DIST)) fail([`Build check failed: ${DIST}/ not found.`]);

  const offenders = [];

  for (const section of PARKED) {
    if (fs.existsSync(path.join(DIST, section.out))) {
      offenders.push(`${DIST}/${section.out}/ was generated`);
    }
  }

  const data = path.join(DIST, 'data');
  if (fs.existsSync(data)) {
    for (const entry of fs.readdirSync(data)) {
      if (PARKED_DATA.some((prefix) => entry === prefix || entry.startsWith(prefix))) {
        offenders.push(`${DIST}/data/${entry} was published`);
      }
    }
  }

  for (const map of ['sitemap-0.xml', 'sitemap-index.xml', 'news-sitemap.xml', 'rss.xml']) {
    const file = path.join(DIST, map);
    if (!fs.existsSync(file)) continue;
    const xml = fs.readFileSync(file, 'utf-8');
    for (const section of PARKED) {
      if (xml.includes(`/${section.out}/`)) offenders.push(`${map} lists /${section.out}/ URLs`);
    }
  }

  if (offenders.length) {
    fail([
      'Build blocked after generating output: a parked section leaked.',
      ...offenders.map((o) => `  - ${o}`),
      '',
      'Do not deploy this build. See docs/parked-sections.md.',
    ]);
  }
}
