/* Guards that the Lancashire Business Observatory does not reach production.
 *
 * It was 1,074 of the site's 1,112 pages — 1,046 of them near-identical company
 * profiles restating the Companies House register — which is the shape Google
 * treats as scaled thin content, on a domain that is otherwise personal and
 * journalistic. It is parked at src/pages/_lancs/ until it moves to its own
 * property. See docs/parked-observatory.md.
 *
 * `pre`  runs before a build: refuses to build while the section is in the routed
 *        tree, which is how scripts/observatory-dev.sh leaves it mid-session.
 * `post` runs after: checks the actual output, so a future change that reintroduces
 *        these URLs by some other route still fails the build rather than shipping.
 */
import fs from 'node:fs';
import path from 'node:path';

const mode = process.argv[2] ?? 'pre';
const LIVE = 'src/pages/lancs';
const DIST = 'dist';

function fail(lines) {
  console.error('');
  for (const line of lines) console.error(`  ${line}`);
  console.error('');
  process.exit(1);
}

if (mode === 'pre') {
  if (fs.existsSync(LIVE)) {
    fail([
      `Build blocked: ${LIVE}/ is in the routed tree.`,
      '',
      'The Business Observatory is parked and must not be published.',
      `Move it back:  git mv ${LIVE} src/pages/_lancs`,
      '',
      'To view it locally instead:  npm run dev:observatory',
    ]);
  }
} else {
  if (!fs.existsSync(DIST)) fail([`Build check failed: ${DIST}/ not found.`]);

  const offenders = [];
  if (fs.existsSync(path.join(DIST, 'lancs'))) offenders.push(`${DIST}/lancs/ was generated`);

  const data = path.join(DIST, 'data');
  if (fs.existsSync(data)) {
    const leaked = fs
      .readdirSync(data)
      .filter((entry) => entry === 'company' || entry.startsWith('biz-'));
    for (const entry of leaked) offenders.push(`${DIST}/data/${entry} was published`);
  }

  for (const map of ['sitemap-0.xml', 'sitemap-index.xml', 'news-sitemap.xml', 'rss.xml']) {
    const file = path.join(DIST, map);
    if (fs.existsSync(file) && fs.readFileSync(file, 'utf-8').includes('/lancs/')) {
      offenders.push(`${map} lists /lancs/ URLs`);
    }
  }

  if (offenders.length) {
    fail([
      'Build blocked after generating output: the parked Observatory leaked.',
      ...offenders.map((o) => `  - ${o}`),
      '',
      'Do not deploy this build. See docs/parked-observatory.md.',
    ]);
  }
}
