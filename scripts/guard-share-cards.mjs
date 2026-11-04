/* Fails the build if an article's share card is the wrong shape or carries the portrait.
 *
 * House rule: a named individual and a headline statistic never share an asset.
 * The portrait card (/og-image.jpg, Tom's photo and name) is for the homepage and
 * about page; no article may use it, whatever its headline. Every article page must
 * also declare a 1200x630 og:image that really is 1200x630, use the large Twitter
 * card, and allow large image previews in its robots meta (Google Discover).
 *
 * Reads the built pages in dist/news/<slug>/index.html, so it checks what ships.
 */
import fs from 'node:fs';
import path from 'node:path';

const DIST = 'dist';
const PORTRAIT = '/og-image.jpg';
const W = 1200;
const H = 630;

function meta(html, attr, name) {
  const re = new RegExp(`<meta[^>]*${attr}="${name}"[^>]*>`, 'i');
  const tag = html.match(re)?.[0];
  return tag?.match(/content="([^"]*)"/i)?.[1];
}

/* Pixel size from the file header: PNG IHDR, or a JPEG start-of-frame marker. */
function imageSize(file) {
  const b = fs.readFileSync(file);
  if (b.slice(1, 4).toString() === 'PNG') return [b.readUInt32BE(16), b.readUInt32BE(20)];
  if (b[0] === 0xff && b[1] === 0xd8) {
    let i = 2;
    while (i < b.length) {
      if (b[i] !== 0xff) { i++; continue; }
      const m = b[i + 1];
      if (m >= 0xc0 && m <= 0xcf && m !== 0xc4 && m !== 0xc8 && m !== 0xcc) {
        return [b.readUInt16BE(i + 7), b.readUInt16BE(i + 5)];
      }
      i += 2 + b.readUInt16BE(i + 2);
    }
  }
  return null;
}

const newsDir = path.join(DIST, 'news');
const pages = fs.existsSync(newsDir)
  ? fs.readdirSync(newsDir, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => path.join(newsDir, e.name, 'index.html'))
      .filter((f) => fs.existsSync(f))
  : [];

const problems = [];
let checked = 0;
for (const file of pages) {
  const html = fs.readFileSync(file, 'utf-8');
  if (meta(html, 'property', 'og:type') !== 'article') continue;
  checked++;
  const where = '/' + path.relative(DIST, path.dirname(file)).split(path.sep).join('/') + '/';
  const fail = (msg) => problems.push(`${where}: ${msg}`);

  const image = meta(html, 'property', 'og:image');
  const url = image ? new URL(image, 'https://tompickup.co.uk').pathname : '';
  if (!image) fail('no og:image');
  else if (url === PORTRAIT) fail('uses the portrait card; articles never carry the portrait');

  if (meta(html, 'property', 'og:image:width') !== String(W) || meta(html, 'property', 'og:image:height') !== String(H)) {
    fail(`og:image:width and og:image:height must be ${W} and ${H}`);
  }
  if (meta(html, 'name', 'twitter:card') !== 'summary_large_image') fail('twitter:card is not summary_large_image');
  if (!/max-image-preview:large/.test(meta(html, 'name', 'robots') ?? '')) fail('robots meta lacks max-image-preview:large');

  if (url && url !== PORTRAIT) {
    const local = path.join(DIST, decodeURI(url));
    if (!fs.existsSync(local)) fail(`share image ${url} is not in dist/`);
    else {
      const size = imageSize(local);
      if (!size || size[0] !== W || size[1] !== H) {
        fail(`share image ${url} is ${size ? size.join('x') : 'of unknown size'}, not ${W}x${H}`);
      }
    }
  }
}

if (problems.length) {
  console.error('\n  Build blocked: article share cards that break the rules:');
  for (const p of problems) console.error(`    - ${p}`);
  console.error('\n  Give the article a 1200x630 card (scripts/generate_og_cards.py) and set ogImage.\n');
  process.exit(1);
}
console.log(`  Share cards: ${checked} articles checked, all 1200x630, none with the portrait.`);
