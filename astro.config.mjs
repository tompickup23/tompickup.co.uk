import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import fs from 'node:fs';
import path from 'node:path';

// Google uses <lastmod> to schedule recrawls, and treats it as a lie if every URL
// carries the build date. So only the pages whose real change date we know get one:
// news articles, read straight from their frontmatter.
const NEWS_DIR = './src/content/news';

function newsLastmod() {
  const map = new Map();
  if (!fs.existsSync(NEWS_DIR)) return map;
  for (const file of fs.readdirSync(NEWS_DIR)) {
    if (!file.endsWith('.md') || file.startsWith('_')) continue;
    const raw = fs.readFileSync(path.join(NEWS_DIR, file), 'utf-8');
    const fm = raw.match(/^---\n([\s\S]*?)\n---/);
    if (!fm) continue;
    const pick = (key) => {
      const m = fm[1].match(new RegExp(`^${key}:\\s*(.+)$`, 'm'));
      return m ? m[1].trim().replace(/^["']|["']$/g, '') : null;
    };
    const when = pick('updated') || pick('date');
    if (!when) continue;
    const d = new Date(when);
    if (Number.isNaN(d.valueOf())) continue;
    map.set(`https://tompickup.co.uk/news/${file.replace(/\.md$/, '')}/`, d.toISOString());
  }
  return map;
}

const LASTMOD = newsLastmod();

export default defineConfig({
  site: 'https://tompickup.co.uk',
  // Astro 7 defaults to 'jsx', which drops whitespace between inline elements
  // and text that sit on separate source lines, joining words on most pages.
  // true keeps the Astro 5 behaviour.
  compressHTML: true,
  integrations: [
    sitemap({
      // Draft prototype: noindex (linked from the homepage and /projects/, decision 3 (b) of 8 Nov 2026), so kept out of the sitemap (and so out of IndexNow pings).
      filter: (page) => !page.includes('/lancashire-voice/'),
      serialize(item) {
        const lastmod = LASTMOD.get(item.url);
        if (lastmod) item.lastmod = lastmod;
        return item;
      },
    }),
  ],
  markdown: {
    shikiConfig: {
      theme: 'github-dark',
    },
  },
  prefetch: {
    defaultStrategy: 'hover',
  },
});
