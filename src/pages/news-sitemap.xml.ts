import type { APIContext } from 'astro';
import { getCollection } from 'astro:content';
import { isoDate } from '../lib/dates';

// Google's news sitemap spec is explicit: include only articles published in the
// last two days, and remove them after that. This file previously listed all 21
// articles regardless of age, which advertised two-month-old pieces as breaking
// news. Everything older is still discoverable through the main sitemap.
const NEWS_WINDOW_DAYS = 2;

export async function GET(context: APIContext) {
  const posts = await getCollection('news', ({ data }) => !data.draft);

  const cutoff = Date.now() - NEWS_WINDOW_DAYS * 24 * 60 * 60 * 1000;

  const newest = [...posts].sort((a, b) => b.data.date.valueOf() - a.data.date.valueOf());

  // The window is on the PUBLICATION date. It used to fall back to `updated`, so
  // an old piece edited this week (the March 2025 Stocks Massey article, revised
  // on 27 Sept 2026) was advertised as news. An edit is not news; Google News
  // wants the date the article first appeared.
  const inWindow = newest.filter((post) => post.data.date.valueOf() >= cutoff);

  const newsEntries = inWindow.map((post) => {
    const url = new URL(`/news/${post.id}/`, context.site).href;
    const pubDate = isoDate(post.data.date);
    const modDate = isoDate(post.data.updated ?? post.data.date);
    const keywords = (post.data.tags || []).join(', ');

    return `  <url>
    <loc>${url}</loc>
    <news:news>
      <news:publication>
        <news:name>Tom Pickup</news:name>
        <news:language>en</news:language>
      </news:publication>
      <news:publication_date>${pubDate}</news:publication_date>
      <news:title>${escapeXml(post.data.title)}</news:title>
      <news:keywords>${escapeXml(keywords)}</news:keywords>
    </news:news>
    <lastmod>${modDate}</lastmod>
  </url>`;
  });

  // Google rejects a <urlset> carrying no <url>: Search Console reports it as
  // "Missing XML tag" with zero pages discovered, which is what this file did
  // from 7 Sept 2026. An empty window is the normal case here, not the exception:
  // articles arrive in bursts, and the static build freezes the file at whatever
  // the last build saw.
  //
  // So when nothing is inside the window, list the newest article as a plain
  // <url> with <loc> and <lastmod> only. The file stays valid, and without a
  // <news:news> block it makes no claim that the piece is current news.
  // Everything older stays discoverable through the main sitemap.
  if (newsEntries.length === 0 && newest.length > 0) {
    const post = newest[0];
    const url = new URL(`/news/${post.id}/`, context.site).href;
    const modDate = isoDate(post.data.updated ?? post.data.date);
    newsEntries.push(`  <url>
    <loc>${url}</loc>
    <lastmod>${modDate}</lastmod>
  </url>`);
  }

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
${newsEntries.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: {
      'Content-Type': 'application/xml; charset=utf-8',
    },
  });
}

function escapeXml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}
