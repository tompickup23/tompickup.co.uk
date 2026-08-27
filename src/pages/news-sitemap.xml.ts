import type { APIContext } from 'astro';
import { getCollection } from 'astro:content';

// Google's news sitemap spec is explicit: include only articles published in the
// last two days, and remove them after that. This file previously listed all 21
// articles regardless of age, which advertised two-month-old pieces as breaking
// news. Everything older is still discoverable through the main sitemap.
const NEWS_WINDOW_DAYS = 2;

export async function GET(context: APIContext) {
  const posts = await getCollection('news', ({ data }) => !data.draft);

  const cutoff = Date.now() - NEWS_WINDOW_DAYS * 24 * 60 * 60 * 1000;

  const recent = posts
    .filter((post) => (post.data.updated ?? post.data.date).valueOf() >= cutoff)
    .sort((a, b) => b.data.date.valueOf() - a.data.date.valueOf());

  const newsEntries = recent
    .map((post) => {
      const url = new URL(`/news/${post.id}/`, context.site).href;
      const pubDate = post.data.date.toISOString();
      const modDate = (post.data.updated ?? post.data.date).toISOString();
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
    })
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
${newsEntries}
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
