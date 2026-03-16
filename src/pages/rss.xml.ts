import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIContext } from 'astro';

export async function GET(context: APIContext) {
  const posts = await getCollection('news', ({ data }) => !data.draft);

  return rss({
    title: 'Tom Pickup - News & Updates',
    description:
      'News and updates from Tom Pickup, Lancashire County Councillor for Padiham and Burnley West.',
    site: context.site!,
    items: posts
      .sort((a, b) => b.data.date.valueOf() - a.data.date.valueOf())
      .map((post) => ({
        title: post.data.title,
        pubDate: post.data.date,
        description: post.data.description,
        link: `/news/${post.id}/`,
      })),
    customData: '<language>en-gb</language>',
  });
}
