import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIContext } from 'astro';

export async function GET(context: APIContext) {
  const posts = await getCollection('news', ({ data }) => !data.draft);

  return rss({
    title: 'Tom Pickup',
    description: 'News and updates from Tom Pickup.',
    site: context.site!,
    items: posts
      .sort((a, b) => b.data.date.valueOf() - a.data.date.valueOf())
      .map((post) => ({
        title: post.data.title,
        pubDate: post.data.date,
        description: post.data.description,
        link: `/news/${post.id}/`,
        categories: post.data.tags || [],
        author: 'Tom Pickup',
      })),
    customData: `<language>en-gb</language>
<managingEditor>tom@tompickup.co.uk (Tom Pickup)</managingEditor>
<webMaster>tom@tompickup.co.uk (Tom Pickup)</webMaster>
<copyright>Copyright ${new Date().getFullYear()} Tom Pickup</copyright>
<image>
  <url>https://tompickup.co.uk/images/headshot.jpg</url>
  <title>Tom Pickup</title>
  <link>https://tompickup.co.uk</link>
</image>`,
  });
}
