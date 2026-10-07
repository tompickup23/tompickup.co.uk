import { CONTACT_EMAIL } from '../config';
import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIContext } from 'astro';

export async function GET(context: APIContext) {
  const posts = await getCollection('news', ({ data }) => !data.draft);

  return rss({
    title: 'Tom Pickup',
    description: 'News and updates from Tom Pickup.',
    site: context.site!,
    xmlns: {
      dc: 'http://purl.org/dc/elements/1.1/',
      atom: 'http://www.w3.org/2005/Atom',
    },
    items: posts
      .sort((a, b) => b.data.date.valueOf() - a.data.date.valueOf())
      .map((post) => ({
        title: post.data.title,
        pubDate: post.data.date,
        description: post.data.description,
        link: `/news/${post.id}/`,
        categories: post.data.tags || [],
        customData: '<dc:creator>Tom Pickup</dc:creator>',
      })),
    customData: `<language>en-gb</language>
<atom:link href="https://tompickup.co.uk/rss.xml" rel="self" type="application/rss+xml" />
<managingEditor>${CONTACT_EMAIL} (Tom Pickup)</managingEditor>
<webMaster>${CONTACT_EMAIL} (Tom Pickup)</webMaster>
<copyright>Copyright ${new Date().getFullYear()} Tom Pickup</copyright>
<image>
  <url>https://tompickup.co.uk/images/headshot.jpg</url>
  <title>Tom Pickup</title>
  <link>https://tompickup.co.uk</link>
</image>`,
  });
}
