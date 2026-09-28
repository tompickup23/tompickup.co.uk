import type { CollectionEntry } from 'astro:content';
import { NEWS_AREAS, newsArea, newsTopic } from './news-categories';
import { ALL_PROJECTS, PROJECT_DETAILS } from '../data/projects';

type Post = CollectionEntry<'news'>;

const GENERIC_TAGS = new Set(['burnley', 'padiham', 'lancashire', 'uk', 'reform-uk', 'news']);

export interface NextRead {
  kicker: string;
  title: string;
  href: string;
  external?: boolean;
}

/* Two or three specific next steps at the foot of an article: the closest
   related article, the project it draws on, and the subject hub. Nothing generic:
   a step is left out rather than filled with a link that fits any article. */
export function nextReads(post: Post, posts: Post[]): NextRead[] {
  const out: NextRead[] = [];
  const area = newsArea(post);
  const topic = newsTopic(post);
  const areaTitle = NEWS_AREAS.find((a) => a.id === area)!.title;
  const others = posts.filter((p) => p.id !== post.id);

  // The closest article: same topic counts most, then same area, then shared
  // subject tags. Place and party tags are left out: nearly every article has them.
  const subjectTags = post.data.tags.filter((t) => !GENERIC_TAGS.has(t));
  const score = (p: Post) =>
    (newsTopic(p).id === topic.id ? 4 : 0) +
    (newsArea(p) === area ? 2 : 0) +
    p.data.tags.filter((t) => subjectTags.includes(t)).length;
  const best = others
    .map((p) => ({ p, s: score(p) }))
    .filter((x) => x.s >= 5 || (x.s >= 4 && newsTopic(x.p).id === topic.id))
    .sort((a, b) => b.s - a.s || b.p.data.date.valueOf() - a.p.data.date.valueOf())[0];
  if (best) {
    out.push({ kicker: newsTopic(best.p).title, title: best.p.data.title, href: `/news/${best.p.id}/` });
  }

  // The project this article is listed against on /projects/.
  const projectTitle = Object.entries(PROJECT_DETAILS).find(([, d]) => d.article?.slug === post.id)?.[0];
  const project = projectTitle && ALL_PROJECTS.find((p) => p.title === projectTitle);
  if (project) {
    out.push({ kicker: 'The project behind it', title: project.title, href: project.href, external: project.external });
  }

  // The subject hub.
  if (post.data.tags.includes('stocks-massey') || post.id.startsWith('stocks-massey')) {
    out.push({ kicker: 'The collection', title: 'Stocks Massey: history, awards and sources', href: '/stocks-massey/' });
  } else if (topic.id === 'public-debates') {
    out.push({ kicker: 'All appearances', title: 'Public debates and speeches', href: '/news/public-debates/' });
  } else {
    const sameTopic = others.filter((p) => newsArea(p) === area && newsTopic(p).id === topic.id).length;
    out.push(
      sameTopic > 0
        ? { kicker: 'More on this subject', title: `${topic.title} in ${areaTitle}`, href: `/news/#${area}-${topic.id}` }
        : {
            kicker: 'More from the area',
            title: area === 'national' ? 'All national writing' : `All writing on ${areaTitle}`,
            href: `/news/#${area}`,
          },
    );
  }

  return out.slice(0, 3);
}
