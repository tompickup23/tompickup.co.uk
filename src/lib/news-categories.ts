import type { CollectionEntry } from 'astro:content';

type Post = CollectionEntry<'news'>;

export const NEWS_AREAS = [
  { id: 'burnley', title: 'Burnley', description: 'Community news, public spending, housing, health and policing in Burnley and Padiham.' },
  { id: 'lancashire', title: 'Lancashire', description: 'Council reorganisation, public services and decisions across the county.' },
  { id: 'national', title: 'National', description: 'UK policy, public money and the systems behind the local picture.' },
] as const;

export const NEWS_TOPICS = [
  { id: 'public-spending', title: 'Public spending' },
  { id: 'housing', title: 'Housing & ownership' },
  { id: 'health-benefits', title: 'Health & benefits' },
  { id: 'crime-policing', title: 'Crime & policing' },
  { id: 'local-government', title: 'Local government' },
  { id: 'social-care', title: 'Adult social care' },
  { id: 'planning-energy', title: 'Planning & energy' },
  { id: 'community-charity', title: 'Community & charity' },
  { id: 'other', title: 'Other news' },
] as const;

/** Keep older category names readable without changing article URLs or copy. */
export function newsArea(post: Post): typeof NEWS_AREAS[number]['id'] {
  const { category, subcategory, tags } = post.data;
  if (category === 'UK' || category === 'National') return 'national';
  if (category === 'Burnley' || subcategory === 'Burnley') return 'burnley';
  if (category === 'Lancashire' || tags.includes('lancashire')) return 'lancashire';
  if (tags.includes('burnley')) return 'burnley';
  return 'national';
}

export function newsTopic(post: Post): typeof NEWS_TOPICS[number] {
  const { subcategory, tags } = post.data;
  let id: typeof NEWS_TOPICS[number]['id'] = 'other';
  if (tags.includes('lgr') || tags.includes('local-government')) id = 'local-government';
  else if (subcategory === 'Adult Social Care') id = 'social-care';
  else if (subcategory === 'Housing') id = 'housing';
  else if (subcategory === 'Health' || subcategory === 'Benefits') id = 'health-benefits';
  else if (subcategory === 'Crime') id = 'crime-policing';
  else if (subcategory === 'Planning' || subcategory === 'Energy') id = 'planning-energy';
  else if (subcategory === 'Community & charity' || tags.includes('charity')) id = 'community-charity';
  else if (subcategory === 'Transparency' || tags.includes('spending')) id = 'public-spending';
  return NEWS_TOPICS.find(topic => topic.id === id)!;
}

export function groupNews(posts: Post[]) {
  return NEWS_AREAS.map(area => {
    const areaPosts = posts.filter(post => newsArea(post) === area.id);
    return {
      ...area,
      count: areaPosts.length,
      topics: NEWS_TOPICS.map(topic => ({
        ...topic,
        posts: areaPosts.filter(post => newsTopic(post).id === topic.id)
          .sort((a, b) => b.data.date.valueOf() - a.data.date.valueOf()),
      })).filter(topic => topic.posts.length > 0),
    };
  }).filter(area => area.count > 0);
}
