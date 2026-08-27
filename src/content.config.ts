import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';

const news = defineCollection({
  loader: glob({ pattern: ['**/*.md', '!**/_*.md'], base: './src/content/news' }),
  schema: z.object({
    title: z.string(),
    date: z.date(),
    // Set when an article is materially revised. Feeds dateModified + article:modified_time.
    updated: z.date().optional(),
    description: z.string(),
    tags: z.array(z.string()).default([]),
    image: z.string().optional(),
    ogImage: z.string().optional(),
    imageCredit: z.string().optional(),
    category: z.string().optional(),
    subcategory: z.string().optional(),
    featured: z.boolean().default(false),
    draft: z.boolean().default(false),
  }),
});

export const collections = { news };
