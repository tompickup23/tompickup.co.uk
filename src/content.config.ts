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
    /* Search-result title and snippet, when the headline or standfirst is too long for
       them. Meta tags only: the page still shows the headline and standfirst. */
    seoTitle: z.string().max(60).optional(),
    seoDescription: z.string().max(160).optional(),
    tags: z.array(z.string()).default([]),
    image: z.string().optional(),
    imageAlt: z.string().optional(),
    imageUncropped: z.boolean().default(false),
    ogImage: z.string().optional(),
    imageCredit: z.string().optional(),
    category: z.string().optional(),
    subcategory: z.string().optional(),
    /* Numbered sources shown at the foot of the article. In the body, cite with
       <sup class="cite"><a href="#source-1">1</a></sup>. Publisher, document,
       page or table, and date go in `text`. */
    sources: z.array(z.object({ text: z.string(), url: z.string().url() })).optional(),
    /* "Data behind this article": deep links to the exact pages on the family
       sites that supplied the figures, an optional CSV of the article's own
       table, and an optional note on method. */
    data: z
      .object({
        links: z.array(z.object({ label: z.string(), url: z.string().url() })).min(1),
        csv: z.string().optional(),
        method: z.string().optional(),
        /* Set once the dataset is deposited on Zenodo, e.g. "10.5281/zenodo.1234567".
           Adds a "Cite this data" line and a schema.org Dataset node. */
        doi: z.string().regex(/^10\.\d{4,9}\/\S+$/).optional(),
        /* The deposit's own title, as it appears on Zenodo. */
        title: z.string().optional(),
        /* Year the deposit was published, for the citation line. */
        year: z.number().int().optional(),
      })
      .optional(),
    featured: z.boolean().default(false),
    draft: z.boolean().default(false),
  }),
});

export const collections = { news };
