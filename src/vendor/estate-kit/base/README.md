# The base layer

Nine small things done once for every site: type, numerals, the record face, links and focus, tables, page transitions, instant next pages, the icon set and dead-end pages. Ideas I057 to I064 and I071 in `~/Documents/Reports/Website_Audits/ideas.csv`. See them at `/base/` in the demo (`npm run dev`).

Everything here is plain CSS, plain modules and Astro components with no dependency beyond `@resvg/resvg-js` for the icon builder, so a site can carry a copy.

## What is in it

| Idea | Files | What a site gets |
|---|---|---|
| I064 Fluid type and a capped measure | `type.css`, `fluid.mjs` | Eight type steps, each one clamp with a rem term; headings balanced; prose capped at 68 characters with no stranded last word |
| I057 Numerals that line up | `numerals.css`, `rehype-figures.mjs`, `satteri.mjs` | Tabular figures where numbers are compared, proportional in prose; a figure never parted from its unit at a line end |
| I058 Monospace for the record | `meta.css` | `ek-meta` for dates, sources and editions in Source Code Pro; `ek-meta-label`; `ek-meta-row` |
| I062 Focus, underline, selection | `links.css` | Two-part focus ring; underlines by thickness and offset; hover only where hover exists; selection, caret and form accent; anchor targets that clear a fixed header |
| I061 One table recipe | `tables.css`, `table-rules.mjs`, `rehype-tables.mjs`, `satteri.mjs`, `DataTable.astro` | A scrolling region per table, numeric columns found and aligned, the source as the caption, a sticky first column on wide tables, edge shading, print rules |
| I059 The card becomes the page | `transitions.css`, `vt.mjs` | Cross-page fade, shared element morph (`ek-morph`), held elements (`ek-hold`) |
| I060 The next page is already there | `speculation.mjs`, `Speculation.astro` | Speculation rules that prefetch on hover or touch, skipping downloads and marked links |
| I063 Theme colour and icon set | `icons.mjs`, `HeadIcons.astro` | Six icon files from one SVG mark, a dark variant inside the SVG, theme colour per scheme |
| I071 Dead ends with a next step | `states.css`, `suggest.mjs`, `NotFound.astro`, `Empty.astro` | A missing page that shows the address asked for, the closest real pages and what happened to a retired section; an empty-list state |

## How a site adopts it

1. **Copy it in.** From the kit: `node scripts/vendor-base.mjs ~/<site>/src/vendor/estate-kit`. That writes `base/` and a `VERSION` file with the kit commit and a hash of every file.
2. **Guard the copy.** Add `node src/vendor/estate-kit/base/verify-vendor.mjs` to the site's build. A hand edit to the copy fails there; change the kit and vendor again.
3. **Import the layer** before the site's own stylesheet: `import "../vendor/estate-kit/base/base.css";` (or the parts one at a time).
4. **Layer the reset.** If the site has an unlayered universal reset (`* { margin: 0; padding: 0 }`), wrap it in `@layer reset { ... }`. Unlayered rules beat every layer, so an unlayered reset would strip the layer's padding. Nothing else in the site changes meaning: unlayered rules still beat the reset.
5. **Map the contract** if the site does not use the estate token names. One block, outside any layer:

   ```css
   :root {
     --ek-bg: var(--paper);
     --ek-text: var(--ink);
     --ek-muted: var(--muted);
     --ek-accent: var(--teal);
     --ek-line: var(--line);
     --ek-line-strong: var(--line-strong);
     --ek-header-offset: 64px;
   }
   ```

   The full list is in `contract.css`. A site on the estate tokens sets only `--ek-header-offset`, and only if its header is fixed.
6. **Delete the site's own versions** of what it now takes from the layer (its link, focus, selection and table rules, its heading size breakpoints). Until a rule is deleted the site's version wins, so adoption can go a part at a time.
7. **Load the record face.** `npm i @fontsource/source-code-pro` and import weight 400 (and 600 if `ek-meta-label` is used). About 12 kB for the Latin 400 file.
8. **Wire the build-time parts** in `astro.config.mjs`. On Astro 7, whose default Markdown processor is Sätteri:

   ```js
   import { satteri } from "@astrojs/markdown-satteri";
   import { estateTables, bindFiguresPlugin } from "./src/vendor/estate-kit/base/satteri.mjs";
   export default defineConfig({ markdown: { processor: satteri({ hastPlugins: [estateTables(), bindFiguresPlugin()] }) } });
   ```

   On Astro 5 and 6, or Astro 7 with `@astrojs/markdown-remark`:

   ```js
   import rehypeEstateTables from "./src/vendor/estate-kit/base/rehype-tables.mjs";
   import rehypeBindFigures from "./src/vendor/estate-kit/base/rehype-figures.mjs";
   export default defineConfig({ markdown: { rehypePlugins: [rehypeEstateTables, rehypeBindFigures] } });
   ```

   Both forms take the same decisions from `planTable()`. Sätteri keeps raw HTML typed into Markdown as a string, so there a hand-written `<table>` is left alone.
9. **Add the components** where they belong: `<Speculation />` at the end of `<body>`; `<HeadIcons light="#..." />` in `<head>`; `<NotFound pages={...} links={...} />` on the 404 page; `class="ek-prose"` on the article body; `ek-meta` on dates and source lines; `vtStyle(vtName("media", url))` on a card image and on the heading image of the page it opens.
10. **Check it.** `/design-check` at phone and desktop sizes, and a tab through one page to see the focus ring.

## Rules the layer keeps

- Every rule is in `@layer estate-base`. A site's own rule always wins.
- Nothing moves for a reader who asked for reduced motion.
- A scroll timeline is never set in the same rule as its animation: CSS minifiers fold `animation-timeline` into the `animation` shorthand, which browsers reject. `tests/base.test.ts` holds this.
- Prefetch, not prerender, by default: a prefetched page runs no script, so an analytics beacon cannot count a page nobody opened.
- The missing-page script writes the address as text, never as markup.

## What was checked, and what was not

- The `@fontsource` Latin builds of Source Sans 3 and Source Serif 4 hold proportional and tabular figures and no old-style figures, small capitals or superscript glyphs (read from the font files with fonttools, 11 November 2026). The layer asks only for what they hold. Old-style figures in prose would need the faces subset from Adobe's own files (idea I066).
- Browser support, from MDN browser-compat-data 8.1.8: cross-page view transitions in Chrome 126 and Safari 18.2, not Firefox; speculation rules prefetch in Chromium and Safari 26.2; `text-wrap: pretty` in Chrome 117 and Safari 26; scroll timelines in Chrome 115, Safari 26 and Firefox 159. Each degrades to the plain behaviour.
- Not tested in Safari or Firefox by hand.
