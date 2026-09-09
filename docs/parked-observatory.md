# The Lancashire Business Observatory is parked

**Parked 10 September 2026.** The code is intact and still runs locally. Nothing about
the project is abandoned — it is coming back on its own domain, sized for the whole UK.

## Why

The Observatory was 1,074 of the site's 1,112 pages: 96.6% of tompickup.co.uk.

| | Before | After |
|---|---|---|
| Pages built | 1,112 | 37 |
| Under `/lancs/business/` | 1,074 | 0 |
| Company profile pages | 1,046 | 0 |
| Observatory JSON in the deploy | 4.4 MB | 0 |

1,046 of those pages were the same template with a different company in it. A company
page carried about 70 words of unique text — number, status, SIC code, postcode,
employee count — restating the Companies House register entry it linked to. That is
what Google's spam policy calls scaled content abuse, and the risk is not a ranking
dip: it is a site-wide manual action on a domain that also carries a sitting
councillor's name, journalism and press page.

Two things made it worse than the raw count suggests. Every page on the site linked
into the section from the footer, so the whole domain's internal link equity flowed
into the thin pages. And a site that is 96.6% company register entries reads, to a
classifier, as a register scraper with a blog attached, rather than as journalism.

## How the parking works

The section lives at `src/pages/_lancs/`. Astro does not route any path containing a
segment that starts with `_`, so the pages are not built and not served. The directory
depth is unchanged from `src/pages/lancs/`, so every relative import inside it still
resolves — nothing in the section was edited.

Also done:

- `src/content/news/lancashire-business-observatory-launch.md` renamed to
  `_archive-lancashire-business-observatory-launch.md`. The content collection glob in
  `src/content.config.ts` excludes `_*.md`, so it is off the site, out of the RSS feed
  and out of both sitemaps, but still in the repo.
- Links into the section removed from `src/components/Footer.astro` (site-wide),
  `src/pages/projects.astro`, `src/pages/press.astro` and `src/pages/lgr.astro`.
- `scripts/strip-parked-data.mjs` deletes `dist/data/biz-*` and `dist/data/company/`
  after each build. The JSON stays in `public/` so local dev works, but does not ship:
  no live page reads it, and the 1,046 dossiers name company officers while the notice
  covering that — `/lancs/business/method/` — is no longer published.
- `scripts/guard-parked.mjs` runs at `prebuild` and `postbuild`. The pre check refuses
  to build while `src/pages/lancs/` exists; the post check inspects the actual output,
  so a future change that reintroduces these URLs some other way fails the build rather
  than shipping.

`.github/workflows/observatory-schemas.yml` is untouched and still validates the
serve-layer JSON on push. That is deliberate: the data contract should keep being
enforced while the project is parked, so the warehouse is still trustworthy when it
comes back.

## Running it locally

```bash
npm run dev:observatory
```

This restores the routed directory name for the session, starts the dev server at
`http://localhost:4321/lancs/business/`, and puts the directory back on exit —
including on Ctrl-C. If it is ever interrupted hard enough to skip that cleanup, the
prebuild guard stops the next build and tells you how to fix it.

Do not use plain `npm run dev` to view the Observatory: Astro applies the underscore
rule in dev too, so the section will 404.

## What still needs doing in Search Console

Removal from the build is only half of it; Google still has ~1,074 URLs indexed. These
steps need a human logged in to Search Console for `tompickup.co.uk`:

1. **Let them 404.** The URLs now return 404 from Cloudflare Pages. That is the correct
   signal and it is what eventually drops them. **Do not add a `Disallow: /lancs/` to
   `robots.txt`** — blocking the crawl stops Google seeing the 404s, and the URLs then
   linger in the index as "indexed, though blocked by robots.txt". This is the single
   most common way people make this worse.
2. **Removals → New request → Remove all URLs with this prefix → `https://tompickup.co.uk/lancs/`.**
   This hides them from results within about a day. It is a 6-month suppression, not a
   deletion — the 404s in step 1 are what makes it permanent.
3. Do the same for the archived article,
   `https://tompickup.co.uk/news/lancashire-business-observatory-launch/`.
4. **Resubmit the sitemap** (`sitemap-index.xml`). It now lists 37 URLs.
5. Watch **Indexing → Pages** over the next few weeks. Expect a large, healthy fall in
   "Crawled – currently not indexed" and in the total. Watch **Search results** for
   impressions on the 37 real pages — that is the number that should recover.

## When it comes back

It goes on its own property, not a subdirectory here. A UK-wide register — roughly
5 million companies — cannot live on a personal site under any circumstances, and a
separate domain also means it can carry its own about page, method, privacy notice and
corrections route as a publication in its own right. Whatever it lands on should have
its own Search Console property and, if it is not ready to be public, Cloudflare Access
in front of it rather than a robots.txt nobody is obliged to honour.
