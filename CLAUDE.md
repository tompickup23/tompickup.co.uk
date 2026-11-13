# tompickup.co.uk: Claude Code Context

## Overview
Personal website and portfolio for Tom Pickup. Central hub linking all projects.

**Stack**: Astro 7.4 (static output; `package-lock.json` pins 7.4.4; Node.js >=22.23.4) | no runtime server
**Hosting**: GitHub Pages serves the live domain from this repo.
**Deploying**: Automatic from `main` through GitHub Actions since 10 September 2026
(`.github/workflows/deploy.yml`). Every push to `main` builds, runs the guards and the
house-style gate, and publishes with `GITHUB_TOKEN`. Merging to main is deploying; work on
a branch. Six green deploy runs on 25 and 26 October 2026. Check recent runs with
`gh run list --workflow deploy.yml`. The old `tompickup23/tompickup23.github.io` mirror
and the `/publish-tompickup` rsync flow (and its expired cross-repo PAT) are superseded.
**Automation**: 3 GitHub Actions (deploy on push to main; data-etl, manual only since 28 Sep 2026; observatory-schemas)
**Branch**: main
**Handover**: newest under `docs/` is `docs/LGR-HANDOVER.md`. The 3 November 2026 audit
cites `docs/FIGURE-REVIEW-2026-09-28.md`; that file is not in the repository (do not create it).

## Key Patterns
- Astro content collections: articles are markdown in `src/content/news/`, schema in `src/content.config.ts`
- Articles embed hand-written HTML for charts (`viz-panel`, `viz-reform-bar`); styling lives in `src/layouts/BlogPost.astro`
- Python scripts under `scripts/` are offline pre-processing only, never request handlers; their output is committed
- Page-level SEO and JSON-LD in `src/layouts/Layout.astro` and `src/layouts/BlogPost.astro`
- Article share cards are 1200x630 and never carry Tom's portrait (a named individual and a headline
  statistic never share an asset). Cards come from `scripts/generate_og_cards.py`; set `ogImage` in the
  article. `scripts/guard-share-cards.mjs` (postbuild) fails the build on a wrong size or the portrait card.
  The portrait card `public/og-image.jpg` is for the homepage and about page only
- The shared base layer (11 Nov 2026) is a vendored copy of the Estate Kit's `base/` at
  `src/vendor/estate-kit/base/`, with its kit commit and file hashes in `src/vendor/estate-kit/VERSION`.
  It supplies type sizes, numerals, the record face (Source Code Pro, class `ek-meta`), links, focus,
  selection, the table recipe, page transitions, prefetch and the 404 page, all in the cascade layer
  `estate-base`; `src/styles/global.css` is unlayered and wins. Never edit the copy: change
  `~/estate-kit/base`, then `node scripts/vendor-base.mjs <this repo>/src/vendor/estate-kit` from the kit.
  `prebuild` fails on a changed copy; `scripts/guard-base-layer.mjs` (postbuild) fails if the minifier
  breaks the layer order. Markdown tables are wrapped at build time by the kit's Sätteri plugin in
  `astro.config.mjs`; a "Source:" paragraph directly after a table becomes its caption
- The projects list is one file, `src/data/projects.ts`. `/projects/` and the footer's Projects
  column both read it, so they cannot drift; add a project there, not in either template

## Commands

```bash
npm install
npm run dev                      # Run locally
npm run build                    # Static build to dist/ (guards run pre and post)
npm run dev:observatory          # Run the parked Observatory locally, then re-park it
```

**Cross-repo data dependency (10 Aug 2026):** `scripts/observatory/aggregate_spend.py` and `scripts/lgr_property/build_lgr_contracts.py` read `~/clawd/burnley-council/data` directly off disk, a hardcoded absolute path, not an API. Local-only (not in CI); output gets committed. Only works on this Mac with `clawd` present at that exact path.

## Parked sections (see `docs/parked-sections.md`)

Two sections are parked: in the repo, runnable locally, not published. Both are held
out of the routed tree by Astro's underscore rule, and `prebuild`/`postbuild` guards
(`scripts/guard-parked.mjs`) fail the build if either the routes or their data reach
`dist/`. View them with `npm run dev:observatory` and `npm run dev:doge`.

**Burnley spending explorer, `/doge/` (11 Sep 2026)** at `src/pages/_doge/`. Retired as
a duplicate: the same payments are on `aidoge.co.uk/councils/burnley/`, checked against
the council's files. The two articles that linked it were repointed there. AI DOGE holds
Burnley from 2024/25 only, so the 2023-24 year is no longer public anywhere.

### Lancashire Business Observatory (10 Sep 2026)

`/lancs/` and `/lancs/business/` are **parked, not deleted**. They were 1,074 of the site's
1,112 pages, 1,046 of them near-identical Companies House profiles, which is scaled thin
content on a domain that carries a councillor's name and journalism. The section now lives at
`src/pages/_lancs/`, unrouted by Astro's underscore rule; the launch article is archived at
`src/content/news/_archive-lancashire-business-observatory-launch.md`.

- **Do not un-park it into a build.** `prebuild` and `postbuild` guards (`scripts/guard-parked.mjs`)
  fail the build if the routes or their data reach `dist/`.
- **Do not add `Disallow: /lancs/` to robots.txt.** The URLs must stay crawlable to be seen as
  404s and dropped from the index.
- To view it: `npm run dev:observatory` (plain `npm run dev` will 404 it).
- Full reasoning, and the outstanding Search Console steps: `docs/parked-sections.md`.

It returns on its own domain, scaled UK-wide, not as a subdirectory here.

## Archive (28 Sep 2026)
Everything in `public/` ships. Files no page uses live in `archive/public/` (same paths),
and `scripts/guard-unreferenced.mjs` fails the build if an unlinked file under
`public/{videos,images,data}` reaches `dist/`. See `archive/README.md`.

## Rules
- Never commit .env or secrets
- The footer imprint (Elections Act 2022, Part 6) is live on every page and must stay
  accurate: `src/data/imprint.ts`. The address is Reform UK's registered office, which
  holds only while the party accepts correspondence there on Tom's behalf. Same field
  names and sentence as the reform-burnley repo's `src/data/site.ts`, deliberately
- British English throughout
- Keep the site fast and lightweight
- SEO: proper meta tags on every page
- Mobile-first responsive design
- All data processing in ETL pipeline, not in request handlers

## Related Projects
Fifteen projects listed in `src/data/projects.ts` since 13 November 2026, Young Lancashire Voice added (a noindex prototype beside Lancashire Voice; the two pass a motion as four numbers after the #, parts in `src/data/young-lancashire-voice/motions.json`) (and twelve public tompickup23 GitHub
repositories as of 3 November 2026). This site is the portfolio hub.

## Cross-repo lessons (5 Jul 2026)

Article method, fact-check protocol, factual anchors and imagery rules live in the clawd
repo: `/Users/tompickup/clawd/docs/lessons/editorial-method.md`. Deploy for this site is
automatic from main (see Deploying above); ignore any manual rsync steps still named in
clawd. Read before publishing, and append new lessons there, not here.
