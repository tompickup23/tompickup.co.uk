# tompickup.co.uk — Claude Code Context

## Overview
Personal website and portfolio for Tom Pickup. Central hub linking all projects.

**Stack**: Astro 5 (static output) | no runtime server
**Hosting**: GitHub Pages serves the live domain, from the `tompickup23/tompickup23.github.io`
repo. Checked 10 Sep 2026: `server: GitHub.com`, A records 185.199.108-111.153. There is no
Cloudflare Pages deploy in front of it.
**Deploying**: MANUAL. `deploy.yml`'s deploy step is `if: false` (PAT expired 20 May 2026), so
pushing source builds and gates but publishes nothing. Follow `/publish-tompickup` in clawd:
rsync `dist/` into the Pages clone, then commit and push there. That rsync uses `--delete`, which
also removes the Pages repo's own `CLAUDE.md`/`AGENTS.md` — restore them before committing.
**Automation**: 3 GitHub Actions (deploy, data-etl, observatory-schemas)
**Branch**: main

## Key Patterns
- Astro content collections: articles are markdown in `src/content/news/`, schema in `src/content.config.ts`
- Articles embed hand-written HTML for charts (`viz-panel`, `viz-reform-bar`); styling lives in `src/layouts/BlogPost.astro`
- Python scripts under `scripts/` are offline pre-processing only, never request handlers; their output is committed
- Page-level SEO and JSON-LD in `src/layouts/Layout.astro` and `src/layouts/BlogPost.astro`
- The projects list is one file, `src/data/projects.ts`. `/projects/` and the footer's Projects
  column both read it, so they cannot drift; add a project there, not in either template

## Commands

```bash
npm install
npm run dev                      # Run locally
npm run build                    # Static build to dist/ (guards run pre and post)
npm run dev:observatory          # Run the parked Observatory locally, then re-park it
```

**Cross-repo data dependency (10 Aug 2026):** `scripts/observatory/aggregate_spend.py` and `scripts/lgr_property/build_lgr_contracts.py` read `~/clawd/burnley-council/data` directly off disk — a hardcoded absolute path, not an API. Local-only (not in CI); output gets committed. Only works on this Mac with `clawd` present at that exact path.

## Parked: Lancashire Business Observatory (10 Sep 2026)

`/lancs/` and `/lancs/business/` are **parked, not deleted**. They were 1,074 of the site's
1,112 pages — 1,046 of them near-identical Companies House profiles — which is scaled thin
content on a domain that carries a councillor's name and journalism. The section now lives at
`src/pages/_lancs/`, unrouted by Astro's underscore rule; the launch article is archived at
`src/content/news/_archive-lancashire-business-observatory-launch.md`.

- **Do not un-park it into a build.** `prebuild` and `postbuild` guards (`scripts/guard-parked.mjs`)
  fail the build if the routes or their data reach `dist/`.
- **Do not add `Disallow: /lancs/` to robots.txt.** The URLs must stay crawlable to be seen as
  404s and dropped from the index.
- To view it: `npm run dev:observatory` (plain `npm run dev` will 404 it).
- Full reasoning, and the outstanding Search Console steps: `docs/parked-observatory.md`.

It returns on its own domain, scaled UK-wide — not as a subdirectory here.

## Rules
- Never commit .env or secrets
- British English throughout
- Keep the site fast and lightweight
- SEO: proper meta tags on every page
- Mobile-first responsive design
- All data processing in ETL pipeline, not in request handlers

## Related Projects
Links to all 13 tompickup23 GitHub repos — this is the portfolio hub.

## Cross-repo lessons (5 Jul 2026)

Article method, fact-check protocol, factual anchors, imagery rules, and the manual deploy flow live in the clawd repo: `/Users/tompickup/clawd/docs/lessons/editorial-method.md`. The publish procedure is also encoded as the `/publish-tompickup` skill in clawd. Read before publishing, and append new lessons there, not here.
