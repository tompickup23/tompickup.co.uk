# tompickup.co.uk — Claude Code Context

## Overview
Personal website and portfolio for Tom Pickup. Central hub linking all projects.

**Stack**: Astro 5 (static output) | no runtime server
**Hosting**: Cloudflare Pages, with a GitHub Pages mirror (manual deploy, `/publish-tompickup` in clawd)
**Automation**: 2 GitHub Actions (deploy, data-etl)
**Branch**: main

## Key Patterns
- Astro content collections: articles are markdown in `src/content/news/`, schema in `src/content.config.ts`
- Articles embed hand-written HTML for charts (`viz-panel`, `viz-reform-bar`); styling lives in `src/layouts/BlogPost.astro`
- Python scripts under `scripts/` are offline pre-processing only, never request handlers; their output is committed
- Page-level SEO and JSON-LD in `src/layouts/Layout.astro` and `src/layouts/BlogPost.astro`

## Commands

```bash
npm install
npm run dev                      # Run locally
npm run build                    # Static build to dist/
```

**Cross-repo data dependency (10 Aug 2026):** `scripts/observatory/aggregate_spend.py` and `scripts/lgr_property/build_lgr_contracts.py` read `~/clawd/burnley-council/data` directly off disk — a hardcoded absolute path, not an API. Local-only (not in CI); output gets committed. Only works on this Mac with `clawd` present at that exact path.

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
