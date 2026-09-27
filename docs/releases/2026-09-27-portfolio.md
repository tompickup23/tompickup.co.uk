# Portfolio release, 27 September 2026

## Published scope

- Eleven projects grouped by purpose, with related reporting and a shared footer list.
- News grouped by Burnley, Lancashire and National, then by subject.
- Stocks Massey collection, sourced historical introduction and 2025/2026 award articles.
- Award articles retain total allocations, funded organisation counts and scholarship recipients' names. No individual grant amounts, universities or subjects.
- Corrected the distinction between the will and the fund's establishment in 1910.
- Mobile article overflow fix, keyboard-safe collapsed navigation, 44px menu button, improved footer contrast, reduced-motion card behaviour and descriptive award hero alt text.
- Limited LGR status correction: programme paused, existing analysis labelled as the July model. Government announcement of 7 September 2026 is linked. Financial model data are unchanged from the previously committed version.
- Deployment guide corrected against actual GitHub Pages settings and workflow.

## Deliberately excluded

Separate LGR financial analysis, generated model revisions and contracts scratch data; Lancashire history research; all larger redesign experiments. Built from an isolated branch based on origin/main after fetching the latest feed updates. Parked `/lancs/` and `/doge/` remain excluded.

## Validation

Production build: 34 pages, pre/post parked-section guards passed. House-style and git diff whitespace gates passed. All 1,452 internal links and asset references resolved before final alt-text and credit corrections; these corrections introduce no new internal targets. External portfolio and Stocks Massey evidence links returned successful responses, with article/PDF content inspected.

Homepage, Projects, News, Stocks Massey, both award articles and LGR checked at 320, 390, 768 and 1440px: no horizontal overflow or broken images. Axe WCAG 2 A/AA and 2.1 AA checks found no violations on Projects, News, Stocks Massey and both award articles at 390 and 1440px after fixes. This is a scoped automated check, not certification of the entire site. Mobile menu open/close, inert state and Escape focus return verified.

Council reports and both allocation appendices were read; the tables were rendered and visually checked. 2025 total: £24,000, including scholarships; 15 funded organisations/services. 2026: £34,000, including scholarships; 18 funded organisations/services. Zero allocations and students are excluded from organisation counts. Student names checked against Burnley College's annual award reports. The 2010 Burnley photograph is credited to Childzy, with its Commons source, licence and crop disclosure in the 2026 article.

Presentation lint has no BLOCK findings. Its grade warnings on Astro files include markup/CSS; the editorial pass was completed separately. Reference articles deliberately retain the requested totals and names. Negative headline wording was not imposed on historical or charitable reference material.

## Deploy procedure used

The source workflow now publishes the custom domain directly. Source `main` must only receive a validated release commit. The separate legacy mirror is updated manually from the same `dist`, retaining its guides and symlink. Its deliberate CNAME removal must be preserved to avoid a domain ownership conflict. Verify both publishing operations and the actual custom-domain pages after pushing.
