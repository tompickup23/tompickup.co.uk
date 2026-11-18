# Content Security Policy: inventory, November 2026

Register row R289. Written on 8 November 2026 from the built output of `origin/main` at
9615e809 (70 pages), so the next session can write the policy against it. No policy is
set yet. This file is the inventory only.

GitHub Pages sends no custom headers, so the policy has to be a
`<meta http-equiv="Content-Security-Policy">` tag. A meta policy cannot carry
`frame-ancestors`, `report-uri`, `report-to` or `sandbox`; those wait for a host that sends
headers. The referrer policy (`strict-origin-when-cross-origin`) went in as a meta tag in
`src/layouts/Layout.astro` in batch C.

## How the inventory was taken

Every `.html` and `.js` file under the baseline `dist/` was scanned for script tags (with
and without `src`), `link` tags, `iframe`, `img`, `source` and `video` hosts, `fetch()`
calls, inline event handler attributes, `style` attributes and blocks, `data:` URIs and
`eval` or `new Function`. Counts are of tags in the built files.

## Pages that use the site layout (68 pages)

| Resource | What and where | Count | Directive it needs |
|---|---|---|---|
| Astro bundle | `/_astro/page.<hash>.js`, module script | 70 pages | `script-src 'self'` |
| Astro inline modules | Hoisted component scripts: mobile menu toggle, scroll reveal, IntersectionObserver check and others | 141 tags on 68 pages | a hash per distinct script, or `'unsafe-inline'` |
| Inline classic script | `document.documentElement.classList.add('js')` in the head of every layout page. Removed on 11 November 2026 with the scroll reveal observer (branch design/motion-layer-2026-11); nothing read the class. The same branch adds one small inline module script in its place, a single scroll listener that sets `motion-armed` on the root | 72 tags at the time of this inventory | a hash (one script, same text everywhere) |
| Structured data | `<script type="application/ld+json">` | 117 tags on 68 pages | none: data blocks are not executed, so script-src does not apply |
| Cloudflare Web Analytics beacon | `https://static.cloudflareinsights.com/beacon.min.js`, `defer`, with a public site token in `data-cf-beacon` | 68 pages (not the two standalone pages below) | `script-src https://static.cloudflareinsights.com`; `connect-src https://cloudflareinsights.com` for the beacon's reports |
| Stylesheets | Astro CSS bundles under `/_astro/`, preloads | 124 stylesheet and 136 preload links | `style-src 'self'` |
| Style blocks and attributes | 70 `<style>` blocks; 1,624 `style="..."` attributes (chart widths, panel colours, article HTML) | all pages | `style-src 'unsafe-inline'`, or `style-src-attr 'unsafe-inline'` plus hashes for the blocks |
| Fonts | `@fontsource` packages, self hosted as `/_astro/*.woff2` (46 files); no `url()` to another host in the CSS | all layout pages | `font-src 'self'` |
| Images | All same origin (`/_astro/`, `/images/`); no external image host found | all pages | `img-src 'self'`; add `data:` only if a page is found to need it |
| Same-origin fetch | `/lgr/contracts/` fetches `/data/lgr-contracts.json` | 1 page | `connect-src 'self'` |
| YouTube embed | One `<iframe src="https://www.youtube-nocookie.com/embed/...">` on `/news/where-your-council-tax-goes-burnley/`. The five other video articles link to `www.youtube.com` and embed nothing | 1 page | `frame-src https://www.youtube-nocookie.com` |
| Contact form | `/contact/` builds a `mailto:` URL in script and sets `window.location`; no form posts anywhere | 1 page | `form-action 'none'` is safe only if the submit handler always prevents the default; test it |
| Inline event handlers | None found (`onclick=` and the like) | 0 | none |
| `eval`, `new Function`, workers | None found | 0 | no `'unsafe-eval'` |

## The two standalone pages (no site layout)

Both skip `Layout.astro`, so a policy set in the layout does not reach them. Each needs its
own meta tag in its wrapper, and each wrapper should also get the referrer meta when the
policy goes in.

### `/lcc-budget-game/`

- Wrapper `src/pages/lcc-budget-game/index.astro`; the game is `src/data/lcc-budget-game/game.html`, generated in Reports and inlined with `set:html`. It must not be edited by hand.
- One large inline classic script (the whole game, drawing with `innerHTML` and inline SVG). Its hash changes every time Reports regenerates the game, so either the wrapper computes the hash at build time from the raw file, or the page uses `'unsafe-inline'` for scripts.
- Google Fonts: `preconnect` to `https://fonts.googleapis.com` and `https://fonts.gstatic.com`, stylesheet from `fonts.googleapis.com` (Public Sans, IBM Plex Mono and one more family). Needs `style-src https://fonts.googleapis.com` and `font-src https://fonts.gstatic.com`, or the fonts moved to the self-hosted `@fontsource` copies the site already ships.
- No Cloudflare beacon on this page. Links to `council.lancashire.gov.uk` are plain anchors and need no directive.
- Inline `<style>` blocks in the wrapper and the game.

### `/burnley-budget-game/`

- Wrapper `src/pages/burnley-budget-game/index.astro`; the game is `src/data/burnley-budget-game/game.html`, generated in Reports (`Burnley_Budget_Model_work/game/build.py`) and inlined with `set:html`. It must not be edited by hand; `scripts/sync-burnley-game.sh` copies it in.
- One inline classic script and inline `<style>` blocks, as the Lancashire game. No web fonts, no external links and no request outside the page. No beacon.
- Reports also builds the same game as three files (`game/dist/`: `index.html`, `game.css`, `game.js`) with nothing inline, made for a strict policy. When the policy goes in, the wrapper can serve those instead of the one-file form.

### `/lancashire-voice/` (noindex, unlinked prototype)

- Wrapper `src/pages/lancashire-voice/index.astro`; the page is `src/data/lancashire-voice/prototype.html`, generated in Reports (`Lancashire_Voice_Game/build.py`), whose build already rejects any network, storage or form call.
- One inline classic script (285 KB page). Same hash problem as the game.
- Google Fonts, as the game (Public Sans and IBM Plex Mono among them).
- No beacon. Every other external URL in it (gov.uk, legislation.gov.uk, council sites) is a plain link.

## Points for whoever writes the policy

1. Start with `default-src 'self'` and add only the hosts above.
2. The inline module scripts are the hard part. If the Astro version in use can emit them as external files, or can produce CSP hashes for them itself, `script-src` can drop `'unsafe-inline'` for the layout pages. Check this against the Astro version on main at the time, since PR 72 (Astro 7.4.4) may change what is available.
3. A meta CSP applies from the point it is parsed, so it must be the first thing in `<head>` after `charset`.
4. `object-src 'none'` and `base-uri 'self'` cost nothing and should go in.
5. Test with `Content-Security-Policy-Report-Only` first. That needs a header, which Pages cannot send, so test locally with `astro preview` behind a header, then ship the meta tag.
6. Rerun this inventory after PR 72 lands and whenever a new embed or third-party script is added.
