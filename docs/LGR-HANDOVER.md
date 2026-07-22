# LGR Model — Handover (18 Jul 2026)

## State
- **PR #1** (branch `claude/vps-control-iphone-ap4ub0` → main) holds the complete Lancashire LGR model. All work committed & pushed. Site builds clean (`npm run build`).
- **Not live**: deploy step in `.github/workflows/deploy.yml` is disabled (`if: false`) pending a fresh fine-grained PAT in the `DEPLOY_TOKEN` secret (scope: tompickup23.github.io, Contents RW). Remove `if: false` after updating the secret.

## Architecture — do not hand-edit figures in pages
- Canonical sourced data: `src/data/lgr/*.json` (authorities, county, decision, precedents, cca, pensions, government, scenarios). Every figure has source + confidence.
- Build: `python3 scripts/lgr_build.py` → `src/data/lgr/model.json` (imported by `src/pages/lgr.astro`) + public feed `public/data/lgr-model.json`. Integrity assertions fail the build on bad sums.
- To change any number: edit the canonical file, re-run the script, rebuild.

## Key verified facts (sources in the JSON files)
- Decision 16 Jul 2026: 15→4 unitaries (N/W/E/S Lancashire), shadow elections May 2027, vesting 1 Apr 2028. Approved plan was the six-council proposal (£81.9m/yr claimed from 2032/33).
- Combined budgets: E £673.0m, W £469.8m, N £450.4m, S £426.6m (needs-basis E £714.2m). All 15 councils' 2026/27 budgets individually sourced.
- Band D gaps: N £204.65, E £90.23, W £85.23, S £52.86. Blackpool debt £606.8m vs £34.7m reserves. LCPF £12bn, 134% funded. 142 LCC contracts (£1.03bn) straddle vesting.
- Scenarios: payback 2030/31–2036/37; 10-yr net +£585m/+£337m/+£28m.

## Open items (priority order)
1. DEPLOY_TOKEN + re-enable deploy, merge PR #1.
2. Boundary map: geojson at ukdemographics repo `data/geography/lad24-simplified.geojson` → inline SVG choropleth on /lgr/.
3. Confirm 5 medium-confidence Band D figures (Lancaster, Preston, Ribble Valley, Fylde, Chorley) against formal council-tax resolutions; update authorities.json.
4. District/unitary contracts mapping (register is LCC-only).
5. CCA carve-out when a published disaggregation exists (`cca.json` ready).
6. Per-unitary deep-link pages; OG share image for /lgr/.

## Data sources used
AI DOGE Drive pack (DOGE folder / Cross-council / Lancashire), ukdemographics + ukelections repos (cloned at /workspace/ in the old session — re-clone if needed), MHCLG returns, each council's own budget papers (URLs in authorities.json).
