# Content Generation Rules

Rules for generating articles, videos, and shareable assets at tompickup.co.uk.
These rules apply to ALL content in the campaign calendar.

---

## 1. Branding

### Reform UK Logo Variant
- **Lancashire-tier articles** (T1): Use `variant='lancashire'` -- shows "REFORM UK" + "LANCASHIRE" subtitle
- **Burnley-tier articles** (T2): Use `variant='burnley'` -- shows "REFORM UK" + "BURNLEY & PADIHAM" subtitle
- **National-tier articles** (T3): Use `variant='lancashire'` (we are Lancashire councillors, even on national topics)
- **NEVER use `variant='full'`** for public content -- it shows only "REFORM UK" without location context

### Where the Logo Appears
- **Video overlay** (top-right): `apply_overlays()` and `add_branding()` in generate_video.py
- **Cover images**: `create_cover_image()` in generate_image.py -- top-right
- **Stat cards**: `create_stat_card()` in generate_image.py -- top-right
- **Data viz cards**: `create_data_viz_card()` in generate_image.py -- top-right

---

## 2. Fact-Checking Rules (MANDATORY)

Every article MUST be fact-checked before publication. These rules are non-negotiable.

### Council Tax
- **CORRECT**: "Lowest rise in 12 years" / "Lowest rise in over a decade"
- **CORRECT**: "After two years of 4.99% under the Conservatives" (emphasise last 2 years)
- **CORRECT**: "The Conservatives raised council tax by the maximum allowed every single year for a decade"
- **CORRECT**: "They never once came in below the referendum cap"
- **FRAMING**: We are NOT celebrating a rise. We are stopping the rot. "Still too high. But the direction has changed."
- **EMPHASIS**: Lead with "lowest in 12 years in our first budget" -- the 4.99% contrast is the punch
- **WRONG**: "They raised it by 4.99% every year" (most years were 3.99%, the cap varied)
- **WRONG**: "The legal maximum in seven of their eight years" (it was the max EVERY year, but 4.99% only in the final two)
- **ASC NUANCE (CRITICAL)**: The 3.80% total has TWO components: 1.80% core (political choice, below 2.99% cap) + 2.00% ASC precept (demand-driven, ring-fenced by law for adult social care). EVERY council in England uses the full ASC precept because of demographic demand. The political choice is the core rate only. Reform went 1.19% below the cap on core. The Conservatives maxed out BOTH components (2.99% + 2.00% = 4.99%)
- **NEVER** say "lowest of four" when comparing council tax rises -- the rise is lowest in 12 years, but "four authorities" refers to who sets the bill, not a ranking
- **CORRECT**: "1.80% core rise, 1.19% below the cap" / "First time below the cap in over a decade"

### Savings & Budget
- **NEVER claim 100% savings delivery** -- the 2025/26 year has not been formally audited
- **NEVER claim specific delivery percentages** -- frame around the overspend trajectory instead
- **CORRECT**: "Inherited a £28 million overspend, reduced to £6.2 million at Q3 -- a 78% reduction"
- **CORRECT**: "Conservative savings programme delivered just 48% in 2024/25" (verified from outturn)
- **CORRECT**: "We have identified £5 in potential savings for every £100 spent" (AI DOGE analysis)
- **FRAMING**: Focus on the overspend trajectory (£28M to £6.2M) and the £5 per £100 identified savings, not claimed delivery percentages

### Care Homes
- **NEVER say "the Conservatives planned to close care homes"** -- there is no documented Conservative cabinet decision to close them
- **NEVER say "we cancelled Tory closures"** -- same reason
- **CORRECT**: Reform launched an 8-week consultation on 5 care homes facing a £5M maintenance backlog
- **CORRECT**: 1,600 residents responded. Reform listened and kept all five open.
- **CORRECT**: National context -- council-run care collapsed from 64% of beds (1979) to ~4% today (CMA, 2023)
- **CORRECT**: 1,578 care homes closed nationally between 2015-2020 (nearly 50,000 residents displaced)
- **FRAMING**: "Against the national trend" -- Reform chose to invest, not close

### Highways / Roads
- **NEVER brag about road improvements** -- the roads are a mess and residents know it
- **Frame as inherited problem**: "£650M backlog created by many years of underinvestment"
- **Use "many years" not "8 years" or "decade"** -- the backlog predates the most recent administration
- **Compare with verified government waste figures**:
  - Foreign aid: **£13 billion/year** (House of Commons Library, 2025/26). Backlog = 18 days.
  - Asylum hotels: **£5.77 million/day** (Home Office, 2024/25). Backlog = 113 days.
  - Chagos Islands: **~£10 billion real terms** over 99 years (Full Fact). 15x the backlog.
  - HS2 spent to date: **£40.5 billion** (Construction Enquirer). Backlog is 1.6%.
  - National roads backlog: **£18.6 billion** (ALARM Survey 2026). Lancashire is 3.5%.
- **WRONG**: "£9.6 billion per year on foreign aid" (outdated -- it is ~£13 billion)
- **WRONG**: "Chagos deal cost £9 billion" (it is ~£10 billion in real terms, ~£3.4B NPV)
- Reform response: £45M resurfacing plan, AI defect detection on bin lorries
- Roads should appear LATE in video scene order (never as a leading stat)

### Bonds / Financial Damage
- **Always include** when discussing the Conservative record
- **Bond discovery**: Reform's financial scrutiny of the Statement of Accounts uncovered the losses. Do NOT attribute personally to Tom -- he is an integral part of the process but does not take sole credit
- £921.5M documented damage (audited accounts, 2017-2025)
- £600M UKMBA bond portfolio, ~£350M estimated loss if sold today
- Total rises to £1.27 billion including unrealised bond losses
- **FRAMING**: "Never transparently reported to councillors or the public"
- **Combine financial damage + bonds into one section** with a clear total (£1.27B), then compare below to other figures (highways backlog, annual budget, asylum spending days)

### General Fact-Check Process
1. Every numerical claim must have a named source (LCC report, GOV.UK, House of Commons Library, etc.)
2. Never round up or exaggerate -- the real numbers are damning enough
3. Distinguish between "audited" (Statement of Accounts) and "estimated" (bond portfolio loss)
4. Date-stamp comparisons -- "2024/25 outturn" not just "last year"
5. If a figure cannot be verified to an official source, do not use it

### Five Core Stats (repeat in every piece of content)
1. **£1.27B** total Tory financial damage (£921.5M audited losses + £350M bond exposure)
2. **3.80%** council tax rise -- lowest in 12 years, in our first budget (was 4.99% for last 2 years)
3. **All 5** care homes saved -- against the national trend
4. **£28M to £6.2M** overspend reduction (78%). £5 in savings identified for every £100 spent.
5. **We are just getting started** -- reversed the 4.99% rot, stopping the rot, trajectory changed

---

## 3. Writing Style Rules (MANDATORY)

### Zero Emdashes
- **NEVER use emdashes** (the long dash character) in any content: articles, video scripts, image text, alt text
- Use commas, full stops, colons, or restructure the sentence instead
- Also avoid en-dashes in prose -- use "to" for ranges (e.g. "2017 to 2025" not "2017-2025" in body text)
- Markdown tables and technical references may use hyphens

### Zero AI Signs
- No flowery language, no "delve into", no "it's important to note", no "in conclusion"
- Short sentences. Punchy. Active voice. Direct.
- Write like a Northern politician, not a chatbot
- No smart quotes -- use straight apostrophes and quote marks

### Tone
- Factual and confident, never boastful
- Let the numbers speak -- do not oversell
- Anger at waste, pride in fixing it, determination to do more
- Close with forward momentum: "We are just getting started"

---

## 4. Cover Images

Every article MUST have a branded cover image generated via `generate_image.py --assets {slug}`.

### What Gets Generated
- **OG cover** (1200x628): Used as `image:` in article frontmatter and og:image meta tag
- **Square cover** (1080x1080): For Instagram/WhatsApp/Facebook sharing
- **Video poster** (1920x1080): Used as `poster=` attribute on the `<video>` element

### Cover Image Design
- Base photograph with dark bar safe zone (bottom 32%) and gradient transition
- Reform UK logo (top-right, measured before placement) with location variant
- Article title within the safe zone (survives CSS cropping)
- Tag pills (above title, teal)
- Full watermark bar (Reform UK Lancashire/Burnley + tompickup.co.uk)

### In Article Frontmatter
```yaml
image: "/images/share/{slug}/{slug}-cover.jpg"
```

### Video Poster
```html
<video poster="/images/share/{slug}/{slug}-poster.jpg">
```

---

## 5. Data Visualisation Images

At least some of each article's key data visualisations MUST be available as saveable
social media images with Reform branded overlays.

### Required Per Article
- **Headline stats card**: The 3-4 key numbers from the article
- **At least 1 table or comparison**: The main data table or before/after comparison
- **Financial damage card** (if applicable): Combined losses + bond exposure + total

### Types Available
- `stat_grid`: Grid of 3-4 stat cards with values, labels, accent colors
- `table`: Data table with headers, rows, and optional highlighted row
- `comparison`: Before/after split panel

### Viz Text Sizing (BlogPost.astro)
- **Values**: 2.25rem base, 2.75rem lg, 3.25rem xl -- large and bold
- **Labels**: 0.9375rem, font-weight 600, colour #b0b0b8 -- readable, not grey
- **Sublabels**: 0.8125rem, colour #8e8e93 -- visible context, not tiny grey text
- **Section headers** (viz-label): 0.8125rem, colour #8e8e93
- All text must be comfortably readable on mobile. If in doubt, go bigger.

### How to Add
1. Add preset data in `ARTICLE_SHAREABLE_PRESETS` dict in generate_image.py
2. Run `python3 scripts/generate_image.py --assets {slug}`
3. Add download links in article markdown inside `<div class="shareable-assets">`
4. Embed key data viz inline using `<div class="article-data-viz">` wrapper

### In Article Markdown (collapsible download section)
```html
<div class="shareable-assets">
  <details>
    <summary>Download shareable images for social media</summary>
    <div class="share-grid">
      <a href="/images/share/{slug}/{slug}-{viz-name}.png" download>Label</a>
    </div>
  </details>
</div>
```

### Inline Data Viz (within article body)
```html
<div class="article-data-viz">
<a href="/images/share/{slug}/{slug}-{viz-name}.png" download title="Download this image to share on social media">
<img src="/images/share/{slug}/{slug}-{viz-name}.png" alt="Description" loading="lazy" />
<span class="download-hint">Tap to download for social media</span>
</a>
</div>
```

---

## 6. Video Content Rules

### Reform UK Overlay
- Top-right: Reform UK logo with LANCASHIRE or BURNLEY subtitle (per tier)
- Top-left: Date badge (red, auto-formatted)
- Bottom: White marquee bar with scrolling context text
- Fallback text if logo fails: "REFORM UK LANCASHIRE" (never just "REFORM UK")

### Voice
- **Political/data content** (default): Piper Northern English Male
- **Softer/community topics**: Kokoro bm_daniel or bf_alice
- Voice assignments in `voice_config.json`

### TTS Pronunciation Rules (CRITICAL)
TTS engines (Piper, Kokoro) read special characters literally. ALL voiceover_text MUST be written as spoken English:

- **URLs**: Write "tom pickup dot co dot UK" NOT "tompickup.co.uk"
- **Slashes**: Write "twenty twenty-four to twenty-five" NOT "2024/25"
- **Percentages**: Write "three point eight percent" NOT "3.80%"
- **Currency**: Write "six hundred and fifty million pounds" NOT "£650M"
- **Abbreviations**: Write "Lancashire County Council" NOT "LCC"
- **Fractions**: Write "one point two seven billion" NOT "£1.27B"
- **NEVER include** `/`, `%`, `£`, `&`, `@`, `#` or any special characters in voiceover_text
- **NEVER include** URLs in their raw form -- always spell out as spoken words
- **Avoid possessive apostrophes**: Piper TTS mangles contractions and possessives. Write "Reform" not "Reform's", "That is" not "That's", "The Lancashire road" not "Lancashire's road", "The council budget" not "The council's budget"
- **Test**: Read the voiceover_text aloud. If it sounds unnatural, rewrite it.

### Script Fact-Check Rules
- Apply ALL rules from Section 2 (Fact-Checking Rules) to video scripts
- voiceover_text, labels, sublabels, and extra_lines must all be fact-checked
- The video is often the ONLY content a voter sees -- accuracy is critical

### Scene Order Priority (for video)
1. Hook (seats won, majority)
2. Council tax (3.80%, lowest in 12 years, contrast with 4.99%)
3. Overspend (£28M to £6.2M, Conservative 48% delivery, £5 per £100 identified)
4. Care homes (5 saved, against national trend)
5. Financial damage (£1.27B total: £921.5M audited + £350M bond exposure)
6. Roads (inherited backlog, government priorities) -- ALWAYS LAST among substantive scenes
7. CTA

---

## 7. Article Content Rules

### Viz-Class System (BlogPost.astro)
- **ALWAYS** use the viz-class CSS system (viz-stat, viz-grid, viz-comparison, viz-callout, viz-panel-reform)
- **NEVER** use inline `style=""` attributes for layout/structure (only for stat value colors)
- Colors are set via the variant classes: `.teal`, `.green`, `.red`, `.orange`
- Inline color is acceptable ONLY on `<span class="value">` elements

### Tables
- Use clean markdown tables (no inline styles)
- BlogPost.astro CSS provides dark navy gradient, teal headers, rounded corners automatically
- Let the CSS system handle all styling

### Stat Grids
```html
<div class="viz-panel-reform">
<div class="viz-grid viz-grid-4">
<div class="viz-stat teal">
<span class="value xl" style="color: #12b6cf;">VALUE</span>
<span class="label">Label</span>
<span class="sublabel">Context</span>
</div>
</div>
</div>
```

### Section Labelled Panels
Use `viz-label` above panels for titled sections:
```html
<div class="viz-label teal">Section Title</div>
<div class="viz-panel-reform">
  ...stat grid or content...
</div>
```

### Comparison Panels
```html
<div class="viz-comparison">
<div class="side before">
<span class="value" style="color: #ff453a;">OLD</span>
<span class="label">Before Label</span>
</div>
<div class="side after">
<span class="value" style="color: #30d158;">NEW</span>
<span class="label">After Label</span>
</div>
</div>
```

### Financial Damage Template (combined section)
Always combine audited losses + bond exposure into a single section with total, then add comparison panel below:
```html
## Financial Damage: The Full Conservative Record
{£921.5M audited losses breakdown}
{£350M bond exposure}
{Total: £1.27B}

<viz-panel: £921.5M / £350M / £1.27B total>

<viz-label: "What £1.27B Looks Like">
<viz-panel: highways backlog / annual budget / asylum days comparison>
```

### Highways Section Template
Always frame roads as inherited problem with verified government waste comparison:
```markdown
## Roads: The Backlog They Left Behind
{£650M backlog, many years of underinvestment}
{Comparison table: foreign aid £13B/yr (18 days), asylum hotels £5.77M/day (113 days),
 Chagos ~£10B real terms, HS2 £40.5B, national backlog £18.6B}
{Reform's response: £45M plan, AI defect detection}
{Clear blame: inherited problem, central government priorities}
```

---

## 8. Asset Generation Workflow

For every new article:

```bash
# 1. Write article markdown in src/content/news/{slug}.md

# 2. Add article presets to generate_image.py:
#    - ARTICLE_PRESETS dict (stat card)
#    - ARTICLE_SHAREABLE_PRESETS dict (cover + viz cards)

# 3. Add video scenes to generate_video.py:
#    - New function generate_{name}_video()
#    - Register in ARTICLE_GENERATORS dict

# 4. FACT-CHECK all content against Section 2 rules
#    - Every number has a named source?
#    - No exaggeration or rounding up?
#    - TTS voiceover text uses spoken English (no special characters)?
#    - Council tax framed as "lowest in 12 years", not celebrating a rise?
#    - Care homes: "consulted and listened", not "cancelled Tory closures"?
#    - Savings: overspend trajectory + £5 per £100, not claimed delivery percentage?
#    - Bonds: Reform's scrutiny found them, not personal attribution?
#    - Zero emdashes in all text?
#    - Zero AI-sounding language?

# 5. Generate all assets:
/usr/bin/python3 scripts/generate_image.py --assets {slug}
/usr/bin/python3 scripts/generate_video.py --article {slug}

# 6. Build and test locally:
export PATH="/opt/homebrew/bin:/usr/bin:/usr/local/bin:/bin:$PATH"
npx astro build

# 7. Commit and push (auto-deploys via GitHub Actions)
```

---

## 9. Campaign Strategy Reference

See `/Users/tompickup/.claude/plans/bright-forging-shell.md` for:
- Full 51-day content calendar (17 Mar to 7 May 2026)
- Bannon framework (Expose, Prove, Empower)
- Coal Clough ward targeting strategy
- Platform distribution priorities
- Voice assignments per article

### Key Messaging Principles
- **Council tax**: "Lowest rise in 12 years in our first budget. After two years of 4.99%. Stopping the rot."
- **Savings**: "Inherited £28M overspend, cut to £6.2M. £5 savings for every £100 identified. Their savings hit 48%."
- **Care homes**: "We consulted 1,600 residents and listened. Against the national trend."
- **Roads**: "Inherited backlog. 113 days of asylum hotel spending would fix every road in Lancashire."
- **Financial damage**: "£1.27B total. £921.5M audited losses plus £350M in concealed bond exposure."
- **Momentum**: "We reversed the 4.99% rot. The trajectory has changed. We are just getting started."
