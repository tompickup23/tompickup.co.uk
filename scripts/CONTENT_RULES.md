# Content Generation Rules

Rules for generating articles, videos, and shareable assets at tompickup.co.uk.
These rules apply to ALL content in the campaign calendar.

---

## 1. Branding

### Reform UK Logo Variant
- **Lancashire-tier articles** (T1): Use `variant='lancashire'` — shows "REFORM UK" + "LANCASHIRE" subtitle
- **Burnley-tier articles** (T2): Use `variant='burnley'` — shows "REFORM UK" + "BURNLEY & PADIHAM" subtitle
- **National-tier articles** (T3): Use `variant='lancashire'` (we are Lancashire councillors, even on national topics)
- **NEVER use `variant='full'`** for public content — it shows only "REFORM UK" without location context

### Where the Logo Appears
- **Video overlay** (top-right): `apply_overlays()` and `add_branding()` in generate_video.py
- **Cover images**: `create_cover_image()` in generate_image.py — top-right
- **Stat cards**: `create_stat_card()` in generate_image.py — top-right
- **Data viz cards**: `create_data_viz_card()` in generate_image.py — top-right

---

## 2. Cover Images

Every article MUST have a branded cover image generated via `generate_image.py --assets {slug}`.

### What Gets Generated
- **OG cover** (1200x628): Used as `image:` in article frontmatter and og:image meta tag
- **Square cover** (1080x1080): For Instagram/WhatsApp/Facebook sharing
- **Video poster** (1920x1080): Used as `poster=` attribute on the `<video>` element

### Cover Image Design
- Base photograph with dark gradient overlay (bottom 60%)
- Reform UK logo (top-right) with location variant
- Article title (bottom, white, bold)
- Tag pills (above title, teal)
- tompickup.co.uk watermark (bottom-right, subtle)

### In Article Frontmatter
```yaml
image: "/images/share/{slug}/{slug}-cover.jpg"
```

### Video Poster
```html
<video poster="/images/share/{slug}/{slug}-poster.jpg">
```

---

## 3. Data Visualisation Images

At least some of each article's key data visualisations MUST be available as saveable
social media images with Reform branded overlays.

### Required Per Article
- **Headline stats card**: The 3-4 key numbers from the article
- **At least 1 table or comparison**: The main data table or before/after comparison
- **Financial damage card** (if applicable): Treasury/bond/overspend data

### Types Available
- `stat_grid`: Grid of 3-4 stat cards with values, labels, accent colors
- `table`: Data table with headers, rows, and optional highlighted row
- `comparison`: Before/after split panel

### How to Add
1. Add preset data in `ARTICLE_SHAREABLE_PRESETS` dict in generate_image.py
2. Run `python3 scripts/generate_image.py --assets {slug}`
3. Add download links in article markdown inside `<div class="shareable-assets">`

### In Article Markdown
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

---

## 4. Video Content Rules

### Reform UK Overlay
- Top-right: Reform UK logo with LANCASHIRE or BURNLEY subtitle (per tier)
- Top-left: Date badge (red, auto-formatted)
- Bottom: White marquee bar with scrolling context text
- Fallback text if logo fails: "REFORM UK LANCASHIRE" (never just "REFORM UK")

### Voice
- **Political/data content** (default): Piper Northern English Male
- **Softer/community topics**: Kokoro bm_daniel or bf_alice
- Voice assignments in `voice_config.json`

### Script Content Rules

#### Council Tax Claims
- **CORRECT**: "The Conservatives raised council tax by the maximum allowed every single year for a decade"
- **CORRECT**: "They never once came in below the referendum cap"
- **WRONG**: "They raised it by 4.99% every year" (most years were 3.99%, the cap varied)
- **WRONG**: "The legal maximum in seven of their eight years" (it was the max EVERY year)

#### Highways / Roads
- **NEVER brag about road improvements** — the roads are a mess and residents know it
- **Frame as inherited Tory problem**: "£650M backlog created by a decade of Conservative neglect"
- **Compare with government waste**: foreign aid (£9.6B/yr), Chagos (£9B), asylum costs
- **Reform is doing its bit**: £45M resurfacing plan, AI defect detection, £5 savings per £100 spent
- **Core message**: "Avoidable. Fixable. A matter of government priorities."
- Roads should appear LATE in video scene order (never as a leading stat)

#### Bonds / Financial Damage
- **Always include** when discussing the Conservative record
- £921M documented damage (audited accounts, 2017-2025)
- £600M UKMBA bond portfolio, ~£350M estimated loss if sold today
- Total rises to £1.27 billion including unrealised bond losses
- "Bought without proper disclosure" / "concealed"

#### Five Core Stats (repeat in every piece of content)
1. **£921M** lost under the Conservatives (Statement of Accounts, audited)
2. **3.80%** council tax rise — lowest in Lancashire in 12 years
3. **All 5** care homes saved after consultation
4. **100%** savings delivery (vs Conservative 48%)
5. **£1.27B** total financial damage including bonds

#### Scene Order Priority (for video)
1. Hook (seats won, majority)
2. Council tax (3.80%, max every year)
3. Savings delivery (48% vs 100%)
4. Care homes (5 saved)
5. Financial damage (£921M)
6. Bonds (£600M portfolio, £350M loss)
7. Roads (inherited backlog, gov priorities) — ALWAYS LAST among substantive scenes
8. CTA

---

## 5. Article Content Rules

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

### Highways Section Template
Always frame roads as inherited problem with government waste comparison:
```markdown
## Roads: The Backlog They Left Behind

{Tory £650M backlog, their fault, decade of neglect}

{Comparison: foreign aid £9.6B/yr, Chagos £9B, asylum costs — entire backlog < 1 month of aid}

{Reform's response: £45M plan, AI, £5 per £100 savings reinvested}

{Clear blame: "This is a problem the Conservatives created... central government could resolve quickly"}
```

---

## 6. Asset Generation Workflow

For every new article:

```bash
# 1. Write article markdown in src/content/news/{slug}.md

# 2. Add article presets to generate_image.py:
#    - ARTICLE_PRESETS dict (stat card)
#    - ARTICLE_SHAREABLE_PRESETS dict (cover + viz cards)

# 3. Add video scenes to generate_video.py:
#    - New function generate_{name}_video()
#    - Register in ARTICLE_GENERATORS dict

# 4. Generate all assets:
python3 scripts/generate_image.py --assets {slug}
python3 scripts/generate_video.py --article {slug}

# 5. Build and test locally:
export PATH="/opt/homebrew/bin:$PATH"
npx astro build

# 6. Commit and push (auto-deploys via GitHub Actions)
```

---

## 7. Campaign Strategy Reference

See `/Users/tompickup/.claude/plans/bright-forging-shell.md` for:
- Full 51-day content calendar (17 Mar → 7 May 2026)
- Bannon framework (Expose → Prove → Empower)
- Coal Clough ward targeting strategy
- Platform distribution priorities
- Voice assignments per article
