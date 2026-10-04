#!/usr/bin/env python3
"""Share cards (Open Graph images) for tompickup.co.uk, in the site's light
editorial style: warm paper, ink, teal and heritage gold, set in Source Serif 4
and Source Sans 3 (the same @fontsource files the site ships).

1200x630, one per article, plus the site default public/og-image.jpg. Several
articles also show their card in the body, so the figures and wording here are
published figures: change one only with its source, never to fit the layout.

Run after `npm install`:  uv run --no-project --with pillow python scripts/generate_og_cards.py
"""
import os
from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630
PAPER = (250, 249, 245)      # #faf9f5
INK = (27, 48, 53)           # #1b3035
MUTED = (83, 99, 104)        # #536368
TEAL = (23, 100, 94)         # #17645e
GOLD = (128, 92, 37)         # #805c25
LINE = (212, 215, 208)       # #d4d7d0
TRACK = (235, 233, 226)      # #ebe9e2
CHART_1 = (0, 132, 111)      # #00846f, chart teal
CHART_2 = (179, 90, 16)      # #b35a10, chart orange

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
FONTS = os.path.join(ROOT, "node_modules", "@fontsource")
FACES = {
    "serif": ("source-serif-4", "source-serif-4-latin-600-normal.woff2"),
    "sans": ("source-sans-3", "source-sans-3-latin-400-normal.woff2"),
    "sans-semibold": ("source-sans-3", "source-sans-3-latin-600-normal.woff2"),
    "sans-bold": ("source-sans-3", "source-sans-3-latin-700-normal.woff2"),
}


def F(kind, size):
    pkg, name = FACES[kind]
    path = os.path.join(FONTS, pkg, "files", name)
    if not os.path.exists(path):
        raise SystemExit(f"Missing {path}. Run npm install first.")
    return ImageFont.truetype(path, size)


def _tracked(d, xy, text, font, fill, track):
    x, y = xy
    for ch in text:
        d.text((x, y), ch, font=font, fill=fill)
        x += d.textlength(ch, font=font) + track
    return x


def _tracked_width(d, text, font, track):
    return sum(d.textlength(c, font=font) + track for c in text) - track


def _wrap(d, text, font, maxw):
    out, cur = [], ""
    for w in text.split():
        t = (cur + " " + w).strip()
        if d.textlength(t, font=font) <= maxw or not cur:
            cur = t
        else:
            out.append(cur)
            cur = w
    if cur:
        out.append(cur)
    return out


M = 72


def _frame(eyebrow):
    """Paper ground, wordmark, eyebrow, footer rule and address."""
    img = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 8], fill=TEAL)

    # Wordmark (left) and eyebrow (right) share a baseline
    d.text((M, 52), "Tom Pickup", font=F("serif", 36), fill=INK)
    if eyebrow:
        ef = F("sans-semibold", 19)
        etext = eyebrow.upper()
        track = 2.2
        _tracked(d, (W - M - _tracked_width(d, etext, ef, track), 64), etext, ef, GOLD, track)

    d.line([M, H - 84, W - M, H - 84], fill=LINE, width=2)
    d.text((M, H - 62), "tompickup.co.uk", font=F("sans-semibold", 26), fill=TEAL)
    return img, d


def _source(d, source):
    """Right-aligned source line, shrunk rather than cut if it is long."""
    text = "Source: " + source
    size = 22
    url_w = d.textlength("tompickup.co.uk", font=F("sans-semibold", 26))
    while size > 15 and d.textlength(text, font=F("sans", size)) > W - 2 * M - url_w - 40:
        size -= 1
    f = F("sans", size)
    d.text((W - M - d.textlength(text, font=f), H - 58 + (22 - size) / 2), text, font=f, fill=MUTED)


def card(stat, label, eyebrow, source, out):
    img, d = _frame(eyebrow)

    # Headline figure in the display serif, ink; a short teal rule beneath
    sf = F("serif", 150)
    sy = 128
    d.text((M - 4, sy), stat, font=sf, fill=INK)
    bb = d.textbbox((M - 4, sy), stat, font=sf)
    d.rectangle([M, bb[3] + 22, M + min(bb[2] - bb[0], 140), bb[3] + 28], fill=TEAL)

    # Label, up to three lines
    lf = F("sans-semibold", 36)
    ly = bb[3] + 50
    for line in _wrap(d, label, lf, W - 2 * M)[:3]:
        d.text((M, ly), line, font=lf, fill=INK)
        ly += 46

    _source(d, source)
    img.save(out, "PNG", optimize=True)
    return out


def bar_card(title, bars, note, source, out):
    """A two-bar comparison card. Bars start at zero and share one scale."""
    img, d = _frame(None)
    tf = F("serif", 40)
    y = 116
    for line in _wrap(d, title, tf, W - 2 * M)[:3]:
        d.text((M, y), line, font=tf, fill=INK)
        y += 49
    y += 14
    top = max(v for _, v, _ in bars)
    full = 760
    lf = F("sans-bold", 18)
    vf = F("sans-bold", 30)
    for label, value, colour in bars:
        _tracked(d, (M, y), label.upper(), lf, MUTED, 1.2)
        y += 27
        w = max(8, round(full * value / top))
        d.rectangle([M, y, M + full, y + 46], fill=TRACK)
        d.rectangle([M, y, M + w, y + 46], fill=colour)
        text = f"{value:g} GW"
        tw = d.textlength(text, font=vf)
        if tw + 40 < w:  # inside the bar, white on the fill (4.6:1 or better)
            d.text((M + 18, y + 5), text, font=vf, fill=(255, 255, 255))
        else:
            d.text((M + w + 14, y + 5), text, font=vf, fill=INK)
        y += 62
    d.text((M, y), note, font=F("sans", 25), fill=INK)
    # The source is long here, so it gets its own lines above the footer rule
    sf = F("sans", 18)
    lines = _wrap(d, "Source: " + source, sf, W - 2 * M)[:2]
    sy = H - 96 - 24 * len(lines)
    for line in lines:
        d.text((M, sy), line, font=sf, fill=MUTED)
        sy += 24
    img.save(out, "PNG", optimize=True)
    return out


def default_card(out):
    """The site-wide share image: name and office beside the chamber portrait."""
    img = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(img)
    photo_w = 520
    photo = Image.open(os.path.join(ROOT, "public", "images", "headshot.jpg")).convert("RGB")
    # Rectangular crop centred on the figure, filling the right-hand panel
    ph = photo.height
    pw = round(ph * photo_w / H)
    cx = round(photo.width * 0.5)
    left = max(0, min(photo.width - pw, cx - pw // 2))
    photo = photo.crop((left, 0, left + pw, ph)).resize((photo_w, H), Image.LANCZOS)
    img.paste(photo, (W - photo_w, 0))
    d.rectangle([0, 0, W - photo_w, 8], fill=TEAL)

    x = M
    d.rectangle([x, 210, x + 56, 214], fill=GOLD)
    d.text((x - 3, 236), "Tom Pickup", font=F("serif", 78), fill=INK)
    d.text((x, 344), "Lancashire County Councillor", font=F("sans-semibold", 32), fill=INK)
    d.text((x, 386), "Padiham and Burnley West", font=F("sans", 32), fill=MUTED)
    d.line([x, H - 84, W - photo_w - M, H - 84], fill=LINE, width=2)
    d.text((x, H - 62), "tompickup.co.uk", font=F("sans-semibold", 26), fill=TEAL)
    img.save(out, "JPEG", quality=86, optimize=True, progressive=True)
    return out


CARDS = [
    dict(out="lytham-supported-living.png", eyebrow="Lancashire  ·  Adult Social Care", stat="9",
         label="self-contained apartments with 24-hour support are being built in Lytham St Annes, for people with a learning disability and autistic people.",
         source="Lancashire County Council, August 2026"),
    dict(out="two-wind-farms-one-view.png", eyebrow="Lancashire  ·  Planning", stat="702",
         label="square kilometres would have a view of turbines of both wind farms. No assessment of them together had been published.",
         source="Bare earth model, OS Terrain 50 and the applicants' own turbine schedules"),
    dict(out="scout-moor-round-two.png", eyebrow="Lancashire  ·  Planning", stat="699",
         label="square kilometres would have a view of turbines of both wind farms. Cutting Scout Moor II from 17 turbines to 12 shrank that by 0.3 per cent.",
         source="Bare earth model, OS Terrain 50; Scout Moor II FEI and Calderdale PEIR turbine schedules"),
    dict(out="who-owns-burnley.png", eyebrow="Burnley housing  ·  1 / 3", stat="27.4%",
         label="of Burnley home sales are now buy-to-let, the highest rate of any district in Lancashire.",
         source="HM Land Registry, 2025"),
    dict(out="who-owns-burnley-the-names.png", eyebrow="Burnley housing  ·  2 / 3", stat="1,204",
         label="Burnley freeholds owned by a single ground-rent fund, the town's biggest property owner.",
         source="HM Land Registry, 2026"),
    dict(out="more-people-fewer-homes.png", eyebrow="Burnley housing  ·  3 / 3", stat="70%",
         label="of Burnley's population growth in a decade came from abroad, while local people are priced out of buying a home.",
         source="ONS Census 2011-2021"),
    dict(out="shared-houses.png", eyebrow="Burnley  ·  Housing", stat="916",
         label="shared houses in Burnley, by the council's own estimate. The official statistics record just 65.",
         source="Burnley Council / ONS, 2023"),
    dict(out="pip-burnley.png", eyebrow="Burnley  ·  Benefits", stat="10,323",
         label="people in Burnley claim PIP, the 47th highest of 543 seats in England.",
         source="DWP, January 2026"),
    dict(out="empty-homes.png", eyebrow="Burnley  ·  Housing", stat="638",
         label="homes in Burnley stand empty long-term, while 2,657 households wait for a home.",
         source="Burnley Council / MHCLG, 2024"),
    dict(out="where-your-rent-goes.png", eyebrow="Burnley  ·  Housing", stat="£622",
         label="the average private rent in Burnley, up 3.2% in a year, while housing support stays frozen.",
         source="ONS, May 2026"),
    dict(out="motability-burnley.png", eyebrow="Lancashire  ·  Benefits", stat="1 in 5",
         label="new cars in Britain now run on benefits. In Burnley, 4,540 people qualify, half of all local PIP claimants.",
         source="Motability / SMMT / DWP"),
    dict(out="pip-fraud.png", eyebrow="UK  ·  Benefits", stat="£6.8bn",
         label="lost to benefit fraud in a single year. PIP was just £410m of it. The system barely checks the rest.",
         source="DWP, FYE 2026"),
    dict(out="the-burnley-trap.png", eyebrow="Burnley  ·  Health", stat="76.5",
         label="a Burnley man's life expectancy, over three years below England, the end of a chain that starts with deprivation.",
         source="ONS / OHID, 2025"),
    dict(out="disability-work-trap.png", eyebrow="Lancashire  ·  Benefits", stat="86%",
         label="of people with a learning disability want to work. Barely one in twenty has a job.",
         source="Mencap / ONS"),
    dict(out="put-police-back.png", eyebrow="Burnley  ·  Crime", stat="+30%",
         label="how far violent crime in Burnley runs above the England average, while neighbourhood police have been cut by more than half.",
         source="ONS / Home Office"),
    dict(out="lancashire-crime-divide.png", eyebrow="Lancashire  ·  Crime", stat="4x",
         label="Blackpool's recorded crime rate against Ribble Valley's. Lancashire's crime map follows its deprivation.",
         source="ONS, 2026 (year to Dec 2025)"),
    dict(out="nine-in-ten-no-charge.png", eyebrow="Lancashire  ·  Crime", stat="9 in 10",
         label="crimes in Lancashire end with no one charged, and Lancashire is one of the better forces in England.",
         source="Home Office, 2025"),
    dict(out="burnley-spending-2025-26.png", eyebrow="Burnley  ·  Transparency", stat="£38m",
         label="every payment of £500 or more Burnley Council made in 2025/26, all 4,489 of them, searchable on AI DOGE.",
         source="Burnley Council, 2025/26"),
    dict(out="temporary-accommodation.png", eyebrow="Burnley  ·  DOGE", stat="1 in 9",
         label="Burnley homelessness cases now come from people leaving Home Office asylum accommodation, up from 1 in 60 two years ago.",
         source="MHCLG / Burnley Council"),
]

# Bespoke chart card. Wording and figures as first published on 36b131c.
BAR_CARDS = [
    dict(out="state-cannot-see-its-own-data-centres.png",
         title="Developers have asked to plug 72.8 GW of data centres into the grid. The system operator expects to connect 5.2 GW by 2030.",
         bars=[("Connection requests to 2039", 72.8, CHART_1), ("Forecast to be connected by 2030", 5.2, CHART_2)],
         note="A queue 14 times the forecast, for a sector the state cannot measure.",
         source="NESO written evidence to the Environmental Audit Committee (DCU0081). NESO says much of the queue is likely to be non-viable."),
]

# Cards for archived articles. Not written by default: public/ ships everything,
# and the build fails on an image no page links. Pass --archived to regenerate.
ARCHIVED_CARDS = [
    dict(out="what-the-councils-own.png", eyebrow="Burnley  ·  Transparency", stat="2,043",
         label="property titles in Burnley are owned by your two councils. I mapped every one I could place.",
         source="HM Land Registry, 2026"),
]

if __name__ == "__main__":
    import sys
    outdir = os.path.join(ROOT, "public", "images", "share")
    os.makedirs(outdir, exist_ok=True)
    todo = CARDS + (ARCHIVED_CARDS if "--archived" in sys.argv else [])
    for c in todo:
        p = os.path.join(outdir, c["out"])
        card(c["stat"], c["label"], c["eyebrow"], c["source"], p)
        print("wrote", os.path.relpath(p, ROOT))
    for c in BAR_CARDS:
        p = os.path.join(outdir, c["out"])
        bar_card(c["title"], c["bars"], c["note"], c["source"], p)
        print("wrote", os.path.relpath(p, ROOT))
    p = default_card(os.path.join(ROOT, "public", "og-image.jpg"))
    print("wrote", os.path.relpath(p, ROOT))
