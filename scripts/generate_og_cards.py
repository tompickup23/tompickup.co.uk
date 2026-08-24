#!/usr/bin/env python3
"""On-brand social / OG cards for the tompickup.co.uk Burnley housing series.

Dark teal aesthetic matching the site (accent #12b6cf, Sora + Manrope).
1200x630, one per article. Used as both the article hero image and the OG card.
Run: python3 scripts/generate_og_cards.py
"""
import os
from PIL import Image, ImageDraw, ImageFont, ImageFilter

W, H = 1200, 630
TEAL = (18, 182, 207)
WHITE = (244, 247, 251)
MUTED = (150, 162, 178)
DIM = (104, 116, 134)
PILL_BG = (16, 42, 56)
BG_TOP = (6, 10, 18)
BG_BOT = (11, 18, 32)
INK_ON_TEAL = (5, 16, 24)

HERE = os.path.dirname(os.path.abspath(__file__))
FONT_DIRS = [
    os.path.join(HERE, "assets", "fonts"),
    os.path.expanduser("~/ukelections/data/fonts"),
    os.path.expanduser("~/ukelections/src/assets/fonts"),
    "/System/Library/Fonts/Supplemental",
]
LOADED = set()


def _find(*names):
    for d in FONT_DIRS:
        for n in names:
            p = os.path.join(d, n)
            if os.path.exists(p):
                return p
    return None


def F(kind, size):
    table = {
        "display": ("Sora-ExtraBold.ttf", "Arial Black.ttf"),
        "bold": ("Manrope-Bold.ttf", "Arial Bold.ttf"),
        "semibold": ("Manrope-SemiBold.ttf", "Arial Bold.ttf"),
        "regular": ("Manrope-Regular.ttf", "Arial.ttf"),
    }[kind]
    p = _find(*table)
    if p:
        LOADED.add(os.path.basename(p))
        return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def _bg():
    col = Image.new("RGB", (1, H))
    cp = col.load()
    for y in range(H):
        t = y / H
        cp[0, y] = (
            int(BG_TOP[0] + (BG_BOT[0] - BG_TOP[0]) * t),
            int(BG_TOP[1] + (BG_BOT[1] - BG_TOP[1]) * t),
            int(BG_TOP[2] + (BG_BOT[2] - BG_TOP[2]) * t),
        )
    img = col.resize((W, H)).convert("RGBA")
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(glow).ellipse([W - 470, -280, W + 170, 350], fill=(18, 182, 207, 70))
    img = Image.alpha_composite(img, glow.filter(ImageFilter.GaussianBlur(130)))
    return img.convert("RGB")


def _tracked(d, xy, text, font, fill, track):
    x, y = xy
    for ch in text:
        d.text((x, y), ch, font=font, fill=fill)
        x += d.textlength(ch, font=font) + track
    return x


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


def card(stat, label, eyebrow, source, out):
    img = _bg()
    d = ImageDraw.Draw(img)
    M = 72
    d.rectangle([0, 0, W, 6], fill=TEAL)  # top accent

    # --- header: logo tile + wordmark (left) ---
    ty = 52
    d.rounded_rectangle([M, ty, M + 54, ty + 54], radius=14, fill=TEAL)
    tpf = F("display", 27)
    bb = d.textbbox((0, 0), "TP", font=tpf)
    d.text((M + 27 - (bb[2] - bb[0]) / 2, ty + 27 - (bb[3] - bb[1]) / 2 - bb[1]), "TP", font=tpf, fill=INK_ON_TEAL)
    wf = F("bold", 31)
    wx = M + 54 + 18
    d.text((wx, ty + 11), "Tom ", font=wf, fill=WHITE)
    d.text((wx + d.textlength("Tom ", font=wf), ty + 11), "Pickup", font=wf, fill=TEAL)

    # --- eyebrow pill (right) ---
    ef = F("semibold", 18)
    etext = eyebrow.upper()
    track = 2.0
    ew = sum(d.textlength(c, font=ef) + track for c in etext)
    pad = 24
    px2, py1 = W - M, ty + 9
    px1, py2 = px2 - (ew + pad * 2), py1 + 38
    d.rounded_rectangle([px1, py1, px2, py2], radius=19, fill=PILL_BG, outline=(30, 80, 100), width=1)
    _tracked(d, (px1 + pad, py1 + 9), etext, ef, TEAL, track)

    # --- hero stat ---
    sf = F("display", 158)
    sy = 184
    d.text((M, sy), stat, font=sf, fill=WHITE)
    sw = d.textlength(stat, font=sf)
    # teal accent underline
    d.rounded_rectangle([M + 4, sy + 178, M + 4 + min(sw, 150), sy + 188], radius=5, fill=TEAL)

    # --- label (wrapped) ---
    lf = F("semibold", 35)
    ly = sy + 214
    for line in _wrap(d, label, lf, W - 2 * M)[:3]:
        d.text((M, ly), line, font=lf, fill=(214, 222, 232))
        ly += 47

    # --- footer ---
    d.line([M, H - 78, W - M, H - 78], fill=(40, 52, 68), width=1)
    bf = F("bold", 26)
    d.text((M, H - 56), "tompickup.co.uk", font=bf, fill=TEAL)
    srcf = F("regular", 22)
    sx = W - M - d.textlength("Source: " + source, font=srcf)
    d.text((sx, H - 53), "Source: " + source, font=srcf, fill=DIM)

    img.save(out, "PNG")
    return out


CARDS = [
    dict(out="two-wind-farms-one-view.png", eyebrow="Lancashire  ·  Planning", stat="702",
         label="square kilometres would have a view of turbines of both wind farms. Neither applicant has assessed them together.",
         source="Bare earth model, OS Terrain 50 and the applicants' own turbine schedules"),
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
    dict(out="what-the-councils-own.png", eyebrow="Burnley  ·  Transparency", stat="2,043",
         label="property titles in Burnley are owned by your two councils. I mapped every one I could place.",
         source="HM Land Registry, 2026"),
    dict(out="put-police-back.png", eyebrow="Burnley  ·  Crime", stat="+30%",
         label="how far violent crime in Burnley runs above the England average, while neighbourhood police have been cut by more than half.",
         source="ONS / Home Office"),
    dict(out="lancashire-crime-divide.png", eyebrow="Lancashire  ·  Crime", stat="4x",
         label="more crime in Blackpool than in Ribble Valley. Lancashire's crime map is a map of its deprivation.",
         source="ONS, 2025"),
    dict(out="nine-in-ten-no-charge.png", eyebrow="Lancashire  ·  Crime", stat="9 in 10",
         label="crimes in Lancashire end with no one charged, and Lancashire is one of the better forces in England.",
         source="Home Office, 2025"),
    dict(out="burnley-spending-2025-26.png", eyebrow="Burnley  ·  Transparency", stat="£38m",
         label="every payment Burnley Council made over £500 last year, all 4,489 of them, now searchable in one place.",
         source="Burnley Council, 2025/26"),
    dict(out="temporary-accommodation.png", eyebrow="Burnley  ·  DOGE", stat="1 in 9",
         label="Burnley homelessness cases now come from people leaving Home Office asylum accommodation, up from 1 in 60 two years ago.",
         source="MHCLG / Burnley Council"),
]

if __name__ == "__main__":
    outdir = os.path.join(HERE, "..", "public", "images", "share")
    os.makedirs(outdir, exist_ok=True)
    for c in CARDS:
        p = os.path.join(outdir, c["out"])
        card(c["stat"], c["label"], c["eyebrow"], c["source"], p)
        print("wrote", os.path.relpath(p, os.path.join(HERE, "..")))
    print("fonts:", sorted(LOADED))
