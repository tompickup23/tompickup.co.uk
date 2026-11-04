#!/usr/bin/env python3
"""Share card for /lcc-budget-game/: the page waffle at 1200x630.

Figures, labels, colours and the source line come only from
src/data/lcc-budget-game/game.html. No portrait and no person's name.

Run after npm install (also called from the site prebuild):
  uv run --no-project --with pillow python scripts/generate_budget_game_card.py
"""
import json
import os
import re

from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630
M = 72
HERO = (15, 27, 38)          # page --hero / #0F1B26
INK = (243, 246, 249)        # page --hero-ink
MUTED = (185, 193, 202)
# Page hero rose for "Lancashire?" on navy (#FF7A8A), not the light-ground --rose.
ROSE = (255, 122, 138)
COLS = {
    "rose": (42, 120, 214),   # page --c1 light
    "blue": (235, 98, 52),
    "green": (27, 175, 122),
    "ochre": (237, 161, 0),
    "slate": (232, 123, 164),
}

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
FONTS = os.path.join(ROOT, "node_modules", "@fontsource")
# Game display face (Big Shoulders Display 900) and body (Public Sans).
FACES = {
    "display": ("big-shoulders-display", "big-shoulders-display-latin-900-normal.woff"),
    "sans": ("public-sans", "public-sans-latin-400-normal.woff"),
    "sans-semibold": ("public-sans", "public-sans-latin-600-normal.woff"),
}


def F(kind, size):
    pkg, name = FACES[kind]
    path = os.path.join(FONTS, pkg, "files", name)
    if not os.path.exists(path):
        raise SystemExit(f"Missing {path}. Run npm install first.")
    return ImageFont.truetype(path, size)


def wrap(d, text, font, maxw):
    out, cur = [], ""
    for word in text.split():
        t = (cur + " " + word).strip()
        if d.textlength(t, font=font) <= maxw or not cur:
            cur = t
        else:
            out.append(cur)
            cur = word
    if cur:
        out.append(cur)
    return out


def load_data():
    path = os.path.join(ROOT, "src", "data", "lcc-budget-game", "game.html")
    html = open(path, encoding="utf-8").read()
    m = re.search(r"const D = (\{.*?\});\nconst PY", html, re.S)
    if not m:
        raise SystemExit(f"Could not find const D in {path}")
    return json.loads(m.group(1))


def waffle_cells(values, total):
    """Largest remainder method, matching waffleSVG in game.html."""
    raw = [v / total * 100 for v in values]
    cells = [int(r) for r in raw]
    remainders = sorted(((raw[i] - cells[i], i) for i in range(len(raw))), reverse=True)
    for _, i in remainders[: 100 - sum(cells)]:
        cells[i] += 1
    return cells


def source_line(D, by):
    """Publisher, document, table or page, edition from the data file."""
    docs = D["meta"]["docs"]
    # Line shares are Annex 5; requirement_src names Table 3 of the same document.
    doc = docs["s272628"]
    title = doc["title"]
    # Shorten for the card foot without inventing a new document name.
    # Title is "Medium Term Financial Strategy, Appendix A to the Revenue Budget report, Full Council, 26 February 2026"
    short = "Medium Term Financial Strategy (Full Council, 26 February 2026)"
    if "Medium Term Financial Strategy" not in title or "26 February 2026" not in title:
        short = title
    return (
        f"Source: Lancashire County Council, {short}, Annex 5 p92. "
        f"Net budget {by}, each square 1%."
    )


def main():
    D = load_data()
    by = D["meta"]["base_year"]
    req = D["requirement"][by]
    block_vals = {b["id"]: 0.0 for b in D["blocks"]}
    for line in D["lines"]:
        block_vals[line["block"]] += line["values"][by]
    items = [
        {
            "id": b["id"],
            "label": b["label"],
            "val": block_vals[b["id"]],
            "color": COLS[b["colour"]],
        }
        for b in D["blocks"]
    ]
    cells = waffle_cells([it["val"] for it in items], req)
    source = source_line(D, by)

    img = Image.new("RGB", (W, H), HERO)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 8], fill=ROSE)

    d.text((M, 48), "Lancashire County Council budget 2026/27", font=F("sans-semibold", 20), fill=MUTED)
    title_font = F("display", 58)
    ty = 96
    title_max = 540
    for line in wrap(d, "Can you balance Lancashire?", title_font, title_max):
        if "Lancashire" in line:
            before, _, after = line.partition("Lancashire")
            x = M
            if before:
                d.text((x, ty), before, font=title_font, fill=INK)
                x += d.textlength(before, font=title_font)
            d.text((x, ty), "Lancashire" + after, font=title_font, fill=ROSE)
        else:
            d.text((M, ty), line, font=title_font, fill=INK)
        ty += 64

    # Legend uses the page's full block names, never a comma truncation.
    cell, gap = 42, 6
    grid = 10 * cell + 9 * gap
    ox = W - M - grid
    legend_max = ox - M - 36
    ly = ty + 14
    legend_font = F("sans", 17)
    for it, n in zip(items, cells):
        d.rounded_rectangle([M, ly + 2, M + 14, ly + 16], radius=2, fill=it["color"])
        label = f"{it['label']}  {n}%"
        lines = wrap(d, label, legend_font, legend_max)
        for i, ln in enumerate(lines):
            d.text((M + 22, ly + i * 20), ln, font=legend_font, fill=INK)
        ly += max(28, 20 * len(lines) + 6)

    oy = (H - grid) // 2 - 10
    palette = []
    for it, n in zip(items, cells):
        palette.extend([it["color"]] * n)
    for k, color in enumerate(palette):
        x = ox + (k % 10) * (cell + gap)
        y = oy + (k // 10) * (cell + gap)
        d.rounded_rectangle([x, y, x + cell, y + cell], radius=6, fill=color)

    d.line([M, H - 96, W - M, H - 96], fill=(46, 59, 72), width=2)
    d.text((M, H - 78), "tompickup.co.uk", font=F("sans-semibold", 24), fill=INK)
    src_font = F("sans", 16)
    src_y = H - 48
    for i, ln in enumerate(wrap(d, source, src_font, W - 2 * M)):
        d.text((M, src_y + i * 18), ln, font=src_font, fill=MUTED)

    outdir = os.path.join(ROOT, "public", "images", "share")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "lcc-budget-game.png")
    img.save(out, "PNG", optimize=True)

    # Sidecar for the build guard (not under public/: it must not ship as a static file).
    # Every percentage on the card must match game.html.
    sidecar = {
        "base_year": by,
        "requirement_m": req,
        "square_m": round(req / 100, 3),
        "blocks": [
            {"id": it["id"], "label": it["label"], "pct": n, "value_m": round(it["val"], 3)}
            for it, n in zip(items, cells)
        ],
    }
    side_dir = os.path.join(ROOT, "src", "data", "lcc-budget-game")
    side_path = os.path.join(side_dir, "card-figures.json")
    with open(side_path, "w", encoding="utf-8") as f:
        json.dump(sidecar, f, indent=2)
        f.write("\n")

    print("wrote", os.path.relpath(out, ROOT))
    print("wrote", os.path.relpath(side_path, ROOT))


if __name__ == "__main__":
    main()
