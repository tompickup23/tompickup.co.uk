#!/usr/bin/env python3
"""Share card for /lcc-budget-game/: the page waffle at 1200x630.

Figures and colours come only from src/data/lcc-budget-game/game.html.
No portrait and no person's name.

Run after npm install:
  uv run --no-project --with pillow python scripts/generate_budget_game_card.py
"""
import json
import os
import re

from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630
M = 72
HERO = (15, 27, 38)
INK = (243, 246, 249)
MUTED = (185, 193, 202)
ROSE = (179, 18, 46)
COLS = {
    "rose": (42, 120, 214),
    "blue": (235, 98, 52),
    "green": (27, 175, 122),
    "ochre": (237, 161, 0),
    "slate": (232, 123, 164),
}

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
FONTS = os.path.join(ROOT, "node_modules", "@fontsource")
FACES = {
    "serif": ("source-serif-4", "source-serif-4-latin-600-normal.woff2"),
    "sans": ("source-sans-3", "source-sans-3-latin-400-normal.woff2"),
    "sans-semibold": ("source-sans-3", "source-sans-3-latin-600-normal.woff2"),
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
    remainders = sorted(((raw[i]-cells[i], i) for i in range(len(raw))), reverse=True)
    for _, i in remainders[:100-sum(cells)]:
        cells[i] += 1
    return cells


def main():
    D = load_data()
    by = D["meta"]["base_year"]
    req = D["requirement"][by]
    block_vals = {b["id"]: 0.0 for b in D["blocks"]}
    for line in D["lines"]:
        block_vals[line["block"]] += line["values"][by]
    items = [
        {"label": b["label"], "val": block_vals[b["id"]], "color": COLS[b["colour"]]}
        for b in D["blocks"]
    ]
    cells = waffle_cells([it["val"] for it in items], req)
    square_m = req / 100
    source = f"Each square is 1% of the budget, about £{square_m:.1f}m. Net budget {by}."

    img = Image.new("RGB", (W, H), HERO)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 8], fill=ROSE)

    d.text((M, 56), "Lancashire County Council", font=F("sans-semibold", 22), fill=MUTED)
    title_font = F("serif", 64)
    ty = 110
    for line in wrap(d, "Can you balance Lancashire?", title_font, 520):
        if "Lancashire" in line:
            before, _, after = line.partition("Lancashire")
            x = M
            if before:
                d.text((x, ty), before, font=title_font, fill=INK)
                x += d.textlength(before, font=title_font)
            d.text((x, ty), "Lancashire" + after, font=title_font, fill=ROSE)
        else:
            d.text((M, ty), line, font=title_font, fill=INK)
        ty += 72

    ly = ty + 18
    for it, n in zip(items, cells):
        d.rounded_rectangle([M, ly, M+14, ly+14], radius=2, fill=it["color"])
        short = it["label"].split(",")[0]
        d.text((M+22, ly-2), f"{short}  {n}%", font=F("sans", 18), fill=INK)
        ly += 28

    cell, gap = 42, 6
    grid = 10*cell + 9*gap
    ox = W-M-grid
    oy = (H-grid)//2-10
    palette = []
    for it, n in zip(items, cells):
        palette.extend([it["color"]] * n)
    for k, color in enumerate(palette):
        x = ox + (k % 10)*(cell+gap)
        y = oy + (k // 10)*(cell+gap)
        d.rounded_rectangle([x, y, x+cell, y+cell], radius=6, fill=color)

    d.line([M, H-84, W-M, H-84], fill=(46, 59, 72), width=2)
    d.text((M, H-62), "tompickup.co.uk", font=F("sans-semibold", 26), fill=INK)
    src_font = F("sans", 20)
    sw = d.textlength(source, font=src_font)
    d.text((W-M-sw, H-58), source, font=src_font, fill=MUTED)

    outdir = os.path.join(ROOT, "public", "images", "share")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "lcc-budget-game.png")
    img.save(out, "PNG", optimize=True)
    print("wrote", os.path.relpath(out, ROOT))


if __name__ == "__main__":
    main()
