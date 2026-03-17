#!/usr/bin/env python3
"""
Shareable stat card image generator for social media.

Generates 1080x1080 stat cards in Reform UK press conference style:
- Dark navy blue background with subtle gradient/vignette
- One huge stat number in teal (#12B6CF)
- One-line context headline in white
- Source attribution in gray
- tompickup.co.uk watermark
- REFORM branding top-right

Output: 1080x1080 PNG suitable for WhatsApp, Facebook, Instagram, X.

Usage:
    python3 scripts/generate_image.py --stat "£921M" --headline "Lost Under the Conservatives" --subtext "Statement of Accounts, 2017-2025" --output /tmp/test_stat_card.png
    python3 scripts/generate_image.py --stat "£650M+" --headline "Highways Maintenance Backlog" --subtext "LCC Highways Assessment, 2025" --color red
    python3 scripts/generate_image.py --article lcc-tory-legacy-921m-losses --output /tmp/tory-legacy.png
"""

import os
import sys
import argparse
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
except ImportError:
    print("ERROR: Pillow not installed. Run: pip3 install Pillow")
    sys.exit(1)

from reform_brand import (
    NAVY, NAVY_MID, TEAL, WHITE, LIGHT_GRAY, MID_GRAY, DARK_LINE,
    CRIMSON, GREEN, RED,
    ACCENT_COLORS, load_font, draw_reform_logo, draw_watermark_bar,
    draw_data_badge, create_branded_background, apply_edge_vignette,
    draw_rounded_rect, draw_accent_line,
)


# --- Design Constants ---

WIDTH = 1080
HEIGHT = 1080

# Article presets for --article mode
ARTICLE_PRESETS = {
    'lcc-tory-legacy-921m-losses': {
        'stat': '£921M',
        'headline': 'Lost Under the Conservatives',
        'subtext': 'LCC Statement of Accounts, 2017-2025',
        'color': 'red',
    },
    'lancashire-highways-650m-backlog': {
        'stat': '£650M+',
        'headline': 'Highways Maintenance Backlog',
        'subtext': 'LCC Highways Assessment, 2025',
        'color': 'amber',
    },
    'lcc-budget-reform-first-year': {
        'stat': '3.80%',
        'headline': 'Lowest Council Tax Rise in a Decade',
        'subtext': "Reform's First Budget, Feb 2026",
        'color': 'green',
    },
    'east-lancashire-waste-crisis': {
        'stat': '£60.3M',
        'headline': 'First Open Waste Tender in a Generation',
        'subtext': 'LCC Waste Procurement, Dec 2025',
        'color': 'teal',
    },
    'lancashire-lgr-contract-crisis': {
        'stat': '3,200',
        'headline': 'Live Contracts at Risk from LGR',
        'subtext': 'Contracts Finder + LCC Procurement Pipeline',
        'color': 'amber',
    },
    'reform-technology-lancashire': {
        'stat': '£4.3M',
        'headline': 'AI-Powered Digital Transformation',
        'subtext': 'LCC Netcall Platform, 2025',
        'color': 'teal',
    },
    'burnley-elections-2026-attendance': {
        'stat': '62%',
        'headline': 'Worst Attendance by a Defending Councillor',
        'subtext': 'Burnley Council Attendance Records, 2022-2026',
        'color': 'red',
    },
    'stocks-massey-bequest-2025': {
        'stat': '£20K+',
        'headline': 'Awarded to Burnley Organisations',
        'subtext': 'Stocks Massey Bequest Fund, 2025',
        'color': 'teal',
    },
    'reform-lancashire-9-months': {
        'stat': '9',
        'headline': 'Months of Reform: Lancashire by the Numbers',
        'subtext': 'Lancashire County Council, May 2025 - March 2026',
        'color': 'teal',
    },
}


def create_background(width, height):
    """Create dark navy background with radial gradient vignette.

    Delegates to the shared reform_brand.create_branded_background().
    """
    return create_branded_background(width, height)


def create_stat_card(stat, headline, subtext, accent_color=TEAL, output_path=None):
    """Generate a 1080x1080 stat card image."""

    img = create_background(WIDTH, HEIGHT)
    draw = ImageDraw.Draw(img)

    # --- Top section: REFORM UK branding with LANCASHIRE subtitle ---
    # Measure logo to position at top-right
    scratch = Image.new('RGBA', (WIDTH, 200), (0, 0, 0, 0))
    sdraw = ImageDraw.Draw(scratch)
    _, _, logo_w, logo_h = draw_reform_logo(sdraw, 0, 0, scale=0.7, variant='lancashire')
    draw_reform_logo(draw, WIDTH - logo_w - 80, 60, scale=0.7, variant='lancashire')

    # --- Decorative accent line (left side, vertical) ---
    draw_accent_line(draw, 72, 200, 72, HEIGHT - 200,
                     color=(*accent_color[:3], 60), width=3)

    # --- Compute layout: measure all elements first, then center vertically ---

    # Stat font: dynamic sizing
    stat_size = 200
    while stat_size > 80:
        stat_font = load_font('black', stat_size)
        stat_bbox = draw.textbbox((0, 0), stat, font=stat_font)
        stat_w = stat_bbox[2] - stat_bbox[0]
        if stat_w < WIDTH - 200:
            break
        stat_size -= 10
    stat_ascent, stat_descent = stat_font.getmetrics()
    stat_h = stat_ascent + stat_descent  # Full line height including descenders

    # Headline font and word-wrap
    headline_font = load_font('bold', 46)
    headline_upper = headline.upper()
    headline_lines = []
    words = headline_upper.split()
    current_line = ""
    for word in words:
        test = f"{current_line} {word}".strip()
        test_bbox = draw.textbbox((0, 0), test, font=headline_font)
        if test_bbox[2] - test_bbox[0] > WIDTH - 200:
            if current_line:
                headline_lines.append(current_line)
            current_line = word
        else:
            current_line = test
    if current_line:
        headline_lines.append(current_line)

    headline_line_h = 56
    headline_total_h = len(headline_lines) * headline_line_h

    # Subtext
    subtext_font = load_font('regular', 26)

    # Total content block height
    gap_stat_headline = 50
    gap_headline_line = 40
    gap_line_subtext = 30
    line_h_px = 3
    subtext_h = 30
    total_h = stat_h + gap_stat_headline + headline_total_h + gap_headline_line + line_h_px + gap_line_subtext + subtext_h

    # Vertical centering in available area (below REFORM branding, above footer)
    avail_top = 150
    avail_bottom = HEIGHT - 100
    avail_h = avail_bottom - avail_top
    content_top = avail_top + (avail_h - total_h) // 2

    stat_y = content_top
    stat_x = (WIDTH - stat_w) // 2

    # --- Glow effect behind stat ---
    glow_img = Image.new('RGBA', (WIDTH, HEIGHT), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow_img)
    for offset in range(14, 0, -2):
        glow_draw.text(
            (stat_x, stat_y + offset),
            stat,
            fill=(*accent_color[:3], 6),
            font=stat_font,
        )
        glow_draw.text(
            (stat_x, stat_y - offset),
            stat,
            fill=(*accent_color[:3], 6),
            font=stat_font,
        )
    img.paste(Image.alpha_composite(
        img.convert('RGBA'),
        glow_img
    ).convert('RGB'))
    draw = ImageDraw.Draw(img)

    # Draw the stat text
    draw.text((stat_x, stat_y), stat, fill=accent_color, font=stat_font)

    # --- Headline text (white, below stat) ---
    headline_y = stat_y + stat_h + gap_stat_headline
    for line in headline_lines:
        line_bbox = draw.textbbox((0, 0), line, font=headline_font)
        line_w = line_bbox[2] - line_bbox[0]
        draw.text(
            ((WIDTH - line_w) // 2, headline_y),
            line,
            fill=WHITE,
            font=headline_font,
        )
        headline_y += headline_line_h

    # --- Horizontal accent line ---
    line_y2 = headline_y + gap_headline_line
    line_w_px = 200
    draw.line(
        [((WIDTH - line_w_px) // 2, line_y2), ((WIDTH + line_w_px) // 2, line_y2)],
        fill=accent_color,
        width=3,
    )

    # --- Subtext / source attribution ---
    sub_bbox = draw.textbbox((0, 0), subtext, font=subtext_font)
    sub_w = sub_bbox[2] - sub_bbox[0]
    draw.text(
        ((WIDTH - sub_w) // 2, line_y2 + gap_line_subtext),
        subtext,
        fill=LIGHT_GRAY,
        font=subtext_font,
    )

    # --- Bottom section: watermark bar ---
    draw_watermark_bar(draw, WIDTH, HEIGHT, left_dot_color=accent_color)

    # --- Edge vignette overlay ---
    img = apply_edge_vignette(img)

    # Save
    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        img.save(output_path, 'PNG', quality=95)
        file_size = os.path.getsize(output_path)
        print(f"Generated: {output_path} ({file_size:,} bytes, {WIDTH}x{HEIGHT})")

    return img


def create_cover_image(photo_path, title, tags=None, variant='lancashire',
                       output_path=None, size=(1200, 628)):
    """Generate a branded article cover image with photo + Reform overlay + title.

    Overlays Reform UK branding, article title, and optional tags onto a base
    photograph. Used as the article cover image and video poster/thumbnail.

    Args:
        photo_path: Path to the base photograph (JPEG/PNG).
        title: Article title text.
        tags: Optional list of tag strings.
        variant: Reform logo variant ('lancashire', 'burnley', 'full').
        output_path: File path to save. If None, image is not saved.
        size: Output dimensions (width, height). Default 1200x628 (OG image).

    Returns:
        PIL Image (RGB mode).
    """
    w, h = size

    # Load and resize photo to fill the frame
    photo = Image.open(photo_path).convert('RGB')
    photo_ratio = photo.width / photo.height
    target_ratio = w / h
    if photo_ratio > target_ratio:
        # Photo is wider — crop sides
        new_h = photo.height
        new_w = int(new_h * target_ratio)
        left = (photo.width - new_w) // 2
        photo = photo.crop((left, 0, left + new_w, new_h))
    else:
        # Photo is taller — crop top/bottom
        new_w = photo.width
        new_h = int(new_w / target_ratio)
        top = (photo.height - new_h) // 2
        photo = photo.crop((0, top, new_w, top + new_h))
    photo = photo.resize((w, h), Image.LANCZOS)

    img = photo.copy()

    # Dark gradient overlay (bottom 60% of image, strong at bottom)
    overlay = Image.new('RGBA', (w, h), (0, 0, 0, 0))
    odraw = ImageDraw.Draw(overlay)
    gradient_start = int(h * 0.3)
    for y_pos in range(gradient_start, h):
        progress = (y_pos - gradient_start) / (h - gradient_start)
        alpha = int(220 * progress ** 1.2)
        odraw.line([(0, y_pos), (w, y_pos)], fill=(10, 22, 40, alpha))
    img = Image.alpha_composite(img.convert('RGBA'), overlay).convert('RGB')
    draw = ImageDraw.Draw(img)

    # Reform UK logo (top-right)
    logo_scale = min(w / 1200, h / 628) * 0.8
    try:
        draw_reform_logo(img, w - int(280 * logo_scale) - 30, 20,
                         scale=logo_scale, variant=variant)
    except Exception:
        pass

    # Tags (above title, bottom area)
    tag_y = h - 180
    if tags:
        tag_font = load_font('bold', max(14, int(16 * logo_scale)))
        tag_x = 40
        for tag in tags[:4]:
            tag_text = tag.upper()
            tbbox = draw.textbbox((0, 0), tag_text, font=tag_font)
            tw = tbbox[2] - tbbox[0]
            th = tbbox[3] - tbbox[1]
            # Pill background
            pill_w = tw + 20
            pill_h = th + 10
            draw_rounded_rect(draw, (tag_x, tag_y, tag_x + pill_w, tag_y + pill_h),
                              pill_h // 2, fill=(12, 60, 72))
            draw.text((tag_x + 10, tag_y + 5), tag_text, fill=TEAL, font=tag_font)
            tag_x += pill_w + 8
        tag_y += pill_h + 12

    # Title text (bottom, large, white)
    title_font_size = max(28, int(42 * min(w / 1200, h / 628)))
    title_font = load_font('bold', title_font_size)
    # Word-wrap title
    title_lines = []
    words = title.split()
    current_line = ""
    for word in words:
        test = f"{current_line} {word}".strip()
        tbbox = draw.textbbox((0, 0), test, font=title_font)
        if tbbox[2] - tbbox[0] > w - 80:
            if current_line:
                title_lines.append(current_line)
            current_line = word
        else:
            current_line = test
    if current_line:
        title_lines.append(current_line)

    # Draw title from bottom up
    line_h = title_font_size + 8
    title_bottom = h - 30
    for i, line in enumerate(reversed(title_lines)):
        y_pos = title_bottom - (i + 1) * line_h
        draw.text((40, y_pos), line, fill=WHITE, font=title_font)

    # Thin teal accent line above title
    accent_y = title_bottom - len(title_lines) * line_h - 12
    draw.line([(40, accent_y), (200, accent_y)], fill=TEAL, width=3)

    # tompickup.co.uk watermark (bottom-right, subtle)
    wm_font = load_font('regular', max(12, int(16 * logo_scale)))
    draw.text((w - 180, h - 30), "tompickup.co.uk", fill=(200, 200, 210, 180),
              font=wm_font)

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        img.save(output_path, 'JPEG', quality=92)
        file_size = os.path.getsize(output_path)
        print(f"Cover: {output_path} ({file_size:,} bytes, {w}x{h})")

    return img


def create_data_viz_card(viz_type, data, title=None, source=None,
                         variant='lancashire', output_path=None):
    """Generate a shareable data visualisation image for social media.

    Renders article data tables, stat grids, and comparison panels as
    branded 1080x1080 images suitable for WhatsApp/Facebook/Instagram sharing.

    Args:
        viz_type: Type of visualisation:
            'stat_grid'    -- Grid of stat cards (data = list of dicts)
            'table'        -- Data table (data = dict with headers/rows)
            'comparison'   -- Before/after comparison (data = dict)
        data: Visualisation data (format depends on viz_type).
        title: Optional title text above the visualisation.
        source: Source attribution text.
        variant: Reform logo variant.
        output_path: File path to save.

    Returns:
        PIL Image (RGB mode).
    """
    img = create_background(WIDTH, HEIGHT)
    draw = ImageDraw.Draw(img)

    # Reform UK logo (top-right)
    scratch = Image.new('RGBA', (WIDTH, 200), (0, 0, 0, 0))
    _, _, logo_w, _ = draw_reform_logo(scratch, 0, 0, scale=0.7, variant=variant)
    draw_reform_logo(draw, WIDTH - logo_w - 60, 40, scale=0.7, variant=variant)

    # DATA badge (top-left)
    from reform_brand import draw_data_badge as _badge
    _badge(draw, 40, 40, text="DATA")

    y_cursor = 120

    # Title
    if title:
        title_font = load_font('bold', 36)
        title_upper = title.upper()
        # Word-wrap
        t_lines = []
        words = title_upper.split()
        cur = ""
        for word in words:
            test = f"{cur} {word}".strip()
            tbbox = draw.textbbox((0, 0), test, font=title_font)
            if tbbox[2] - tbbox[0] > WIDTH - 120:
                if cur:
                    t_lines.append(cur)
                cur = word
            else:
                cur = test
        if cur:
            t_lines.append(cur)

        for line in t_lines:
            lbbox = draw.textbbox((0, 0), line, font=title_font)
            lw = lbbox[2] - lbbox[0]
            draw.text(((WIDTH - lw) // 2, y_cursor), line, fill=WHITE, font=title_font)
            y_cursor += 44
        y_cursor += 20

        # Accent line below title
        draw_accent_line(draw, (WIDTH - 200) // 2, y_cursor,
                         (WIDTH + 200) // 2, y_cursor, color=TEAL, width=3)
        y_cursor += 30

    if viz_type == 'stat_grid':
        # data = [{'value': '53', 'label': 'Reform UK Seats', 'sublabel': 'of 84', 'color': 'teal'}, ...]
        cols = min(len(data), 4)
        rows_count = (len(data) + cols - 1) // cols
        card_w = (WIDTH - 120 - (cols - 1) * 20) // cols
        card_h = min(200, (HEIGHT - y_cursor - 160) // rows_count - 20)

        for i, item in enumerate(data):
            row = i // cols
            col = i % cols
            cx = 60 + col * (card_w + 20)
            cy = y_cursor + row * (card_h + 20)

            # Card background
            draw_rounded_rect(draw, (cx, cy, cx + card_w, cy + card_h),
                              12, fill=(18, 34, 58))
            draw_rounded_rect(draw, (cx, cy, cx + card_w, cy + card_h),
                              12, outline=(28, 48, 72), width=1)

            accent = ACCENT_COLORS.get(item.get('color', 'teal'), TEAL)

            # Value
            val_font_size = min(56, card_h // 3)
            val_font = load_font('black', val_font_size)
            val_text = item['value']
            vbbox = draw.textbbox((0, 0), val_text, font=val_font)
            vw = vbbox[2] - vbbox[0]
            draw.text((cx + (card_w - vw) // 2, cy + 20), val_text,
                      fill=accent, font=val_font)

            # Label
            lbl_font = load_font('bold', 18)
            lbl_text = item.get('label', '')
            lbbox = draw.textbbox((0, 0), lbl_text, font=lbl_font)
            lw = lbbox[2] - lbbox[0]
            draw.text((cx + (card_w - lw) // 2, cy + 20 + val_font_size + 10),
                      lbl_text, fill=WHITE, font=lbl_font)

            # Sublabel
            if 'sublabel' in item:
                sub_font = load_font('regular', 14)
                sub_text = item['sublabel']
                sbbox = draw.textbbox((0, 0), sub_text, font=sub_font)
                sw = sbbox[2] - sbbox[0]
                draw.text((cx + (card_w - sw) // 2, cy + 20 + val_font_size + 36),
                          sub_text, fill=MID_GRAY, font=sub_font)

    elif viz_type == 'table':
        # data = {'headers': ['Year', 'Control', 'Rise'], 'rows': [['2016/17', 'Con', '3.99%'], ...]}
        headers = data.get('headers', [])
        rows = data.get('rows', [])
        highlight_rows = data.get('highlight_rows', [])

        n_cols = len(headers)
        table_w = WIDTH - 120
        col_w = table_w // n_cols
        row_h = max(36, min(44, (HEIGHT - y_cursor - 160) // (len(rows) + 1)))

        # Header row
        hdr_font = load_font('bold', 18)
        hdr_y = y_cursor
        draw.rectangle([(60, hdr_y), (WIDTH - 60, hdr_y + row_h)],
                       fill=(16, 36, 60))  # dark navy-teal tint
        for j, hdr in enumerate(headers):
            hx = 60 + j * col_w + col_w // 2
            hbbox = draw.textbbox((0, 0), hdr, font=hdr_font)
            hw = hbbox[2] - hbbox[0]
            draw.text((hx - hw // 2, hdr_y + (row_h - 18) // 2), hdr,
                      fill=TEAL, font=hdr_font)
        y_cursor = hdr_y + row_h

        # Data rows
        cell_font = load_font('regular', 16)
        bold_font = load_font('bold', 16)
        for i, row in enumerate(rows):
            ry = y_cursor + i * row_h
            # Alternating background
            if i % 2 == 0:
                draw.rectangle([(60, ry), (WIDTH - 60, ry + row_h)],
                               fill=(14, 26, 46))
            # Highlight row
            is_highlight = i in highlight_rows
            if is_highlight:
                draw.rectangle([(60, ry), (WIDTH - 60, ry + row_h)],
                               fill=(14, 40, 56))  # subtle teal tint

            for j, cell in enumerate(row):
                cx_center = 60 + j * col_w + col_w // 2
                use_font = bold_font if is_highlight else cell_font
                color = TEAL if is_highlight else WHITE
                cbbox = draw.textbbox((0, 0), str(cell), font=use_font)
                cw = cbbox[2] - cbbox[0]
                draw.text((cx_center - cw // 2, ry + (row_h - 16) // 2),
                          str(cell), fill=color, font=use_font)

    elif viz_type == 'comparison':
        # data = {'before': {'value': '48%', 'label': '...', 'sublabel': '...'}, 'after': {...}}
        before = data.get('before', {})
        after = data.get('after', {})

        panel_w = (WIDTH - 140) // 2
        panel_h = 300

        for i, (side, panel_data) in enumerate([(before, 'before'), (after, 'after')]):
            px = 60 + i * (panel_w + 20)
            py = y_cursor + 20

            # Panel background
            accent = CRIMSON if panel_data == 'before' else GREEN
            draw_rounded_rect(draw, (px, py, px + panel_w, py + panel_h),
                              16, fill=(18, 34, 58))
            # Subtle accent border (mix accent with navy for dark mode)
            border_color = tuple(max(20, c // 4) for c in accent[:3])
            draw_rounded_rect(draw, (px, py, px + panel_w, py + panel_h),
                              16, outline=border_color, width=2)

            # Label at top
            lbl = "BEFORE" if panel_data == 'before' else "AFTER"
            lbl_font = load_font('bold', 18)
            lbbox = draw.textbbox((0, 0), lbl, font=lbl_font)
            lw = lbbox[2] - lbbox[0]
            draw.text((px + (panel_w - lw) // 2, py + 20), lbl,
                      fill=accent, font=lbl_font)

            # Big value
            val_font = load_font('black', 72)
            val = side.get('value', '')
            vbbox = draw.textbbox((0, 0), val, font=val_font)
            vw = vbbox[2] - vbbox[0]
            draw.text((px + (panel_w - vw) // 2, py + 60), val,
                      fill=accent, font=val_font)

            # Description
            desc_font = load_font('bold', 16)
            desc = side.get('label', '')
            dbbox = draw.textbbox((0, 0), desc, font=desc_font)
            dw = dbbox[2] - dbbox[0]
            draw.text((px + (panel_w - dw) // 2, py + 160), desc,
                      fill=WHITE, font=desc_font)

            # Sublabel
            if 'sublabel' in side:
                sub_font = load_font('regular', 13)
                sub = side['sublabel']
                sbbox = draw.textbbox((0, 0), sub, font=sub_font)
                sw = sbbox[2] - sbbox[0]
                draw.text((px + (panel_w - sw) // 2, py + 190), sub,
                          fill=MID_GRAY, font=sub_font)

    # Source attribution
    if source:
        src_font = load_font('regular', 16)
        src_bold = load_font('bold', 16)
        draw.text((60, HEIGHT - 90), "Source: ", fill=LIGHT_GRAY, font=src_bold)
        prefix_bbox = draw.textbbox((0, 0), "Source: ", font=src_bold)
        prefix_w = prefix_bbox[2] - prefix_bbox[0]
        draw.text((60 + prefix_w, HEIGHT - 90), source, fill=MID_GRAY, font=src_font)

    # Watermark bar
    draw_watermark_bar(draw, WIDTH, HEIGHT, left_dot_color=TEAL)

    # Edge vignette
    img = apply_edge_vignette(img)

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        img.save(output_path, 'PNG', quality=95)
        file_size = os.path.getsize(output_path)
        print(f"Data viz: {output_path} ({file_size:,} bytes, {WIDTH}x{HEIGHT})")

    return img


# Article shareable asset presets
ARTICLE_SHAREABLE_PRESETS = {
    'reform-lancashire-9-months': {
        'cover': {
            'photo': 'county-hall-preston.jpg',
            'title': '9 Months of Reform: Lancashire by the Numbers',
            'tags': ['lancashire', 'reform', 'finance'],
        },
        'viz_cards': [
            {
                'slug': 'headline-stats',
                'type': 'stat_grid',
                'title': '9 Months of Reform: The Numbers',
                'source': 'Lancashire County Council official records',
                'data': [
                    {'value': '53', 'label': 'Reform UK Seats', 'sublabel': 'of 84 total (63%)', 'color': 'teal'},
                    {'value': '3.80%', 'label': 'Council Tax Rise', 'sublabel': 'Lowest in a decade', 'color': 'green'},
                    {'value': '5', 'label': 'Care Homes Saved', 'sublabel': 'All five kept open', 'color': 'green'},
                    {'value': '£921M', 'label': 'Tory Losses', 'sublabel': 'Audited 2017-2025', 'color': 'red'},
                ],
            },
            {
                'slug': 'council-tax-table',
                'type': 'table',
                'title': 'Council Tax: 10 Years of Maximum Rises',
                'source': 'LCC council tax breakdown PDFs, lancashire.gov.uk',
                'data': {
                    'headers': ['Year', 'Control', 'Total Rise'],
                    'rows': [
                        ['2016/17', 'Conservative', '3.99%'],
                        ['2017/18', 'Conservative', '3.99%'],
                        ['2018/19', 'Conservative', '5.99%'],
                        ['2019/20', 'Conservative', '3.99%'],
                        ['2020/21', 'Conservative', '3.99%'],
                        ['2021/22', 'Conservative', '3.99%'],
                        ['2022/23', 'Conservative', '3.99%'],
                        ['2023/24', 'Conservative', '3.99%'],
                        ['2024/25', 'Conservative', '4.99%'],
                        ['2025/26', 'Conservative*', '4.99%'],
                        ['2026/27', 'Reform UK', '3.80%'],
                    ],
                    'highlight_rows': [10],
                },
            },
            {
                'slug': 'savings-comparison',
                'type': 'comparison',
                'title': 'Savings Programme Delivery',
                'source': 'LCC quarterly monitoring reports',
                'data': {
                    'before': {'value': '48%', 'label': 'Conservative Delivery', 'sublabel': '2024/25 final outturn'},
                    'after': {'value': '100%', 'label': 'Reform Delivery', 'sublabel': '2025/26 Q3 position'},
                },
            },
            {
                'slug': 'financial-damage',
                'type': 'stat_grid',
                'title': 'Conservative Financial Damage: 8 Years',
                'source': 'LCC Statement of Accounts, 2017/18 to 2024/25',
                'data': [
                    {'value': '£416.9M', 'label': 'Treasury Losses', 'color': 'red'},
                    {'value': '£254.5M', 'label': 'Disposal Losses', 'color': 'red'},
                    {'value': '£138.4M', 'label': 'Overspends', 'color': 'orange'},
                    {'value': '£111.7M', 'label': 'Subsidies & Costs', 'color': 'orange'},
                ],
            },
            {
                'slug': 'bonds-damage',
                'type': 'stat_grid',
                'title': 'The Bond Scandal',
                'source': 'LCC Statement of Accounts + Treasury reports',
                'data': [
                    {'value': '£600M', 'label': 'UKMBA Bond Portfolio', 'sublabel': 'Face value', 'color': 'red'},
                    {'value': '£350M', 'label': 'Estimated Loss', 'sublabel': 'If sold today', 'color': 'red'},
                    {'value': '£1.27B', 'label': 'Total Damage', 'sublabel': 'Including unrealised losses', 'color': 'red'},
                ],
            },
            {
                'slug': 'highways-inherited',
                'type': 'stat_grid',
                'title': 'The Inherited Highways Backlog',
                'source': 'LCC Highways Condition Assessment',
                'data': [
                    {'value': '£650M', 'label': 'Backlog Inherited', 'sublabel': 'From Conservative era', 'color': 'orange'},
                    {'value': '£45M', 'label': 'Reform 3-Year Plan', 'sublabel': 'Resurfacing programme', 'color': 'teal'},
                    {'value': '£9.6B', 'label': 'UK Foreign Aid', 'sublabel': 'Annual spend, 2025', 'color': 'red'},
                ],
            },
        ],
    },
}


def generate_article_assets(article_slug, output_dir=None):
    """Generate all shareable assets for an article.

    Creates:
    - Cover image (1200x628 for OG/social sharing + 1080x1080 square)
    - Data visualisation cards (1080x1080 each)
    - Stat card (from ARTICLE_PRESETS if available)

    Args:
        article_slug: Article identifier (e.g. 'reform-lancashire-9-months').
        output_dir: Directory for output files. Default: public/images/share/{slug}/

    Returns:
        List of generated file paths.
    """
    if output_dir is None:
        output_dir = str(Path(__file__).parent.parent / "public" / "images" / "share" / article_slug)
    os.makedirs(output_dir, exist_ok=True)

    generated = []
    preset = ARTICLE_SHAREABLE_PRESETS.get(article_slug, {})

    # 1. Cover image
    cover_cfg = preset.get('cover')
    if cover_cfg:
        photo_path = str(Path(__file__).parent.parent / "public" / "images" / cover_cfg['photo'])
        if os.path.exists(photo_path):
            # OG/social cover (1200x628)
            og_path = os.path.join(output_dir, f"{article_slug}-cover.jpg")
            create_cover_image(
                photo_path, cover_cfg['title'], tags=cover_cfg.get('tags'),
                output_path=og_path, size=(1200, 628),
            )
            generated.append(og_path)

            # Square cover for social sharing (1080x1080)
            sq_path = os.path.join(output_dir, f"{article_slug}-cover-square.jpg")
            create_cover_image(
                photo_path, cover_cfg['title'], tags=cover_cfg.get('tags'),
                output_path=sq_path, size=(1080, 1080),
            )
            generated.append(sq_path)

            # Video poster (16:9 for video element)
            poster_path = os.path.join(output_dir, f"{article_slug}-poster.jpg")
            create_cover_image(
                photo_path, cover_cfg['title'], tags=cover_cfg.get('tags'),
                output_path=poster_path, size=(1920, 1080),
            )
            generated.append(poster_path)

    # 2. Data viz cards
    for viz_cfg in preset.get('viz_cards', []):
        viz_path = os.path.join(output_dir, f"{article_slug}-{viz_cfg['slug']}.png")
        create_data_viz_card(
            viz_type=viz_cfg['type'],
            data=viz_cfg['data'],
            title=viz_cfg.get('title'),
            source=viz_cfg.get('source'),
            output_path=viz_path,
        )
        generated.append(viz_path)

    # 3. Stat card (from ARTICLE_PRESETS)
    stat_preset = ARTICLE_PRESETS.get(article_slug)
    if stat_preset:
        stat_path = os.path.join(output_dir, f"{article_slug}-stat.png")
        accent = ACCENT_COLORS.get(stat_preset.get('color', 'teal'), TEAL)
        create_stat_card(
            stat=stat_preset['stat'],
            headline=stat_preset['headline'],
            subtext=stat_preset.get('subtext', 'tompickup.co.uk'),
            accent_color=accent,
            output_path=stat_path,
        )
        generated.append(stat_path)

    print(f"\nGenerated {len(generated)} assets in {output_dir}/")
    return generated


def main():
    parser = argparse.ArgumentParser(
        description="Generate shareable stat card images for social media.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --stat "£921M" --headline "Lost Under the Conservatives" --subtext "Statement of Accounts, 2017-2025"
  %(prog)s --article lcc-tory-legacy-921m-losses --output /tmp/tory.png
  %(prog)s --stat "70" --headline "Roads Rated Poor or Below" --subtext "LCC 2025" --color red
  %(prog)s --all --output-dir /tmp/stat-cards/
  %(prog)s --assets reform-lancashire-9-months
        """,
    )

    parser.add_argument('--stat', help='The big stat number (e.g. "£921M", "42%%", "70")')
    parser.add_argument('--headline', help='Context line below the stat')
    parser.add_argument('--subtext', default='tompickup.co.uk', help='Source attribution (default: tompickup.co.uk)')
    parser.add_argument('--output', default='/tmp/stat_card.png', help='Output file path')
    parser.add_argument('--color', default='teal', choices=list(ACCENT_COLORS.keys()), help='Accent color (default: teal)')
    parser.add_argument('--article', help='Article slug to auto-generate from preset data')
    parser.add_argument('--all', action='store_true', help='Generate cards for all article presets')
    parser.add_argument('--output-dir', default='/tmp/stat-cards', help='Output directory for --all mode')
    parser.add_argument('--assets', help='Generate all shareable assets for an article slug')
    parser.add_argument('--assets-dir', help='Output directory for --assets mode')

    args = parser.parse_args()

    if args.assets:
        generate_article_assets(args.assets, output_dir=args.assets_dir)
        return

    if args.all:
        os.makedirs(args.output_dir, exist_ok=True)
        for slug, preset in ARTICLE_PRESETS.items():
            output = os.path.join(args.output_dir, f"{slug}.png")
            accent = ACCENT_COLORS.get(preset.get('color', 'teal'), TEAL)
            create_stat_card(
                stat=preset['stat'],
                headline=preset['headline'],
                subtext=preset.get('subtext', 'tompickup.co.uk'),
                accent_color=accent,
                output_path=output,
            )
        print(f"\nGenerated {len(ARTICLE_PRESETS)} stat cards in {args.output_dir}/")
        return

    if args.article:
        preset = ARTICLE_PRESETS.get(args.article)
        if not preset:
            print(f"ERROR: No preset for article '{args.article}'")
            print(f"Available: {', '.join(ARTICLE_PRESETS.keys())}")
            sys.exit(1)
        stat = args.stat or preset['stat']
        headline = args.headline or preset['headline']
        subtext = args.subtext if args.subtext != 'tompickup.co.uk' else preset.get('subtext', 'tompickup.co.uk')
        color = args.color if args.color != 'teal' else preset.get('color', 'teal')
    elif args.stat and args.headline:
        stat = args.stat
        headline = args.headline
        subtext = args.subtext
        color = args.color
    else:
        parser.error("Provide --stat and --headline, or --article, or --all")
        return

    accent = ACCENT_COLORS.get(color, TEAL)
    create_stat_card(
        stat=stat,
        headline=headline,
        subtext=subtext,
        accent_color=accent,
        output_path=args.output,
    )


if __name__ == '__main__':
    main()
