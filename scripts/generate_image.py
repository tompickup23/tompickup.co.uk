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

    args = parser.parse_args()

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
