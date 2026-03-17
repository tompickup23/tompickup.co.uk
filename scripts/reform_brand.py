#!/usr/bin/env python3
"""
Reform UK shared branding system for social media image and video generation.

Provides the canonical Reform UK press conference aesthetic:
- Dark navy backgrounds with radial gradient vignette
- Reform teal (#12B6CF) and red (#E4003B) accent colors
- REFORM UK logo drawn via PIL with location variants
- Watermark bar, data badges, marquee bar, accent lines
- Cross-platform font loading (macOS + Linux VPS)

Used by:
    generate_image.py  -- 1080x1080 stat cards
    generate_video.py  -- 1080x1920 vertical shorts

Usage as module:
    from reform_brand import (
        COLORS, TEAL, NAVY, WHITE, RED,
        load_font, draw_reform_logo, create_branded_background,
        draw_watermark_bar, draw_data_badge, draw_marquee_bar,
        draw_accent_line, draw_rounded_rect, generate_logo_png,
    )

Usage standalone (generates test logos):
    python3 scripts/reform_brand.py
    python3 scripts/reform_brand.py --output-dir /tmp/reform-logos
"""

import os
import sys
import math
import argparse
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageChops
except ImportError:
    print("ERROR: Pillow not installed. Run: pip3 install Pillow")
    sys.exit(1)


# ============================================================
# ASSET PATHS
# ============================================================

ASSETS_DIR = Path(__file__).parent / "assets"


# ============================================================
# DESIGN CONSTANTS
# ============================================================

# Primary brand colors
NAVY = (10, 22, 40)             # #0A1628 -- primary background
NAVY_MID = (14, 30, 54)         # slightly lighter navy for gradients
NAVY_LIGHT = (18, 38, 66)       # lighter still, for hover/active states
CARD_BG = (18, 34, 58)          # card/panel background
CARD_BORDER = (28, 48, 72)      # card border

TEAL = (18, 182, 207)           # #12B6CF -- Reform UK accent
TEAL_BRIGHT = (30, 210, 240)    # lighter teal for highlights
TEAL_DIM = (14, 120, 140)       # darker teal for subtle elements
TEAL_GLOW = (18, 182, 207, 40)  # teal with alpha for glow effects

RED = (228, 0, 59)              # #E4003B -- Reform UK red
RED_DIM = (180, 0, 45)          # darker red for press/active

WHITE = (240, 240, 245)         # off-white, softer than pure 255
PURE_WHITE = (255, 255, 255)    # for when true white is needed
LIGHT_GRAY = (200, 200, 210)    # secondary text
MID_GRAY = (130, 130, 140)      # tertiary text, watermarks
DIM_GRAY = (80, 82, 88)         # very subtle elements
DARK_LINE = (40, 55, 80)        # subtle structural lines

# Accent color palette (for stat cards, charts, etc.)
GREEN = (48, 209, 88)           # #30D158 -- positive/growth
AMBER = (255, 159, 10)          # #FF9F0A -- warning/caution
GOLD = (255, 214, 10)           # #FFD60A -- achievement/highlight
ORANGE = (255, 120, 30)         # vivid orange
CRIMSON = (255, 69, 58)         # #FF453A -- softer red for stats

# Named accent color map
ACCENT_COLORS = {
    'teal': TEAL,
    'red': CRIMSON,
    'green': GREEN,
    'amber': AMBER,
    'gold': GOLD,
    'orange': ORANGE,
    'white': PURE_WHITE,
}

# Marquee bar
MARQUEE_BG = (240, 240, 245)    # white bar background
MARQUEE_TEXT_COLOR = (10, 22, 40)  # navy text on white bar

# Full color dictionary (for backward compat with generate_video.py)
COLORS = {
    'bg':           NAVY,
    'bg2':          NAVY_MID,
    'card':         CARD_BG,
    'card_border':  CARD_BORDER,
    'teal':         TEAL,
    'teal_bright':  TEAL_BRIGHT,
    'teal_dim':     TEAL_DIM,
    'teal_glow':    TEAL_GLOW,
    'white':        WHITE,
    'light':        LIGHT_GRAY,
    'muted':        MID_GRAY,
    'dim':          DIM_GRAY,
    'red':          RED,
    'red_dim':      RED_DIM,
    'red_badge':    RED,
    'orange':       ORANGE,
    'green':        GREEN,
    'marquee_bar':  MARQUEE_BG,
    'marquee_text': MARQUEE_TEXT_COLOR,
}

# Party colors (for election/political content)
PARTY_COLORS = {
    'Conservative': (0, 135, 220),
    'Labour':       (228, 0, 59),
    'Lib Dem':      (250, 166, 26),
    'Green':        (106, 176, 35),
    'Independent':  (170, 170, 180),
    'Reform UK':    TEAL,
}

PARTY_ABBR = {
    'Conservative': 'CON',
    'Labour': 'LAB',
    'Lib Dem': 'LD',
    'Green': 'GRN',
    'Independent': 'IND',
    'Reform UK': 'REF',
}


# ============================================================
# FONT SYSTEM
# ============================================================

# Font search paths: each key maps to a list of candidates,
# tried in order. First existing path wins. Covers macOS and Linux.
FONT_PATHS = {
    'black': [
        '/System/Library/Fonts/Supplemental/Arial Black.ttf',
        '/usr/share/fonts/truetype/msttcorefonts/Arial_Black.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    ],
    'bold': [
        '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
        '/usr/share/fonts/truetype/msttcorefonts/Arial_Bold.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    ],
    'regular': [
        '/System/Library/Fonts/Supplemental/Arial.ttf',
        '/usr/share/fonts/truetype/msttcorefonts/Arial.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
    ],
    'din_bold': [
        '/System/Library/Fonts/Supplemental/DIN Alternate Bold.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    ],
    'din_condensed': [
        '/System/Library/Fonts/Supplemental/DIN Condensed Bold.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    ],
    'narrow_bold': [
        '/System/Library/Fonts/Supplemental/Arial Narrow Bold.ttf',
        '/usr/share/fonts/truetype/msttcorefonts/Arial_Bold.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    ],
    'sf_bold': [
        '/System/Library/Fonts/SFPro-Bold.otf',
        '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    ],
    'sf_regular': [
        '/System/Library/Fonts/SFPro-Regular.otf',
        '/System/Library/Fonts/SFPro.ttf',
        '/System/Library/Fonts/Supplemental/Arial.ttf',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
    ],
}

# Module-level font cache
_font_cache = {}


def _resolve_font_path(style):
    """Resolve the first existing font path for a given style.

    Args:
        style: Font style key from FONT_PATHS (e.g. 'bold', 'din_bold').

    Returns:
        Absolute path string, or None if no candidate exists.
    """
    candidates = FONT_PATHS.get(style, [])
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def load_font(style_or_size, size_or_bold=None, bold=None):
    """Load a PIL ImageFont with cross-platform fallback.

    Supports two calling conventions for backward compatibility:

        # Named style + size (generate_image.py pattern):
        load_font('din_bold', 28)
        load_font('bold', 46)
        load_font('regular', 22)

        # Size + bold flag (generate_video.py pattern):
        load_font(32, bold=True)
        load_font(22)

    Args:
        style_or_size: Either a style string key or an integer font size.
        size_or_bold: Integer font size (when first arg is style),
                      or bool bold flag, or None.
        bold: Explicit bool bold flag (for size-first calling convention).

    Returns:
        PIL ImageFont.FreeTypeFont or default bitmap font.
    """
    # Determine calling convention
    if isinstance(style_or_size, int):
        # Called as load_font(size) or load_font(size, bold=True)
        font_size = style_or_size
        is_bold = bold if bold is not None else (
            size_or_bold if isinstance(size_or_bold, bool) else False
        )
        style = 'bold' if is_bold else 'regular'
    else:
        # Called as load_font('style', size)
        style = style_or_size
        font_size = size_or_bold if isinstance(size_or_bold, int) else 16
        is_bold = style in ('bold', 'black', 'din_bold', 'din_condensed',
                            'narrow_bold', 'sf_bold')

    cache_key = (style, font_size)
    if cache_key in _font_cache:
        return _font_cache[cache_key]

    # Try the requested style first
    path = _resolve_font_path(style)
    if path:
        try:
            font = ImageFont.truetype(path, font_size)
            _font_cache[cache_key] = font
            return font
        except Exception:
            pass

    # Fallback chain: bold -> black -> regular -> default
    for fallback_style in ('bold', 'black', 'regular'):
        if fallback_style == style:
            continue
        fb_path = _resolve_font_path(fallback_style)
        if fb_path:
            try:
                font = ImageFont.truetype(fb_path, font_size)
                _font_cache[cache_key] = font
                return font
            except Exception:
                continue

    # Last resort: PIL default bitmap font
    font = ImageFont.load_default()
    _font_cache[cache_key] = font
    return font


# ============================================================
# DRAWING UTILITIES
# ============================================================

def draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    """Draw a rounded rectangle.

    Args:
        draw: PIL ImageDraw instance.
        xy: Tuple (x0, y0, x1, y1) bounding box.
        radius: Corner radius in pixels.
        fill: Fill color tuple.
        outline: Outline color tuple.
        width: Outline width in pixels.
    """
    x0, y0, x1, y1 = xy
    r = min(radius, (x1 - x0) // 2, (y1 - y0) // 2)
    if r < 1:
        if fill:
            draw.rectangle(xy, fill=fill)
        return

    if fill:
        draw.rectangle([(x0 + r, y0), (x1 - r, y1)], fill=fill)
        draw.rectangle([(x0, y0 + r), (x1, y1 - r)], fill=fill)
        draw.pieslice([(x0, y0), (x0 + 2 * r, y0 + 2 * r)], 180, 270, fill=fill)
        draw.pieslice([(x1 - 2 * r, y0), (x1, y0 + 2 * r)], 270, 360, fill=fill)
        draw.pieslice([(x0, y1 - 2 * r), (x0 + 2 * r, y1)], 90, 180, fill=fill)
        draw.pieslice([(x1 - 2 * r, y1 - 2 * r), (x1, y1)], 0, 90, fill=fill)

    if outline:
        draw.arc([(x0, y0), (x0 + 2 * r, y0 + 2 * r)], 180, 270, fill=outline, width=width)
        draw.arc([(x1 - 2 * r, y0), (x1, y0 + 2 * r)], 270, 360, fill=outline, width=width)
        draw.arc([(x0, y1 - 2 * r), (x0 + 2 * r, y1)], 90, 180, fill=outline, width=width)
        draw.arc([(x1 - 2 * r, y1 - 2 * r), (x1, y1)], 0, 90, fill=outline, width=width)
        draw.line([(x0 + r, y0), (x1 - r, y0)], fill=outline, width=width)
        draw.line([(x0 + r, y1), (x1 - r, y1)], fill=outline, width=width)
        draw.line([(x0, y0 + r), (x0, y1 - r)], fill=outline, width=width)
        draw.line([(x1, y0 + r), (x1, y1 - r)], fill=outline, width=width)


def draw_accent_line(draw, x1, y1, x2, y2, color=None, width=3):
    """Draw a subtle accent line.

    Args:
        draw: PIL ImageDraw instance.
        x1, y1: Start coordinates.
        x2, y2: End coordinates.
        color: Line color tuple (defaults to TEAL).
        width: Line width in pixels.
    """
    if color is None:
        color = TEAL
    draw.line([(x1, y1), (x2, y2)], fill=color, width=width)


# ============================================================
# BACKGROUND GENERATION
# ============================================================

def create_branded_background(width, height, center_offset_y=-40):
    """Create the navy background with radial gradient vignette.

    Produces the signature Reform UK press conference dark navy
    background. The center of the gradient is slightly above the
    image center, drawing the eye upward toward the main content.

    Args:
        width: Image width in pixels.
        height: Image height in pixels.
        center_offset_y: Vertical offset for gradient center
                         (negative = above center). Default -40.

    Returns:
        PIL Image (RGB mode) with gradient background.
    """
    img = Image.new('RGB', (width, height), NAVY)

    center_x = width // 2
    center_y = height // 2 + center_offset_y

    # Draw concentric ellipses from outside in, interpolating
    # from NAVY (edges) to NAVY_MID (center) for a subtle vignette.
    gradient = Image.new('RGB', (width, height), NAVY)
    gdraw = ImageDraw.Draw(gradient)

    steps = 60
    for i in range(steps):
        t = i / steps  # 0 = outermost, 1 = innermost
        color = tuple(
            int(NAVY[c] + (NAVY_MID[c] - NAVY[c]) * t)
            for c in range(3)
        )
        rx = int(width * 0.8 * (1 - t * 0.6))
        ry = int(height * 0.8 * (1 - t * 0.6))
        gdraw.ellipse(
            [center_x - rx, center_y - ry, center_x + rx, center_y + ry],
            fill=color
        )

    return gradient


def apply_edge_vignette(img, top=80, bottom=80, left=40, right=40, intensity=40):
    """Apply subtle edge darkening to an image.

    Args:
        img: PIL Image (RGB mode).
        top: Fade depth from top edge in pixels.
        bottom: Fade depth from bottom edge in pixels.
        left: Fade depth from left edge in pixels.
        right: Fade depth from right edge in pixels.
        intensity: Maximum alpha for darkening (0-255).

    Returns:
        PIL Image (RGB mode) with vignette applied.
    """
    width, height = img.size
    vignette = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    vdraw = ImageDraw.Draw(vignette)

    # Top edge fade
    for i in range(top):
        alpha = int(intensity * (1 - i / top))
        vdraw.line([(0, i), (width, i)], fill=(0, 0, 0, alpha))

    # Bottom edge fade
    for i in range(bottom):
        y = height - 1 - i
        alpha = int(intensity * (1 - i / bottom))
        vdraw.line([(0, y), (width, y)], fill=(0, 0, 0, alpha))

    # Left edge fade
    for i in range(left):
        alpha = int(int(intensity * 0.625) * (1 - i / left))
        vdraw.line([(i, 0), (i, height)], fill=(0, 0, 0, alpha))

    # Right edge fade
    for i in range(right):
        x = width - 1 - i
        alpha = int(int(intensity * 0.625) * (1 - i / right))
        vdraw.line([(x, 0), (x, height)], fill=(0, 0, 0, alpha))

    return Image.alpha_composite(img.convert('RGBA'), vignette).convert('RGB')


# ============================================================
# REFORM UK LOGO -- OFFICIAL PNG ASSET
# ============================================================

# Module-level logo cache (loaded once, reused everywhere)
_logo_cache = {}

# The O circle-arrow symbol region within the 340x43 logo PNG.
# Determined by pixel analysis: cols 115-163, with thin F-connection at 115-118.
_LOGO_O_CROP = (115, 0, 164, 43)  # (left, top, right, bottom) -- the circle-arrow O

# REFORM portion (before the gap at col 164)
_LOGO_REFORM_CROP = (0, 0, 164, 43)

# UK portion (after the gap at col 257)
_LOGO_UK_CROP = (261, 0, 340, 43)


def _load_logo_png():
    """Load the official Reform UK logo PNG from assets, cached at module level.

    Returns:
        PIL Image (RGBA mode, 340x43, white on transparent).
    """
    if 'original' not in _logo_cache:
        logo_path = ASSETS_DIR / "reform_uk_logo.png"
        if not logo_path.exists():
            raise FileNotFoundError(
                f"Reform UK logo not found at {logo_path}. "
                "Place the official 340x43 RGBA logo PNG in scripts/assets/"
            )
        _logo_cache['original'] = Image.open(logo_path).convert('RGBA')
    return _logo_cache['original']


def _tint_logo(logo_img, color):
    """Tint a white-on-transparent logo image to a specific color.

    The source logo has white (255,255,255) pixels with varying alpha.
    This replaces the white with the target color while preserving alpha.

    Args:
        logo_img: PIL Image (RGBA) -- white on transparent.
        color: RGB tuple (r, g, b) to tint to. If None or white, returns as-is.

    Returns:
        PIL Image (RGBA) tinted to the specified color.
    """
    if color is None or color == PURE_WHITE or color == (255, 255, 255):
        return logo_img.copy()

    cache_key = ('tinted', color)
    if cache_key in _logo_cache:
        return _logo_cache[cache_key]

    # Create a solid color layer matching the target color
    r, g, b = color[:3]
    tinted = Image.new('RGBA', logo_img.size, (r, g, b, 255))

    # Use the original alpha channel as mask
    _, _, _, alpha = logo_img.split()
    tinted.putalpha(alpha)

    _logo_cache[cache_key] = tinted
    return tinted


def _scale_logo(logo_img, scale):
    """Scale a logo image by the given factor, using high-quality resampling.

    Args:
        logo_img: PIL Image (RGBA).
        scale: Float multiplier (1.0 = original size).

    Returns:
        PIL Image (RGBA) scaled.
    """
    if abs(scale - 1.0) < 0.001:
        return logo_img.copy()

    new_w = max(1, int(logo_img.width * scale))
    new_h = max(1, int(logo_img.height * scale))
    return logo_img.resize((new_w, new_h), Image.LANCZOS)


def _get_image_from_draw(draw):
    """Extract the PIL Image from an ImageDraw instance.

    Args:
        draw: PIL ImageDraw.Draw instance.

    Returns:
        PIL Image that the draw context is attached to.
    """
    return draw.im  # ImageDraw stores the underlying image as .im


def _draw_spaced_text(draw, x, y, text, font, fill, tracking=0):
    """Draw text with letter spacing (tracking).

    PIL does not natively support letter-spacing, so we draw each
    character individually with extra horizontal offset.

    Args:
        draw: PIL ImageDraw instance.
        x, y: Top-left position.
        text: String to draw.
        font: PIL ImageFont.
        fill: Color tuple.
        tracking: Extra pixels between each character.

    Returns:
        Total width of the drawn text (including tracking).
    """
    cursor_x = x
    for i, char in enumerate(text):
        draw.text((cursor_x, y), char, fill=fill, font=font)
        bbox = draw.textbbox((0, 0), char, font=font)
        char_w = bbox[2] - bbox[0]
        cursor_x += char_w
        if i < len(text) - 1:
            cursor_x += tracking
    return cursor_x - x


def draw_reform_logo(img_or_draw, x, y, scale=1.0, variant='full', color=None,
                     img=None):
    """Draw the official Reform UK logo from the PNG asset.

    Loads the official 340x43 RGBA logo (white on transparent), scales it,
    optionally tints it, and composites it onto the target image. Supports
    location subtitle variants and the compact circle-arrow-O extract.

    Args:
        img_or_draw: PIL Image or PIL ImageDraw instance. When an Image is
                     provided, the logo is composited directly. When an
                     ImageDraw is provided, the underlying Image is extracted
                     via draw._image (standard Pillow internals).
        x, y: Top-left position for the logo.
        scale: Overall size multiplier. At scale=1.0 the logo is 340px wide
               (original asset size). Typical usage: 0.5-3.0.
        variant: Logo variant:
            'full'        -- Official REFORM UK logo (full width)
            'lancashire'  -- Official logo + LANCASHIRE subtitle below
            'burnley'     -- Official logo + BURNLEY & PADIHAM subtitle
            'compact'     -- Just the circle-arrow O symbol cropped out
            'tompickup'   -- Official logo + tompickup.co.uk styled below
        color: RGB tuple to tint the logo. Defaults to white (no tint).
               Pass TEAL for the Reform teal variant.
        img: DEPRECATED. Kept for backward compatibility. Ignored.

    Returns:
        Tuple (x, y, w, h) bounding box of the full logo drawn.
    """
    # Resolve the target Image for compositing
    if isinstance(img_or_draw, Image.Image):
        target_img = img_or_draw
    else:
        # ImageDraw -- extract the underlying image
        # Pillow's ImageDraw stores the image as ._image
        target_img = img_or_draw._image if hasattr(img_or_draw, '_image') else None
        if target_img is None:
            # Fallback: try im attribute (older Pillow versions)
            target_img = getattr(img_or_draw, 'im', None)
        if target_img is None:
            # Cannot composite without an Image -- fall back to measuring only
            # (used by scratch-surface measurement calls)
            pass

    # Load the official logo
    logo_raw = _load_logo_png()

    # Tint if requested
    if color is not None:
        logo = _tint_logo(logo_raw, color)
    else:
        logo = logo_raw.copy()

    if variant == 'compact':
        # Crop just the circle-arrow O symbol
        logo = logo.crop(_LOGO_O_CROP)

    # Scale
    logo_scaled = _scale_logo(logo, scale)
    logo_w, logo_h = logo_scaled.size

    total_w = logo_w
    total_h = logo_h

    # Paste the main logo onto the target
    if target_img is not None:
        # Ensure target supports alpha compositing
        if target_img.mode == 'RGB':
            target_img_rgba = target_img.convert('RGBA')
            target_img_rgba.paste(logo_scaled, (int(x), int(y)), logo_scaled)
            # Copy back to RGB target
            rgb_result = target_img_rgba.convert('RGB')
            target_img.paste(rgb_result)
        elif target_img.mode == 'RGBA':
            target_img.paste(logo_scaled, (int(x), int(y)), logo_scaled)
        else:
            # Best effort for other modes
            target_img.paste(logo_scaled, (int(x), int(y)), logo_scaled)

    # Draw subtitle text for location variants
    if variant in ('lancashire', 'burnley', 'tompickup'):
        location_text = {
            'lancashire': 'LANCASHIRE',
            'burnley': 'BURNLEY & PADIHAM',
            'tompickup': 'tompickup.co.uk',
        }[variant]

        # Subtitle font: sized relative to the logo height
        sub_size = max(8, int(logo_h * 0.42))
        sub_font = load_font('regular', sub_size)

        # Measure the raw subtitle text width (without extra tracking)
        raw_bbox = ImageDraw.Draw(Image.new('RGBA', (1, 1))).textbbox(
            (0, 0), location_text, font=sub_font
        )
        raw_w = raw_bbox[2] - raw_bbox[0]

        # Calculate letter-spacing to approximately match the logo width
        n_gaps = max(1, len(location_text) - 1)
        if raw_w < logo_w:
            loc_tracking = max(0, int((logo_w - raw_w) / n_gaps))
        else:
            loc_tracking = 0

        # Measure with tracking to get actual width
        loc_w = 0
        tmp_draw = ImageDraw.Draw(Image.new('RGBA', (1, 1)))
        for i, ch in enumerate(location_text):
            bbox = tmp_draw.textbbox((0, 0), ch, font=sub_font)
            loc_w += bbox[2] - bbox[0]
            if i < len(location_text) - 1:
                loc_w += loc_tracking

        # Gap between logo and subtitle
        loc_gap = max(3, int(logo_h * 0.25))
        loc_y = int(y) + logo_h + loc_gap

        # Center subtitle under the logo
        loc_x = int(x) + (logo_w - loc_w) // 2

        # Determine subtitle color
        sub_color = LIGHT_GRAY if variant != 'tompickup' else WHITE

        # We need a draw context for the text
        if target_img is not None:
            if target_img.mode == 'RGB':
                target_draw = ImageDraw.Draw(target_img)
            else:
                target_draw = ImageDraw.Draw(target_img)
            _draw_spaced_text(
                target_draw, loc_x, loc_y, location_text,
                sub_font, sub_color, loc_tracking
            )

        sub_bbox_check = ImageDraw.Draw(Image.new('RGBA', (1, 1))).textbbox(
            (0, 0), "L", font=sub_font
        )
        sub_h = sub_bbox_check[3] - sub_bbox_check[1]
        total_h = (logo_h + loc_gap + sub_h)
        total_w = max(logo_w, loc_w)

    return (int(x), int(y), total_w, total_h)


# ============================================================
# PRESS CONFERENCE OVERLAYS
# ============================================================

def draw_watermark_bar(draw, img_width, img_height, text="tompickup.co.uk",
                       left_text="REFORM UK LANCASHIRE", left_dot_color=None,
                       location="lancashire"):
    """Draw the bottom watermark bar with tompickup.co.uk branding.

    Places the site URL at bottom-right and a small Reform UK logo + location
    on the left (the official PNG scaled down, followed by 'LANCASHIRE' or
    'BURNLEY' text).

    Args:
        draw: PIL ImageDraw instance.
        img_width: Image width.
        img_height: Image height.
        text: Watermark text (default: tompickup.co.uk).
        left_text: Left-side label text (fallback if logo fails).
        left_dot_color: Color for the dot indicator. Defaults to TEAL.
        location: Location suffix after the logo: 'lancashire', 'burnley',
                  or None for just the logo.

    Returns:
        Height of the watermark area in pixels.
    """
    if left_dot_color is None:
        left_dot_color = TEAL

    watermark_font = load_font('regular', 22)
    wm_bbox = draw.textbbox((0, 0), text, font=watermark_font)
    wm_w = wm_bbox[2] - wm_bbox[0]

    wm_y = img_height - 60
    draw.text(
        (img_width - wm_w - 60, wm_y),
        text,
        fill=MID_GRAY,
        font=watermark_font,
    )

    if left_text:
        # Colored dot indicator
        dot_y = wm_y + 5
        draw.ellipse(
            [60, dot_y, 72, dot_y + 12],
            fill=left_dot_color,
        )

        # Small official Reform UK logo (scaled to ~120px wide, tinted to MID_GRAY)
        logo_end_x = 82
        try:
            logo_scale = 120.0 / 340.0  # ~0.35 scale => 120x15px
            _, _, lw, _ = draw_reform_logo(
                draw, 82, dot_y - 2,
                scale=logo_scale, variant='full', color=MID_GRAY,
            )
            logo_end_x = 82 + lw
        except Exception:
            # Fallback to text if logo fails
            label_font = load_font('bold', 20)
            draw.text(
                (82, dot_y - 4),
                left_text,
                fill=MID_GRAY,
                font=label_font,
            )

        # Location text after the logo (LANCASHIRE / BURNLEY)
        if location:
            loc_text = location.upper()
            loc_font = load_font('regular', 16)
            draw.text(
                (logo_end_x + 10, dot_y - 1),
                loc_text,
                fill=MID_GRAY,
                font=loc_font,
            )

    return 60


def draw_data_badge(draw, x, y, text="DATA", scale=1.0):
    """Draw a red badge (like broadcast 'LIVE' badges).

    Creates a red rounded-rectangle pill with white text, matching
    the Reform UK press conference aesthetic.

    Args:
        draw: PIL ImageDraw instance.
        x, y: Top-left position of the badge.
        text: Badge label (default: "DATA").
        scale: Size multiplier.

    Returns:
        Tuple (badge_width, badge_height) of the drawn badge.
    """
    font_size = max(10, int(18 * scale))
    pad_x = max(8, int(14 * scale))
    pad_y = max(4, int(8 * scale))

    font = load_font('bold', font_size)
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]

    badge_w = text_w + pad_x * 2
    badge_h = text_h + pad_y * 2

    # Red background pill
    draw_rounded_rect(
        draw,
        (x, y, x + badge_w, y + badge_h),
        badge_h // 2,
        fill=RED,
    )

    # White text centered in pill
    draw.text(
        (x + pad_x, y + pad_y),
        text,
        fill=WHITE,
        font=font,
    )

    return (badge_w, badge_h)


def draw_marquee_bar(draw, img_width, y, text, progress=0.0,
                     bar_height=60, accent_line=True):
    """Draw a white marquee bar with scrolling text.

    Mimics the Reform UK press conference lower-third ticker bar.
    For static images, progress controls the text scroll position.
    For video, increment progress each frame (or pass frame_idx / total).

    Args:
        draw: PIL ImageDraw instance.
        img_width: Image width.
        y: Top Y coordinate of the marquee bar.
        text: Scrolling text content. Should be long enough to fill
              the bar (repeat with bullet separators).
        progress: Scroll position as 0.0-1.0 ratio, or can exceed 1.0
                  for continuous scrolling.
        bar_height: Height of the marquee bar in pixels.
        accent_line: Whether to draw a teal line above the bar.

    Returns:
        Bottom Y coordinate of the marquee bar.
    """
    bar_bottom = y + bar_height

    # White bar background
    draw.rectangle([(0, y), (img_width, bar_bottom)], fill=MARQUEE_BG)

    # Thin teal accent line above the bar
    if accent_line:
        draw.rectangle([(0, y - 2), (img_width, y)], fill=TEAL)

    # Scrolling text
    font = load_font('bold', 22)
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]

    # Calculate scroll offset
    if text_w > 0:
        offset = -(progress * text_w) % text_w
        offset = -abs(offset)
    else:
        offset = 0

    # Center text vertically in bar
    text_y = y + (bar_height - text_h) // 2

    # Draw text tiles to fill the full width seamlessly
    cursor_x = int(offset)
    while cursor_x < img_width:
        draw.text(
            (cursor_x, text_y),
            text,
            fill=MARQUEE_TEXT_COLOR,
            font=font,
        )
        cursor_x += text_w

    return bar_bottom


# ============================================================
# STANDALONE LOGO PNG GENERATION
# ============================================================

def generate_logo_png(width, height, variant='full', output_path=None,
                      transparent=True, padding=0.15, color=None):
    """Generate a standalone Reform UK logo as a PNG image.

    Uses the official logo PNG asset, scaled to fit the output dimensions.

    Args:
        width: Output image width in pixels.
        height: Output image height in pixels.
        variant: Logo variant ('full', 'lancashire', 'burnley', 'compact',
                 'tompickup').
        output_path: File path to save PNG. If None, image is not saved.
        transparent: If True, background is transparent (RGBA mode).
                     If False, uses navy background.
        padding: Padding ratio (0.0-0.5) around the logo within the image.
        color: RGB tuple to tint the logo. None = white (original).

    Returns:
        PIL Image (RGBA if transparent, else RGB).
    """
    mode = 'RGBA' if transparent else 'RGB'
    bg = (0, 0, 0, 0) if transparent else NAVY
    img = Image.new(mode, (width, height), bg)

    # Calculate available area after padding
    pad_x = int(width * padding)
    pad_y = int(height * padding)
    avail_w = width - 2 * pad_x
    avail_h = height - 2 * pad_y

    # Binary search for optimal scale that fits the available area.
    # We measure by calling draw_reform_logo on a scratch surface.
    lo, hi = 0.01, 20.0
    best_scale = 1.0

    for _ in range(25):
        mid = (lo + hi) / 2.0
        scratch = Image.new('RGBA', (max(width * 2, 2000), max(height * 2, 2000)),
                            (0, 0, 0, 0))
        _, _, w, h = draw_reform_logo(
            scratch, 0, 0, scale=mid, variant=variant, color=color
        )
        if w <= avail_w and h <= avail_h:
            best_scale = mid
            lo = mid
        else:
            hi = mid

    # Measure at optimal scale
    scratch = Image.new('RGBA', (max(width * 2, 2000), max(height * 2, 2000)),
                        (0, 0, 0, 0))
    _, _, logo_w, logo_h = draw_reform_logo(
        scratch, 0, 0, scale=best_scale, variant=variant, color=color
    )

    # Draw centered on the actual output image
    logo_x = (width - logo_w) // 2
    logo_y = (height - logo_h) // 2

    draw_reform_logo(
        img, logo_x, logo_y, scale=best_scale, variant=variant, color=color
    )

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        img.save(output_path, 'PNG')
        file_size = os.path.getsize(output_path)
        print(f"  Logo: {output_path} ({file_size:,} bytes, {width}x{height}, {variant})")

    return img


# ============================================================
# MAIN -- TEST GENERATION
# ============================================================

def main():
    """Generate test logos at different scales and variants to /tmp/reform-logos/."""
    parser = argparse.ArgumentParser(
        description="Reform UK branding system -- generate test logos and assets.",
    )
    parser.add_argument(
        '--output-dir', default='/tmp/reform-logos',
        help='Output directory for test assets (default: /tmp/reform-logos/)',
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Reform UK Branding System -- Test Generation")
    print(f"Output: {output_dir}/")
    print()

    # 1. Generate logo variants at standard sizes
    variants = ['full', 'lancashire', 'burnley', 'compact', 'tompickup']
    sizes = [(200, 100), (400, 200), (800, 400)]

    print("=== Logo Variants ===")
    for variant in variants:
        for w, h in sizes:
            fname = f"logo_{variant}_{w}x{h}.png"
            generate_logo_png(
                w, h, variant=variant,
                output_path=str(output_dir / fname),
                transparent=True,
            )

    # 2. Generate a logo on navy background (for social media profile)
    print()
    print("=== Profile Images ===")
    for variant in ['full', 'lancashire', 'burnley', 'tompickup']:
        fname = f"profile_{variant}_512x512.png"
        generate_logo_png(
            512, 512, variant=variant,
            output_path=str(output_dir / fname),
            transparent=False,
            padding=0.2,
        )

    # 3. Generate a full branded background test
    print()
    print("=== Branded Background Test ===")
    for dims in [(1080, 1080), (1080, 1920)]:
        w, h = dims
        img = create_branded_background(w, h)
        draw = ImageDraw.Draw(img)

        # Add logo top-right (measure first to avoid clipping)
        scratch = Image.new('RGBA', (w, 200), (0, 0, 0, 0))
        _, _, logo_w, logo_h = draw_reform_logo(scratch, 0, 0, scale=2.0, variant='full')
        draw_reform_logo(img, w - logo_w - 60, 50, scale=2.0, variant='full')

        # Add data badge top-left
        draw_data_badge(draw, 30, 28, text="DATA")

        # Add accent line
        draw_accent_line(draw, 72, 200, 72, h - 200, color=(*TEAL[:3], 60), width=3)

        # Add watermark bar
        draw_watermark_bar(draw, w, h)

        # Add marquee bar (if vertical/video format)
        if h > w:
            marquee_text = (
                "tompickup.co.uk  \u2022  Reform UK  \u2022  "
                "Coal Clough with Deerplay  \u2022  Vote May 7th  \u2022  "
            ) * 2
            draw_marquee_bar(draw, w, h - 60, marquee_text, progress=0.3)

        # Apply edge vignette
        img = apply_edge_vignette(img)

        fname = f"branded_bg_{w}x{h}.png"
        out_path = str(output_dir / fname)
        img.save(out_path, 'PNG', quality=95)
        file_size = os.path.getsize(out_path)
        print(f"  Background: {out_path} ({file_size:,} bytes, {w}x{h})")

    # 4. Generate a stat card mockup using shared components
    print()
    print("=== Stat Card Mockup ===")
    img = create_branded_background(1080, 1080)
    draw = ImageDraw.Draw(img)

    scratch = Image.new('RGBA', (1080, 200), (0, 0, 0, 0))
    _, _, lw, _ = draw_reform_logo(scratch, 0, 0, scale=2.0, variant='full')
    draw_reform_logo(img, 1080 - lw - 60, 50, scale=2.0, variant='full')
    draw_data_badge(draw, 30, 28, text="REFORM")
    draw_accent_line(draw, 72, 200, 72, 880, color=TEAL, width=3)

    stat_font = load_font('black', 180)
    stat = "\u00a3921M"
    stat_bbox = draw.textbbox((0, 0), stat, font=stat_font)
    stat_w = stat_bbox[2] - stat_bbox[0]
    stat_x = (1080 - stat_w) // 2
    draw.text((stat_x, 320), stat, fill=CRIMSON, font=stat_font)

    headline_font = load_font('bold', 46)
    headline = "LOST UNDER THE CONSERVATIVES"
    hl_bbox = draw.textbbox((0, 0), headline, font=headline_font)
    hl_w = hl_bbox[2] - hl_bbox[0]
    draw.text(((1080 - hl_w) // 2, 560), headline, fill=WHITE, font=headline_font)

    draw_accent_line(draw, 440, 630, 640, 630, color=CRIMSON, width=3)

    sub_font = load_font('regular', 26)
    subtext = "LCC Statement of Accounts, 2017-2025"
    sub_bbox = draw.textbbox((0, 0), subtext, font=sub_font)
    sub_w = sub_bbox[2] - sub_bbox[0]
    draw.text(((1080 - sub_w) // 2, 660), subtext, fill=LIGHT_GRAY, font=sub_font)

    draw_watermark_bar(draw, 1080, 1080)

    img = apply_edge_vignette(img)

    out_path = str(output_dir / "mockup_stat_card.png")
    img.save(out_path, 'PNG', quality=95)
    file_size = os.path.getsize(out_path)
    print(f"  Mockup: {out_path} ({file_size:,} bytes, 1080x1080)")

    print()
    print(f"Done. {len(list(output_dir.glob('*.png')))} files generated in {output_dir}/")


if __name__ == '__main__':
    main()
