#!/usr/bin/env python3
"""
Social media short video generator for tompickup.co.uk articles.

UPGRADED v2: Professional-grade political shorts with:
- edge-tts voiceover (en-GB-RyanNeural)
- Animated data visualisations (growing bars, counting numbers)
- Phrase-by-phrase captions synced to narration
- Ken Burns zoom on background images
- Crossfade transitions between scenes
- Gradient bar fills with glow effects
- Premium dark theme matching the website

Generates 9:16 vertical shorts (1080x1920) suitable for:
- Instagram Reels, TikTok, YouTube Shorts, Facebook Reels

Usage:
    python3 scripts/generate_video.py --article burnley-elections-2026-attendance
    python3 scripts/generate_video.py --article all
    python3 scripts/generate_video.py --article burnley-elections-2026-attendance --preview
    python3 scripts/generate_video.py --article burnley-elections-2026-attendance --no-voice
"""

import os
import sys
import argparse
import textwrap
import math
import asyncio
import subprocess
import tempfile
import shutil
import json
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter

# edge-tts for voiceover
try:
    import edge_tts
    HAS_TTS = True
except ImportError:
    HAS_TTS = False

# Paths
BASE_DIR = Path(__file__).parent.parent
IMAGES_DIR = BASE_DIR / "public" / "images"
CONTENT_DIR = BASE_DIR / "src" / "content" / "news"
OUTPUT_DIR = BASE_DIR / "public" / "videos"
LOGO_PATH = Path("/tmp/reform-uk-logo.png")
FFMPEG = "/opt/homebrew/bin/ffmpeg"
FFPROBE = "/opt/homebrew/bin/ffprobe"

# Video dimensions (9:16 vertical)
W, H = 1080, 1920
FPS = 30

# TTS config
TTS_VOICE = "en-GB-RyanNeural"
TTS_RATE = "+10%"
TTS_PITCH = "+0Hz"

# ============================================================
# DESIGN SYSTEM
# ============================================================

COLORS = {
    'bg':           (13, 17, 23),
    'bg2':          (17, 21, 28),
    'card':         (22, 27, 34),
    'card_border':  (33, 38, 45),
    'teal':         (18, 182, 207),
    'teal_bright':  (30, 210, 240),
    'teal_dim':     (14, 120, 140),
    'teal_glow':    (18, 182, 207, 40),
    'white':        (240, 240, 245),
    'light':        (200, 200, 210),
    'muted':        (130, 130, 140),
    'dim':          (80, 82, 88),
    'red':          (255, 69, 58),
    'red_dim':      (180, 40, 35),
    'orange':       (255, 159, 10),
    'green':        (48, 209, 88),
}

PARTY_COLORS = {
    'Conservative': (0, 135, 220),
    'Labour':       (228, 0, 59),
    'Lib Dem':      (250, 166, 26),
    'Green':        (106, 176, 35),
    'Independent':  (170, 170, 180),
    'Reform UK':    (18, 182, 207),
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

_font_cache = {}

def load_font(size, bold=False):
    key = (size, bold)
    if key in _font_cache:
        return _font_cache[key]

    if bold:
        paths = [
            "/System/Library/Fonts/SFPro-Bold.otf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/SFPro.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
        ]
    else:
        paths = [
            "/System/Library/Fonts/SFPro-Regular.otf",
            "/System/Library/Fonts/SFPro.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "/Library/Fonts/Arial.ttf",
        ]

    for p in paths:
        try:
            idx = 1 if p.endswith('.ttc') and bold else 0
            f = ImageFont.truetype(p, size, index=idx)
            _font_cache[key] = f
            return f
        except Exception:
            try:
                f = ImageFont.truetype(p, size)
                _font_cache[key] = f
                return f
            except Exception:
                continue

    f = ImageFont.load_default()
    _font_cache[key] = f
    return f


# ============================================================
# DRAWING UTILITIES
# ============================================================

def draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    """Draw a rounded rectangle."""
    x0, y0, x1, y1 = xy
    r = min(radius, (x1 - x0) // 2, (y1 - y0) // 2)
    if r < 1:
        if fill:
            draw.rectangle(xy, fill=fill)
        return
    if fill:
        draw.rectangle([(x0+r, y0), (x1-r, y1)], fill=fill)
        draw.rectangle([(x0, y0+r), (x1, y1-r)], fill=fill)
        draw.pieslice([(x0, y0), (x0+2*r, y0+2*r)], 180, 270, fill=fill)
        draw.pieslice([(x1-2*r, y0), (x1, y0+2*r)], 270, 360, fill=fill)
        draw.pieslice([(x0, y1-2*r), (x0+2*r, y1)], 90, 180, fill=fill)
        draw.pieslice([(x1-2*r, y1-2*r), (x1, y1)], 0, 90, fill=fill)
    if outline:
        draw.arc([(x0, y0), (x0+2*r, y0+2*r)], 180, 270, fill=outline, width=width)
        draw.arc([(x1-2*r, y0), (x1, y0+2*r)], 270, 360, fill=outline, width=width)
        draw.arc([(x0, y1-2*r), (x0+2*r, y1)], 90, 180, fill=outline, width=width)
        draw.arc([(x1-2*r, y1-2*r), (x1, y1)], 0, 90, fill=outline, width=width)
        draw.line([(x0+r, y0), (x1-r, y0)], fill=outline, width=width)
        draw.line([(x0+r, y1), (x1-r, y1)], fill=outline, width=width)
        draw.line([(x0, y0+r), (x0, y1-r)], fill=outline, width=width)
        draw.line([(x1, y0+r), (x1, y1-r)], fill=outline, width=width)


def draw_gradient_bar(img, xy, color, progress=1.0):
    """Draw a horizontal bar with gradient fill and subtle glow."""
    x0, y0, x1, y1 = xy
    bar_w = int((x1 - x0) * progress)
    if bar_w < 4:
        return

    draw = ImageDraw.Draw(img)
    h = y1 - y0

    # Glow layer (subtle outer glow)
    glow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    glow_color = (*color, 25)
    gd.rectangle([(x0 - 2, y0 - 3), (x0 + bar_w + 2, y1 + 3)], fill=glow_color)
    glow = glow.filter(ImageFilter.GaussianBlur(radius=6))
    img_rgba = img.convert("RGBA")
    img_rgba = Image.alpha_composite(img_rgba, glow)

    # Main bar with vertical gradient (lighter top, darker bottom)
    for py in range(y0, y1):
        ratio = (py - y0) / max(h - 1, 1)
        # Top: brighter. Bottom: slightly darker
        r = int(color[0] * (1.15 - 0.3 * ratio))
        g = int(color[1] * (1.15 - 0.3 * ratio))
        b = int(color[2] * (1.15 - 0.3 * ratio))
        r, g, b = min(255, r), min(255, g), min(255, b)
        ImageDraw.Draw(img_rgba).line([(x0, py), (x0 + bar_w, py)], fill=(r, g, b, 255))

    # Specular highlight (thin bright line at top)
    highlight_color = tuple(min(255, c + 80) for c in color) + (60,)
    gd2 = ImageDraw.Draw(img_rgba)
    gd2.line([(x0 + 2, y0 + 1), (x0 + bar_w - 2, y0 + 1)], fill=highlight_color)

    # Copy back to RGB
    result = img_rgba.convert("RGB")
    img.paste(result)


def draw_vignette(img, intensity=0.4):
    """Add a subtle radial vignette to the image."""
    w, h = img.size
    vignette = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    vd = ImageDraw.Draw(vignette)

    cx, cy = w // 2, h // 2
    max_dist = math.sqrt(cx * cx + cy * cy)

    # Draw concentric rectangles for performance (not per-pixel)
    steps = 40
    for i in range(steps):
        ratio = i / steps
        alpha = int(255 * intensity * (ratio ** 2))
        margin = int((1 - ratio) * min(cx, cy) * 0.7)
        vd.rectangle(
            [(margin, margin), (w - margin, h - margin)],
            outline=(0, 0, 0, alpha), width=max(1, min(cx, cy) // steps)
        )

    vignette = vignette.filter(ImageFilter.GaussianBlur(radius=60))
    img_rgba = img.convert("RGBA")
    result = Image.alpha_composite(img_rgba, vignette).convert("RGB")
    img.paste(result)


def text_center_x(draw, text, font, y, fill, img_width=W):
    """Draw text centered horizontally."""
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    x = (img_width - tw) // 2
    draw.text((x, y), text, fill=fill, font=font)
    return tw


def ease_out_expo(t):
    """Exponential ease-out for smooth animations."""
    return 1 if t >= 1 else 1 - math.pow(2, -10 * t)


def ease_out_cubic(t):
    """Cubic ease-out."""
    return 1 - math.pow(1 - t, 3)


def lerp(a, b, t):
    """Linear interpolation."""
    return a + (b - a) * t


# ============================================================
# FRAME GENERATORS (base components)
# ============================================================

def create_frame_base(with_scanlines=False):
    """Create a base frame with premium dark background."""
    img = Image.new("RGB", (W, H), COLORS['bg'])
    draw = ImageDraw.Draw(img)

    # Subtle vertical gradient
    for y in range(H):
        ratio = y / H
        r = int(COLORS['bg'][0] + (COLORS['bg2'][0] - COLORS['bg'][0]) * ratio)
        g = int(COLORS['bg'][1] + (COLORS['bg2'][1] - COLORS['bg'][1]) * ratio)
        b = int(COLORS['bg'][2] + (COLORS['bg2'][2] - COLORS['bg'][2]) * ratio)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # Top accent bar
    draw.rectangle([(0, 0), (W, 3)], fill=COLORS['teal'])

    # Bottom accent bar
    draw.rectangle([(0, H - 3), (W, H)], fill=COLORS['teal'])

    if with_scanlines:
        # Subtle scanline effect for premium feel
        overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        for y in range(0, H, 4):
            od.line([(0, y), (W, y)], fill=(0, 0, 0, 8))
        img_rgba = img.convert("RGBA")
        img = Image.alpha_composite(img_rgba, overlay).convert("RGB")
        draw = ImageDraw.Draw(img)

    return img, draw


def add_branding(img, draw, show_logo=True, show_site=True):
    """Add branding elements to frame."""
    if show_logo:
        try:
            logo = Image.open(LOGO_PATH).convert("RGBA")
            logo = logo.resize((48, 48), Image.LANCZOS)
            img.paste(logo, (36, 32), logo)
        except Exception:
            pass
        font_brand = load_font(15, bold=True)
        draw.text((96, 36), "REFORM UK", fill=COLORS['teal'], font=font_brand)
        font_loc = load_font(12)
        draw.text((96, 54), "BURNLEY", fill=COLORS['muted'], font=font_loc)

    if show_site:
        font_site = load_font(16, bold=True)
        text_center_x(draw, "tompickup.co.uk", font_site, H - 50, COLORS['teal'])


def add_caption_bar(img, draw, text, y_pos=None, font_size=32, bg_alpha=180):
    """Add a caption bar with semi-transparent background."""
    if y_pos is None:
        y_pos = H - 280

    font = load_font(font_size, bold=True)
    lines = textwrap.wrap(text, width=28)

    line_h = font_size + 10
    total_h = len(lines) * line_h + 40

    # Semi-transparent background
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rectangle([(30, y_pos), (W - 30, y_pos + total_h)], fill=(13, 17, 23, bg_alpha))

    # Teal left accent
    od.rectangle([(30, y_pos), (34, y_pos + total_h)], fill=(*COLORS['teal'], 255))

    img_rgba = img.convert("RGBA")
    img_rgba = Image.alpha_composite(img_rgba, overlay)

    draw2 = ImageDraw.Draw(img_rgba)
    cy = y_pos + 20
    for line in lines:
        bbox = draw2.textbbox((0, 0), line, font=font)
        tw = bbox[2] - bbox[0]
        draw2.text(((W - tw) // 2, cy), line, fill=COLORS['white'], font=font)
        cy += line_h

    result = img_rgba.convert("RGB")
    img.paste(result)

    # Update draw object
    return ImageDraw.Draw(img)


# ============================================================
# ANIMATED SCENE GENERATORS
# ============================================================

class Scene:
    """A scene that generates multiple frames for animation."""

    def __init__(self, name, duration, voiceover_text=None, caption_phrases=None):
        self.name = name
        self.duration = duration  # seconds
        self.voiceover_text = voiceover_text
        self.caption_phrases = caption_phrases or []  # list of (start_ratio, text)

    def frame_count(self):
        return int(self.duration * FPS)

    def render_frame(self, frame_idx, total_frames):
        """Override in subclasses. Returns PIL Image."""
        raise NotImplementedError


class StatCountScene(Scene):
    """Animated stat counter - number counts up with easing."""

    def __init__(self, name, duration, target_value, suffix, label, sublabel,
                 party=None, extra_lines=None, voiceover_text=None, caption_phrases=None,
                 is_fraction=False, fraction_text=None):
        super().__init__(name, duration, voiceover_text, caption_phrases)
        self.target_value = target_value
        self.suffix = suffix
        self.label = label
        self.sublabel = sublabel
        self.party = party
        self.extra_lines = extra_lines or []
        self.is_fraction = is_fraction
        self.fraction_text = fraction_text  # e.g. "0 of 3"

    def render_frame(self, frame_idx, total_frames):
        img, draw = create_frame_base()
        add_branding(img, draw)

        color = PARTY_COLORS.get(self.party, COLORS['teal']) if self.party else COLORS['teal']

        # Animation progress (count up in first 40% of scene)
        anim_duration = 0.4
        t = min(1.0, (frame_idx / total_frames) / anim_duration)
        progress = ease_out_expo(t)

        # Value text
        if self.is_fraction:
            display = self.fraction_text or f"{self.target_value}"
            # Fade in instead of count
            alpha_t = min(1.0, (frame_idx / total_frames) / 0.2)
            if alpha_t < 1.0:
                # During fade, show placeholder
                display_color = tuple(int(c * alpha_t + COLORS['bg'][i] * (1 - alpha_t))
                                      for i, c in enumerate(color))
            else:
                display_color = color
        else:
            current = int(self.target_value * progress)
            display = f"{current}{self.suffix}"
            display_color = color

        # Large stat value
        font_value = load_font(160, bold=True)
        text_center_x(draw, display, font_value, 480, display_color)

        # Pulsing glow effect on the number (subtle)
        pulse = 0.5 + 0.5 * math.sin(frame_idx * 0.08)
        glow_alpha = int(20 + 15 * pulse)
        glow = Image.new("RGBA", img.size, (0, 0, 0, 0))
        gd = ImageDraw.Draw(glow)
        bbox = draw.textbbox((0, 0), display, font=font_value)
        tw = bbox[2] - bbox[0]
        cx = (W - tw) // 2
        gd.rectangle([(cx - 20, 470), (cx + tw + 20, 670)], fill=(*color[:3], glow_alpha))
        glow = glow.filter(ImageFilter.GaussianBlur(radius=30))
        img_rgba = img.convert("RGBA")
        img = Image.alpha_composite(img_rgba, glow).convert("RGB")
        draw = ImageDraw.Draw(img)

        # Label
        font_label = load_font(32, bold=True)
        text_center_x(draw, self.label, font_label, 700, COLORS['white'])

        # Sublabel
        font_sub = load_font(22)
        text_center_x(draw, self.sublabel, font_sub, 750, COLORS['muted'])

        # Party pill
        if self.party:
            font_pill = load_font(18, bold=True)
            pill_text = self.party.upper()
            pb = draw.textbbox((0, 0), pill_text, font=font_pill)
            pw = pb[2] - pb[0]
            px = (W - pw - 24) // 2
            py = 810
            pill_bg = tuple(max(0, c // 4) for c in color)
            draw_rounded_rect(draw, (px, py, px + pw + 24, py + 34), 17, fill=pill_bg)
            draw_rounded_rect(draw, (px, py, px + pw + 24, py + 34), 17, outline=color, width=2)
            draw.text((px + 12, py + 6), pill_text, fill=color, font=font_pill)

        # Extra text lines (fade in after counter)
        if self.extra_lines:
            extra_t = max(0, (frame_idx / total_frames - 0.35) / 0.15)
            extra_t = min(1.0, extra_t)
            if extra_t > 0:
                font_extra = load_font(20)
                ey = 880
                for line in self.extra_lines:
                    wrapped = textwrap.wrap(line, width=42)
                    for wl in wrapped:
                        alpha_val = int(180 * ease_out_cubic(extra_t))
                        c = tuple(int(COLORS['dim'][i] * extra_t) for i in range(3))
                        text_center_x(draw, wl, font_extra, ey, c)
                        ey += 30
                    ey += 6

        # Caption phrases
        self._draw_captions(img, draw, frame_idx, total_frames)

        return img

    def _draw_captions(self, img, draw, frame_idx, total_frames):
        """Draw caption phrases based on timing."""
        t = frame_idx / total_frames
        for start_ratio, text in self.caption_phrases:
            if t >= start_ratio:
                # Fade in over 0.05 of the scene
                fade_t = min(1.0, (t - start_ratio) / 0.05)
                if fade_t > 0:
                    alpha = int(220 * ease_out_cubic(fade_t))
                    add_caption_bar(img, draw, text, bg_alpha=min(alpha, 180))


class AnimatedBarChartScene(Scene):
    """Animated horizontal bar chart - bars grow from left."""

    def __init__(self, name, duration, title, data, subtitle=None,
                 voiceover_text=None, caption_phrases=None):
        """
        data: list of (name, ward, party, percentage, counts_text)
        """
        super().__init__(name, duration, voiceover_text, caption_phrases)
        self.title = title
        self.data = data
        self.subtitle = subtitle

    def render_frame(self, frame_idx, total_frames):
        img, draw = create_frame_base()
        add_branding(img, draw)

        # Title (fade in)
        title_t = min(1.0, (frame_idx / total_frames) / 0.1)
        font_title = load_font(34, bold=True)
        title_alpha = int(245 * ease_out_cubic(title_t))
        title_color = tuple(int(COLORS['white'][i] * title_t) for i in range(3))
        draw.text((60, 130), self.title, fill=title_color, font=font_title)

        # Accent line under title
        tbbox = draw.textbbox((60, 130), self.title, font=font_title)
        line_w = int(260 * ease_out_cubic(title_t))
        draw.rectangle([(60, tbbox[3] + 10), (60 + line_w, tbbox[3] + 13)], fill=COLORS['teal'])

        if self.subtitle:
            font_sub = load_font(18)
            sub_color = tuple(int(COLORS['muted'][i] * title_t) for i in range(3))
            draw.text((60, tbbox[3] + 24), self.subtitle, fill=sub_color, font=font_sub)

        # Bars - staggered animation
        bar_x = 60
        bar_max_w = W - 120
        bar_h = 70
        gap = 40
        y = 320

        font_name = load_font(26, bold=True)
        font_detail = load_font(16)
        font_pill = load_font(13, bold=True)
        font_pct = load_font(30, bold=True)
        font_count = load_font(15)

        for idx, (name, ward, party, pct, counts) in enumerate(self.data):
            color = PARTY_COLORS.get(party, COLORS['teal'])

            # Stagger: each bar starts 0.08 later
            bar_start = 0.12 + idx * 0.08
            bar_t = max(0, (frame_idx / total_frames - bar_start) / 0.25)
            bar_t = min(1.0, bar_t)
            bar_progress = ease_out_expo(bar_t)

            # Name and ward (fade in)
            name_t = max(0, (frame_idx / total_frames - (bar_start - 0.04)) / 0.1)
            name_t = min(1.0, name_t)
            name_alpha = ease_out_cubic(name_t)

            if name_alpha > 0.01:
                name_color = tuple(int(COLORS['white'][i] * name_alpha) for i in range(3))
                draw.text((bar_x, y), name, fill=name_color, font=font_name)

                # Ward text
                ward_color = tuple(int(COLORS['muted'][i] * name_alpha) for i in range(3))
                draw.text((bar_x, y + 34), ward, fill=ward_color, font=font_detail)

                # Party pill
                wb = draw.textbbox((0, 0), ward, font=font_detail)
                pill_x = bar_x + (wb[2] - wb[0]) + 14
                abbr = PARTY_ABBR.get(party, party[:3].upper())
                pb = draw.textbbox((0, 0), abbr, font=font_pill)
                ppw = pb[2] - pb[0] + 14
                pph = pb[3] - pb[1] + 8
                pill_bg = tuple(max(0, c // 4) for c in color)
                pill_color = tuple(int(color[i] * name_alpha) for i in range(3))
                draw_rounded_rect(draw, (pill_x, y + 33, pill_x + ppw, y + 33 + pph),
                                  pph // 2, fill=pill_bg)
                draw_rounded_rect(draw, (pill_x, y + 33, pill_x + ppw, y + 33 + pph),
                                  pph // 2, outline=pill_color, width=1)
                draw.text((pill_x + 7, y + 36), abbr, fill=pill_color, font=font_pill)

            y += 60

            # Bar track
            if name_alpha > 0.01:
                draw_rounded_rect(draw, (bar_x, y, bar_x + bar_max_w, y + bar_h), 10,
                                  fill=COLORS['card'], outline=COLORS['card_border'])

            # Animated bar fill
            fill_w = int(bar_max_w * (pct / 100) * bar_progress)
            if fill_w > 4 and name_alpha > 0.01:
                draw_gradient_bar(img, (bar_x, y, bar_x + fill_w, y + bar_h), color, 1.0)
                draw = ImageDraw.Draw(img)  # Refresh after gradient bar

                # Percentage label
                current_pct = pct * bar_progress
                pct_text = f"{current_pct:.0f}%"
                pct_bbox = draw.textbbox((0, 0), pct_text, font=font_pct)
                pct_w = pct_bbox[2] - pct_bbox[0]

                if fill_w > pct_w + 28:
                    # Inside bar
                    draw.text((bar_x + fill_w - pct_w - 16, y + 16), pct_text,
                              fill=(10, 13, 18), font=font_pct)
                else:
                    # Outside bar
                    draw.text((bar_x + fill_w + 14, y + 16), pct_text,
                              fill=color, font=font_pct)

            # Counts text (below bar, right-aligned)
            if bar_progress > 0.8 and name_alpha > 0.01:
                count_t = (bar_progress - 0.8) / 0.2
                count_color = tuple(int(COLORS['dim'][i] * count_t) for i in range(3))
                cb = draw.textbbox((0, 0), counts, font=font_count)
                cw = cb[2] - cb[0]
                draw.text((bar_x + bar_max_w - cw, y + bar_h + 6), counts,
                          fill=count_color, font=font_count)

            y += bar_h + gap

        # Caption phrases
        t = frame_idx / total_frames
        for start_ratio, text in self.caption_phrases:
            if t >= start_ratio:
                fade_t = min(1.0, (t - start_ratio) / 0.05)
                if fade_t > 0:
                    add_caption_bar(img, draw, text, bg_alpha=int(180 * ease_out_cubic(fade_t)))

        return img


class TextRevealScene(Scene):
    """Text content with line-by-line reveal animation."""

    def __init__(self, name, duration, heading, body_lines, accent_color=None,
                 voiceover_text=None, caption_phrases=None):
        super().__init__(name, duration, voiceover_text, caption_phrases)
        self.heading = heading
        self.body_lines = body_lines
        self.accent_color = accent_color or COLORS['teal']

    def render_frame(self, frame_idx, total_frames):
        img, draw = create_frame_base()
        add_branding(img, draw)

        # Heading (slide in from left)
        head_t = min(1.0, (frame_idx / total_frames) / 0.12)
        head_progress = ease_out_expo(head_t)

        font_heading = load_font(38, bold=True)
        h_lines = textwrap.wrap(self.heading, width=24)
        y = 280
        x_offset = int(-200 * (1 - head_progress))

        for line in h_lines:
            head_color = tuple(int(COLORS['white'][i] * head_progress) for i in range(3))
            draw.text((60 + x_offset, y), line, fill=head_color, font=font_heading)
            y += 52

        # Accent line (grows)
        line_w = int(220 * head_progress)
        draw.rectangle([(60, y + 8), (60 + line_w, y + 11)], fill=self.accent_color)
        y += 36

        # Body lines (staggered reveal)
        font_body = load_font(24)

        # Flatten body lines with wrapping
        all_lines = []
        for line in self.body_lines:
            if line == "":
                all_lines.append(("", True))
            else:
                wrapped = textwrap.wrap(line, width=36)
                for i, wl in enumerate(wrapped):
                    all_lines.append((wl, i == 0))

        for idx, (line, is_first) in enumerate(all_lines):
            if line == "":
                y += 16
                continue

            # Stagger start based on line index
            line_start = 0.15 + idx * 0.04
            line_t = max(0, (frame_idx / total_frames - line_start) / 0.08)
            line_t = min(1.0, line_t)
            alpha = ease_out_cubic(line_t)

            if alpha > 0.01:
                line_color = tuple(int(COLORS['light'][i] * alpha) for i in range(3))
                # Slide up slightly
                y_off = int(12 * (1 - alpha))
                draw.text((60, y + y_off), line, fill=line_color, font=font_body)

            y += 36

        # Caption phrases
        t = frame_idx / total_frames
        for start_ratio, text in self.caption_phrases:
            if t >= start_ratio:
                fade_t = min(1.0, (t - start_ratio) / 0.05)
                if fade_t > 0:
                    add_caption_bar(img, draw, text, bg_alpha=int(180 * ease_out_cubic(fade_t)))

        return img


class TitleScene(Scene):
    """Opening/closing title with Ken Burns effect on background."""

    def __init__(self, name, duration, title, subtitle=None, bg_image_path=None,
                 voiceover_text=None, caption_phrases=None, zoom_direction="in"):
        super().__init__(name, duration, voiceover_text, caption_phrases)
        self.title = title
        self.subtitle = subtitle
        self.bg_image_path = bg_image_path
        self.zoom_direction = zoom_direction
        self._bg_loaded = None

    def _load_bg(self):
        """Load and prep background image once."""
        if self._bg_loaded is not None:
            return self._bg_loaded

        if self.bg_image_path and os.path.exists(self.bg_image_path):
            bg = Image.open(self.bg_image_path).convert("RGB")
            # Scale to fit with extra margin for Ken Burns
            margin = 1.15
            bg_w, bg_h = bg.size
            scale = max(W * margin / bg_w, H * margin / bg_h)
            bg = bg.resize((int(bg_w * scale), int(bg_h * scale)), Image.LANCZOS)
            self._bg_loaded = bg
        else:
            self._bg_loaded = False
        return self._bg_loaded

    def render_frame(self, frame_idx, total_frames):
        img, draw = create_frame_base()

        bg = self._load_bg()
        if bg:
            # Ken Burns: slow zoom
            t = frame_idx / total_frames
            if self.zoom_direction == "in":
                zoom = 1.0 + 0.08 * t  # Zoom in slightly
            else:
                zoom = 1.08 - 0.08 * t  # Zoom out

            bw, bh = bg.size
            crop_w = int(W / zoom)
            crop_h = int(H / zoom)
            cx = bw // 2
            cy = bh // 2
            # Slight pan
            pan_x = int(20 * math.sin(t * math.pi))
            pan_y = int(10 * math.cos(t * math.pi))

            left = cx - crop_w // 2 + pan_x
            top = cy - crop_h // 2 + pan_y
            left = max(0, min(left, bw - crop_w))
            top = max(0, min(top, bh - crop_h))

            cropped = bg.crop((left, top, left + crop_w, top + crop_h))
            cropped = cropped.resize((W, H), Image.LANCZOS)

            # Blur slightly
            cropped = cropped.filter(ImageFilter.GaussianBlur(radius=4))

            # Dark gradient overlay
            overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            od = ImageDraw.Draw(overlay)
            for y in range(H):
                alpha = int(140 + 80 * (y / H))
                od.rectangle([(0, y), (W, y + 1)], fill=(13, 17, 23, alpha))

            img = Image.alpha_composite(cropped.convert("RGBA"), overlay).convert("RGB")
            draw = ImageDraw.Draw(img)
            draw.rectangle([(0, 0), (W, 3)], fill=COLORS['teal'])
            draw.rectangle([(0, H - 3), (W, H)], fill=COLORS['teal'])

        add_branding(img, draw)

        # Reform logo centered
        try:
            logo = Image.open(LOGO_PATH).convert("RGBA")
            logo = logo.resize((100, 100), Image.LANCZOS)
            img.paste(logo, ((W - 100) // 2, 380), logo)
        except Exception:
            pass

        # Title (fade in + slide up)
        title_t = min(1.0, (frame_idx / total_frames) / 0.2)
        title_progress = ease_out_expo(title_t)

        font_title = load_font(48, bold=True)
        lines = textwrap.wrap(self.title, width=20)
        title_y = 540 + int(30 * (1 - title_progress))

        for line in lines:
            title_color = tuple(int(COLORS['white'][i] * title_progress) for i in range(3))
            text_center_x(draw, line, font_title, int(title_y), title_color)
            title_y += 62

        # Accent line (grows from center)
        if title_progress > 0.3:
            line_t = (title_progress - 0.3) / 0.7
            line_w = int(200 * ease_out_expo(line_t))
            cx = W // 2
            draw.rectangle([(cx - line_w // 2, int(title_y) + 16),
                            (cx + line_w // 2, int(title_y) + 20)], fill=COLORS['teal'])

        # Subtitle
        if self.subtitle:
            sub_t = max(0, (frame_idx / total_frames - 0.15) / 0.15)
            sub_t = min(1.0, sub_t)
            sub_progress = ease_out_cubic(sub_t)

            font_sub = load_font(26)
            sub_lines = textwrap.wrap(self.subtitle, width=30)
            sub_y = int(title_y) + 46
            for line in sub_lines:
                sub_color = tuple(int(COLORS['light'][i] * sub_progress) for i in range(3))
                text_center_x(draw, line, font_sub, int(sub_y), sub_color)
                sub_y += 36

        return img


class CTAScene(Scene):
    """Call-to-action closing frame."""

    def __init__(self, name, duration, text, url="tompickup.co.uk",
                 voiceover_text=None, caption_phrases=None):
        super().__init__(name, duration, voiceover_text, caption_phrases)
        self.cta_text = text
        self.url = url

    def render_frame(self, frame_idx, total_frames):
        img, draw = create_frame_base()
        add_branding(img, draw, show_site=False)

        t = frame_idx / total_frames

        # Large Reform UK logo (pulse animation)
        try:
            logo = Image.open(LOGO_PATH).convert("RGBA")
            pulse = 1.0 + 0.02 * math.sin(frame_idx * 0.1)
            logo_size = int(140 * pulse)
            logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
            img.paste(logo, ((W - logo_size) // 2, 500), logo)
        except Exception:
            pass

        # CTA text (fade in)
        cta_t = min(1.0, t / 0.2)
        cta_progress = ease_out_expo(cta_t)

        font_cta = load_font(34, bold=True)
        cta_color = tuple(int(COLORS['white'][i] * cta_progress) for i in range(3))
        text_center_x(draw, self.cta_text, font_cta, 700, cta_color)

        # URL (bigger, teal, pulsing)
        url_t = max(0, (t - 0.1) / 0.15)
        url_t = min(1.0, url_t)
        url_progress = ease_out_expo(url_t)

        font_url = load_font(36, bold=True)
        glow_pulse = 0.8 + 0.2 * math.sin(frame_idx * 0.08)
        url_color = tuple(int(COLORS['teal'][i] * url_progress * glow_pulse) for i in range(3))
        text_center_x(draw, self.url, font_url, 770, url_color)

        # Accent line
        if url_progress > 0.3:
            lp = (url_progress - 0.3) / 0.7
            lw = int(160 * ease_out_expo(lp))
            cx = W // 2
            draw.rectangle([(cx - lw // 2, 840), (cx + lw // 2, 844)], fill=COLORS['teal'])

        # "7 May 2026" date
        date_t = max(0, (t - 0.25) / 0.15)
        date_t = min(1.0, date_t)
        if date_t > 0:
            font_date = load_font(22)
            date_color = tuple(int(COLORS['muted'][i] * ease_out_cubic(date_t)) for i in range(3))
            text_center_x(draw, "7 May 2026  |  All 15 Wards", font_date, 880, date_color)

        return img


class TransitionScene(Scene):
    """Black transition with optional text flash."""

    def __init__(self, name, duration=0.5, flash_text=None):
        super().__init__(name, duration)
        self.flash_text = flash_text

    def render_frame(self, frame_idx, total_frames):
        img = Image.new("RGB", (W, H), COLORS['bg'])
        draw = ImageDraw.Draw(img)

        # Teal bars
        draw.rectangle([(0, 0), (W, 3)], fill=COLORS['teal'])
        draw.rectangle([(0, H - 3), (W, H)], fill=COLORS['teal'])

        if self.flash_text:
            t = frame_idx / total_frames
            # Flash: appear then fade
            if t < 0.5:
                alpha = ease_out_cubic(t * 2)
            else:
                alpha = ease_out_cubic((1 - t) * 2)

            font = load_font(28, bold=True)
            flash_color = tuple(int(COLORS['teal'][i] * alpha) for i in range(3))
            text_center_x(draw, self.flash_text, font, H // 2 - 14, flash_color)

        return img


# ============================================================
# TTS VOICEOVER (per-scene generation + duration matching)
# ============================================================

def get_audio_duration(path):
    """Get duration of an audio file in seconds."""
    result = subprocess.run(
        [FFPROBE, "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", path],
        capture_output=True, text=True
    )
    return float(result.stdout.strip()) if result.stdout.strip() else 0


async def generate_scene_audio(scene, output_path):
    """Generate voiceover for a single scene."""
    if not scene.voiceover_text:
        return 0

    communicate = edge_tts.Communicate(scene.voiceover_text, TTS_VOICE, rate=TTS_RATE, pitch=TTS_PITCH)
    await communicate.save(output_path)
    return get_audio_duration(output_path)


async def generate_voiceover_per_scene(scenes, tmpdir):
    """Generate voiceover per scene and adjust scene durations to match.

    Returns: list of (scene, audio_path_or_none) tuples, total audio duration.
    """
    if not HAS_TTS:
        print("  edge-tts not available, skipping voiceover")
        return [(s, None) for s in scenes], 0

    print("  Generating per-scene voiceover...")
    scene_audio = []
    total_audio = 0

    for i, scene in enumerate(scenes):
        if scene.voiceover_text:
            audio_path = os.path.join(tmpdir, f"vo_{i:02d}_{scene.name}.mp3")
            duration = await generate_scene_audio(scene, audio_path)

            # Add 0.3s padding for breathing room
            padded_duration = duration + 0.3

            # Adjust scene duration to match voiceover (min = original duration)
            old_dur = scene.duration
            scene.duration = max(scene.duration, padded_duration)

            if scene.duration > old_dur:
                print(f"    {scene.name}: {old_dur:.1f}s -> {scene.duration:.1f}s (audio: {duration:.1f}s)")

            scene_audio.append((scene, audio_path))
            total_audio += duration
        else:
            scene_audio.append((scene, None))

    print(f"  Total voiceover: {total_audio:.1f}s")
    return scene_audio, total_audio


def concat_scene_audio(scene_audio_pairs, output_path):
    """Concatenate per-scene audio files with silence gaps into one track.

    Generates a single audio file where each scene's voiceover starts
    at the correct offset matching the video timeline.
    """
    # Build an ffmpeg filter that places each audio at the right offset
    inputs = []
    filter_parts = []
    stream_labels = []
    timeline_offset = 0.0
    input_idx = 0

    for scene, audio_path in scene_audio_pairs:
        if audio_path and os.path.exists(audio_path):
            inputs.extend(["-i", audio_path])
            # Delay audio to match scene's position in timeline
            delay_ms = int(timeline_offset * 1000)
            label = f"a{input_idx}"
            filter_parts.append(f"[{input_idx}]adelay={delay_ms}|{delay_ms}[{label}]")
            stream_labels.append(f"[{label}]")
            input_idx += 1
        timeline_offset += scene.duration

    if not filter_parts:
        return None

    # Mix all delayed audio streams
    n_streams = len(stream_labels)
    mix_inputs = "".join(stream_labels)
    filter_graph = ";".join(filter_parts) + f";{mix_inputs}amix=inputs={n_streams}:duration=longest[out]"

    cmd = [FFMPEG, "-y"] + inputs + [
        "-filter_complex", filter_graph,
        "-map", "[out]",
        "-c:a", "aac", "-b:a", "128k",
        output_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Audio concat error: {result.stderr[-400:]}")
        return None

    duration = get_audio_duration(output_path)
    print(f"  Combined audio: {duration:.1f}s")
    return output_path


# ============================================================
# VIDEO ASSEMBLY (ffmpeg)
# ============================================================

def render_all_frames(scenes, tmpdir):
    """Render all frames from all scenes to disk."""
    frame_idx = 0
    total_frames = sum(s.frame_count() for s in scenes)

    for scene_idx, scene in enumerate(scenes):
        scene_frames = scene.frame_count()
        print(f"  Scene {scene_idx + 1}/{len(scenes)}: {scene.name} ({scene.duration}s, {scene_frames} frames)")

        for sf in range(scene_frames):
            img = scene.render_frame(sf, scene_frames)

            # Add crossfade at scene boundaries
            is_near_end = sf >= scene_frames - int(FPS * 0.3)  # Last 0.3s
            is_near_start = sf < int(FPS * 0.3) and scene_idx > 0

            frame_path = os.path.join(tmpdir, f"frame_{frame_idx:06d}.png")
            img.save(frame_path, "PNG")
            frame_idx += 1

    return frame_idx


def assemble_video(tmpdir, frame_count, output_path, audio_path=None):
    """Assemble frames + optional audio into final MP4."""
    input_pattern = os.path.join(tmpdir, "frame_%06d.png")

    cmd = [FFMPEG, "-y"]

    # Video input
    cmd.extend(["-framerate", str(FPS), "-i", input_pattern])

    # Audio input
    if audio_path and os.path.exists(audio_path):
        cmd.extend(["-i", audio_path])

    # Video encoding
    cmd.extend([
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-preset", "medium",
        "-crf", "20",  # Higher quality than before
        "-movflags", "+faststart",
    ])

    # Audio encoding
    if audio_path and os.path.exists(audio_path):
        cmd.extend([
            "-c:a", "aac",
            "-b:a", "128k",
            "-shortest",  # Match shortest stream
        ])

    cmd.append(output_path)

    print(f"  Encoding {frame_count} frames at {FPS}fps...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ffmpeg error: {result.stderr[-800:]}")
        return False

    return True


# ============================================================
# ARTICLE-SPECIFIC VIDEO: Burnley Elections 2026
# ============================================================

def generate_burnley_elections_video(duration=45, no_voice=False):
    """
    Burnley Elections 2026: The Councillors Who Didn't Show Up.

    Bannon-style narrative:
    1. HOOK    - Lead with outrage (50% attendance)
    2. ESCALATE - Stack the evidence (Sollis, Hall)
    3. PATTERN  - Bar chart makes it undeniable
    4. BETRAY   - They cancelled your elections
    5. CONTRAST - Every party supported it
    6. VINDICATE - Reform stood alone
    7. EMPOWER  - You decide on 7 May
    """

    scenes = []

    # Scene 1: HOOK - 50% stat counter (punchy, short)
    scenes.append(StatCountScene(
        name="hook_50pct",
        duration=5.0,
        target_value=50,
        suffix="%",
        label="Your councillor missed HALF his meetings",
        sublabel="Neil Mottershead, Gannow Ward",
        party="Conservative",
        extra_lines=[
            "Paid thousands from your council tax.",
            "Sat on just 2 committees. Missed 26 of 52.",
        ],
        voiceover_text="Fifty percent. Your councillor missed half his meetings. Twenty-six out of fifty-two.",
        caption_phrases=[
            (0.55, "26 of 52 meetings missed"),
        ],
    ))

    # Scene 1b: Quick transition
    scenes.append(TransitionScene("trans_1", 0.35, flash_text="IT GETS WORSE"))

    # Scene 2: ESCALATE - Sollis
    scenes.append(StatCountScene(
        name="escalate_sollis",
        duration=4.5,
        target_value=56,
        suffix="%",
        label="Getting worse, not better",
        sublabel="Christine Sollis, Brunshaw Ward",
        party="Independent",
        extra_lines=[
            "Vice-Chair of Audit & Standards.",
            "Missed 3 meetings of her own committee.",
            "69% overall, collapsing to 56% this year.",
        ],
        voiceover_text="Christine Sollis. Vice-Chair of Audit. Missing her own committee. Fifty-six percent.",
        caption_phrases=[
            (0.5, "Missed 3 of her own committee meetings"),
        ],
    ))

    # Scene 3: ESCALATE - Hall with fraction
    scenes.append(StatCountScene(
        name="escalate_hall",
        duration=4.5,
        target_value=0,
        suffix="",
        label="Zero. Not one.",
        sublabel="Alex Hall, Trinity Ward",
        party="Green",
        is_fraction=True,
        fraction_text="0 of 3",
        extra_lines=[
            "Audit & Standards meetings this year.",
            "73.8% overall, crashing to 53%.",
            "4 absences without apology.",
        ],
        voiceover_text="Alex Hall. Green. Zero out of three Audit meetings. Crashing to fifty-three percent.",
        caption_phrases=[
            (0.5, "4 absences without apology"),
        ],
    ))

    # Scene 3b: Transition
    scenes.append(TransitionScene("trans_2", 0.35, flash_text="THE FULL PICTURE"))

    # Scene 4: PATTERN - Animated bar chart
    scenes.append(AnimatedBarChartScene(
        name="pattern_bars",
        duration=5.0,
        title="Three Councillors Below 75%",
        data=[
            ("Neil Mottershead", "Gannow", "Conservative", 50.0, "26 of 52"),
            ("Christine Sollis", "Brunshaw", "Independent", 69.0, "60 of 87"),
            ("Alex Hall", "Trinity", "Green", 73.8, "48 of 65"),
        ],
        subtitle="Defending councillors asking for your vote again.",
        voiceover_text="Three councillors below seventy-five percent. Conservative, Independent, Green. All asking for your vote again.",
        caption_phrases=[
            (0.65, "All asking for your vote again"),
        ],
    ))

    # Scene 4b: Transition
    scenes.append(TransitionScene("trans_3", 0.35, flash_text="BUT THAT'S NOT ALL"))

    # Scene 5: BETRAY - Elections cancelled
    scenes.append(TextRevealScene(
        name="betrayal",
        duration=6.0,
        heading="They Tried to Cancel\nYour Elections",
        body_lines=[
            "January 2026.",
            "",
            "Burnley's Executive voted",
            "UNANIMOUSLY to cancel",
            "the May elections.",
            "",
            "Five councillors.",
            "One vote.",
            "Your democratic right, gone.",
        ],
        voiceover_text="Then they tried to cancel your elections. The Executive voted unanimously. Your democratic right, gone.",
    ))

    # Scene 6: CONTRAST - All parties complicit
    scenes.append(TextRevealScene(
        name="contrast",
        duration=5.5,
        heading="Every Other Party\nSupported It",
        body_lines=[
            "Labour started it.",
            "",
            "Conservatives backed it.",
            "",
            "Lib Dems, Greens, Independents:",
            "all voted for it in Burnley.",
            "",
            "Every establishment party",
            "wanted your vote cancelled.",
        ],
        voiceover_text="Labour started it. Conservatives backed it. Every establishment party wanted your vote cancelled.",
    ))

    # Scene 6b: Transition
    scenes.append(TransitionScene("trans_4", 0.4, flash_text="ONE PARTY SAID NO"))

    # Scene 7: VINDICATE - Reform stood alone
    scenes.append(TextRevealScene(
        name="vindicate",
        duration=6.0,
        heading="Reform UK\nSaved Your Elections",
        body_lines=[
            "Reform UK launched a",
            "judicial review.",
            "",
            "The government backed down.",
            "",
            "29 council areas.",
            "Millions of voters.",
            "Elections restored.",
        ],
        accent_color=COLORS['teal'],
        voiceover_text="Reform UK launched a judicial review. The government backed down. Elections restored.",
    ))

    # Scene 8: EMPOWER - Title + background
    bg_path = str(IMAGES_DIR / "burnley-town-hall-raw.jpg")
    if not os.path.exists(bg_path):
        bg_path = None

    scenes.append(TitleScene(
        name="empower_title",
        duration=3.0,
        title="Burnley Elections 2026",
        subtitle="7 May. 15 wards. Your choice.",
        bg_image_path=bg_path,
        voiceover_text="Seventh of May. Fifteen wards. Your choice.",
        zoom_direction="out",
    ))

    # Scene 9: CTA
    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="Ask them: will you turn up?",
        url="tompickup.co.uk",
        voiceover_text="Ask them. Will you turn up?",
    ))

    return scenes


# ============================================================
# ARTICLE: Tory Legacy - £1.27 Billion Losses
# ============================================================

def generate_tory_legacy_video(duration=45, no_voice=False):
    """The Tory Legacy: Up to £1.27 Billion of Financial Damage at LCC."""
    scenes = []

    # HOOK: The number
    scenes.append(StatCountScene(
        name="hook_1_27bn",
        duration=5.0,
        target_value=1270,
        suffix="M",
        label="Financial damage at Lancashire County Council",
        sublabel="Eight years of Conservative control",
        party="Conservative",
        extra_lines=["Every figure from official council accounts."],
        voiceover_text="One point two seven billion pounds. That's the financial damage left by eight years of Conservative control at Lancashire County Council.",
        caption_phrases=[(0.6, "Every figure from official accounts")],
    ))

    scenes.append(TransitionScene("trans_1", 0.35, flash_text="HOW DID THIS HAPPEN?"))

    # ESCALATE: Key loss categories
    scenes.append(AnimatedBarChartScene(
        name="loss_breakdown",
        duration=5.5,
        title="Where the Money Went",
        data=[
            ("Treasury Losses", "Investments", "Conservative", 42, "£416.9M"),
            ("UKMBA Bond Losses", "Est. sale loss", "Conservative", 35, "£350M"),
            ("Disposal & Academy", "Asset transfers", "Conservative", 16, "£160M"),
            ("Overspends", "Annual deficits", "Conservative", 7, "£94.3M"),
        ],
        subtitle="Official Statement of Accounts figures",
        voiceover_text="Treasury investment losses. Bond portfolio write-downs. Asset disposal failures. Annual overspends. All on their watch.",
        caption_phrases=[(0.65, "Statement of Accounts 2017-2025")],
    ))

    # BETRAY: Peak year
    scenes.append(StatCountScene(
        name="peak_year",
        duration=4.5,
        target_value=274,
        suffix="M",
        label="Worst single year: 2022/23",
        sublabel="Two hundred and seventy-four million pounds",
        party="Conservative",
        extra_lines=["More than the entire highways backlog fix."],
        voiceover_text="Their worst year. Two hundred and seventy-four million pounds lost in a single year. More than it costs to fix every road in Lancashire.",
    ))

    scenes.append(TransitionScene("trans_2", 0.4, flash_text="NOBODY KNEW"))

    # CONTRAST: Hidden from public
    scenes.append(TextRevealScene(
        name="hidden",
        duration=5.0,
        heading="Hidden in the\nSmall Print",
        body_lines=[
            "Buried in Statement of Accounts.",
            "",
            "No headlines. No scrutiny.",
            "",
            "While they raised your",
            "council tax every year.",
            "",
            "£514 added to Band D bills",
            "over eight budgets.",
        ],
        voiceover_text="All buried in the small print of the Statement of Accounts. No headlines. While they raised your council tax by five hundred and fourteen pounds.",
    ))

    # VINDICATE: Reform exposed it
    scenes.append(TextRevealScene(
        name="vindicate",
        duration=5.0,
        heading="Reform Exposed\nthe Full Damage",
        body_lines=[
            "Every figure sourced.",
            "Every page referenced.",
            "",
            "Eight years of accounts.",
            "Line by line.",
            "",
            "Now you know what they did",
            "with your money.",
        ],
        accent_color=COLORS['teal'],
        voiceover_text="Reform went through eight years of accounts, line by line. Every figure sourced. Now you know what they did with your money.",
    ))

    # CTA
    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="Read the full investigation",
        url="tompickup.co.uk",
        voiceover_text="Read the full investigation at tompickup.co.uk",
    ))

    return scenes


# ============================================================
# ARTICLE: Highways £650M Backlog
# ============================================================

def generate_highways_video(duration=45, no_voice=False):
    """Lancashire's Roads: A £650 Million Backlog."""
    scenes = []

    scenes.append(StatCountScene(
        name="hook_650m",
        duration=5.0,
        target_value=650,
        suffix="M",
        label="Lancashire's highways maintenance backlog",
        sublabel="And the real figure is likely far higher",
        party="Conservative",
        extra_lines=["7,035 km of roads. 1,832 bridges. 163,000 streetlights."],
        voiceover_text="Six hundred and fifty million pounds. That's the backlog on Lancashire's roads. Twelve years of Conservative neglect built it.",
        caption_phrases=[(0.55, "7,035 km of roads falling apart")],
    ))

    scenes.append(TransitionScene("trans_1", 0.35, flash_text="WHAT REFORM DID"))

    # Bar chart: AI results
    scenes.append(AnimatedBarChartScene(
        name="ai_results",
        duration=5.0,
        title="AI Sensors on Every Road",
        data=[
            ("Defects Found", "Before AI", "Conservative", 100, "61,000"),
            ("Defects Found", "After AI (12 months)", "Reform UK", 57, "35,000"),
        ],
        subtitle="42% reduction in outstanding defects",
        voiceover_text="Reform put AI sensors on bin lorries. Every road scanned every week. Defects down forty-two percent in twelve months.",
        caption_phrases=[(0.6, "42% fewer defects in 12 months")],
    ))

    # Contrast
    scenes.append(TextRevealScene(
        name="contrast",
        duration=5.0,
        heading="Old System vs\nReform's Approach",
        body_lines=[
            "Before: Manual inspections.",
            "Less than 5% of defects caught.",
            "",
            "Now: AI scans every road,",
            "every week.",
            "",
            "Repair costs halved.",
            "Average repair size tripled.",
        ],
        voiceover_text="The old system caught less than five percent of defects. Reform's AI catches them all. Repair costs halved. Average repair size tripled.",
    ))

    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="See the full highways analysis",
        url="tompickup.co.uk",
        voiceover_text="Read the full highways analysis at tompickup.co.uk",
    ))

    return scenes


# ============================================================
# ARTICLE: East Lancashire Waste Crisis
# ============================================================

def generate_waste_crisis_video(duration=45, no_voice=False):
    """East Lancashire's Waste Crisis: A Decade of Failure."""
    scenes = []

    scenes.append(StatCountScene(
        name="hook_2bn",
        duration=5.0,
        target_value=2,
        suffix="BN",
        label="Failed PFI deal that started it all",
        sublabel="Global Renewables: collapsed, abandoned",
        party="Conservative",
        extra_lines=["Then a £600M procurement was paused. A decade of indecision."],
        voiceover_text="A two billion pound PFI that collapsed. Then a six hundred million procurement shelved. A decade of waste sitting in landfill.",
    ))

    scenes.append(TransitionScene("trans_1", 0.35, flash_text="THE COST OF INDECISION"))

    scenes.append(StatCountScene(
        name="cost_spiral",
        duration=4.5,
        target_value=25,
        suffix="%",
        label="Waste costs up 25% in four years",
        sublabel="£75.5M to £94.3M per year",
        party="Conservative",
        extra_lines=["£8-10 million extra per year across East Lancashire."],
        voiceover_text="While they dithered, waste costs spiralled twenty-five percent in four years. Eight to ten million extra every year.",
    ))

    scenes.append(TransitionScene("trans_2", 0.35, flash_text="REFORM FIXED IT"))

    scenes.append(TextRevealScene(
        name="vindicate",
        duration=5.0,
        heading="First Open Tender\nin a Generation",
        body_lines=[
            "Reform put it out to bid.",
            "",
            "Result: £60.3 million contract.",
            "Local processing in Burnley.",
            "",
            "No more trucking waste",
            "25 miles to Farington.",
            "",
            "Projected savings:",
            "£6.3 million per year.",
        ],
        accent_color=COLORS['teal'],
        voiceover_text="Reform put it out to open tender. Result: sixty million pound contract, local processing in Burnley, six point three million saved per year.",
    ))

    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="Read the full waste investigation",
        url="tompickup.co.uk",
        voiceover_text="Read the full investigation at tompickup.co.uk",
    ))

    return scenes


# ============================================================
# ARTICLE: LGR £3.7 Billion Contract Crisis
# ============================================================

def generate_lgr_contracts_video(duration=45, no_voice=False):
    """Lancashire's £3.7 Billion Contract Problem."""
    scenes = []

    scenes.append(StatCountScene(
        name="hook_3200",
        duration=5.0,
        target_value=3200,
        suffix="",
        label="Active contracts across 15 Lancashire councils",
        sublabel="Worth £3.7 billion in total",
        extra_lines=["Every one needs dealing with before LGR."],
        voiceover_text="Three thousand two hundred active contracts. Three point seven billion pounds. Every single one needs dealing with before they reorganise Lancashire.",
    ))

    scenes.append(TransitionScene("trans_1", 0.35, flash_text="18 MONTHS. REALLY?"))

    scenes.append(AnimatedBarChartScene(
        name="mega_contracts",
        duration=5.5,
        title="Mega-Contracts Crossing Vesting",
        data=[
            ("SEND Schools", "6 years", "Conservative", 92, "£920M"),
            ("Transition to Adult", "10 years", "Conservative", 66, "£660M"),
            ("Residential Care", "8 years", "Conservative", 70, "£700M"),
        ],
        subtitle="Awarded by outgoing councils, inherited by new ones",
        voiceover_text="Nine hundred and twenty million. Six hundred and sixty million. Seven hundred million. Mega-contracts awarded by outgoing councils, inherited by new ones who had no say.",
    ))

    scenes.append(TextRevealScene(
        name="context",
        duration=5.0,
        heading="The Government's\n18-Month Fantasy",
        body_lines=[
            "Dorset had 800 contracts.",
            "Took 24 months.",
            "",
            "Lancashire has 3,200.",
            "Four times the complexity.",
            "",
            "The government's timeline",
            "does not account for this.",
        ],
        voiceover_text="Dorset had eight hundred contracts and took twenty-four months. Lancashire has four times that. The government's timeline is a fantasy.",
    ))

    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="Read the contract analysis",
        url="tompickup.co.uk",
        voiceover_text="Read the full analysis at tompickup.co.uk",
    ))

    return scenes


# ============================================================
# ARTICLE: Reform's First Budget
# ============================================================

def generate_budget_video(duration=45, no_voice=False):
    """Reform's First Budget: The Numbers Don't Lie."""
    scenes = []

    scenes.append(StatCountScene(
        name="hook_380",
        duration=4.5,
        target_value=380,
        suffix="",
        label="3.80% - Lowest council tax rise in a decade",
        sublabel="Reform's first budget, 2026/27",
        extra_lines=["Balanced. No front-line cuts. £1.33 billion."],
        voiceover_text="Three point eight percent. The lowest council tax rise in a decade. Reform's first budget.",
        caption_phrases=[(0.55, "Lowest in a decade")],
    ))

    scenes.append(TransitionScene("trans_1", 0.35, flash_text="COMPARE THAT"))

    scenes.append(AnimatedBarChartScene(
        name="ct_comparison",
        duration=5.5,
        title="Council Tax Rises: A Decade",
        data=[
            ("Labour 17/18", "", "Labour", 80, "3.99%"),
            ("Conservative avg", "8 budgets", "Conservative", 100, "4.99%"),
            ("Reform 26/27", "First budget", "Reform UK", 76, "3.80%"),
        ],
        subtitle="Band D council tax increase per year",
        voiceover_text="Labour. Three point nine nine. Conservatives averaged nearly five percent over eight budgets. Reform. Three point eight.",
        caption_phrases=[(0.7, "+£514 added under Conservatives")],
    ))

    scenes.append(StatCountScene(
        name="savings",
        duration=4.5,
        target_value=62,
        suffix="M",
        label="Savings identified - 5% of net budget",
        sublabel="No front-line service cuts",
        extra_lines=["Inherited a £28 million overspend. Fixed it."],
        voiceover_text="Sixty-two million in savings found. Five percent of the entire budget. No front-line cuts. We inherited a twenty-eight million overspend and fixed it.",
    ))

    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="See the budget breakdown",
        url="tompickup.co.uk",
        voiceover_text="Read the full budget breakdown at tompickup.co.uk",
    ))

    return scenes


# ============================================================
# ARTICLE: Reform Technology Lancashire
# ============================================================

def generate_technology_video(duration=45, no_voice=False):
    """How Reform Is Using Technology to Transform Lancashire."""
    scenes = []

    scenes.append(StatCountScene(
        name="hook_42pct",
        duration=4.5,
        target_value=42,
        suffix="%",
        label="Fewer road defects in just 12 months",
        sublabel="AI sensors deployed on bin lorries",
        extra_lines=["61,000 down to 35,000 outstanding defects."],
        voiceover_text="Forty-two percent fewer road defects. In just twelve months. AI sensors on bin lorries scanning every road, every week.",
        caption_phrases=[(0.55, "61,000 → 35,000 defects")],
    ))

    scenes.append(TransitionScene("trans_1", 0.35, flash_text="THAT'S JUST THE START"))

    scenes.append(AnimatedBarChartScene(
        name="tech_wins",
        duration=5.5,
        title="Technology Wins",
        data=[
            ("Road Defects", "AI detection", "Reform UK", 42, "Down 42%"),
            ("SEND Transport", "Route optimisation", "Reform UK", 30, "Down 30%"),
            ("Staff Trained", "Microsoft Copilot", "Reform UK", 70, "1,400"),
        ],
        subtitle="Results in Reform's first year",
        voiceover_text="Road defects down forty-two percent. SEND transport costs down thirty percent. Fourteen hundred staff trained on AI tools.",
    ))

    scenes.append(TextRevealScene(
        name="old_system",
        duration=4.5,
        heading="What We Inherited",
        body_lines=[
            "Paper-based inspections.",
            "Less than 5% of defects caught.",
            "",
            "Legacy IT systems.",
            "No digital front door.",
            "",
            "Twelve years of Conservative",
            "technological neglect.",
        ],
        voiceover_text="We inherited paper-based inspections that caught less than five percent of defects. Legacy IT. No digital front door. Twelve years of neglect.",
    ))

    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="See Reform's tech transformation",
        url="tompickup.co.uk",
        voiceover_text="Read the full story at tompickup.co.uk",
    ))

    return scenes


# ============================================================
# ARTICLE: Stocks Massey Bequest
# ============================================================

def generate_stocks_massey_video(duration=45, no_voice=False):
    """Stocks Massey Bequest 2025 Awards."""
    scenes = []

    scenes.append(StatCountScene(
        name="hook_18k",
        duration=4.5,
        target_value=18,
        suffix="K",
        label="Awarded to 15 Burnley organisations in 2025",
        sublabel="Edward Stocks Massey Bequest Fund",
        extra_lines=["A gift to Burnley since 1909. Still giving."],
        voiceover_text="Eighteen thousand pounds. Awarded to fifteen Burnley organisations. From a fund established in nineteen oh nine.",
    ))

    scenes.append(AnimatedBarChartScene(
        name="awards",
        duration=5.0,
        title="2025 Awards Breakdown",
        data=[
            ("Towneley Hall", "Restoration", "Reform UK", 36, "£4,000"),
            ("Mechanics Theatre", "Programme", "Reform UK", 27, "£3,000"),
            ("Music Centre", "Education", "Reform UK", 18, "£2,000"),
            ("Libraries", "Activities", "Reform UK", 18, "£2,000"),
        ],
        subtitle="Supporting arts, culture and education in Burnley",
        voiceover_text="Towneley Hall. The Mechanics Theatre. Burnley Music Centre. Public libraries. Arts, culture, and education.",
    ))

    scenes.append(TextRevealScene(
        name="apply",
        duration=4.5,
        heading="2026 Applications\nOpen Late April",
        body_lines=[
            "Does your Burnley",
            "organisation do good work?",
            "",
            "Educational, cultural,",
            "or community benefit?",
            "",
            "Applications open late April.",
            "Get in touch for details.",
        ],
        accent_color=COLORS['teal'],
        voiceover_text="Applications for twenty twenty-six open late April. If your Burnley organisation does good work, get in touch.",
    ))

    scenes.append(CTAScene(
        name="cta",
        duration=3.5,
        text="Learn more and apply",
        url="tompickup.co.uk",
        voiceover_text="Visit tompickup.co.uk for details.",
    ))

    return scenes


# ============================================================
# ARTICLE REGISTRY
# ============================================================

ARTICLE_GENERATORS = {
    "burnley-elections-2026-attendance": generate_burnley_elections_video,
    "lcc-tory-legacy-921m-losses": generate_tory_legacy_video,
    "lancashire-highways-650m-backlog": generate_highways_video,
    "east-lancashire-waste-crisis": generate_waste_crisis_video,
    "lancashire-lgr-contract-crisis": generate_lgr_contracts_video,
    "lcc-budget-reform-first-year": generate_budget_video,
    "reform-technology-lancashire": generate_technology_video,
    "stocks-massey-bequest-2025": generate_stocks_massey_video,
}


# ============================================================
# VOICEOVER SCRIPT EXPORT
# ============================================================

def export_voiceover_script(scenes, output_path):
    """Export the voiceover script as a text file for review."""
    with open(output_path, "w") as f:
        f.write("VOICEOVER SCRIPT\n")
        f.write("=" * 60 + "\n\n")

        total_time = 0
        for scene in scenes:
            if scene.voiceover_text:
                f.write(f"[{total_time:.1f}s] {scene.name}\n")
                f.write(f"{scene.voiceover_text}\n\n")
            total_time += scene.duration

        f.write(f"\nTotal duration: {total_time:.1f}s\n")
        word_count = sum(len(s.voiceover_text.split()) for s in scenes if s.voiceover_text)
        f.write(f"Word count: {word_count}\n")
        f.write(f"Speaking rate: ~{word_count / (total_time / 60):.0f} words/min\n")

    print(f"  Script exported: {output_path}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Generate social media short videos")
    parser.add_argument("--article", required=True, help="Article slug or 'all'")
    parser.add_argument("--duration", type=int, default=45, help="Target duration (seconds)")
    parser.add_argument("--preview", action="store_true", help="Save key frames as images only")
    parser.add_argument("--no-voice", action="store_true", help="Skip TTS voiceover")
    parser.add_argument("--script-only", action="store_true", help="Export voiceover script only")
    parser.add_argument("--output", type=str, default=None, help="Output path")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    articles = list(ARTICLE_GENERATORS.keys()) if args.article == "all" else [args.article]

    for slug in articles:
        if slug not in ARTICLE_GENERATORS:
            print(f"No video generator for: {slug}")
            print(f"Available: {', '.join(ARTICLE_GENERATORS.keys())}")
            continue

        print(f"\n{'=' * 60}")
        print(f"Generating: {slug}")
        print(f"{'=' * 60}")

        gen_func = ARTICLE_GENERATORS[slug]
        scenes = gen_func(duration=args.duration, no_voice=args.no_voice)

        total_duration = sum(s.duration for s in scenes)
        total_frames = sum(s.frame_count() for s in scenes)
        print(f"  {len(scenes)} scenes, {total_duration:.1f}s, {total_frames} frames")

        # Export voiceover script
        script_path = str(OUTPUT_DIR / f"{slug}_script.txt")
        export_voiceover_script(scenes, script_path)

        if args.script_only:
            continue

        if args.preview:
            # Save one frame per scene
            preview_dir = str(OUTPUT_DIR / f"{slug}_frames")
            os.makedirs(preview_dir, exist_ok=True)
            for i, scene in enumerate(scenes):
                # Render middle frame
                mid = scene.frame_count() // 2
                img = scene.render_frame(mid, scene.frame_count())
                path = os.path.join(preview_dir, f"{i:02d}_{scene.name}.jpg")
                img.save(path, "JPEG", quality=92)
                print(f"  Frame {i}: {scene.name} -> {path}")
            # Also render first and last frame of animated scenes
            for i, scene in enumerate(scenes):
                if isinstance(scene, (AnimatedBarChartScene, StatCountScene)):
                    img_start = scene.render_frame(0, scene.frame_count())
                    img_end = scene.render_frame(scene.frame_count() - 1, scene.frame_count())
                    img_start.save(os.path.join(preview_dir, f"{i:02d}_{scene.name}_start.jpg"), "JPEG", quality=92)
                    img_end.save(os.path.join(preview_dir, f"{i:02d}_{scene.name}_end.jpg"), "JPEG", quality=92)
            print(f"  Preview frames saved to {preview_dir}/")
            continue

        # Render video
        tmpdir = tempfile.mkdtemp(prefix="tpvideo_")
        try:
            # Step 1: Generate per-scene voiceover and adjust durations
            audio_path = None
            if not args.no_voice and HAS_TTS:
                scene_audio, vo_duration = asyncio.run(
                    generate_voiceover_per_scene(scenes, tmpdir)
                )

                # Recalculate total duration after adjustment
                total_duration = sum(s.duration for s in scenes)
                total_frames = sum(s.frame_count() for s in scenes)
                print(f"  Adjusted: {len(scenes)} scenes, {total_duration:.1f}s, {total_frames} frames")

                # Concat all scene audio into one track
                combined_audio = os.path.join(tmpdir, "voiceover.m4a")
                audio_path = concat_scene_audio(scene_audio, combined_audio)
            elif not HAS_TTS:
                print("  edge-tts not installed, generating without voiceover")

            # Step 2: Render all frames
            frame_count = render_all_frames(scenes, tmpdir)

            # Step 3: Assemble final video
            output_path = args.output or str(OUTPUT_DIR / f"{slug}.mp4")
            success = assemble_video(tmpdir, frame_count, output_path, audio_path)

            if success:
                size_mb = os.path.getsize(output_path) / (1024 * 1024)
                print(f"\n  VIDEO SAVED: {output_path} ({size_mb:.1f}MB)")
                vid_duration = get_audio_duration(output_path)
                if vid_duration:
                    print(f"  Duration: {vid_duration:.1f}s")
            else:
                print("  FAILED to create video!")

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    print("\nDone!")


if __name__ == "__main__":
    main()
