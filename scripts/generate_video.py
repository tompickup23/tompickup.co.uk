#!/usr/bin/env python3
"""
Social media short video generator for tompickup.co.uk articles.

Generates 9:16 vertical shorts (1080x1920) suitable for:
- Instagram Reels
- TikTok
- YouTube Shorts
- Facebook Reels

Design system:
- Dark premium theme (#0d1117) matching the website
- Official UK party colours
- Reform UK teal (#12B6CF) as primary accent
- Animated data visualisations with slide-in effects
- tompickup.co.uk branding throughout
- Reform UK logo badge

Usage:
    python3 scripts/generate_video.py --article burnley-elections-2026-attendance
    python3 scripts/generate_video.py --article all
    python3 scripts/generate_video.py --article burnley-elections-2026-attendance --duration 30
"""

import os
import sys
import argparse
import textwrap
import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import numpy as np

# Attempt moviepy import
try:
    from moviepy import (
        ImageClip, TextClip, CompositeVideoClip,
        concatenate_videoclips, AudioFileClip, ColorClip
    )
    HAS_MOVIEPY = True
except ImportError:
    try:
        from moviepy.editor import (
            ImageClip, TextClip, CompositeVideoClip,
            concatenate_videoclips, AudioFileClip, ColorClip
        )
        HAS_MOVIEPY = True
    except ImportError:
        HAS_MOVIEPY = False

# Paths
BASE_DIR = Path(__file__).parent.parent
IMAGES_DIR = BASE_DIR / "public" / "images"
CONTENT_DIR = BASE_DIR / "src" / "content" / "news"
OUTPUT_DIR = BASE_DIR / "public" / "videos"
LOGO_PATH = Path("/tmp/reform-uk-logo.png")

# Video dimensions (9:16 vertical)
W, H = 1080, 1920
FPS = 30

# Design tokens
COLORS = {
    'bg':           (13, 17, 23),
    'card':         (22, 27, 34),
    'card_border':  (33, 38, 45),
    'teal':         (18, 182, 207),
    'teal_dim':     (14, 120, 140),
    'white':        (240, 240, 245),
    'light':        (200, 200, 210),
    'muted':        (130, 130, 140),
    'dim':          (80, 82, 88),
}

PARTY_COLORS = {
    'Conservative': (0, 135, 220),
    'Labour':       (228, 0, 59),
    'Lib Dem':      (250, 166, 26),
    'Green':        (106, 176, 35),
    'Independent':  (170, 170, 180),
    'Reform UK':    (18, 182, 207),
}


def load_font(size, bold=False):
    if bold:
        for p in [
            "/System/Library/Fonts/SFPro.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
        ]:
            try:
                idx = 1 if p.endswith('.ttc') else 0
                return ImageFont.truetype(p, size, index=idx)
            except:
                try:
                    return ImageFont.truetype(p, size)
                except:
                    continue
    for p in [
        "/System/Library/Fonts/SFPro.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
    ]:
        try:
            return ImageFont.truetype(p, size)
        except:
            continue
    return ImageFont.load_default()


def draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    x0, y0, x1, y1 = xy
    r = min(radius, (x1 - x0) // 2, (y1 - y0) // 2)
    if r < 1:
        if fill: draw.rectangle(xy, fill=fill)
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


def create_frame_base():
    """Create a base frame with background and branding."""
    img = Image.new("RGB", (W, H), COLORS['bg'])
    draw = ImageDraw.Draw(img)
    # Top teal accent bar
    draw.rectangle([(0, 0), (W, 4)], fill=COLORS['teal'])
    return img, draw


def add_branding(img, draw):
    """Add Reform UK logo and tompickup.co.uk branding to frame."""
    # Reform UK logo top-left
    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
        logo = logo.resize((56, 56), Image.LANCZOS)
        img.paste(logo, (40, 40), logo)
    except:
        pass
    font_brand = load_font(16, bold=True)
    draw.text((108, 46), "REFORM UK", fill=COLORS['teal'], font=font_brand)
    font_loc = load_font(13)
    draw.text((108, 66), "BURNLEY", fill=COLORS['muted'], font=font_loc)

    # tompickup.co.uk bottom
    font_site = load_font(18, bold=True)
    site = "tompickup.co.uk"
    sb = draw.textbbox((0, 0), site, font=font_site)
    sw = sb[2] - sb[0]
    draw.text(((W - sw) // 2, H - 60), site, fill=COLORS['teal'], font=font_site)

    # Bottom teal bar
    draw.rectangle([(0, H - 4), (W, H)], fill=COLORS['teal'])

    return img


def create_title_frame(title, subtitle=None, bg_image_path=None):
    """Create the opening title frame."""
    img, draw = create_frame_base()

    # Background image if available
    if bg_image_path and os.path.exists(bg_image_path):
        bg = Image.open(bg_image_path).convert("RGB")
        bg_w, bg_h = bg.size
        scale = max(W / bg_w, H / bg_h)
        bg = bg.resize((int(bg_w * scale), int(bg_h * scale)), Image.LANCZOS)
        left = (bg.size[0] - W) // 2
        top = (bg.size[1] - H) // 2
        bg = bg.crop((left, top, left + W, top + H))
        bg = bg.filter(ImageFilter.GaussianBlur(radius=8))

        # Gradient overlay
        overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        odraw = ImageDraw.Draw(overlay)
        for y in range(H):
            alpha = int(160 + 60 * (y / H))
            odraw.rectangle([(0, y), (W, y + 1)], fill=(13, 17, 23, alpha))
        img = Image.alpha_composite(bg.convert("RGBA"), overlay).convert("RGB")
        draw = ImageDraw.Draw(img)
        draw.rectangle([(0, 0), (W, 4)], fill=COLORS['teal'])

    add_branding(img, draw)

    # Reform UK logo large centered
    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
        logo = logo.resize((120, 120), Image.LANCZOS)
        img.paste(logo, ((W - 120) // 2, 400), logo)
    except:
        pass

    # Title text
    font_title = load_font(52, bold=True)
    lines = textwrap.wrap(title, width=20)
    title_y = 580
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font_title)
        lw = bbox[2] - bbox[0]
        draw.text(((W - lw) // 2, title_y), line, fill=COLORS['white'], font=font_title)
        title_y += 66

    # Teal accent line
    draw.rectangle([(W // 2 - 100, title_y + 20), (W // 2 + 100, title_y + 24)], fill=COLORS['teal'])

    # Subtitle
    if subtitle:
        font_sub = load_font(28)
        sub_lines = textwrap.wrap(subtitle, width=30)
        sub_y = title_y + 50
        for line in sub_lines:
            bbox = draw.textbbox((0, 0), line, font=font_sub)
            lw = bbox[2] - bbox[0]
            draw.text(((W - lw) // 2, sub_y), line, fill=COLORS['light'], font=font_sub)
            sub_y += 38

    return img


def create_stat_frame(stat_value, stat_label, stat_sublabel, party=None, extra_text=None):
    """Create a frame showing a single large statistic."""
    img, draw = create_frame_base()
    add_branding(img, draw)

    color = PARTY_COLORS.get(party, COLORS['teal']) if party else COLORS['teal']

    # Large stat value
    font_value = load_font(180, bold=True)
    bbox = draw.textbbox((0, 0), stat_value, font=font_value)
    vw = bbox[2] - bbox[0]
    draw.text(((W - vw) // 2, 500), stat_value, fill=color, font=font_value)

    # Label
    font_label = load_font(36, bold=True)
    bbox = draw.textbbox((0, 0), stat_label, font=font_label)
    lw = bbox[2] - bbox[0]
    draw.text(((W - lw) // 2, 730), stat_label, fill=COLORS['white'], font=font_label)

    # Sublabel
    font_sub = load_font(24)
    bbox = draw.textbbox((0, 0), stat_sublabel, font=font_sub)
    sw = bbox[2] - bbox[0]
    draw.text(((W - sw) // 2, 790), stat_sublabel, fill=COLORS['muted'], font=font_sub)

    # Party pill if specified
    if party:
        font_pill = load_font(20, bold=True)
        pill_text = party.upper()
        pb = draw.textbbox((0, 0), pill_text, font=font_pill)
        pw = pb[2] - pb[0]
        px = (W - pw - 24) // 2
        py = 860
        pill_bg = tuple(max(0, c // 4) for c in color)
        draw_rounded_rect(draw, (px, py, px + pw + 24, py + 36), 18, fill=pill_bg)
        draw_rounded_rect(draw, (px, py, px + pw + 24, py + 36), 18, outline=color, width=2)
        draw.text((px + 12, py + 6), pill_text, fill=color, font=font_pill)

    # Extra text
    if extra_text:
        font_extra = load_font(22)
        lines = textwrap.wrap(extra_text, width=38)
        ey = 940
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font_extra)
            ew = bbox[2] - bbox[0]
            draw.text(((W - ew) // 2, ey), line, fill=COLORS['dim'], font=font_extra)
            ey += 32

    return img


def create_bar_chart_frame(title, data, subtitle=None):
    """
    Create a frame with horizontal bar chart.
    data: list of (name, ward, party, percentage, counts_text)
    """
    img, draw = create_frame_base()
    add_branding(img, draw)

    # Title
    font_title = load_font(36, bold=True)
    draw.text((60, 130), title, fill=COLORS['white'], font=font_title)

    # Teal accent line
    tbbox = draw.textbbox((60, 130), title, font=font_title)
    draw.rectangle([(60, tbbox[3] + 10), (320, tbbox[3] + 13)], fill=COLORS['teal'])

    if subtitle:
        font_sub = load_font(20)
        draw.text((60, tbbox[3] + 24), subtitle, fill=COLORS['muted'], font=font_sub)

    # Bar chart
    bar_x = 60
    bar_max_w = W - 120
    bar_h = 80
    gap = 44
    y = 340

    font_name = load_font(28, bold=True)
    font_detail = load_font(18)
    font_pill = load_font(14, bold=True)
    font_pct = load_font(34, bold=True)
    font_count = load_font(16)

    for name, ward, party, pct, counts in data:
        color = PARTY_COLORS.get(party, COLORS['teal'])

        # Name
        draw.text((bar_x, y), name, fill=COLORS['white'], font=font_name)

        # Ward + party pill
        ward_text = ward
        draw.text((bar_x, y + 38), ward_text, fill=COLORS['muted'], font=font_detail)
        wb = draw.textbbox((0, 0), ward_text, font=font_detail)
        pill_x = bar_x + (wb[2] - wb[0]) + 16

        # Party pill
        abbr = party[:3].upper()
        pb = draw.textbbox((0, 0), abbr, font=font_pill)
        ppw = pb[2] - pb[0] + 16
        pph = pb[3] - pb[1] + 10
        pill_bg = tuple(max(0, c // 4) for c in color)
        draw_rounded_rect(draw, (pill_x, y + 36, pill_x + ppw, y + 36 + pph), pph // 2,
                          fill=pill_bg)
        draw_rounded_rect(draw, (pill_x, y + 36, pill_x + ppw, y + 36 + pph), pph // 2,
                          outline=color, width=1)
        draw.text((pill_x + 8, y + 40), abbr, fill=color, font=font_pill)

        y += 68

        # Bar track
        draw_rounded_rect(draw, (bar_x, y, bar_x + bar_max_w, y + bar_h), 12,
                          fill=COLORS['card'], outline=COLORS['card_border'])

        # Bar fill
        fill_w = int(bar_max_w * pct / 100)
        if fill_w > 24:
            draw_rounded_rect(draw, (bar_x, y, bar_x + fill_w, y + bar_h), 12, fill=color)

        # Percentage on bar
        pct_text = f"{pct:.0f}%"
        pct_bbox = draw.textbbox((0, 0), pct_text, font=font_pct)
        pct_w = pct_bbox[2] - pct_bbox[0]
        if fill_w > pct_w + 32:
            draw.text((bar_x + fill_w - pct_w - 20, y + 18), pct_text,
                      fill=(10, 13, 18), font=font_pct)
        else:
            draw.text((bar_x + fill_w + 16, y + 18), pct_text,
                      fill=color, font=font_pct)

        # Counts
        draw.text((bar_x + bar_max_w - 80, y + bar_h + 8), counts,
                  fill=COLORS['dim'], font=font_count)

        y += bar_h + gap

    return img


def create_text_frame(heading, body_lines, accent_color=None):
    """Create a frame with text content."""
    img, draw = create_frame_base()
    add_branding(img, draw)

    color = accent_color or COLORS['teal']

    # Heading
    font_heading = load_font(40, bold=True)
    h_lines = textwrap.wrap(heading, width=24)
    y = 300
    for line in h_lines:
        draw.text((60, y), line, fill=COLORS['white'], font=font_heading)
        y += 54

    # Accent line
    draw.rectangle([(60, y + 8), (280, y + 11)], fill=color)
    y += 36

    # Body
    font_body = load_font(26)
    for line in body_lines:
        wrapped = textwrap.wrap(line, width=36)
        for wl in wrapped:
            draw.text((60, y), wl, fill=COLORS['light'], font=font_body)
            y += 38
        y += 12

    return img


def create_cta_frame(text="Read the full article", url="tompickup.co.uk"):
    """Create the closing call-to-action frame."""
    img, draw = create_frame_base()
    add_branding(img, draw)

    # Large Reform UK logo
    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
        logo = logo.resize((160, 160), Image.LANCZOS)
        img.paste(logo, ((W - 160) // 2, 500), logo)
    except:
        pass

    # CTA text
    font_cta = load_font(36, bold=True)
    bbox = draw.textbbox((0, 0), text, font=font_cta)
    tw = bbox[2] - bbox[0]
    draw.text(((W - tw) // 2, 720), text, fill=COLORS['white'], font=font_cta)

    # URL
    font_url = load_font(32, bold=True)
    bbox = draw.textbbox((0, 0), url, font=font_url)
    uw = bbox[2] - bbox[0]
    draw.text(((W - uw) // 2, 800), url, fill=COLORS['teal'], font=font_url)

    # Teal accent line
    draw.rectangle([(W // 2 - 80, 870), (W // 2 + 80, 874)], fill=COLORS['teal'])

    return img


# ============================================================
# ARTICLE-SPECIFIC VIDEO GENERATORS
# ============================================================

def generate_burnley_elections_video(duration=30):
    """
    Generate video for the Burnley Elections 2026 attendance article.

    Bannon-style messaging structure:
    1. HOOK - Lead with the most outrageous fact (50% attendance)
    2. ESCALATE - Stack the evidence, build the pattern
    3. BETRAY - They tried to cancel your right to vote
    4. CONTRAST - Every other party supported cancellation
    5. VINDICATE - Only Reform stood for democracy
    6. EMPOWER - Call to action, put the power back in voters' hands
    """
    print("Generating: Burnley Elections 2026 attendance video...")

    frames = []

    # === HOOK: Open with outrage, not context ===
    # Frame 1: Shocking stat - punch them in the face with the number
    frame1 = create_stat_frame(
        "50%", "Your councillor missed HALF his meetings",
        "Neil Mottershead, Conservative, Gannow",
        party="Conservative",
        extra_text="Paid from your council tax. Sat on just 2 committees. Missed 26 of 52 meetings in 4 years."
    )
    frames.append(("hook_50pct", frame1, 4))

    # === ESCALATE: It's not just one, it's a pattern ===
    # Frame 2: Second worst
    frame2 = create_stat_frame(
        "56%", "Getting worse, not better",
        "Christine Sollis, Independent, Brunshaw",
        party="Independent",
        extra_text="Vice-Chair of Audit & Standards. Missed 3 meetings of her own committee. 69% overall, dropping to 56%."
    )
    frames.append(("escalate_sollis", frame2, 4))

    # Frame 3: Third - zero attendance on key committee
    frame3 = create_stat_frame(
        "0 of 3", "Zero. Not one.",
        "Alex Hall, Green, Trinity: Audit & Standards this year",
        party="Green",
        extra_text="73.8% overall, collapsing to 53% in the last 6 months. 4 absences without apology."
    )
    frames.append(("escalate_hall", frame3, 4))

    # Frame 4: Bar chart - the full picture, the pattern is clear
    frame4 = create_bar_chart_frame(
        "Three Councillors Below 75%",
        [
            ("Neil Mottershead", "Gannow", "Conservative", 50.0, "26/52"),
            ("Christine Sollis", "Brunshaw", "Independent", 69.0, "60/87"),
            ("Alex Hall", "Trinity", "Green", 73.8, "48/65"),
        ],
        subtitle="These people are asking for your vote again."
    )
    frames.append(("pattern", frame4, 4))

    # === BETRAY: They tried to cancel your right to vote ===
    # Frame 5: The betrayal
    frame5 = create_text_frame(
        "Then They Tried to Cancel Your Elections",
        [
            "January 2026. Burnley's Executive voted UNANIMOUSLY to cancel the May elections.",
            "",
            "Five councillors. One vote. Your democratic right, gone.",
            "",
            "Independents. Lib Dems. Greens. All voted yes.",
            "",
            "The Scrutiny Committee said no. They ignored it.",
        ]
    )
    frames.append(("betrayal", frame5, 5))

    # === CONTRAST: Every party failed you ===
    # Frame 6: All parties complicit
    frame6 = create_text_frame(
        "Every Other Party Supported It",
        [
            "Labour started it. They wrote to every council asking to cancel.",
            "",
            "Conservatives requested cancellation across Lancashire too.",
            "",
            "Lib Dems, Greens, Independents: all voted for it in Burnley.",
            "",
            "Every single establishment party wanted to take away your vote.",
        ]
    )
    frames.append(("contrast", frame6, 5))

    # === VINDICATE: Reform stood alone ===
    # Frame 7: Reform as the only defender
    frame7 = create_text_frame(
        "One Party Said No",
        [
            "Reform UK launched a judicial review to save your elections.",
            "",
            "On 16 February, the government backed down.",
            "",
            "29 council areas. Millions of voters. Elections restored.",
            "",
            "Because one party fought for your right to choose.",
        ],
        accent_color=COLORS['teal']
    )
    frames.append(("vindicate", frame7, 5))

    # === EMPOWER: Give them the weapon ===
    # Frame 8: Title card with background - the full picture
    bg_img = str(IMAGES_DIR / "burnley-town-hall-raw.jpg")
    frame8 = create_title_frame(
        "Burnley Elections 2026",
        "7 May. 15 wards. Your choice.",
        bg_image_path=bg_img
    )
    frames.append(("context", frame8, 3))

    # Frame 9: CTA - empower the viewer
    frame9 = create_cta_frame(
        "Ask them: will you turn up?",
        "tompickup.co.uk"
    )
    frames.append(("cta", frame9, 3))

    return frames


# ============================================================
# VIDEO ASSEMBLY
# ============================================================

def assemble_video_ffmpeg(frames, output_path, fps=FPS):
    """Assemble frames into video using ffmpeg directly (no moviepy needed)."""
    import subprocess
    import tempfile

    tmpdir = tempfile.mkdtemp(prefix="tpvideo_")

    # Save each frame as images, repeated for duration
    frame_idx = 0
    for name, img, duration_secs in frames:
        num_frames = int(duration_secs * fps)
        frame_path = os.path.join(tmpdir, f"frame_{name}.png")
        img.save(frame_path, "PNG")

        for i in range(num_frames):
            link_path = os.path.join(tmpdir, f"seq_{frame_idx:06d}.png")
            os.symlink(frame_path, link_path)
            frame_idx += 1

    # Assemble with ffmpeg
    input_pattern = os.path.join(tmpdir, "seq_%06d.png")
    cmd = [
        "/opt/homebrew/bin/ffmpeg", "-y",
        "-framerate", str(fps),
        "-i", input_pattern,
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-preset", "medium",
        "-crf", "23",
        "-movflags", "+faststart",
        output_path
    ]

    print(f"  Encoding {frame_idx} frames at {fps}fps...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ffmpeg error: {result.stderr[-500:]}")
        return False

    # Cleanup
    import shutil
    shutil.rmtree(tmpdir)

    return True


def assemble_video_moviepy(frames, output_path, fps=FPS):
    """Assemble frames into video using moviepy."""
    clips = []
    for name, img, duration_secs in frames:
        arr = np.array(img)
        clip = ImageClip(arr, duration=duration_secs)
        clips.append(clip)

    final = concatenate_videoclips(clips, method="compose")
    final.write_videofile(output_path, fps=fps, codec="libx264",
                          audio=False, preset="medium",
                          ffmpeg_params=["-crf", "23", "-pix_fmt", "yuv420p"])
    return True


# ============================================================
# ARTICLE REGISTRY
# ============================================================

ARTICLE_GENERATORS = {
    "burnley-elections-2026-attendance": generate_burnley_elections_video,
}


def save_frames_as_images(frames, output_dir):
    """Save individual frames as images for review."""
    os.makedirs(output_dir, exist_ok=True)
    for i, (name, img, duration) in enumerate(frames):
        path = os.path.join(output_dir, f"{i:02d}_{name}.jpg")
        img.save(path, "JPEG", quality=92)
        print(f"  Frame {i}: {name} ({duration}s) -> {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate social media short videos for articles")
    parser.add_argument("--article", required=True,
                        help="Article slug or 'all'")
    parser.add_argument("--duration", type=int, default=30,
                        help="Target video duration in seconds")
    parser.add_argument("--preview", action="store_true",
                        help="Save frames as images instead of video")
    parser.add_argument("--output", type=str, default=None,
                        help="Output path (default: public/videos/)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    articles = list(ARTICLE_GENERATORS.keys()) if args.article == "all" else [args.article]

    for slug in articles:
        if slug not in ARTICLE_GENERATORS:
            print(f"No video generator for article: {slug}")
            print(f"Available: {', '.join(ARTICLE_GENERATORS.keys())}")
            continue

        gen_func = ARTICLE_GENERATORS[slug]
        frames = gen_func(duration=args.duration)

        total_duration = sum(d for _, _, d in frames)
        print(f"  {len(frames)} frames, {total_duration}s total")

        if args.preview:
            preview_dir = str(OUTPUT_DIR / f"{slug}_frames")
            save_frames_as_images(frames, preview_dir)
            print(f"  Preview frames saved to {preview_dir}/")
        else:
            output_path = args.output or str(OUTPUT_DIR / f"{slug}.mp4")
            print(f"  Assembling video: {output_path}")

            success = assemble_video_ffmpeg(frames, output_path)
            if success:
                size_mb = os.path.getsize(output_path) / (1024 * 1024)
                print(f"  Video saved: {output_path} ({size_mb:.1f}MB)")
            else:
                print("  Failed to create video!")

    print("\nDone!")


if __name__ == "__main__":
    main()
