#!/usr/bin/env python3
"""
Armed Forces Covenant Gold Award video generator.

Special video: real footage of the national anthem at Full Council,
with voiceover segments mixed over ducked anthem audio + text overlays.

Unlike generate_video.py (which renders frames from scratch), this script:
1. Takes the raw anthem video as base footage
2. Generates TTS voiceover segments at specific timestamps
3. Overlays text/logo using ffmpeg drawtext filters
4. Mixes audio with ducking (anthem quieter during voiceover)

Usage:
    python3 scripts/generate_anthem_video.py
    python3 scripts/generate_anthem_video.py --no-voice    # Skip TTS, anthem only
    python3 scripts/generate_anthem_video.py --script-only  # Export script timing only
"""

import os
import sys
import subprocess
import tempfile
import shutil
import argparse
import json
import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = Path(__file__).parent
IMAGES_DIR = BASE_DIR / "public" / "images"
OUTPUT_DIR = BASE_DIR / "public" / "videos"
VOICE_CONFIG_PATH = SCRIPTS_DIR / "voice_config.json"
PIPER_VENV = BASE_DIR / ".venv"
PIPER_MODEL_DIR = PIPER_VENV / "piper-voices"
FFMPEG = "/opt/homebrew/bin/ffmpeg"
FFPROBE = "/opt/homebrew/bin/ffprobe"

RAW_VIDEO = OUTPUT_DIR / "lancashire-armed-forces-covenant-raw.mp4"
OUTPUT_VIDEO = OUTPUT_DIR / "lancashire-armed-forces-covenant.mp4"
LOGO_PATH = IMAGES_DIR / "lcc-logo.png"
PHOTO_GROUP = IMAGES_DIR / "lcc-armed-forces-covenant.jpg"
PHOTO_GOLD = IMAGES_DIR / "lcc-armed-forces-covenant-gold.jpg"

# Target output: 1080x1920 (9:16 vertical) to match other videos
W, H = 1080, 1920
FPS = 30

# ============================================================
# VOICEOVER SEGMENTS
# ============================================================
# Each segment: (start_time, text, visual_description)
# Gaps between segments = anthem plays at full volume
# Structure avoids talking over the key anthem lines

VOICEOVER_SEGMENTS = [
    {
        # Before anthem starts (0-30s is pre-anthem)
        "start": 3.0,
        "text": "Lancashire County Council. Armed Forces Covenant. Gold Award.",
        "overlay": "ARMED FORCES COVENANT\nEMPLOYER RECOGNITION SCHEME\nGOLD AWARD",
        "duration_est": 4.0,
        "photo": None,
    },
    {
        "start": 10.0,
        "text": "The highest recognition from the Ministry of Defence for employers who champion veterans, reservists and military families.",
        "overlay": "MINISTRY OF DEFENCE\nHighest level of recognition",
        "duration_est": 6.0,
        "photo": "gold",  # 4-person award photo
    },
    {
        "start": 20.0,
        "text": "Fifty-three thousand, five hundred and sixty-seven veterans call Lancashire home. Four point three percent of the population.",
        "overlay": "53,567\nVETERANS IN LANCASHIRE",
        "duration_est": 6.5,
        "photo": None,
    },
    {
        # Anthem starts ~30s — gap from ~27s to 39s, no voiceover
        "start": 39.0,
        "text": "Eleven of Lancashire's eighty-four county councillors are veterans or reservists. At Full Council, they stood to be acknowledged for their service.",
        "overlay": "11\nVETERAN OR RESERVIST\nCOUNCILLORS",
        "duration_est": 7.0,
        "photo": "group",  # group ceremony photo
    },
    {
        "start": 53.0,
        "text": "Armed Forces Champion Councillor Gary Kniveton, an ex-Royal Marine, received the certificate. He founded the Bay Veterans Association in Morecambe, the North West's largest veterans support group.",
        "overlay": "COUNCILLOR GARY KNIVETON\nArmed Forces Champion\nEx-Royal Marine",
        "duration_est": 9.0,
        "photo": "gold",  # 4-person award photo
    },
    {
        "start": 66.0,
        "text": "Lancashire first signed the Armed Forces Covenant in twenty thirteen and has held Gold since twenty twenty. The brass band played the national anthem at County Hall for the presentation.",
        "overlay": "GOLD SINCE 2020\nArmed Forces Covenant\nCounty Hall, Preston",
        "duration_est": 7.0,
        "photo": None,
    },
]


def get_audio_duration(path):
    """Get duration of an audio file in seconds."""
    result = subprocess.run(
        [FFPROBE, "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", path],
        capture_output=True, text=True
    )
    return float(result.stdout.strip()) if result.stdout.strip() else 0


def get_voice_assignment():
    """Get voice config for this article."""
    try:
        with open(VOICE_CONFIG_PATH) as f:
            config = json.load(f)
        assignments = config.get("article_voice_assignments", {})
        if "lancashire-armed-forces-covenant" in assignments:
            return assignments["lancashire-armed-forces-covenant"]
    except Exception:
        pass
    return {"engine": "kokoro", "voice": "bm_daniel"}


def generate_tts(text, output_path):
    """Generate TTS audio for a voiceover segment."""
    assignment = get_voice_assignment()
    engine = assignment.get("engine", "kokoro")
    voice = assignment.get("voice", "bm_daniel")

    if engine == "kokoro":
        safe_text = text.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")
        script = f'''
import soundfile as sf
from kokoro import KPipeline
import warnings
warnings.filterwarnings("ignore")
pipe = KPipeline(lang_code="b")
generator = pipe("{safe_text}", voice="{voice}", speed=1.0)
for gs, ps, audio in generator:
    sf.write("{output_path}", audio, 24000)
    break
'''
        result = subprocess.run(
            ["/usr/bin/python3", "-c", script],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            print(f"    Kokoro TTS error: {result.stderr[:200]}")
            return 0
    else:
        # Piper fallback
        model_name = "en_GB-northern_english_male-medium"
        model_path = str(PIPER_MODEL_DIR / f"{model_name}.onnx")
        piper_bin = str(PIPER_VENV / "bin" / "python3")
        safe_text = text.replace('"', '\\"').replace("'", "\\'")
        cmd = f'echo "{safe_text}" | {piper_bin} -m piper --model {model_path} --output_file {output_path}'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"    Piper TTS error: {result.stderr[:200]}")
            return 0

    return get_audio_duration(output_path)


def generate_voiceover_segments(tmpdir):
    """Generate all TTS segments and return timing info."""
    segments = []

    for i, seg in enumerate(VOICEOVER_SEGMENTS):
        audio_path = os.path.join(tmpdir, f"vo_{i:02d}.wav")
        print(f"  Segment {i+1}/{len(VOICEOVER_SEGMENTS)}: {seg['text'][:50]}...")

        duration = generate_tts(seg["text"], audio_path)
        if duration > 0:
            print(f"    Duration: {duration:.1f}s (starts at {seg['start']}s)")
            segments.append({
                "start": seg["start"],
                "audio_path": audio_path,
                "duration": duration,
                "overlay": seg["overlay"],
            })
        else:
            print(f"    TTS failed, skipping segment")
            segments.append({
                "start": seg["start"],
                "audio_path": None,
                "duration": seg["duration_est"],
                "overlay": seg["overlay"],
            })

    return segments


def build_audio_mix(segments, tmpdir, raw_video_path):
    """Mix anthem audio with voiceover segments using ffmpeg sidechain ducking.

    Strategy:
    - Extract anthem audio from raw video
    - Place voiceover segments at their start times
    - Duck (lower) anthem volume during voiceover
    - Output combined audio track
    """
    output_audio = os.path.join(tmpdir, "mixed_audio.m4a")

    # Build ffmpeg filter graph for audio mixing with ducking
    # Input 0: raw video (anthem audio)
    # Inputs 1..N: voiceover segments

    inputs = ["-i", str(raw_video_path)]
    filter_parts = []
    vo_labels = []
    input_idx = 1

    for seg in segments:
        if seg["audio_path"] and os.path.exists(seg["audio_path"]):
            inputs.extend(["-i", seg["audio_path"]])
            delay_ms = int(seg["start"] * 1000)
            label = f"vo{input_idx}"
            # Delay voiceover to its start time, boost volume slightly
            filter_parts.append(
                f"[{input_idx}]adelay={delay_ms}|{delay_ms},volume=1.8[{label}]"
            )
            vo_labels.append(f"[{label}]")
            input_idx += 1

    if not vo_labels:
        # No voiceover - just extract anthem audio
        cmd = [FFMPEG, "-y", "-i", str(raw_video_path),
               "-vn", "-c:a", "aac", "-b:a", "128k", output_audio]
        subprocess.run(cmd, capture_output=True)
        return output_audio

    # Combine all voiceover segments into one track
    n_vo = len(vo_labels)
    vo_mix = "".join(vo_labels)
    if n_vo > 1:
        filter_parts.append(f"{vo_mix}amix=inputs={n_vo}:duration=longest[vo_combined]")
        vo_combined_label = "[vo_combined]"
    else:
        vo_combined_label = vo_labels[0]

    # Use sidechaincompress to duck anthem when voiceover is active
    # This automatically lowers anthem volume when voiceover plays
    filter_parts.append(
        f"[0:a]{vo_combined_label}sidechaincompress="
        f"threshold=0.01:ratio=6:attack=50:release=300:level_in=1:level_sc=1"
        f"[anthem_ducked]"
    )

    # Re-add voiceover on top of ducked anthem
    # Need to recreate vo_combined since sidechaincompress consumed it
    # Alternative: use amerge or amix
    # Actually, let's use a different approach: volume automation

    # Simpler approach: mix anthem (lowered during VO) + voiceover
    # 1. Lower anthem overall to -12dB
    # 2. Mix voiceover at 0dB
    filter_parts_v2 = []
    vo_labels_v2 = []
    input_idx_v2 = 1

    for seg in segments:
        if seg["audio_path"] and os.path.exists(seg["audio_path"]):
            delay_ms = int(seg["start"] * 1000)
            label = f"v{input_idx_v2}"
            filter_parts_v2.append(
                f"[{input_idx_v2}]adelay={delay_ms}|{delay_ms},volume=2.0[{label}]"
            )
            vo_labels_v2.append(f"[{label}]")
            input_idx_v2 += 1

    # Build volume envelope for anthem ducking
    # Lower anthem volume during each voiceover segment
    # Use enable-based volume filters for each segment (simpler than nested ifs)
    anthem_chain = "[0:a]"
    for i, seg in enumerate(segments):
        if seg["audio_path"]:
            start = seg["start"]
            end = start + seg["duration"] + 0.5
            # Duck anthem to 15% volume during voiceover, with fade
            anthem_chain += f"volume=enable='between(t,{start},{end})':volume=0.15,"
    anthem_chain = anthem_chain.rstrip(",")
    anthem_filter = f"{anthem_chain}[anthem_ducked]"

    # Rebuild filter graph with volume envelope approach
    all_filter_parts = [anthem_filter] + filter_parts_v2

    # Mix ducked anthem + all voiceover segments
    all_labels = ["[anthem_ducked]"] + vo_labels_v2
    n_total = len(all_labels)
    mix_input = "".join(all_labels)
    all_filter_parts.append(
        f"{mix_input}amix=inputs={n_total}:duration=first:dropout_transition=0[out]"
    )

    filter_graph = ";".join(all_filter_parts)

    cmd = [FFMPEG, "-y"] + inputs + [
        "-filter_complex", filter_graph,
        "-map", "[out]",
        "-c:a", "aac", "-b:a", "192k",
        output_audio
    ]

    print(f"  Mixing audio ({n_total} tracks)...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Audio mix error: {result.stderr[-600:]}")
        # Fallback: try simpler mix without ducking
        print("  Trying simpler mix (no ducking)...")
        return build_simple_mix(segments, tmpdir, raw_video_path)

    duration = get_audio_duration(output_audio)
    print(f"  Mixed audio: {duration:.1f}s")
    return output_audio


def build_simple_mix(segments, tmpdir, raw_video_path):
    """Fallback: simple mix with anthem at lower volume + voiceover on top."""
    output_audio = os.path.join(tmpdir, "mixed_audio_simple.m4a")

    inputs = ["-i", str(raw_video_path)]
    filter_parts = []
    labels = []

    # Anthem at reduced volume
    filter_parts.append("[0:a]volume=0.25[anthem]")
    labels.append("[anthem]")

    input_idx = 1
    for seg in segments:
        if seg["audio_path"] and os.path.exists(seg["audio_path"]):
            inputs.extend(["-i", seg["audio_path"]])
            delay_ms = int(seg["start"] * 1000)
            label = f"v{input_idx}"
            filter_parts.append(
                f"[{input_idx}]adelay={delay_ms}|{delay_ms},volume=2.0[{label}]"
            )
            labels.append(f"[{label}]")
            input_idx += 1

    n = len(labels)
    mix_input = "".join(labels)
    filter_parts.append(f"{mix_input}amix=inputs={n}:duration=first:dropout_transition=0[out]")

    filter_graph = ";".join(filter_parts)

    cmd = [FFMPEG, "-y"] + inputs + [
        "-filter_complex", filter_graph,
        "-map", "[out]",
        "-c:a", "aac", "-b:a", "192k",
        output_audio
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Simple mix also failed: {result.stderr[-400:]}")
        return None

    return output_audio


def load_font(size, bold=False):
    """Load system font (macOS)."""
    candidates = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFPro.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def text_center_x(draw, text, font, y, color, shadow=True):
    """Draw centered text with optional drop shadow."""
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    x = (W - tw) // 2
    if shadow:
        draw.text((x + 3, y + 3), text, font=font, fill=(0, 0, 0, 180))
    draw.text((x, y), text, font=font, fill=color)


_logo_cache = None
_photo_cache = {}


def _load_logo():
    """Load and cache the LCC logo (transparent PNG)."""
    global _logo_cache
    if _logo_cache is not None:
        return _logo_cache
    if LOGO_PATH.exists():
        try:
            logo = Image.open(LOGO_PATH).convert("RGBA")
            logo = logo.resize((160, 160), Image.LANCZOS)
            _logo_cache = logo
            return logo
        except Exception:
            pass
    _logo_cache = False
    return False


def _load_photo(key):
    """Load and cache a ceremony photo, scaled for bottom-half overlay."""
    if key in _photo_cache:
        return _photo_cache[key]

    path = PHOTO_GROUP if key == "group" else PHOTO_GOLD
    if path.exists():
        try:
            photo = Image.open(path).convert("RGBA")
            # Scale to fit width, max height 40% of frame
            pw, ph = photo.size
            target_w = W - 80  # 40px margin each side
            scale = target_w / pw
            target_h = min(int(ph * scale), int(H * 0.35))
            scale = min(target_w / pw, target_h / ph)
            photo = photo.resize((int(pw * scale), int(ph * scale)), Image.LANCZOS)
            # Add rounded corners and subtle border
            _photo_cache[key] = photo
            return photo
        except Exception:
            pass
    _photo_cache[key] = None
    return None


def render_overlay(frame_time, segments):
    """Render a transparent overlay image for the given timestamp.

    Returns a PIL RGBA image with text/logo overlays, or None if no overlay needed.
    """
    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    has_content = False

    # LCC logo (top-right, always visible, transparent background)
    logo = _load_logo()
    if logo:
        overlay.paste(logo, (W - 180, 20), logo)
        has_content = True

    # Find active segment
    active_seg = None
    for seg in segments:
        start = seg["start"]
        end = start + seg["duration"] + 0.5
        if start <= frame_time < end:
            active_seg = seg
            break

    # "LANCASHIRE COUNTY COUNCIL" title (first 3 seconds only)
    if frame_time < 3.0:
        font_title = load_font(36, bold=True)
        text_center_x(draw, "LANCASHIRE COUNTY COUNCIL", font_title,
                      int(H * 0.06), (255, 255, 255, 230))
        has_content = True

    # Active segment overlay
    if active_seg:
        photo_key = active_seg.get("photo")
        photo = _load_photo(photo_key) if photo_key else None

        if photo:
            # Photo overlay: centered in middle of frame with dark surround
            pw, ph = photo.size
            photo_x = (W - pw) // 2
            photo_y = int(H * 0.38)

            # Dark semi-transparent backdrop behind photo
            backdrop_margin = 12
            for y in range(photo_y - backdrop_margin, photo_y + ph + backdrop_margin):
                alpha = 160
                draw.rectangle([(photo_x - backdrop_margin, y),
                               (photo_x + pw + backdrop_margin, y + 1)],
                              fill=(0, 0, 0, alpha))

            # Paste photo
            overlay.paste(photo, (photo_x, photo_y), photo)

        # Dark gradient bar at bottom for text readability
        for y in range(int(H * 0.74), H):
            alpha = min(210, int((y - H * 0.74) / (H * 0.26) * 210))
            draw.rectangle([(0, y), (W, y + 1)], fill=(0, 0, 0, alpha))

        lines = active_seg["overlay"].split("\n")
        if lines:
            first_line = lines[0]
            is_stat = len(first_line) <= 10
            font_main = load_font(72 if is_stat else 40, bold=True)
            y_start = int(H * 0.80)
            text_center_x(draw, first_line, font_main, y_start, (255, 255, 255, 255))

            font_sub = load_font(26)
            for i, line in enumerate(lines[1:], 1):
                y = y_start + (72 if is_stat else 46) + i * 34
                text_center_x(draw, line, font_sub, y,
                              (200, 200, 210, 220))

        has_content = True

    # Closing text (last 4 seconds)
    if frame_time >= 71.0:
        for y in range(int(H * 0.78), H):
            alpha = min(180, int((y - H * 0.78) / (H * 0.22) * 180))
            draw.rectangle([(0, y), (W, y + 1)], fill=(0, 0, 0, alpha))

        font_close = load_font(40, bold=True)
        font_close_sub = load_font(26)
        text_center_x(draw, "Lancashire County Council", font_close,
                      int(H * 0.85), (255, 255, 255, 240))
        text_center_x(draw, "Supporting Those Who Serve", font_close_sub,
                      int(H * 0.90), (200, 200, 210, 200))
        has_content = True

    return overlay if has_content else None


def build_video_with_overlays(raw_video_path, audio_path, segments, output_path):
    """Build final video: extract frames, add PIL overlays, re-encode with mixed audio.

    Since ffmpeg drawtext is not available (no libfreetype), we:
    1. Extract raw video frames
    2. Scale to 1080x1920
    3. Composite PIL text/logo overlays
    4. Re-encode with mixed audio
    """
    tmpdir = tempfile.mkdtemp(prefix="anthem_frames_")

    try:
        # Step 1: Extract frames from raw video
        print(f"  Extracting frames from raw video...")
        frame_pattern = os.path.join(tmpdir, "raw_%06d.png")
        cmd = [FFMPEG, "-y", "-i", str(raw_video_path),
               "-vf", f"scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H}",
               "-r", str(FPS),
               frame_pattern]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  Frame extraction error: {result.stderr[-400:]}")
            return False

        # Count extracted frames
        raw_frames = sorted([f for f in os.listdir(tmpdir) if f.startswith("raw_")])
        n_frames = len(raw_frames)
        print(f"  Extracted {n_frames} frames")

        # Step 2: Add overlays to each frame
        print(f"  Adding overlays...")
        out_pattern = os.path.join(tmpdir, "out_%06d.png")
        for i, fname in enumerate(raw_frames):
            frame_time = i / FPS
            frame_path = os.path.join(tmpdir, fname)
            out_path = os.path.join(tmpdir, f"out_{i+1:06d}.png")

            # Load raw frame
            frame = Image.open(frame_path).convert("RGBA")

            # Render overlay
            overlay = render_overlay(frame_time, segments)
            if overlay:
                frame = Image.alpha_composite(frame, overlay)

            # Save as RGB (ffmpeg needs it)
            frame.convert("RGB").save(out_path, "PNG")

            if (i + 1) % 300 == 0:
                print(f"    {i+1}/{n_frames} frames processed ({frame_time:.1f}s)")

        # Step 3: Re-encode with audio
        print(f"  Encoding final video...")
        out_frame_pattern = os.path.join(tmpdir, "out_%06d.png")
        cmd = [FFMPEG, "-y",
               "-framerate", str(FPS),
               "-i", out_frame_pattern]

        if audio_path and os.path.exists(audio_path):
            cmd.extend(["-i", audio_path])
        else:
            cmd.extend(["-i", str(raw_video_path)])

        cmd.extend([
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-preset", "medium",
            "-crf", "20",
            "-c:a", "aac",
            "-b:a", "192k",
            "-movflags", "+faststart",
            "-shortest",
        ])

        if audio_path and os.path.exists(audio_path):
            cmd.extend(["-map", "0:v", "-map", "1:a"])
        else:
            cmd.extend(["-map", "0:v", "-map", "1:a"])

        cmd.append(str(output_path))

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  Video encoding error: {result.stderr[-600:]}")
            return False

        return True

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def export_script():
    """Export the voiceover script with timing."""
    total_vo = 0
    print("\nVOICEOVER SCRIPT — Armed Forces Covenant Gold Award")
    print("=" * 60)
    print()
    for i, seg in enumerate(VOICEOVER_SEGMENTS):
        dur = seg["duration_est"]
        print(f"[{seg['start']:.1f}s] segment_{i+1}")
        print(f"{seg['text']}")
        print(f"[Visual: {seg['overlay']}]")
        print()
        total_vo += dur

    video_dur = get_audio_duration(str(RAW_VIDEO)) if RAW_VIDEO.exists() else 74.6
    print(f"Total video: {video_dur:.1f}s")
    print(f"Total voiceover: ~{total_vo:.1f}s")
    print(f"Anthem full volume: ~{video_dur - total_vo:.1f}s")
    words = sum(len(s["text"].split()) for s in VOICEOVER_SEGMENTS)
    print(f"Word count: {words}")


def main():
    parser = argparse.ArgumentParser(description="Generate Armed Forces Covenant video")
    parser.add_argument("--no-voice", action="store_true", help="Skip TTS, anthem only with overlays")
    parser.add_argument("--script-only", action="store_true", help="Export script timing only")
    parser.add_argument("--output", type=str, default=None, help="Output path")
    args = parser.parse_args()

    if args.script_only:
        export_script()
        return

    if not RAW_VIDEO.exists():
        print(f"Raw video not found: {RAW_VIDEO}")
        print("Copy the national anthem video to public/videos/lancashire-armed-forces-covenant-raw.mp4")
        sys.exit(1)

    output = args.output or str(OUTPUT_VIDEO)

    print(f"\n{'=' * 60}")
    print("Armed Forces Covenant Gold Award Video")
    print(f"{'=' * 60}")
    print(f"  Raw video: {RAW_VIDEO}")
    print(f"  Output: {output}")
    print(f"  Logo: {'YES' if LOGO_PATH.exists() else 'NO'}")
    print(f"  Voice: {'SKIP' if args.no_voice else 'YES'}")

    tmpdir = tempfile.mkdtemp(prefix="anthem_video_")
    try:
        if args.no_voice:
            # Just overlays on raw video, keep original audio
            segments = [{"start": s["start"], "audio_path": None,
                        "duration": s["duration_est"], "overlay": s["overlay"]}
                       for s in VOICEOVER_SEGMENTS]
            success = build_video_with_overlays(RAW_VIDEO, None, segments, output)
        else:
            # Generate TTS + mix audio + build video
            print("\n  Step 1: Generating voiceover segments...")
            segments = generate_voiceover_segments(tmpdir)

            print("\n  Step 2: Mixing audio (anthem + voiceover with ducking)...")
            mixed_audio = build_audio_mix(segments, tmpdir, RAW_VIDEO)
            if not mixed_audio:
                print("  Audio mixing failed!")
                sys.exit(1)

            print("\n  Step 3: Building video with overlays...")
            success = build_video_with_overlays(RAW_VIDEO, mixed_audio, segments, output)

        if success:
            size_mb = os.path.getsize(output) / (1024 * 1024)
            duration = get_audio_duration(output)
            print(f"\n  VIDEO SAVED: {output} ({size_mb:.1f}MB, {duration:.1f}s)")
        else:
            print("\n  FAILED to create video!")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
