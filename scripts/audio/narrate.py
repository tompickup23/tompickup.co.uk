"""Narrate one article: spoken script in, MP3 plus accuracy report out.

    ~/clawd/.venv-video/bin/python scripts/audio/narrate.py stocks-massey-bequest-2026 [--voice bf_emma]

Reads scripts/audio/scripts/<slug>.txt (the spoken script, one paragraph per
block, figures written as words) and optional <slug>.unread.txt (figures
deliberately not read out). Writes public/audio/news/<slug>.mp3, checks
every figure in the article against a Whisper transcript of the audio, and
prints the `audio:` frontmatter block to paste into the article.

Offline pre-processing only. Uses Kokoro (Apache 2.0) and mlx-whisper from
~/clawd/.venv-video, and the Kokoro model files in ~/clawd/models/kokoro.
"""
import argparse, datetime, hashlib, json, re, subprocess, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELS = Path.home() / "clawd/models/kokoro"


def article_body(slug):
    text = (ROOT / f"src/content/news/{slug}.md").read_text()
    return text.split("---", 2)[2]


def text_hash(body):
    # Must match textHash() in src/lib/audio.ts: whitespace collapsed, trimmed.
    return hashlib.sha256(re.sub(r"\s+", " ", body).strip().encode()).hexdigest()


def numbers(text):
    text = re.sub(r"\]\([^)]*\)", "]", text)          # markdown link targets
    text = re.sub(r'href="[^"]*"', "", text)
    text = re.sub(r"(\d)[ ,](\d{3})\b", r"\1\2", text)  # 34,000 and 34 000
    return set(re.findall(r"\d+", text))


def synthesise(script, voice, tmp):
    """Writes tmp/raw.wav plus one wav per paragraph (checked one by one: Whisper
    drops whole stretches when given a long file in one pass)."""
    import numpy as np, soundfile as sf
    from kokoro_onnx import Kokoro
    k = Kokoro(str(MODELS / "kokoro-v1.0.int8.onnx"), str(MODELS / "voices-v1.0.bin"))
    chunks, paras, timings, sr, t = [], [], [], 24000, 0.0
    for i, p in enumerate(x.strip() for x in script.split("\n\n") if x.strip()):
        samples, sr = k.create(p, voice=voice, speed=1.0, lang="en-gb")
        paras.append(f"{tmp}/p{i:03}.wav")
        sf.write(paras[-1], samples, sr)
        gap = 0.9 if len(p) < 80 else 0.6
        timings.append({"start": round(t, 3), "end": round(t + len(samples) / sr, 3), "text": p})
        t += len(samples) / sr + gap
        chunks += [samples, np.zeros(int(sr * gap), dtype=samples.dtype)]
    sf.write(f"{tmp}/raw.wav", np.concatenate(chunks), sr)
    return t, paras, timings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--voice", default="bf_emma")
    a = ap.parse_args()
    script = (ROOT / f"scripts/audio/scripts/{a.slug}.txt").read_text()
    mp3 = ROOT / f"public/audio/news/{a.slug}.mp3"
    import mlx_whisper
    with tempfile.TemporaryDirectory() as tmp:
        seconds, paras, timings = synthesise(script, a.voice, tmp)
        subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-i", f"{tmp}/raw.wav", "-af",
                        "loudnorm=I=-16:TP=-1.5:LRA=11", "-ac", "1", "-ar", "44100",
                        "-c:a", "libmp3lame", "-b:a", "96k", str(mp3)], check=True)
        heard = "\n".join(mlx_whisper.transcribe(w, path_or_hf_repo="mlx-community/whisper-large-v3-turbo",
                                                  language="en")["text"].strip() for w in paras)
    (ROOT / f"scripts/audio/scripts/{a.slug}.heard.txt").write_text(heard)
    # Paragraph timings, for captions, chapters and video scenes.
    (ROOT / f"scripts/audio/scripts/{a.slug}.timings.json").write_text(json.dumps(timings, indent=1))
    body = article_body(a.slug)
    # <slug>.unread.txt lists figures deliberately not read out (photo credit,
    # link text, file names), with the reason after a #.
    unread_file = ROOT / f"scripts/audio/scripts/{a.slug}.unread.txt"
    unread = set(unread_file.read_text().split("#")[0].split()) if unread_file.exists() else set()
    missing = numbers(body) - numbers(heard) - unread
    extra = numbers(heard) - numbers(body)
    print(f"{mp3.relative_to(ROOT)}: {seconds:.0f}s, {mp3.stat().st_size / 1e6:.1f} MB")
    print("figures in article not heard:", " ".join(sorted(missing, key=int)) or "none")
    print("figures heard not in article:", " ".join(sorted(extra, key=int)) or "none")
    print(f"\naudio:\n  src: /audio/news/{a.slug}.mp3\n  duration: {round(seconds)}\n"
          f"  voice: \"Kokoro {a.voice}\"\n  generated: {datetime.date.today()}\n"
          f"  textHash: \"{text_hash(body)}\"")
    sys.exit(1 if missing or extra else 0)


if __name__ == "__main__":
    main()
