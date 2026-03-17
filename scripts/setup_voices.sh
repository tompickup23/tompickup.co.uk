#!/bin/bash
# Setup voice models for video generation
# Run once: bash scripts/setup_voices.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Setting up TTS voice models ==="

# 1. Piper TTS (via venv with system Python 3.9)
echo ""
echo "--- Piper TTS ---"
if [ ! -d ".venv" ]; then
    echo "Creating venv with system Python..."
    /usr/bin/python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q piper-tts pathvalidate

# Download Piper Northern English Male model
mkdir -p .venv/piper-voices
PIPER_MODEL=".venv/piper-voices/en_GB-northern_english_male-medium.onnx"
if [ ! -f "$PIPER_MODEL" ]; then
    echo "Downloading Piper Northern English Male model (60MB)..."
    curl -sL "https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_GB/northern_english_male/medium/en_GB-northern_english_male-medium.onnx" -o "$PIPER_MODEL"
    curl -sL "https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_GB/northern_english_male/medium/en_GB-northern_english_male-medium.onnx.json" -o "${PIPER_MODEL}.json"
    echo "Downloaded Piper model."
else
    echo "Piper model already exists."
fi
deactivate

# 2. Kokoro TTS (via system Python user packages)
echo ""
echo "--- Kokoro TTS ---"
/usr/bin/python3 -m pip install --user -q kokoro soundfile 2>/dev/null || echo "Kokoro already installed"
echo "Kokoro ready (voices: bm_daniel, bm_george, bm_lewis, bm_fable, bf_alice, bf_isabella, bf_emma, bf_lily)"

echo ""
echo "=== Voice setup complete ==="
echo "Run: python3 scripts/generate_video.py --article all"
