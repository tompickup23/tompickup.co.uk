#!/usr/bin/env bash
# Copies the Burnley budget game from Reports into this site.
#
# The game is generated in the Reports repo (Burnley_Budget_Model_work/game/, by build.py,
# which the model's gate runs). Nothing under src/data/burnley-budget-game/ is edited by hand:
# rebuild in Reports, then run this.
#
# Usage: scripts/sync-burnley-game.sh <path to Burnley_Budget_Model_work/game>
#
# Copies preview.html (the whole page as one self-contained file) to
# src/data/burnley-budget-game/game.html and writes source.json beside it with the model
# run id the game prints, the Reports commit and the file's SHA-256. The page wrapper
# (src/pages/burnley-budget-game/index.astro) fails the build if the two disagree.
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <path to Burnley_Budget_Model_work/game>" >&2
  exit 2
fi

SRC="${1%/}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/src/data/burnley-budget-game"
PAGE="$SRC/preview.html"

[ -f "$PAGE" ] || { echo "not found: $PAGE" >&2; exit 1; }

RUN_ID="$(grep -o '"model_run_id": *"[^"]*"' "$PAGE" | head -n 1 | sed 's/.*"\([^"]*\)"$/\1/')"
[ -n "$RUN_ID" ] || { echo "no model_run_id in $PAGE" >&2; exit 1; }

# The game must ask for nothing outside its own page.
if grep -Eqi '(src|href)="(https?:)?//|url\((https?:)?//|@import' "$PAGE"; then
  echo "refusing: $PAGE refers to something outside the page" >&2
  exit 1
fi

mkdir -p "$DEST"
cp "$PAGE" "$DEST/game.html"

SHA="$(shasum -a 256 "$DEST/game.html" | cut -d ' ' -f 1)"
COMMIT="$(git -C "$SRC" log -1 --format=%H -- preview.html 2>/dev/null || true)"

cat > "$DEST/source.json" <<JSON
{
  "note": "Generated in Reports. Do not edit game.html or this file by hand; run scripts/sync-burnley-game.sh.",
  "generated_in": "tompickup23/reports, Burnley_Budget_Model_work/game/preview.html",
  "reports_commit": "${COMMIT}",
  "model_run_id": "${RUN_ID}",
  "game_sha256": "${SHA}"
}
JSON

echo "copied $PAGE"
echo "model run: $RUN_ID"
echo "sha256:    $SHA"
echo "Now check the edition line in src/pages/burnley-budget-game/index.astro, then npm run build."
