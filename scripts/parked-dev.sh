#!/usr/bin/env bash
# Runs a parked section locally.
#
#   npm run dev:observatory   the Lancashire Business Observatory
#   npm run dev:doge          the Burnley spending explorer
#
# Parked sections live under a `_`-prefixed directory so Astro's underscore rule
# keeps them out of the routed tree (see docs/parked-sections.md). Astro applies
# that rule in dev too, so viewing one means restoring the routed directory name
# for the session and putting it back afterwards.
#
# The dev server runs as a background child so Ctrl-C reaches this script's trap
# immediately rather than being swallowed while bash waits on a foreground child.
# Even if cleanup is somehow missed, the prebuild guard stops the next build.
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  observatory) LIVE="src/pages/lancs"; PATH_HINT="/lancs/business/" ;;
  doge)        LIVE="src/pages/doge";  PATH_HINT="/doge/" ;;
  *) echo "usage: $0 observatory|doge" >&2; exit 2 ;;
esac
PARKED="$(dirname "$LIVE")/_$(basename "$LIVE")"

if [ -d "$LIVE" ]; then
  echo "error: $LIVE already exists, so a previous run may not have cleaned up." >&2
  echo "Check 'git status', then: git mv $LIVE $PARKED" >&2
  exit 1
fi
if [ ! -d "$PARKED" ]; then
  echo "error: $PARKED not found. Nothing to un-park." >&2
  exit 1
fi

DEV_PID=""
cleanup() {
  trap - EXIT INT TERM
  if [ -n "$DEV_PID" ] && kill -0 "$DEV_PID" 2>/dev/null; then
    kill "$DEV_PID" 2>/dev/null || true
    wait "$DEV_PID" 2>/dev/null || true
  fi
  if [ -d "$LIVE" ]; then
    mv "$LIVE" "$PARKED"
    echo ""
    echo "Re-parked at $PARKED, so it is safe to build and deploy."
  fi
}
trap cleanup EXIT INT TERM

mv "$PARKED" "$LIVE"
echo "Un-parked for this session only: http://localhost:4321${PATH_HINT}"
echo "It is NOT published. Ctrl-C to stop and re-park."
echo ""

npm run dev &
DEV_PID=$!
wait "$DEV_PID"
