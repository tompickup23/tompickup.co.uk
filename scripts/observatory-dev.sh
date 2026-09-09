#!/usr/bin/env bash
# Runs the parked Lancashire Business Observatory locally.
#
# The section lives at src/pages/_lancs/ so Astro's underscore rule keeps it out of
# the routed tree — see docs/parked-observatory.md. Astro applies that rule in dev
# too, so viewing it means restoring the routed directory name for the session and
# putting it back afterwards.
#
# The dev server runs as a background child so that Ctrl-C reaches this script's
# trap immediately rather than being swallowed while bash waits on a foreground
# child. Even if cleanup is somehow missed (SIGKILL, power cut), the prebuild guard
# in scripts/guard-parked.mjs stops the section reaching a build.
set -euo pipefail
cd "$(dirname "$0")/.."

PARKED="src/pages/_lancs"
LIVE="src/pages/lancs"

if [ -d "$LIVE" ]; then
  echo "error: $LIVE already exists — a previous run may not have cleaned up." >&2
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
    echo "Observatory re-parked at $PARKED — safe to build and deploy."
  fi
}
trap cleanup EXIT INT TERM

mv "$PARKED" "$LIVE"
echo "Observatory un-parked for this session only: http://localhost:4321/lancs/business/"
echo "It is NOT published. Ctrl-C to stop and re-park."
echo ""

npm run dev &
DEV_PID=$!
wait "$DEV_PID"
