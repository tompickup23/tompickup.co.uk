#!/usr/bin/env bash
# run_vps.sh - copy this checkout's waterfall code to vps-main and run a stage.
#
# The waterfall reads the warehouse (bronze, silver, gold) read-only and works
# in /root/pp-phase1c, outside /opt/observatory. Nothing here touches the
# warehouse install, a cron entry, the site or Pages.
#
# The run is stamped with this checkout's HEAD; a dirty tree refuses to run,
# so decided_by = pipeline:<sha> always names a commit that holds the code.
#
# Usage: scripts/observatory/pound/run_vps.sh <script.py> [args...]
set -euo pipefail
HOST="${POUND_HOST:-vps-main}"
WORKDIR=/root/pp-phase1c
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SRC/../../.." && pwd)"
if ! git -C "$REPO" diff --quiet -- scripts/observatory || ! git -C "$REPO" diff --cached --quiet -- scripts/observatory; then
  echo "uncommitted changes under scripts/observatory; commit first" >&2
  exit 2
fi
SHA="$(git -C "$REPO" rev-parse HEAD)"
rsync -a "$REPO/scripts/observatory/resolve_suppliers.py" "$REPO/scripts/observatory/_common.py" \
      "$REPO/scripts/observatory/build_pound.py" "$HOST:$WORKDIR/code/"
rsync -a --exclude __pycache__ "$SRC/" "$HOST:$WORKDIR/code/pound/"
ssh "$HOST" "export PATH=/root/.local/bin:\$PATH POUND_GIT_SHA=$SHA; cd $WORKDIR/code/pound && \
  uv run -q --no-project --with 'duckdb>=1.4,<1.5' --with python-calamine --with pyarrow python -u $*"
