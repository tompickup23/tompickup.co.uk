#!/usr/bin/env bash
# deploy_warehouse.sh - put this warehouse on a host and tell it which commit it is.
#
# /opt/observatory/warehouse on vps-main is an rsync target, not a git checkout,
# which is why every manifest written in M1 to M4 carries pipelineGitSha: null.
# This script closes that: it rsyncs the warehouse directory and writes
# PIPELINE_STAMP.json beside the scripts, which driver.pipeline_git_sha() reads
# before it falls back to git.
#
# The stamp is written from the checkout being deployed FROM, so it names the
# code that is arriving. It records the working tree's cleanliness too: a
# manifest whose sha points at a commit that does not contain the code that
# built it is worse than a null, so a dirty deploy says so out loud.
#
# This script deploys the WAREHOUSE only. It never touches /opt/observatory/site,
# the Pages repo, monthly_refresh.sh or any cron entry. Cutting the monthly run
# over to the new driver is a separate, supervised step and its runbook is
# CUTOVER-RUNBOOK.md in the clawd briefing pack.
#
# Usage:
#   scripts/observatory/warehouse/deploy_warehouse.sh vps-main
#   scripts/observatory/warehouse/deploy_warehouse.sh vps-main --dry-run
set -euo pipefail

HOST="${1:-vps-main}"
shift || true
DRY=""
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY="--dry-run"; done

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SRC/../../.." && pwd)"
DST=/opt/observatory/warehouse

GIT_SHA="$(git -C "$REPO" rev-parse HEAD)"
GIT_BRANCH="$(git -C "$REPO" rev-parse --abbrev-ref HEAD)"
if git -C "$REPO" diff --quiet -- "$SRC" && git -C "$REPO" diff --cached --quiet -- "$SRC"; then
  CLEAN=true
else
  CLEAN=false
  echo "WARNING: the warehouse directory has uncommitted changes."
  echo "         The stamp will record dirty=true so no manifest claims to"
  echo "         have been built by a commit that does not contain its code."
fi

STAMP="$SRC/PIPELINE_STAMP.json"
cat > "$STAMP" <<JSON
{
  "gitSha": "$GIT_SHA",
  "gitBranch": "$GIT_BRANCH",
  "workingTreeClean": $CLEAN,
  "installedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "installedFrom": "$(hostname -s)",
  "installedTo": "$HOST:$DST",
  "note": "Read by driver.pipeline_git_sha(). /opt/observatory/warehouse is an rsync target, not a git checkout, so this file is how a manifest names the code that built it."
}
JSON

echo "stamping $GIT_SHA ($GIT_BRANCH, clean=$CLEAN)"
echo "rsync $SRC/ -> $HOST:$DST/"
# --delete is deliberately absent: the target also holds __pycache__ and, on a
# bad day, a file somebody is mid-way through debugging. Removing files on a
# deploy is a separate decision from shipping new ones.
rsync -a $DRY \
  --exclude '__pycache__' --exclude '*.pyc' \
  "$SRC/" "$HOST:$DST/"

if [ -z "$DRY" ]; then
  ssh "$HOST" "cat $DST/PIPELINE_STAMP.json"
  ssh "$HOST" "/opt/observatory/venv/bin/python -c \"
import sys; sys.path.insert(0, '$DST')
import driver as D
print('pipelineGitSha resolves to', D.pipeline_git_sha())\""
fi
