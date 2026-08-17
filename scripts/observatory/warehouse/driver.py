"""Shared plumbing for the Snakemake driver (M5).

The driver's job is to make the pipeline a DAG rather than a script, so that a
half-finished run is a resumable state instead of a mystery, and so that the
question "what actually produced this figure" has a file-based answer.

Three design points that are not obvious:

1. **Markers, not partitions, are the dependency edges.** A builder writes to
   `<table>/snapshot_date=YYYY-MM-DD/`, and the date is not known until the
   build has read bronze, so a Snakemake rule cannot name its own output path
   in advance. Each rule therefore writes one marker under `state/`, holding
   the partition it produced, the row count and the manifest's own content
   hash. Downstream rules depend on the marker. A rebuild that produces
   identical bytes writes an identical marker, and Snakemake's own mtime
   tracking then reruns exactly the steps whose inputs really moved.

2. **The run id is stamped once and travels.** Every marker and every report
   carries the same `runId`, so an artefact, a gate report and a log line can
   be tied together after the fact without guessing from timestamps.

3. **Nothing here writes to the site.** The driver builds the warehouse and
   emits a candidate site tree into a shadow root. Deploying is a separate,
   supervised act (CUTOVER-RUNBOOK.md in the clawd briefing pack), because a
   driver that can deploy is one bug away from deploying something wrong.

## Gate map (DATA-INTEGRITY s6 to its implementation)

The rulebook names nine gates. They do not all live in the same place, and the
plan requires each one to say where it lives:

| gate | what it asserts | implemented in |
|---|---|---|
| V-T1 | entityType from the s2 enum, matching id scheme present | `checks/*.sql` (silver), `schemas/` (serve) |
| V-T2 | no CRN-less record carries a company-only field | `checks/*.sql`, `checks_gold/*.sql` |
| V-T3 | modelled fields only in whitelisted keys, caption present | `schemas/biz-areas.schema.json` + `check_schemas.py` |
| V-T4 | society numbers never appear in CRN fields | `checks/ch_register.sql`, `checks_gold/crosswalk.sql` |
| V-R1 | staleness budgets, warn at 1x, fail at 2x | `staleness.py`, rule `staleness` |
| V-R2 | joined-vintage label on composite fields | `pointblank_suite.py` (warn tier) |
| V-R3 | every $meta.sources[] has asAt + retrievedAt + licence | `check_schemas.py`, PENDING at serve (see below) |
| V-L1 | restricted-licence output carries its attribution block | `check_schemas.py` + `staleness.py` licence register |
| existing | websites zero-false-positive, PETITION_PREFIX, conflict markers | `validate_outputs.py`, unchanged |

**V-R3 is declared PENDING at the serve layer and that is deliberate.** The
published `$meta.sources[]` contract carries `name`, `url`, `retrieved` and
`licence`; it carries no `asAt` at all, on any source entry in the
nine published files (56 entries as measured on the 17 August edition). Making
V-R3 a hard gate today would block every deploy,
and closing it changes published files, which is a Tom sign-off task in the
same class as the six reproduced faults. `check_schemas.py` therefore measures
the gap exactly and reports it, and the fix is scoped in the cutover runbook.
Bronze already satisfies V-R3 in full, so the gap is in the serve contract and
not in the warehouse.
"""
import datetime as _dt
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402

HERE = Path(__file__).resolve().parent


def log(msg):
    print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def warehouse_root(cfg=None):
    """The warehouse root. `OBS_WAREHOUSE_ROOT` wins so a shadow cycle can be
    pointed somewhere else without editing the registry."""
    if cfg and cfg.get("root"):
        return Path(cfg["root"])
    env = os.environ.get("OBS_WAREHOUSE_ROOT")
    return Path(env) if env else S.root()


def state_dir(cfg=None):
    d = warehouse_root(cfg) / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def report_dir(cfg=None):
    d = warehouse_root(cfg) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def run_id():
    """One id per driver invocation, taken from the environment so every rule
    in the same `snakemake` call agrees. Snakemake runs rules in separate
    processes, so a module-level uuid would give each rule its own."""
    rid = os.environ.get("OBS_RUN_ID")
    if not rid:
        rid = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") \
            + "-" + uuid.uuid4().hex[:6]
        os.environ["OBS_RUN_ID"] = rid
    return rid


def pipeline_git_sha():
    """The commit the deployed pipeline came from.

    Resolution order, most explicit first:

      1. `OBS_PIPELINE_GIT_SHA` in the environment (a deploy or a CI run can
         state it outright).
      2. `PIPELINE_STAMP.json` beside the scripts, written by
         `deploy_warehouse.sh` at rsync time. This is the case that matters:
         `/opt/observatory/warehouse` on vps-main is not a git checkout, which
         is why every manifest M1 to M4 wrote carries a null sha.
      3. `git rev-parse HEAD` in the script directory, for a developer machine.

    Returns None rather than a guess. A wrong sha is worse than an absent one:
    it points a later reader at code that did not build the artefact.
    """
    env = os.environ.get("OBS_PIPELINE_GIT_SHA")
    if env:
        return env.strip()
    stamp = HERE / "PIPELINE_STAMP.json"
    if stamp.exists():
        try:
            s = json.loads(stamp.read_text())
            v = s.get("gitSha")
            if v:
                # A dirty deploy says so. A manifest naming a commit that does
                # not contain the code that built it is the exact failure this
                # field exists to prevent, and it is worse than a null because
                # it looks authoritative.
                return v if s.get("workingTreeClean") else f"{v}-dirty"
        except Exception:
            pass
    import subprocess
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(HERE),
            stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return None


def pipeline_stamp():
    """The full deploy stamp, or None. Used by the manifest gate to know from
    when a null sha stops being legacy and starts being a failure."""
    stamp = HERE / "PIPELINE_STAMP.json"
    if not stamp.exists():
        return None
    try:
        return json.loads(stamp.read_text())
    except Exception:
        return None


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def latest_partition(layer_dir, table):
    """The newest built partition of a table, or None."""
    base = Path(layer_dir) / table
    if not base.exists():
        return None
    parts = [p for p in sorted(base.glob("snapshot_date=*"))
             if (p / "part.parquet").exists()]
    return parts[-1] if parts else None


def describe_partition(part):
    """Marker payload for one built partition: what it is and what it hashes
    to. The manifest hash is what makes a marker meaningful. Two runs that
    produce the same manifest produce the same marker, so an unchanged rebuild
    does not cascade a rerun through the whole DAG."""
    if part is None:
        return None
    mf = part / "manifest.json"
    raw = mf.read_bytes() if mf.exists() else b""
    m = json.loads(raw) if raw else {}
    return {
        "partition": str(part),
        "snapshotDate": m.get("snapshotDate"),
        "rows": m.get("rows"),
        "bytes": m.get("bytes"),
        "pipelineGitSha": m.get("pipelineGitSha"),
        "manifestSha256": sha256_bytes(raw) if raw else None,
    }


def write_marker(path, rule, artefacts=None, extra=None):
    """Write a rule's completion marker.

    Deliberately NOT atomic-on-content: the marker is written after the rule
    body succeeded, so its existence means the artefact exists. Snakemake
    deletes the marker itself if the rule fails.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "rule": rule,
        "runId": run_id(),
        "finishedAt": _dt.datetime.now(_dt.timezone.utc)
                         .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "host": S.host(),
        "pipelineGitSha": pipeline_git_sha(),
        "artefacts": artefacts or {},
    }
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload, indent=2) + "\n")
    return payload


def read_marker(path):
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else None
