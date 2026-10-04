"""Shared paths, pinned inputs and helpers for the Public Pound waterfall.

Public Pound Phase 1 (Reports Public_Pound_work/phase0/WATERFALL.md). The
waterfall runs where the warehouse lives (vps-main), in a scratch work
directory outside /opt/observatory, and reads bronze, silver and gold
read-only. Every input file read is hashed and listed in the run manifest.

Environment:
  POUND_WORK   work directory (default /root/pp-phase1c on vps-main)
  OBS_ROOT     warehouse data root (default /opt/observatory)
"""
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from resolve_suppliers import normalise, supplier_variants  # noqa: E402,F401

WORK = Path(os.environ.get("POUND_WORK", "/root/pp-phase1c"))
OBS = Path(os.environ.get("OBS_ROOT", "/opt/observatory"))
BRONZE, SILVER, GOLD = OBS / "bronze", OBS / "silver", OBS / "gold"
EXTRACT = WORK / "extract"
OUT = WORK / "out"
INPUTS = WORK / "inputs"      # files copied in from the Mac (spend input, clawd)

BODIES = ["blackburn", "blackpool", "burnley", "fylde", "hyndburn",
          "lancashire_cc", "lancaster", "pendle", "preston", "ribble_valley",
          "rossendale", "south_ribble", "west_lancashire", "wyre"]
YEARS = ["2024/25", "2025/26"]

# Payment window per financial year, for the ownership walk (rule 2.6).
FY_DATES = {"2024/25": ("2024-04-01", "2025-03-31"),
            "2025/26": ("2025-04-01", "2026-03-31")}

WITHHELD_KEY = "REDACTED: PAYMENTS TO INDIVIDUALS"
NAMELESS_KEY = "(NO SUPPLIER TEXT)"

CRN_RE = re.compile(r"^(\d{8}|(SC|NI|OC|SO|NC|NF|FC|SL|SF|LP|R0|IP|RS|SP|CE|CS|GE|GS)\d{6})$")


def norm_crn(v):
    """WATERFALL.md s1: strip spaces, upper-case, left-pad 6 or 7 digit
    numbers to 8. Returns the normalised value and whether it has a valid
    shape (presence in a register snapshot is checked separately)."""
    if v is None:
        return None, False
    s = re.sub(r"\s+", "", str(v)).upper()
    if re.fullmatch(r"\d{6,7}", s):
        s = s.zfill(8)
    return s, bool(CRN_RE.fullmatch(s))


def norm_pc(pc):
    if not pc:
        return None
    return re.sub(r"\s+", "", str(pc).upper()) or None


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def git_sha():
    """The pipeline sha: POUND_GIT_SHA (set by the runner, which deploys a
    clean checkout) or the checkout's own HEAD."""
    s = os.environ.get("POUND_GIT_SHA")
    if s:
        return s
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE,
                                       stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return "unknown"


# Distinctive-token test for step 1 (identifier and name agree). Tokens that
# say nothing about which organisation it is do not count.
GENERIC_TOKENS = {
    "THE", "AND", "OF", "FOR", "IN", "ON", "AT", "TO", "A", "AN", "BY",
    "LTD", "LIMITED", "PLC", "LLP", "LP", "CIC", "CIO", "INC", "CO", "COMPANY",
    "GROUP", "HOLDINGS", "UK", "GB", "BRITAIN", "BRITISH", "ENGLAND", "NORTH",
    "SOUTH", "EAST", "WEST", "NORTHERN", "SOUTHERN", "SERVICES", "SERVICE",
    "SOLUTIONS", "SYSTEMS", "INTERNATIONAL", "NATIONAL", "TRUST", "ASSOCIATION",
    "CENTRE", "CENTER", "LANCASHIRE", "LANCS", "MANAGEMENT", "TRADING",
    "ENTERPRISES", "PARTNERSHIP", "PARTNERS", "CONSULTING", "CONSULTANTS",
    "CARE", "HOME", "HOMES", "PROPERTY", "PROPERTIES", "BUSINESS", "SUPPLIES",
    "SUPPLY", "CONTRACTORS", "CONTRACTS", "TRADE", "TRADERS", "GENERAL",
    "NEW", "OLD", "GREAT", "ROYAL", "COMMUNITY", "SOCIETY", "LANC",
}


def distinctive_tokens(norm_name):
    return {t for t in norm_name.split()
            if t not in GENERIC_TOKENS and (len(t) >= 3 or t.isdigit())}


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, ensure_ascii=False, default=str))


def latest_partition(layer_dir, name="part.parquet"):
    parts = sorted(p for p in layer_dir.glob("snapshot_date=*") if (p / name).exists())
    if not parts:
        raise SystemExit(f"no partition under {layer_dir}")
    return parts[-1] / name
