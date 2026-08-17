"""Shared plumbing for the entity resolution layer (M3).

Three things live here because every M3 script needs them and none of them may
drift between scripts:

1. **Scheme codes.** The identifier axis is org-id.guide, the codelist that
   OCDS, BODS and 360Giving all normatively reference. Codes are validated
   against the vendored bronze copy at load time, so a typo in a scheme code is
   a build failure rather than an orphan edge. Sources with no org-id list get
   a LOCAL code, and a local code is flagged as local on every edge it appears
   on. It is never presented as a standard identifier.

2. **Name canonicalisation.** A verbatim port of resolve_suppliers.normalise
   and supplier_variants. It is a PORT, not an import, because the warehouse
   must be able to rebuild from bronze without the fetcher tree present, and a
   port that silently drifts is worse than no port at all. `check_gold.py`
   re-imports the original and asserts the two agree on a fixture, so drift
   fails the build.

3. **Mint-once entity ids.** A ULID is minted for an entity the first time its
   ANCHOR is seen and never again. The anchor is the highest-precedence
   identifier in the entity's edge set, so an entity anchored on a company
   number keeps its id through any amount of churn in the weaker edges around
   it. The registry is append-only: an id is never re-minted, never reassigned,
   and never deleted. That is the invariant the re-run test asserts.
"""
import datetime as _dt
import json
import os
import re
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources as S  # noqa: E402
import silver as SV  # noqa: E402

# --- Scheme registry ------------------------------------------------------

# org-id.guide codes we use, with the register each one identifies. Every code
# here is checked against the vendored codelist at load.
ORG_ID_SCHEMES = {
    "GB-COH": "Companies House company number",
    "GB-CHC": "Charity Commission for England and Wales number",
    "GB-SC": "Scottish Charity Register number",
    "GB-NIC": "Charity Commission for Northern Ireland number",
    "GB-MPR": "FCA Mutuals Public Register society number",
    "GB-NHS": "NHS Organisation Data Service code",
    "GB-EDU": "Register of Schools England and Wales URN",
    "GB-UKPRN": "UK Register of Learning Providers number",
}

# Identifiers with no org-id list. These are OUR keys for OUR sources and they
# are labelled local everywhere they surface. A local code is a join key, never
# a public identifier.
LOCAL_SCHEMES = {
    "LBO-NNDR": "Ratepayer name as published in a billing authority NNDR file",
    "LBO-SUPPLIER": "Supplier name as published in a council spend ledger",
    "LBO-WEB": "Verified company website, registrable domain",
}

# Anchor precedence. Lower number wins. The point of the ordering is that an
# entity anchored on a statutory register number never loses that anchor to a
# name-shaped local key, so its id survives a matcher rerun.
SCHEME_PRECEDENCE = {
    "GB-COH": 10,
    "GB-CHC": 20,
    "GB-SC": 21,
    "GB-NIC": 22,
    "GB-MPR": 30,
    "GB-NHS": 40,
    "GB-EDU": 50,
    "GB-UKPRN": 60,
    "LBO-WEB": 200,
    "LBO-NNDR": 210,
    "LBO-SUPPLIER": 220,
}

ALL_SCHEMES = {**ORG_ID_SCHEMES, **LOCAL_SCHEMES}


def load_org_id_codelist(h=None):
    """The vendored org-id.guide codelist, read from bronze and nowhere else."""
    snap, path, manifest = SV.resolve_bronze("org_id_guide", h=h)
    fpath, sha = SV.bronze_file(manifest, path, ".json")
    data = json.loads(Path(fpath).read_text())
    codes = {e["code"]: e for e in data["lists"] if e.get("code")}
    return snap, sha, codes


def validate_schemes(h=None):
    """Every org-id scheme we claim must exist in the vendored codelist.

    A scheme code that has quietly been deprecated or renamed upstream is
    exactly the kind of silent breakage a crosswalk cannot afford, because the
    edges outlive the code. Fail the build instead.
    """
    snap, sha, codes = load_org_id_codelist(h)
    missing = sorted(c for c in ORG_ID_SCHEMES if c not in codes)
    if missing:
        raise SystemExit(
            f"FATAL: scheme codes absent from the vendored org-id codelist "
            f"({snap}): {missing}. Either the code is wrong or the codelist "
            "moved. Do not invent a code.")
    deprecated = sorted(c for c in ORG_ID_SCHEMES if codes[c].get("deprecated"))
    clash = sorted(set(LOCAL_SCHEMES) & set(codes))
    if clash:
        raise SystemExit(
            f"FATAL: local scheme code(s) {clash} now exist upstream in "
            "org-id.guide. Rename ours; a local code must never collide with "
            "a standard one.")
    return {
        "codelistSnapshot": snap,
        "codelistSha256": sha,
        "codelistLists": len(codes),
        "orgIdSchemes": sorted(ORG_ID_SCHEMES),
        "localSchemes": sorted(LOCAL_SCHEMES),
        "deprecatedUpstream": deprecated,
    }


def is_local(scheme):
    return scheme in LOCAL_SCHEMES


# --- Name canonicalisation (port of resolve_suppliers.py) -----------------
# Ported verbatim on 17 Aug 2026 from
# scripts/observatory/resolve_suppliers.py. Any change to the original must be
# mirrored here; check_gold.py asserts the two agree.

LEGAL_SUFFIX = re.compile(
    r"\b(LTD|LIMITED|PLC|LLP|LP|CIC|CIO|INC|CO|COMPANY|GROUP|HOLDINGS|UK)\b\.?$")
VAT_TAG = re.compile(r"\s*-\s*(NET|GROSS)\s*$", re.I)


def normalise(name):
    n = VAT_TAG.sub("", (name or "").upper().strip())
    n = re.sub(r"\bT/A\b.*$", " ", n)
    n = re.sub(r"\b(LTD|LIMITED)\s+TA\s+.+$", " LTD ", n)
    n = re.sub(r"\s*-?\s*SALES LEDGER\s*$", " ", n)
    n = re.sub(r"[^A-Z0-9& ]+", " ", n)
    n = n.replace("&", " AND ")
    n = re.sub(r"\s+", " ", n).strip()
    prev = None
    while prev != n:
        prev = n
        n = LEGAL_SUFFIX.sub("", n).strip()
    return n


def supplier_variants(raw):
    out, seen = [], set()

    def add(s):
        k = normalise(s)
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    add(raw)
    add(re.sub(r"[\(\[].*?[\)\]]", " ", raw))
    base = normalise(raw)
    toks = base.split()
    if toks and len(toks[-1]) <= 2 and len(base) >= 24:
        add(" ".join(toks[:-1]))
    return out


# Blocking and comparison need tokens, which the production matcher never
# needed because it only ever did exact-key lookups. New here, so it is stated
# rather than assumed: these are the words that carry no discriminating signal
# in a UK company name and would otherwise dominate a token-overlap block.
NAME_STOPWORDS = {
    "THE", "AND", "OF", "FOR", "SERVICES", "SERVICE", "SOLUTIONS", "UK",
    "GROUP", "HOLDINGS", "TRADING", "ENTERPRISES", "ASSOCIATES", "PARTNERS",
    "INTERNATIONAL", "CONSULTING", "CONSULTANTS", "MANAGEMENT",
}


def name_tokens(normalised):
    return [t for t in normalised.split() if t not in NAME_STOPWORDS]


# --- Lancashire geography prefilter --------------------------------------
# Ported from _common.py for the same reason as normalise: the warehouse must
# not depend on the fetcher tree. This is the postcode prefilter the current
# site spine already uses. Real LAD assignment is F1 geocoding, not M3, and
# nothing here claims to be an LAD.
LANCS_PC_AREAS = ("BB", "PR", "FY", "LA")
LANCS_FRINGE_OUTCODES = ("L39", "L40", "OL12", "OL13", "WN8", "WN6", "OL14", "M26")


def lancs_postcode_sql(col):
    """SQL predicate equivalent to _common.looks_lancs_pc."""
    parts = []
    for a in LANCS_PC_AREAS:
        parts.append(
            f"({col} LIKE '{a}%' AND length({col}) > {len(a)} "
            f"AND substr({col}, {len(a)+1}, 1) BETWEEN '0' AND '9')")
    for oc in LANCS_FRINGE_OUTCODES:
        parts.append(f"{col} LIKE '{oc}%'")
    return "(" + " OR ".join(parts) + ")"


# --- ULID -----------------------------------------------------------------
# 26 characters, Crockford base32: 48 bits of millisecond timestamp then 80
# bits of randomness. Implemented here rather than pulled in as a dependency
# because it is fifteen lines and an entity id must not be able to break on a
# package upgrade.
_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
ULID_RE = "^[0-9ABCDEFGHJKMNPQRSTVWXYZ]{26}$"


def new_ulid(when=None):
    ms = int((when or _dt.datetime.now(_dt.timezone.utc)).timestamp() * 1000)
    rand = secrets.randbits(80)
    n = (ms << 80) | rand
    out = []
    for _ in range(26):
        out.append(_B32[n & 31])
        n >>= 5
    return "".join(reversed(out))


# --- Entity id registry (append-only) -------------------------------------

def gold_dir(h=None):
    d = S.root(h) / "gold"
    d.mkdir(parents=True, exist_ok=True)
    return d


def registry_path(h=None):
    return gold_dir(h) / "entity_id_registry.jsonl"


def load_registry(h=None):
    """anchor key -> {entity_id, mintedAt, mintedInRun}."""
    p = registry_path(h)
    if not p.exists():
        return {}
    out = {}
    with open(p) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            out[r["anchor"]] = r
    return out


def anchor_key(scheme, source_id):
    return f"{scheme}:{source_id}"


class Minter:
    """Mint-once id allocation.

    `entity_id_for(anchor)` returns the existing id if the anchor has ever been
    seen, and otherwise mints one and appends it to the registry file. Nothing
    in this class can return a different id for an anchor it has already
    issued, which is the whole point: the append-only file is the guarantee,
    not the code path.
    """

    def __init__(self, run_id, h=None):
        self.run_id = run_id
        self.path = registry_path(h)
        self.known = load_registry(h)
        self.minted_this_run = 0
        self.reused_this_run = 0
        self._pending = []

    def entity_id_for(self, scheme, source_id):
        key = anchor_key(scheme, source_id)
        rec = self.known.get(key)
        if rec:
            self.reused_this_run += 1
            return rec["entity_id"], False
        eid = new_ulid()
        rec = {
            "anchor": key,
            "entity_id": eid,
            "mintedAt": _dt.datetime.now(_dt.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"),
            "mintedInRun": self.run_id,
        }
        self.known[key] = rec
        self._pending.append(rec)
        self.minted_this_run += 1
        return eid, True

    def bind(self, scheme, source_id, entity_id):
        """Point another anchor at an id that already exists.

        Used when a cluster gains a higher-precedence identifier than the one
        it was minted on. The entity keeps its id and the new anchor becomes a
        second way of finding it. Binding an anchor that already resolves to a
        DIFFERENT id is refused: that would be reassignment, which is the one
        thing this class exists to make impossible.
        """
        key = anchor_key(scheme, source_id)
        rec = self.known.get(key)
        if rec:
            if rec["entity_id"] != entity_id:
                raise SystemExit(
                    f"FATAL: anchor {key} already resolves to "
                    f"{rec['entity_id']} and something tried to bind it to "
                    f"{entity_id}. An id is never reassigned. This means two "
                    "clusters merged that both already had ids; the merge is "
                    "recorded as an alias, not as a rebinding.")
            return False
        rec = {
            "anchor": key,
            "entity_id": entity_id,
            "mintedAt": _dt.datetime.now(_dt.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"),
            "mintedInRun": self.run_id,
            "bound": True,
        }
        self.known[key] = rec
        self._pending.append(rec)
        return True

    def flush(self):
        """Append the run's new ids. Appended, never rewritten."""
        if not self._pending:
            return 0
        with open(self.path, "a") as f:
            for rec in self._pending:
                f.write(json.dumps(rec) + "\n")
            f.flush()
            os.fsync(f.fileno())
        n = len(self._pending)
        self._pending = []
        return n


# --- Gold table plumbing --------------------------------------------------

def table_dir(table, snapshot, h=None):
    d = gold_dir(h) / table / f"snapshot_date={snapshot}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_manifest(out_dir, table, snapshot, rows, nbytes, duckdb_version,
                   inputs, assertions=None, notes=None, extra=None):
    m = {
        "table": table,
        "layer": "gold",
        "snapshotDate": snapshot,
        "rows": rows,
        "bytes": nbytes,
        "inputs": inputs,
        "duckdbVersion": duckdb_version,
        "pipelineGitSha": SV.git_sha(),
        "builtOnHost": S.host(),
        "builtAt": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "assertions": assertions or {},
        "notes": notes,
    }
    if extra:
        m.update(extra)
    with open(out_dir / "manifest.json", "w") as f:
        json.dump(m, f, indent=2)
        f.write("\n")
    return m


def run_id():
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
