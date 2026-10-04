#!/usr/bin/env python3
"""Aggregate council transparency spend per supplier, per body, FY2023-24 to 2025-26.

Two sources, chosen with --source:

  banks   (default) the AI DOGE reconciled payment banks, read from the
          per-year Parquet files the data.aidoge.co.uk/v1 interface publishes
          (<base>/<body>/manifest.json, then each year's file under
          <body>/spending/). Files are cached under --bank-dir and checked
          against the sha256 in the manifest. The data contract is
          ~/aidoge-site/docs/data-contract.md.
  legacy  the old monorepo files, <legacy-dir>/<body>/spending-<FY>.json
          (or .json.gz), type == "spend" ONLY. This is the reader the
          Observatory used up to Phase 0 of the Public Pound plan, kept so the
          two can be reconciled.

Rows counted in banks mode: every row the bank counts in its reconciled net
(everything except record_class procurement_committed), less three disclosed
exclusions that are not supplier spend: record_class internal_transfer,
record_class settlement_noise, and type purchase_cards (merchant-level, as the
legacy reader). The excluded values are written per body and year under
"excludedFromSuppliers", so that total plus the exclusions equals the bank's
own net for the years read ("bankNet"); the script stops if the rows do not
sum to the net in the bank's manifest. Rows with record_class
direct_payment_individual are counted under one withheld key and never by the
name on the row.

Writes ~/observatory-data/processed/council_supplier_spend.json (or --out).
The schema is backward compatible: every key the legacy output had is still
there with the same meaning. Added keys: per body "source", "byYear",
"supplierIds" (company_number and supplier_canonical for each kept supplier),
"bankNet", "excludedFromSuppliers" and "bank" (manifest provenance).

Suppliers below the floor are rolled into a disclosed tail so coverage maths
stay honest. Nothing here is published directly; build_pound.py consumes it.

Run (banks needs duckdb, which the system python does not have):
  uv run --with duckdb python scripts/observatory/aggregate_spend.py
  uv run --with duckdb python scripts/observatory/aggregate_spend.py --source legacy
"""
import argparse
import gc
import gzip
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

BODIES = ["blackburn", "blackpool", "burnley", "chorley", "fylde", "hyndburn",
          "lancashire_cc", "lancashire_fire", "lancashire_pcc", "lancaster",
          "pendle", "preston", "ribble_valley", "rossendale", "south_ribble",
          "west_lancashire", "wyre"]
YEARS = ["2023-24", "2024-25", "2025-26"]
FLOOR = 10_000.0  # per-supplier 3yr total below this rolls into the tail

# One key for every payment the bank classes as made to an individual. The
# name on such a row is never used. resolve_suppliers.classify() reads any key
# starting REDACT as an excluded payee.
WITHHELD_KEY = "REDACTED: PAYMENTS TO INDIVIDUALS"

# Bank rows that are not supplier spend. Disclosed per body, never dropped
# silently. procurement_committed is outside the bank's own net as well.
EXCLUDE_RECORD_CLASS = {"internal_transfer", "settlement_noise"}
EXCLUDE_TYPE = {"purchase_cards", "purchase_card", "contracts"}

DEFAULT_OUT = Path.home() / "observatory-data/processed/council_supplier_spend.json"
DEFAULT_LEGACY = Path.home() / "clawd/burnley-council/data"
DEFAULT_BANK_DIR = Path.home() / "observatory-data/raw/aidoge-banks"
DEFAULT_BANK_BASE = "https://data.aidoge.co.uk/v1"


def fy_label(fy):
    """'2023-24' -> '2023/24' (the bank's financial_year form)."""
    return fy.replace("-", "/")


# ---------------------------------------------------------------- legacy --
def legacy_rows(legacy_dir, body, fy):
    for name in (f"spending-{fy}.json", f"spending-{fy}.json.gz"):
        f = legacy_dir / body / name
        if f.exists():
            opener = gzip.open if name.endswith(".gz") else open
            with opener(f, "rt") as fh:
                return json.load(fh), f
    return None, None


def read_legacy(body, legacy_dir):
    """Yield (fy, row) for type == 'spend' rows, as the original reader."""
    found = []

    def gen():
        for fy in YEARS:
            recs, f = legacy_rows(legacy_dir, body, fy)
            if recs is None:
                continue
            found.append(fy)
            for r in recs:
                if r.get("type") != "spend":
                    continue
                yield fy, r
            del recs
            gc.collect()
    return gen(), found


# ----------------------------------------------------------------- banks --
def _fetch(url, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    subprocess.run(["curl", "-sf", "--max-time", "300", "-A",
                    "LancashireBusinessObservatory/1.0 (research; tompickup.co.uk)",
                    "-o", str(tmp), url], check=True)
    tmp.rename(dest)


def bank_manifest(body, base, bank_dir, offline):
    dest = bank_dir / body / "manifest.json"
    if not offline:
        try:
            _fetch(f"{base}/{body}/manifest.json", dest)
        except subprocess.CalledProcessError:
            return None
    if not dest.exists():
        return None
    return json.loads(dest.read_text())


def bank_year_file(body, manifest, fy, base, bank_dir, offline):
    """Local path of the year's Parquet file, sha256-checked, or None."""
    entry = next((f for f in manifest.get("files", [])
                  if f.get("kind") == "parquet" and f["name"].startswith(fy)),
                 None)
    if entry is None:
        return None, None
    dest = bank_dir / body / entry["name"]

    def ok():
        return dest.exists() and hashlib.sha256(
            dest.read_bytes()).hexdigest() == entry.get("sha256")
    if not ok():
        if offline:
            raise SystemExit(f"{body} {fy}: cached file missing or stale, "
                             f"rerun without --offline")
        root = base.rsplit("/v1", 1)[0]
        _fetch(f"{root}/{entry['key']}", dest)
        if not ok():
            raise SystemExit(f"{body} {fy}: sha256 mismatch against manifest")
    return dest, entry


def read_bank(body, manifest, base, bank_dir, offline, excluded, bank_net):
    import duckdb
    con = duckdb.connect()
    found = []

    def gen():
        for fy in YEARS:
            path, entry = bank_year_file(body, manifest, fy, base, bank_dir,
                                         offline)
            if path is None:
                continue
            found.append(fy)
            cols = {r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}

            def col(c):
                return f'"{c}"' if c in cols else "NULL"
            q = (f"SELECT amount, supplier, {col('supplier_canonical')}, "
                 f"{col('record_class')}, {col('type')}, "
                 f"{col('company_number')}, {col('supplier_company_number')} "
                 f"FROM read_parquet('{path}')")
            for amt, sup, canon, rc, typ, cn, scn in con.execute(q).fetchall():
                if rc == "procurement_committed":
                    continue  # outside the bank's own net
                amt = amt or 0.0
                bank_net[fy] += amt
                if rc in EXCLUDE_RECORD_CLASS:
                    excluded[fy][f"record_class:{rc}"] += amt
                    continue
                if typ in EXCLUDE_TYPE:
                    excluded[fy][f"type:{typ}"] += amt
                    continue
                yield fy, {"amount": amt, "supplier": sup,
                           "supplier_canonical": canon, "record_class": rc,
                           "company_number": (cn or scn or "").strip() or None}
    return gen(), found


# ------------------------------------------------------------- aggregate --
def aggregate(rows, withhold):
    agg = defaultdict(float)
    by_year = defaultdict(lambda: {"total": 0.0, "transactions": 0})
    ids = defaultdict(lambda: {"cn": Counter(), "canon": Counter()})
    total, ntx = 0.0, 0
    for fy, r in rows:
        amt = r.get("amount") or 0.0
        if withhold and r.get("record_class") == "direct_payment_individual":
            name = WITHHELD_KEY
        else:
            name = (r.get("supplier_canonical") or r.get("supplier") or "").strip()
            if not name:
                continue
            canon = (r.get("supplier_canonical") or "").strip()
            if canon:
                ids[name]["canon"][canon] += 1
            cn = (r.get("company_number") or r.get("supplier_company_number")
                  or "")
            cn = cn.strip() if isinstance(cn, str) else ""
            if cn:
                ids[name]["cn"][cn] += 1
        agg[name] += amt
        total += amt
        ntx += 1
        by_year[fy]["total"] += amt
        by_year[fy]["transactions"] += 1
    return agg, by_year, ids, total, ntx


def supplier_ids(name, ids):
    d = ids.get(name)
    if not d:
        return {"company_number": None, "supplier_canonical": None}
    cn = d["cn"].most_common()
    out = {"company_number": cn[0][0] if cn else None,
           "supplier_canonical": (d["canon"].most_common(1)[0][0]
                                  if d["canon"] else None)}
    if len(cn) > 1:
        out["company_number_alternatives"] = [c for c, _ in cn[1:]]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--source", choices=["banks", "legacy"], default="banks")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--legacy-dir", type=Path, default=DEFAULT_LEGACY)
    ap.add_argument("--bank-dir", type=Path, default=DEFAULT_BANK_DIR)
    ap.add_argument("--bank-base", default=DEFAULT_BANK_BASE)
    ap.add_argument("--offline", action="store_true",
                    help="banks: use the cache only, fetch nothing")
    ap.add_argument("--no-legacy-fallback", action="store_true",
                    help="banks: leave out a body that has no bank instead "
                         "of reading its legacy files")
    a = ap.parse_args()

    result = {}
    for body in BODIES:
        source, prov = a.source, None
        excluded = defaultdict(lambda: defaultdict(float))
        bank_net = defaultdict(float)
        if a.source == "banks":
            m = bank_manifest(body, a.bank_base, a.bank_dir, a.offline)
            if m is None:
                if a.no_legacy_fallback:
                    print(f"{body}: no bank published, left out")
                    continue
                source = "legacy-fallback"
            else:
                prov = {"manifest": f"{a.bank_base}/{body}/manifest.json",
                        "generated": m.get("generated"),
                        "generator_git_sha": m.get("generator_git_sha"),
                        "presentation": m.get("presentation"),
                        "sqlite_sha256": (m.get("sqlite") or {}).get("sha256")}
        if source == "banks":
            rows, found = read_bank(body, m, a.bank_base, a.bank_dir,
                                    a.offline, excluded, bank_net)
        else:
            rows, found = read_legacy(body, a.legacy_dir)
        agg, by_year, ids, total, ntx = aggregate(rows, source == "banks")
        kept = {k: round(v, 2) for k, v in agg.items() if v >= FLOOR}
        tail_v = round(sum(v for v in agg.values() if v < FLOOR), 2)
        entry = {
            "years": found, "total": round(total, 2), "transactions": ntx,
            "suppliers": dict(sorted(kept.items(), key=lambda kv: -kv[1])),
            "tail": {"value": tail_v, "suppliers": len(agg) - len(kept)},
            "source": source,
            "byYear": {fy: {"total": round(v["total"], 2),
                            "transactions": v["transactions"]}
                       for fy, v in sorted(by_year.items())},
            "supplierIds": {k: supplier_ids(k, ids) for k in kept},
        }
        if source == "banks":
            entry["bank"] = prov
            entry["bankNet"] = {fy: round(v, 2) for fy, v in sorted(bank_net.items())}
            entry["excludedFromSuppliers"] = {
                fy: {k: round(v, 2) for k, v in sorted(ex.items())}
                for fy, ex in sorted(excluded.items())}
            # The bank's own net for each year read must be reproduced from
            # its rows, or the cache is not the bank the manifest describes.
            for fy, v in bank_net.items():
                want = ((m.get("years") or {}).get(fy_label(fy)) or {}).get("net")
                if want is not None and abs(v - want) > 0.01:
                    raise SystemExit(f"{body} {fy}: rows sum to {v:.2f}, "
                                     f"manifest net is {want:.2f}")
        result[body] = entry
        print(f"{body} [{source}]: £{total/1e6:.1f}m over {found}, "
              f"{len(kept)} suppliers >= £10k (tail £{tail_v/1e6:.2f}m / "
              f"{len(agg)-len(kept)})")

    if a.source == "banks":
        src = ("AI DOGE reconciled payment banks (data.aidoge.co.uk/v1, "
               "per-year Parquet), bank net less internal transfers, "
               "settlement noise and purchase cards")
        note = ("Over £500 transparency data, not total budget. Commitments, "
                "internal transfers, settlement noise and purchase cards "
                "excluded and disclosed per body. Payments to individuals "
                "counted under one withheld key. Bodies with no bank read the "
                "legacy files and say so in their source field.")
    else:
        src = "AI DOGE per-council transparency spend files, type=spend only"
        note = ("Over £500 transparency data, not total budget. Contracts and "
                "purchase cards excluded (double-count / merchant-level).")
    out = {"$meta": {"source": src, "note": note, "floor": FLOOR,
                     "years": YEARS, "input": a.source},
           "bodies": result}
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(out))
    print("written", a.out)


if __name__ == "__main__":
    main()
