#!/usr/bin/env python3
"""Aggregate council transparency spend per supplier, per body, FY2024-25 and 2025-26.

Public Pound Phase 1 scope (Reports Public_Pound_work/phase0/RULES.md,
decision 10, 4 October 2026): financial years 2024/25 and 2025/26, fourteen
bodies on their AI DOGE banks. Chorley, Lancashire Fire and the PCC are
reported as not yet covered (no bank, or a bank ending January 2025); no rows
are read for them and the output says so under "$meta.notCovered".

Two sources, chosen with --source:

  banks   (default) the AI DOGE reconciled payment banks, read from the
          per-year Parquet files the data.aidoge.co.uk/v1 interface publishes
          (<base>/<body>/manifest.json, then each year's file under
          <body>/spending/). Files are cached under --bank-dir and checked
          against the sha256 in the manifest. The data contract is
          ~/aidoge-site/docs/data-contract.md. There is no fallback to the
          legacy files: a body in scope with no manifest, an HTTP error or a
          missing cached manifest stops the run.
  legacy  the old monorepo files, <legacy-dir>/<body>/spending-<FY>.json
          (or .json.gz), type == "spend" ONLY, for the same bodies and years.
          Kept so the two inputs can be reconciled; never mixed into a banks
          run.

Rows counted in banks mode. A commitment is defined as the publisher defines
it (clawd scripts/publish_data_r2.py@087a1aa4 L271 to 279): record_class
procurement_committed, or no record_class and type contracts. Commitments are
outside the bank's own net and are disclosed under "commitments". Every other
row is in the bank net. Three disclosed exclusions are not supplier spend:
record_class internal_transfer, record_class settlement_noise and type
purchase_cards (merchant level, as the legacy reader). Their values are
written per body and year under "excludedFromSuppliers".

Rule RC03 (RULES.md decision 9): record_class accrual_journal rows (the
County only) are payments, counted by supplier like any other row, and shown
on their own line per body and year under "separateLines".

Rows with record_class direct_payment_individual, and rows whose supplier
text is exactly REDACTED or REDACT whatever their class, are counted under
one withheld key and never by the name on the row. Rows with no supplier text
are counted under the key "(NO SUPPLIER TEXT)" and disclosed, never dropped.

Value conservation, in integer pence, per body and year: suppliers total plus
exclusions equals the bank net from the rows, which must equal the manifest's
years[<fy>].net; a missing manifest net stops the run. Every Parquet file
read is pinned in the output by key, sha256, bytes and rows.

Each body carries its publication threshold and VAT basis from
body_vat_threshold.csv (Reports Public_Pound_work/phase1/spend_input), copied
beside this script; the copy's sha256 is written to the output.

Writes ~/observatory-data/processed/council_supplier_spend.json (or --out) and,
with --payee-keys-out, one JSON line per (body, financial year, payee key) for
the resolution waterfall. The payee key is upper(strip(supplier_canonical or
supplier)), the fuel register's key, so overlays join on it. The payee-key
file is internal (it carries raw name variants) and is never published.

Suppliers below the floor are rolled into a disclosed tail so coverage maths
stay honest. Nothing here is published directly; build_pound.py consumes it.

Run (banks needs duckdb, which the system python does not have):
  uv run --no-project --with 'duckdb>=1.4,<1.5' python scripts/observatory/aggregate_spend.py
  uv run --no-project --with 'duckdb>=1.4,<1.5' python scripts/observatory/aggregate_spend.py --source legacy
"""
import argparse
import csv
import gc
import gzip
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

BODIES = ["blackburn", "blackpool", "burnley", "fylde", "hyndburn",
          "lancashire_cc", "lancaster", "pendle", "preston", "ribble_valley",
          "rossendale", "south_ribble", "west_lancashire", "wyre"]
NOT_COVERED = {
    "chorley": "No bank published (manifest HTTP 404, 4 October 2026). "
               "Not yet covered (RULES.md decision 10).",
    "lancashire_fire": "No bank published (manifest HTTP 404, 4 October "
                       "2026). Not yet covered (RULES.md decision 10).",
    "lancashire_pcc": "Bank ends January 2025 (its newest file covers April "
                      "2024 to January 2025). Not yet covered (RULES.md "
                      "decision 10).",
}
YEARS = ["2024-25", "2025-26"]
FLOOR = 10_000.0  # per-supplier total over the window below this rolls into the tail

# One key for every payment the bank classes as made to an individual. The
# name on such a row is never used. resolve_suppliers.classify() reads any key
# starting REDACT as an excluded payee.
WITHHELD_KEY = "REDACTED: PAYMENTS TO INDIVIDUALS"
WITHHELD_RAW = {"REDACTED", "REDACT"}
NAMELESS_KEY = "(NO SUPPLIER TEXT)"

# Bank rows that are not supplier spend. Disclosed per body, never dropped
# silently. Commitments are outside the bank's own net (is_commitment below).
EXCLUDE_RECORD_CLASS = {"internal_transfer", "settlement_noise"}
EXCLUDE_TYPE = {"purchase_cards", "purchase_card"}
# RC03: payments, counted by supplier, shown on their own line.
SEPARATE_LINE_RECORD_CLASS = {
    "accrual_journal": "Payments the County reported against its accrual "
                       "accounts (RULES.md decision 9, rule RC03)",
}

HERE = Path(__file__).resolve().parent
VAT_CSV = HERE / "body_vat_threshold.csv"
DEFAULT_OUT = Path.home() / "observatory-data/processed/council_supplier_spend.json"
DEFAULT_LEGACY = Path.home() / "clawd/burnley-council/data"
DEFAULT_BANK_DIR = Path.home() / "observatory-data/raw/aidoge-banks"
DEFAULT_BANK_BASE = "https://data.aidoge.co.uk/v1"
UA = "LancashireBusinessObservatory/1.0 (research; tompickup.co.uk)"


def fy_label(fy):
    """'2024-25' -> '2024/25' (the bank's financial_year form)."""
    return fy.replace("-", "/")


def pence(amount):
    """Integer pence. The banks store amounts as DOUBLE with at most two
    decimals; a value that is not a whole number of pence stops the run."""
    if amount is None:
        return 0
    p = round(amount * 100)
    if abs(amount * 100 - p) > 1e-4:
        raise SystemExit(f"amount {amount!r} is not a whole number of pence")
    return p


def pounds(p):
    return round(p / 100, 2)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def is_commitment(rc, typ):
    """The publisher's definition (publish_data_r2.py@087a1aa4 L271 to 279)."""
    return rc == "procurement_committed" if rc else typ == "contracts"


# ------------------------------------------------------- threshold and VAT --
def threshold_pounds(stated):
    """250 or 500 when the stated wording names exactly one of them, else
    None (the wording is kept and the figure left blank, never guessed)."""
    s = (stated or "").replace(",", "")
    has250, has500 = "£250" in s, "£500" in s
    if has250 and not has500:
        return 250
    if has500 and not has250:
        return 500
    return None


def load_vat_threshold(path):
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            stated = r["threshold_stated"].strip()
            out[r["body"]] = {
                "threshold": {
                    "pounds": threshold_pounds(stated),
                    "stated": stated or None,
                    "source": r["page_url"] or None,
                    "note": (None if threshold_pounds(stated) else
                             "Threshold not established from the council's "
                             "own page or file; left blank."),
                },
                "vatBasis": {
                    "basis": r["vat_basis"] or None,
                    "evidence": r["vat_basis_source"] or None,
                    "pageWording": r["vat_wording_page"] or None,
                    "fileWording": r["vat_wording_file"] or None,
                    "note": r["note"] or None,
                },
            }
    return out


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
    subprocess.run(["curl", "-sf", "--max-time", "300", "-A", UA,
                    "-o", str(tmp), url], check=True)
    tmp.rename(dest)


def _http_code(url):
    return subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-A", UA,
         "--max-time", "60", url],
        capture_output=True, text=True).stdout.strip()


def bank_manifest(body, base, bank_dir, offline):
    """The body's manifest. Every body in BODIES has a bank, so a 404, any
    other HTTP failure, or a missing cached manifest stops the run: there is
    no silent fallback to the legacy files (REVIEW_PR31.md patch B)."""
    dest = bank_dir / body / "manifest.json"
    if not offline:
        url = f"{base}/{body}/manifest.json"
        code = _http_code(url)
        if code != "200":
            raise SystemExit(f"{body}: manifest HTTP {code or 'no response'} "
                             f"at {url}; stopping (no legacy fallback)")
        _fetch(url, dest)
    if not dest.exists():
        raise SystemExit(f"{body}: no cached manifest at {dest}; rerun "
                         f"without --offline")
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
        return dest.exists() and sha256_file(dest) == entry.get("sha256")
    if not ok():
        if offline:
            raise SystemExit(f"{body} {fy}: cached file missing or stale, "
                             f"rerun without --offline")
        root = base.rsplit("/v1", 1)[0]
        _fetch(f"{root}/{entry['key']}", dest)
        if not ok():
            raise SystemExit(f"{body} {fy}: sha256 mismatch against manifest")
    return dest, entry


def new_ledger():
    return {
        "bank_net": defaultdict(int),
        "excluded": defaultdict(lambda: defaultdict(int)),
        "excluded_rows": defaultdict(lambda: defaultdict(int)),
        "commit": defaultdict(lambda: [0, 0]),
        "separate": defaultdict(lambda: defaultdict(lambda: [0, 0])),
        "null_amount_rows": defaultdict(int),
        "files": {},
    }


def read_bank(body, manifest, base, bank_dir, offline, led):
    import duckdb
    con = duckdb.connect()
    found = []

    def gen():
        for fy in YEARS:
            path, entry = bank_year_file(body, manifest, fy, base, bank_dir,
                                         offline)
            if path is None:
                print(f"  WARNING {body} {fy}: no Parquet file in the "
                      f"manifest; year not read")
                continue
            found.append(fy)
            cols = {r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}

            def col(c):
                return f'"{c}"' if c in cols else "NULL"
            q = (f"SELECT amount, supplier, {col('supplier_canonical')}, "
                 f"{col('record_class')}, {col('type')}, "
                 f"{col('company_number')}, {col('supplier_company_number')}, "
                 f"{col('charity_number')} "
                 f"FROM read_parquet('{path}')")
            rows_read = 0
            for amt, sup, canon, rc, typ, cn, scn, chn in con.execute(q).fetchall():
                rows_read += 1
                if amt is None:
                    led["null_amount_rows"][fy] += 1
                p = pence(amt)
                if is_commitment(rc, typ):
                    led["commit"][fy][0] += 1
                    led["commit"][fy][1] += p
                    continue  # outside the bank's own net
                led["bank_net"][fy] += p
                if rc in EXCLUDE_RECORD_CLASS:
                    led["excluded"][fy][f"record_class:{rc}"] += p
                    led["excluded_rows"][fy][f"record_class:{rc}"] += 1
                    continue
                if typ in EXCLUDE_TYPE:
                    led["excluded"][fy][f"type:{typ}"] += p
                    led["excluded_rows"][fy][f"type:{typ}"] += 1
                    continue
                if rc in SEPARATE_LINE_RECORD_CLASS:
                    led["separate"][fy][rc][0] += 1
                    led["separate"][fy][rc][1] += p
                yield fy, {"pence": p, "supplier": sup,
                           "supplier_canonical": canon, "record_class": rc,
                           "company_number": (cn or scn or "").strip() or None,
                           "charity_number": (chn or "").strip() or None}
            led["files"][fy] = {"key": entry["key"], "sha256": entry["sha256"],
                                "bytes": entry.get("bytes"),
                                "manifestRows": entry.get("rows"),
                                "rowsRead": rows_read}
            if entry.get("rows") is not None and rows_read != entry["rows"]:
                raise SystemExit(f"{body} {fy}: read {rows_read} rows, "
                                 f"manifest says {entry['rows']}")
    return gen(), found


# ------------------------------------------------------------- aggregate --
def payee_key(r):
    """The fuel register's key: upper(strip(supplier_canonical or supplier))."""
    return (r.get("supplier_canonical") or r.get("supplier") or "").strip().upper()


def aggregate(rows, withhold, keys_out=None):
    agg = defaultdict(int)
    by_year = defaultdict(lambda: {"pence": 0, "transactions": 0})
    ids = defaultdict(lambda: {"cn": Counter(), "canon": Counter()})
    nameless = defaultdict(lambda: [0, 0])
    withheld = defaultdict(lambda: [0, 0])
    total, ntx = 0, 0
    for fy, r in rows:
        p = r["pence"] if "pence" in r else pence(r.get("amount"))
        raw = (r.get("supplier") or "").strip().upper()
        if withhold and (r.get("record_class") == "direct_payment_individual"
                         or raw in WITHHELD_RAW):
            name = WITHHELD_KEY
            withheld[fy][0] += 1
            withheld[fy][1] += p
            pk = WITHHELD_KEY
        else:
            name = (r.get("supplier_canonical") or r.get("supplier") or "").strip()
            if not name:
                name = NAMELESS_KEY
                nameless[fy][0] += 1
                nameless[fy][1] += p
                pk = NAMELESS_KEY
            else:
                pk = payee_key(r)
                canon = (r.get("supplier_canonical") or "").strip()
                if canon:
                    ids[name]["canon"][canon] += 1
                cn = (r.get("company_number") or r.get("supplier_company_number")
                      or "")
                cn = cn.strip() if isinstance(cn, str) else ""
                if cn:
                    ids[name]["cn"][cn] += 1
        if keys_out is not None:
            k = keys_out[(fy, pk)]
            k["pence"] += p
            k["rows"] += 1
            if pk not in (WITHHELD_KEY, NAMELESS_KEY):
                k["raw"][(r.get("supplier") or "").strip()] += 1
                if r.get("supplier_canonical"):
                    k["canon"][r["supplier_canonical"].strip()] += 1
                if r.get("company_number"):
                    k["cn"][r["company_number"]][0] += 1
                    k["cn"][r["company_number"]][1] += p
                if r.get("charity_number"):
                    k["chn"][r["charity_number"]][0] += 1
                    k["chn"][r["charity_number"]][1] += p
            k["rc"][r.get("record_class") or "(none)"] += p
        agg[name] += p
        total += p
        ntx += 1
        by_year[fy]["pence"] += p
        by_year[fy]["transactions"] += 1
    return agg, by_year, ids, total, ntx, nameless, withheld


def new_key():
    return {"pence": 0, "rows": 0, "raw": Counter(), "canon": Counter(),
            "cn": defaultdict(lambda: [0, 0]), "chn": defaultdict(lambda: [0, 0]),
            "rc": Counter()}


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


def lines(d):
    return {fy: {"rows": v[0], "value": pounds(v[1])}
            for fy, v in sorted(d.items())}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--source", choices=["banks", "legacy"], default="banks")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--payee-keys-out", type=Path, default=None,
                    help="banks: also write one JSON line per (body, year, "
                         "payee key) for the waterfall (internal; gzip if "
                         "the name ends .gz)")
    ap.add_argument("--legacy-dir", type=Path, default=DEFAULT_LEGACY)
    ap.add_argument("--bank-dir", type=Path, default=DEFAULT_BANK_DIR)
    ap.add_argument("--bank-base", default=DEFAULT_BANK_BASE)
    ap.add_argument("--vat-csv", type=Path, default=VAT_CSV)
    ap.add_argument("--offline", action="store_true",
                    help="banks: use the cache only, fetch nothing")
    a = ap.parse_args()

    vat = load_vat_threshold(a.vat_csv)
    missing = [b for b in BODIES if b not in vat]
    if missing:
        raise SystemExit(f"{a.vat_csv}: no row for {missing}")

    result, key_lines = {}, []
    for body in BODIES:
        source, prov, m = a.source, None, None
        led = new_ledger()
        keys_out = defaultdict(new_key) if a.payee_keys_out else None
        if a.source == "banks":
            m = bank_manifest(body, a.bank_base, a.bank_dir, a.offline)
            prov = {"manifest": f"{a.bank_base}/{body}/manifest.json",
                    "generated": m.get("generated"),
                    "generator_git_sha": m.get("generator_git_sha"),
                    "presentation": m.get("presentation"),
                    "sqlite_sha256": (m.get("sqlite") or {}).get("sha256")}
            rows, found = read_bank(body, m, a.bank_base, a.bank_dir,
                                    a.offline, led)
        else:
            rows, found = read_legacy(body, a.legacy_dir)
        agg, by_year, ids, total, ntx, nameless, withheld = aggregate(
            rows, source == "banks", keys_out)
        kept = {k: pounds(v) for k, v in agg.items() if v >= FLOOR * 100}
        tail_p = sum(v for v in agg.values() if v < FLOOR * 100)
        entry = {
            "years": found, "total": pounds(total), "transactions": ntx,
            "suppliers": dict(sorted(kept.items(), key=lambda kv: -kv[1])),
            "tail": {"value": pounds(tail_p), "suppliers": len(agg) - len(kept)},
            "source": source,
            "byYear": {fy: {"total": pounds(v["pence"]),
                            "transactions": v["transactions"]}
                       for fy, v in sorted(by_year.items())},
            "supplierIds": {k: supplier_ids(k, ids) for k in kept},
            "threshold": vat[body]["threshold"],
            "vatBasis": vat[body]["vatBasis"],
        }
        if source == "banks":
            entry["bank"] = dict(prov, files=led["files"])
            entry["bankNet"] = {fy: pounds(v) for fy, v in sorted(led["bank_net"].items())}
            entry["excludedFromSuppliers"] = {
                fy: {k: pounds(v) for k, v in sorted(ex.items())}
                for fy, ex in sorted(led["excluded"].items())}
            entry["excludedRows"] = {
                fy: dict(sorted(ex.items()))
                for fy, ex in sorted(led["excluded_rows"].items())}
            entry["commitments"] = {
                "definition": "record_class procurement_committed, or no "
                              "record_class and type contracts (the "
                              "publisher's is_commitment); outside the bank net",
                "byYear": lines(led["commit"])}
            entry["separateLines"] = {
                rc: {"label": label,
                     "byYear": {fy: {"rows": led["separate"][fy][rc][0],
                                     "value": pounds(led["separate"][fy][rc][1])}
                                for fy in found}}
                for rc, label in SEPARATE_LINE_RECORD_CLASS.items()}
            entry["withheld"] = {"key": WITHHELD_KEY, "byYear": lines(withheld)}
            entry["nameless"] = {"key": NAMELESS_KEY, "byYear": lines(nameless)}
            entry["nullAmountRows"] = dict(sorted(led["null_amount_rows"].items()))
            # Value conservation to the penny: the bank's own net for each
            # year read must be reproduced from its rows, and the suppliers
            # total plus the exclusions must equal it.
            cons = {}
            for fy in found:
                want = ((m.get("years") or {}).get(fy_label(fy)) or {}).get("net")
                if want is None:
                    raise SystemExit(f"{body} {fy}: manifest has no net; "
                                     f"conservation cannot be checked")
                net_p = led["bank_net"][fy]
                ex_p = sum(led["excluded"][fy].values())
                sup_p = by_year[fy]["pence"]
                if net_p != round(want * 100):
                    raise SystemExit(f"{body} {fy}: rows sum to {pounds(net_p)}, "
                                     f"manifest net is {want:.2f}")
                if sup_p + ex_p != net_p:
                    raise SystemExit(f"{body} {fy}: suppliers {pounds(sup_p)} "
                                     f"plus exclusions {pounds(ex_p)} is not "
                                     f"the bank net {pounds(net_p)}")
                cons[fy] = {"manifestNet": round(want, 2),
                            "rowsNet": pounds(net_p),
                            "suppliersTotal": pounds(sup_p),
                            "excluded": pounds(ex_p),
                            "differencePence": net_p - sup_p - ex_p}
            entry["conservation"] = cons
        if keys_out is not None:
            for (fy, pk), k in sorted(keys_out.items()):
                key_lines.append({
                    "body_id": body, "financial_year": fy_label(fy),
                    "payee_key": pk, "value_pence": k["pence"],
                    "rows": k["rows"],
                    "raw_names": dict(k["raw"].most_common()),
                    "supplier_canonical": dict(k["canon"].most_common()),
                    "bank_company_numbers": {c: {"rows": v[0], "value_pence": v[1]}
                                             for c, v in k["cn"].items()},
                    "bank_charity_numbers": {c: {"rows": v[0], "value_pence": v[1]}
                                             for c, v in k["chn"].items()},
                    "record_class_pence": dict(k["rc"]),
                })
        result[body] = entry
        print(f"{body} [{source}]: £{total/1e8:.1f}m over {found}, "
              f"{len(kept)} suppliers >= £10k (tail £{tail_p/1e8:.2f}m / "
              f"{len(agg)-len(kept)})")

    if a.source == "banks":
        src = ("AI DOGE reconciled payment banks (data.aidoge.co.uk/v1, "
               "per-year Parquet), bank net less internal transfers, "
               "settlement noise and purchase cards")
        note = ("Transparency payments at or above each body's own "
                "publication threshold (£250 or £500), on each body's own VAT "
                "basis; see threshold and vatBasis per body. Not total budget. "
                "Spend under each threshold is not published and is unknown "
                "in size. Commitments are outside the bank net; internal "
                "transfers, settlement noise and purchase cards are excluded "
                "and disclosed per body. County accrual-coded payments are "
                "counted and shown on their own line (rule RC03). Payments to "
                "individuals are counted under one withheld key.")
    else:
        src = "AI DOGE per-council transparency spend files, type=spend only"
        note = ("Transparency payments at or above each body's own "
                "publication threshold (£250 or £500), on each body's own VAT "
                "basis; see threshold and vatBasis per body. Contracts and "
                "purchase cards excluded (double-count / merchant-level).")
    out = {"$meta": {"source": src, "note": note, "floor": FLOOR,
                     "years": YEARS, "input": a.source,
                     "notCovered": NOT_COVERED,
                     "vatThresholdFile": {"path": a.vat_csv.name,
                                          "sha256": sha256_file(a.vat_csv)}},
           "bodies": result}
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(out))
    print("written", a.out)
    if a.payee_keys_out:
        a.payee_keys_out.parent.mkdir(parents=True, exist_ok=True)
        opener = gzip.open if a.payee_keys_out.suffix == ".gz" else open
        with opener(a.payee_keys_out, "wt") as fh:
            for line in key_lines:
                fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        print("written", a.payee_keys_out, f"({len(key_lines)} lines)")


if __name__ == "__main__":
    main()
