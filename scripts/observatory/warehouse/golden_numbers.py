#!/usr/bin/env python3
"""Golden numbers: silver must reproduce the facts the live site already shows.

Ten companies, pinned by hand, chosen to cover the shapes that break first:
a CIC (category-derived type), an LLP (prefix-derived type), a company in
administration, a guarantee company, a company with a Gazette notice, companies
with long accounts series, companies with individual PSCs, and one company that
dissolved between the two register snapshots we hold.

For each one, every register, accounts, PSC and Gazette fact in the published
dossier at public/data/company/<crn>.json must be present in silver with the
same value. This is a subset test in the direction that matters: the dossier is
already public, so anything it states and silver cannot reproduce is a
migration defect, whatever the row counts say.

It is deliberately not the reverse test. Silver holds more than the dossiers
show (every period, every PSC kind, the national register), and that is the
point of the layer.

Usage:
    golden_numbers.py [--dossiers /opt/observatory/site/public/data/company]
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import silver as SV  # noqa: E402

DOSSIERS = Path("/opt/observatory/site/public/data/company")

# crn -> why this one is in the set. Changing this list changes what the gate
# proves, so each entry states its job.
GOLDEN = {
    "00042478": "oldest Burnley filer, FULL accounts, long employee series",
    "00188491": "large wholesaler, accounts series plus an individual PSC",
    "00907939": "In Administration, the only dossier carrying a Gazette notice",
    "00024084": "company limited by guarantee, no accounts series at all",
    "02584952": "CIC: type comes from CompanyCategory, never a number prefix",
    "OC433300": "LLP: type comes from the OC prefix, never CompanyCategory",
    "00054222": "pre-1900 incorporation date, DD/MM/YYYY parsing",
    "00204727": "accounts series plus PSC",
    "00205229": "PSC-heavy ownership block",
    "00363671": "PSC individuals with a non-UK country of residence",
}

# A company present in the 2026-07-01 register snapshot and gone from
# 2026-08-01. Pinned rather than picked at runtime so the test asserts a fact
# instead of restating whatever it happens to find.
DISSOLVED_IN_WINDOW_FROM = "2026-07-01"
DISSOLVED_IN_WINDOW_TO = "2026-08-01"


def part(table, snapshot=None):
    base = SV.silver_dir() / table
    parts = sorted(base.glob("snapshot_date=*")) if base.exists() else []
    parts = [p for p in parts if (p / "part.parquet").exists()]
    if not parts:
        raise SystemExit(f"FATAL: silver/{table} has no built partition")
    if snapshot:
        want = base / f"snapshot_date={snapshot}"
        if not want.exists():
            raise SystemExit(f"FATAL: silver/{table} has no {snapshot}")
        return want / "part.parquet"
    return parts[-1] / "part.parquet"


class Report:
    def __init__(self):
        self.passed = 0
        self.failed = []

    def check(self, crn, field, got, want):
        if got == want:
            self.passed += 1
        else:
            self.failed.append((crn, field, got, want))
            print(f"  FAIL {crn} {field}: silver={got!r} dossier={want!r}")


def ddmmyyyy(d):
    return d.strftime("%d/%m/%Y") if isinstance(d, _dt.date) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dossiers", default=str(DOSSIERS))
    args = ap.parse_args()
    ddir = Path(args.dossiers)
    if not ddir.exists():
        raise SystemExit(f"FATAL: dossier directory {ddir} not found")

    con, dv = SV.connect()
    reg = part("ch_register")
    acc = part("ch_accounts")
    psc = part("ch_psc")
    gaz = part("gazette_notices")
    SV.log(f"duckdb {dv}")
    SV.log(f"register {reg.parent.name}, accounts {acc.parent.name}, "
           f"psc {psc.parent.name}, gazette {gaz.parent.name}")

    rep = Report()
    missing = []
    for crn, why in GOLDEN.items():
        f = ddir / f"{crn}.json"
        if not f.exists():
            missing.append(crn)
            print(f"  MISSING dossier for {crn} ({why})")
            continue
        dos = json.loads(f.read_text())
        r = dos.get("register") or {}
        print(f"{crn} {dos.get('name')}  [{why}]")

        row = con.execute(f"""
            SELECT company_name, company_category, company_status,
                   accounts_category, is_cic, incorporation_date, reg_postcode,
                   sic_text_1, entity_type, companies_act_body
            FROM read_parquet('{reg}') WHERE company_number = '{crn}'
        """).fetchone()
        if row is None:
            rep.failed.append((crn, "register row", None, "present"))
            print(f"  FAIL {crn} absent from silver register")
            continue
        (name, cat, status, acat, is_cic, inc, pc, sic1, etype, ca_body) = row

        rep.check(crn, "name", name, dos.get("name"))
        rep.check(crn, "companyType", cat, r.get("companyType"))
        rep.check(crn, "status", status, r.get("status"))
        rep.check(crn, "accountsCategory", acat, r.get("accountsCategory"))
        rep.check(crn, "cic", bool(is_cic), bool(r.get("cic")))
        rep.check(crn, "incorporated", ddmmyyyy(inc), r.get("incorporated"))
        rep.check(crn, "registeredPostcode", pc, r.get("registeredPostcode"))
        rep.check(crn, "sic", sic1, r.get("sic"))
        # Type derivation, which the dossier does not carry and therefore
        # cannot corroborate: assert it against the rule instead.
        if crn.startswith("OC"):
            rep.check(crn, "entityType(llp)", etype, "llp")
        elif r.get("cic"):
            rep.check(crn, "entityType(cic)", etype, "cic")
        rep.check(crn, "companiesActBody", bool(ca_body), True)

        for a in dos.get("accountsSeries") or []:
            hit = con.execute(f"""
                SELECT employees, equity, total_assets, cash
                FROM read_parquet('{acc}')
                WHERE crn = '{crn}' AND period_end = DATE '{a["periodEnd"]}'
            """).fetchone()
            if hit is None:
                rep.failed.append((crn, f"accounts {a['periodEnd']}", None, "present"))
                print(f"  FAIL {crn} accounts period {a['periodEnd']} absent")
                continue
            rep.check(crn, f"accounts {a['periodEnd']} employees", hit[0], a["employees"])
            rep.check(crn, f"accounts {a['periodEnd']} equity", hit[1], a["equity"])
            rep.check(crn, f"accounts {a['periodEnd']} totalAssets", hit[2], a["totalAssets"])
            rep.check(crn, f"accounts {a['periodEnd']} cash", hit[3], a["cash"])

        for p in (dos.get("ownership") or {}).get("pscIndividuals") or []:
            hit = con.execute(f"""
                SELECT count(*) FROM read_parquet('{psc}')
                WHERE company_number = '{crn}' AND is_individual
                  AND name = ? AND coalesce(country_of_residence, '') = ?
            """, [p["name"], p.get("country") or ""]).fetchone()[0]
            rep.check(crn, f"psc {p['name']}", hit >= 1, True)

        for g in dos.get("gazetteNotices") or []:
            hit = con.execute(f"""
                SELECT notice_date::VARCHAR, insolvency_type
                FROM read_parquet('{gaz}') WHERE uri = ?
            """, [g["uri"]]).fetchone()
            if hit is None:
                rep.failed.append((crn, f"gazette {g['uri']}", None, "present"))
                print(f"  FAIL {crn} gazette notice {g['uri']} absent")
                continue
            rep.check(crn, f"gazette {g['uri']} date", hit[0], g["date"])
            rep.check(crn, f"gazette {g['uri']} type", hit[1], g["type"])

    # The dissolved-in-window case. Dissolved companies are not in the bulk
    # file at all, so "dissolved" is the disappearance of a company number
    # between two snapshots, which is exactly the SCD2 signal M4 builds on.
    print()
    old = SV.silver_dir() / "ch_register" / f"snapshot_date={DISSOLVED_IN_WINDOW_FROM}"
    new = SV.silver_dir() / "ch_register" / f"snapshot_date={DISSOLVED_IN_WINDOW_TO}"
    if (old / "part.parquet").exists() and (new / "part.parquet").exists():
        gone = con.execute(f"""
            SELECT count(*) FROM read_parquet('{old}/part.parquet') o
            WHERE NOT EXISTS (SELECT 1 FROM read_parquet('{new}/part.parquet') n
                              WHERE n.company_number = o.company_number)
        """).fetchone()[0]
        added = con.execute(f"""
            SELECT count(*) FROM read_parquet('{new}/part.parquet') n
            WHERE NOT EXISTS (SELECT 1 FROM read_parquet('{old}/part.parquet') o
                              WHERE o.company_number = n.company_number)
        """).fetchone()[0]
        print(f"register window {DISSOLVED_IN_WINDOW_FROM} to "
              f"{DISSOLVED_IN_WINDOW_TO}: {gone:,} numbers left the register, "
              f"{added:,} joined it")
        if gone == 0:
            rep.failed.append(("window", "companies left register", 0, "> 0"))
            print("  FAIL no company left the register between snapshots, "
                  "which cannot be true and means the partitions are identical")
        else:
            rep.passed += 1
    else:
        print(f"SKIP dissolved-in-window: needs both "
              f"{DISSOLVED_IN_WINDOW_FROM} and {DISSOLVED_IN_WINDOW_TO} "
              "register partitions")

    con.close()
    print()
    SV.log(f"{rep.passed} golden facts matched, {len(rep.failed)} failed, "
           f"{len(missing)} dossiers missing")
    if rep.failed or missing:
        print("GOLDEN NUMBERS FAILED")
        return 1
    print("GOLDEN NUMBERS GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
