#!/usr/bin/env python3
"""Unit test for the SCD2 builder on a synthetic three-snapshot fixture.

The fixture is deliberately tiny and hand-checkable. Six companies, each one
exercising exactly one behaviour, so a failure names the behaviour that broke:

  00000001 CONSTANT      same in all three snapshots      -> 1 version, baseline
  00000002 RENAMED       name changes at snapshot 2       -> 2 versions
  00000003 DISSOLVING    status changes then row leaves   -> 2 versions, gone
  00000004 NEWCO         absent at 1, appears at 2        -> 1 version, new
  00000005 FLICKER       present, absent, present again   -> 2 versions
  00000006 TWICE CHANGED changes at 2 and again at 3      -> 3 versions

Run: python3 test_entity_history.py   (exit 0 = green, prints each assertion)
"""
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_entity_history as BEH  # noqa: E402
import marts as M  # noqa: E402

SNAPS = ["2026-01-01", "2026-02-01", "2026-03-01"]

# (crn, name, status, category, entity_type, postcode, acct_cat, dissolution,
#  sic, strike_off, incorporation)
def row(crn, name, status="Active", dissolution=None, strike=False):
    return dict(company_number=crn, company_name=name, company_status=status,
                company_category="Private Limited Company",
                entity_type="ltd", reg_postcode_norm="BB111AA",
                accounts_category="MICRO ENTITY", dissolution_date=dissolution,
                sic_text_1="62020 - Computer consultancy",
                proposed_strike_off=strike, incorporation_date="2010-05-04",
                companies_act_body=True)


FIXTURE = {
    SNAPS[0]: [
        row("00000001", "CONSTANT LTD"),
        row("00000002", "RENAMED LTD"),
        row("00000003", "DISSOLVING LTD"),
        row("00000005", "FLICKER LTD"),
        row("00000006", "TWICE CHANGED LTD"),
    ],
    SNAPS[1]: [
        row("00000001", "CONSTANT LTD"),
        row("00000002", "RENAMED HOLDINGS LTD"),
        row("00000003", "DISSOLVING LTD", status="Liquidation"),
        row("00000004", "NEWCO LTD"),
        row("00000006", "TWICE CHANGED LTD", strike=True),
    ],
    SNAPS[2]: [
        row("00000001", "CONSTANT LTD"),
        row("00000002", "RENAMED HOLDINGS LTD"),
        row("00000004", "NEWCO LTD"),
        row("00000005", "FLICKER LTD"),
        row("00000006", "TWICE CHANGED LTD", status="Dissolved", strike=True),
    ],
}

FAILURES = []


def check(name, got, want):
    ok = got == want
    print(f"  {'ok  ' if ok else 'FAIL'} {name}: {got!r}" +
          ("" if ok else f" (want {want!r})"))
    if not ok:
        FAILURES.append(name)


def main():
    tmp = Path(tempfile.mkdtemp(prefix="entity-history-test-"))
    try:
        con, ver = M.connect()
        parts = []
        for snap, rows in FIXTURE.items():
            d = tmp / f"snapshot_date={snap}"
            d.mkdir(parents=True)
            f = d / "part.parquet"
            cols = list(rows[0])
            values = ",\n".join(
                "(" + ", ".join(
                    "NULL" if r[c] is None else
                    ("true" if r[c] is True else "false") if isinstance(r[c], bool)
                    else ("DATE '%s'" % r[c] if c in ("dissolution_date",
                                                      "incorporation_date")
                          else "'%s'" % r[c])
                    for c in cols) + ")"
                for r in rows)
            con.execute(
                f"COPY (SELECT * FROM (VALUES\n{values}) "
                f"AS t({', '.join(cols)})) TO '{f}' (FORMAT PARQUET)")
            parts.append((snap, f))

        out = tmp / "out"
        out.mkdir()
        rows_out, _ = BEH.build(con, parts, out)
        f = out / "part.parquet"
        got = {}
        for r in con.execute(
                f"SELECT company_number, version_no, CAST(valid_from AS VARCHAR), "
                f"CAST(valid_to AS VARCHAR), change_type, gone_from_register, "
                f"snapshots_observed, company_name, company_status "
                f"FROM read_parquet('{f}') ORDER BY 1, 2").fetchall():
            got.setdefault(r[0], []).append(r[1:])

        print("entity_history synthetic fixture")
        check("total versions", rows_out, 11)

        # 1. unchanged company: one open baseline version seen three times
        check("00000001 versions", len(got["00000001"]), 1)
        check("00000001 v1", got["00000001"][0][:6],
              (1, SNAPS[0], None, "baseline", False, 3))

        # 2. rename: baseline closes exactly where version 2 opens
        check("00000002 versions", len(got["00000002"]), 2)
        check("00000002 v1", got["00000002"][0][:6],
              (1, SNAPS[0], SNAPS[1], "baseline", False, 1))
        check("00000002 v2 open", got["00000002"][1][:6],
              (2, SNAPS[1], None, "change", False, 2))
        check("00000002 v2 name", got["00000002"][1][6], "RENAMED HOLDINGS LTD")

        # 3. status change then the row leaves the file: the last version is
        #    closed and flagged gone, which is a fact about the file
        check("00000003 versions", len(got["00000003"]), 2)
        check("00000003 v2 gone", got["00000003"][1][:6],
              (2, SNAPS[1], SNAPS[2], "change", True, 1))

        # 4. appears in snapshot 2: 'new', never 'baseline'
        check("00000004 versions", len(got["00000004"]), 1)
        check("00000004 v1", got["00000004"][0][:6],
              (1, SNAPS[1], None, "new", False, 2))

        # 5. flicker: absent for one snapshot then back. A gap is not
        #    stability, so the return is a fresh version even though nothing
        #    about the company changed (DATA-INTEGRITY s4 rule 5).
        check("00000005 versions", len(got["00000005"]), 2)
        check("00000005 v1 gone", got["00000005"][0][:6],
              (1, SNAPS[0], SNAPS[1], "baseline", True, 1))
        check("00000005 v2 returned", got["00000005"][1][:6],
              (2, SNAPS[2], None, "returned", False, 1))

        # 6. two changes: three contiguous versions, no hole
        check("00000006 versions", len(got["00000006"]), 3)
        check("00000006 chain",
              [v[:5] for v in got["00000006"]],
              [(1, SNAPS[0], SNAPS[1], "baseline", False),
               (2, SNAPS[1], SNAPS[2], "change", False),
               (3, SNAPS[2], None, "change", False)])
        check("00000006 v3 status", got["00000006"][2][7], "Dissolved")

        # contiguity invariant, the same one the builder asserts
        holes = con.execute(f"""
          SELECT count(*) FROM (
            SELECT company_number, valid_to, gone_from_register,
                   lead(valid_from) OVER (PARTITION BY company_number
                                          ORDER BY version_no) AS nxt
            FROM read_parquet('{f}'))
          WHERE nxt IS NOT NULL AND NOT gone_from_register
            AND valid_to IS DISTINCT FROM nxt""").fetchone()[0]
        check("interval holes (excluding real register gaps)", holes, 0)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    if FAILURES:
        print(f"\n{len(FAILURES)} FAILED: {FAILURES}")
        sys.exit(1)
    print("\nall entity_history fixture assertions green")


if __name__ == "__main__":
    main()
