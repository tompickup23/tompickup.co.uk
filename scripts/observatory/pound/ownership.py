#!/usr/bin/env python3
"""Ownership seed and walk for the Public Pound (method rules 2.1, 2.2, 2.6).

Seed (stored as time intervals, rule 2.6):
  * BODS UK, Open Ownership's Beneficial Ownership Data Standard v0.4 copy of
    the Companies House PSC register, published 5 March 2025 (bronze bods_uk;
    the edition's own date is 11 March 2025). One edge per relationship
    statement, with the voting-rights band (share_minimum, share_maximum) and
    the interest start and end dates. Persons are carried as a count and a
    country of residence (BODS taxResidencies) only: no name, no birth date.
  * GLEIF Level 2 relationship records (direct and ultimate accounting
    consolidation parents) and the reporting-exceptions file, 4 October 2026,
    for companies with an LEI whose Companies House chain ends at a foreign
    or unnamed parent.

Walk: in DuckDB, over company-to-company edges whose voting-rights band has a
lower bound of 50% or more (the Companies House bands are "more than 25% to
50%", "more than 50% to less than 75%" and "75% or more", so a lower bound of
50 means more than 50%; bands are never multiplied, rule 2.1), valid in the
payment year, to a unit nobody controls. Where no unit passes 50% the pound
goes to an explicit class (rule 2.2). Cycles are counted, never followed.

The BODS edition ends on 11 March 2025. For 2025/26 the walk uses edges open
on that date and says so; each parent edge is checked against the 8 September
2026 corporate PSC list (gold mart_psc_corporate), and an edge ceased there
during 2025/26 is flagged, not silently trusted.

Outputs in $POUND_WORK/out:
  bods_edges.parquet          the interval edges (built once by --build)
  ownership_walk.csv          one row per company and financial year
  ownership_manifest.json     inputs, counts, cycle count
"""
import argparse
import csv
import glob
import json
import zipfile
from pathlib import Path

import duckdb

from common import (BRONZE, EXTRACT, FY_DATES, GOLD, OUT, WORK, git_sha,
                    sha256_file, write_json)

BODS_DIR = WORK / "bods"
BODS_AS_AT = "2025-03-11"
EDGES = OUT / "bods_edges.parquet"
BODS_FILES = ["relationship_statement", "relationship_recorddetails_interests",
              "entity_statement", "entity_recorddetails_identifiers",
              "person_statement", "person_recorddetails_taxresidencies"]


def bods_zip():
    return sorted(glob.glob(str(BRONZE / "source=bods_uk/snapshot_date=*/*.zip")))[-1]


def build_edges(con):
    z = bods_zip()
    BODS_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(z) as zf:
        for f in BODS_FILES:
            if not (BODS_DIR / f"{f}.parquet").exists():
                zf.extract(f"{f}.parquet", BODS_DIR)
    for f in BODS_FILES:
        con.execute(f"CREATE OR REPLACE VIEW {f} AS SELECT * FROM '{BODS_DIR}/{f}.parquet'")
    con.execute(f"""
    COPY (
      WITH r AS (
        SELECT _link, statementId, recordId, recordStatus, statementDate,
               replace(declarationSubject, 'GB-COH-', '') AS subject_crn,
               recordDetails_interestedParty AS party,
               recordDetails_interestedParty_reason AS reason
        FROM relationship_statement),
      vr AS (
        SELECT _link_relationship_statement AS rl,
               max(share_minimum) FILTER (WHERE type = 'votingRights') AS voting_min,
               max(share_maximum) FILTER (WHERE type = 'votingRights') AS voting_max,
               max(share_minimum) FILTER (WHERE type = 'shareholding') AS shares_min,
               bool_or(type = 'appointmentOfBoard') AS appoints_board,
               bool_or(type = 'trustee') AS trustee,
               min(startDate) AS start_date, max(endDate) AS end_date
        FROM relationship_recorddetails_interests GROUP BY 1),
      ent AS (
        SELECT e.recordId, e.recordDetails_jurisdiction_code AS jurisdiction,
               e.recordDetails_publicListing_hasPublicListing AS listed,
               e.recordDetails_entityType_type AS entity_type,
               max(i.id) FILTER (WHERE i.scheme = 'GB-COH') AS coh,
               max(i.scheme) FILTER (WHERE i.scheme <> 'GB-COH') AS other_scheme,
               max(i.id) FILTER (WHERE i.scheme <> 'GB-COH') AS other_id
        FROM entity_statement e
        LEFT JOIN entity_recorddetails_identifiers i ON i._link_entity_statement = e._link
        GROUP BY 1, 2, 3, 4),
      per AS (
        SELECT p.recordId, min(t.code) AS residence
        FROM person_statement p
        LEFT JOIN person_recorddetails_taxresidencies t ON t._link_person_statement = p._link
        GROUP BY 1)
      SELECT r.statementId AS statement_id, r.recordStatus AS record_status,
             r.statementDate AS statement_date, r.subject_crn,
             CASE WHEN r.party IS NULL THEN 'statement'
                  WHEN r.party LIKE 'GB-COH-PER-%' THEN 'person'
                  ELSE 'entity' END AS party_kind,
             CASE WHEN r.party LIKE 'GB-COH-PER-%' THEN NULL
                  ELSE coalesce(ent.coh, CASE WHEN r.party LIKE 'GB-COH-%' AND r.party NOT LIKE 'GB-COH-ENT-%'
                                              THEN replace(r.party, 'GB-COH-', '') END) END AS parent_crn_raw,
             ent.other_scheme AS parent_scheme, ent.other_id AS parent_id,
             ent.jurisdiction AS parent_jurisdiction, ent.listed AS parent_listed,
             ent.entity_type AS parent_entity_type,
             per.residence AS person_residence,
             r.reason, vr.voting_min, vr.voting_max, vr.shares_min, vr.appoints_board, vr.trustee,
             TRY_CAST(vr.start_date AS DATE) AS start_date,
             coalesce(TRY_CAST(vr.end_date AS DATE),
                      CASE WHEN r.recordStatus = 'closed' THEN TRY_CAST(r.statementDate AS DATE) END) AS end_date
      FROM r
      LEFT JOIN vr ON vr.rl = r._link
      LEFT JOIN ent ON ent.recordId = r.party
      LEFT JOIN per ON per.recordId = r.party
    ) TO '{EDGES}.tmp.parquet' (FORMAT parquet)""")
    # Normalise the parent number (upper case, digits padded to 8) and keep it
    # only when it has a Companies House shape; otherwise it is an entity
    # without a usable register number (BODS carries some foreign numbers
    # under GB-COH, for example C0800-01-012662).
    con.execute(f"""COPY (
        SELECT * EXCLUDE (parent_crn_raw, n),
               CASE WHEN regexp_full_match(n, '^(\\d{{8}}|(SC|NI|OC|SO|NC|NF|FC|SL|SF|LP|R0|IP|RS|SP|CE|CS|GE|GS)\\d{{6}})$')
                    THEN n END AS parent_crn,
               parent_crn_raw
        FROM (SELECT *, CASE WHEN regexp_full_match(upper(trim(parent_crn_raw)), '\\d{{6,7}}')
                             THEN lpad(upper(trim(parent_crn_raw)), 8, '0')
                             ELSE upper(trim(parent_crn_raw)) END AS n
              FROM '{EDGES}.tmp.parquet')
        ) TO '{EDGES}' (FORMAT parquet)""")
    Path(f"{EDGES}.tmp.parquet").unlink()
    n = con.execute(f"SELECT count(*) FROM '{EDGES}'").fetchone()[0]
    return {"bods_zip": z, "bods_zip_sha256": sha256_file(z), "edges": n,
            "edges_sha256": sha256_file(EDGES)}


def gleif_l2(con):
    rr = sorted(glob.glob(str(BRONZE / "source=gleif_rr/snapshot_date=*/*.zip")))[-1]
    rx = sorted(glob.glob(str(BRONZE / "source=gleif_repex/snapshot_date=*/*.zip")))[-1]
    tmp = WORK / "tmp"
    tmp.mkdir(exist_ok=True)
    paths = []
    for z in (rr, rx):
        with zipfile.ZipFile(z) as zf:
            n = zf.namelist()[0]
            if not (tmp / n).exists():
                zf.extract(n, tmp)
            paths.append(tmp / n)
    con.execute(f"""CREATE OR REPLACE TABLE gl2 AS
        SELECT "Relationship.StartNode.NodeID" AS child_lei, "Relationship.EndNode.NodeID" AS parent_lei,
               "Relationship.RelationshipType" AS rel_type, "Relationship.RelationshipStatus" AS rel_status,
               TRY_CAST(substr("Relationship.Period.1.startDate", 1, 10) AS DATE) AS start_date,
               TRY_CAST(substr("Relationship.Period.1.endDate", 1, 10) AS DATE) AS end_date
        FROM read_csv('{paths[0]}', header=true, all_varchar=true)
        WHERE "Relationship.RelationshipType" IN ('IS_DIRECTLY_CONSOLIDATED_BY', 'IS_ULTIMATELY_CONSOLIDATED_BY')""")
    con.execute(f"""CREATE OR REPLACE TABLE grx AS
        SELECT "LEI" AS lei, "Exception.Category" AS category, "Exception.Reason.1" AS reason
        FROM read_csv('{paths[1]}', header=true, all_varchar=true)""")
    con.execute(f"""CREATE OR REPLACE TABLE glei AS
        SELECT DISTINCT lei, ra_entity_id, jurisdiction, ra_id FROM '{EXTRACT}/gleif.parquet'""")
    return {"gleif_rr": rr, "gleif_rr_sha256": sha256_file(rr),
            "gleif_repex": rx, "gleif_repex_sha256": sha256_file(rx)}


def walk(con, start_crns, years):
    """Recursive walk per financial year. Returns rows for ownership_walk.csv."""
    import pyarrow as pa
    con.register("_starts", pa.table({"crn": pa.array(sorted(start_crns), type=pa.string())}))
    con.execute("CREATE OR REPLACE TEMP TABLE starts AS SELECT * FROM _starts")
    mart = sorted((GOLD / "mart_psc_corporate").glob("snapshot_date=*/part.parquet"))[-1]
    con.execute(f"""CREATE OR REPLACE TEMP TABLE psc_now AS
        SELECT company_number, registration_number, ceased_on, active FROM read_parquet('{mart}')""")
    out = []
    cycles_total = 0
    for fy in years:
        fs, fe = FY_DATES[fy]
        # The edition ends 11 March 2025: an edge counts for the year when its
        # interval overlaps the year, read on the edges as recorded at that date.
        view_end = min(fe, BODS_AS_AT)
        con.execute(f"""CREATE OR REPLACE TABLE e AS
            SELECT * FROM '{EDGES}'
            WHERE (start_date IS NULL OR start_date <= DATE '{view_end}')
              AND (end_date IS NULL OR end_date >= DATE '{fs}')""")
        con.execute("""CREATE OR REPLACE TEMP TABLE ctl AS
            SELECT subject_crn, parent_crn, voting_min, voting_max, statement_id
            FROM e WHERE party_kind = 'entity' AND parent_crn IS NOT NULL
                     AND voting_min >= 50 AND parent_crn <> subject_crn""")
        rows = con.execute("""
            WITH RECURSIVE w(start_crn, crn, depth, path, cyc) AS (
              SELECT crn, crn, 0, [crn], false FROM starts
              UNION ALL
              SELECT w.start_crn, c.parent_crn, w.depth + 1, list_append(w.path, c.parent_crn),
                     list_contains(w.path, c.parent_crn)
              FROM w JOIN ctl c ON c.subject_crn = w.crn
              WHERE NOT w.cyc AND w.depth < 12)
            SELECT start_crn, crn, depth, path, cyc FROM w""").fetchall()
        by_start = {}
        for s, crn, depth, path, cyc in rows:
            cur = by_start.get(s)
            if cyc:
                by_start.setdefault(("cyc", s), True)
            if cur is None or depth > cur[1]:
                by_start[s] = (crn, depth, path)
        ncyc = sum(1 for k in by_start if isinstance(k, tuple))
        cycles_total += ncyc
        tops = {s: by_start.get(s, (s, 0, [s])) for s in start_crns}
        con.register("_tops", pa.table({"crn": pa.array(sorted({v[0] for v in tops.values()}), type=pa.string())}))
        con.execute("CREATE OR REPLACE TEMP TABLE tops AS SELECT * FROM _tops")
        above_all = {}
        for r in con.execute("""SELECT e.subject_crn, party_kind, parent_crn, parent_scheme, parent_jurisdiction,
                                       parent_listed, parent_entity_type, person_residence, reason, voting_min,
                                       voting_max, trustee, statement_id
                                FROM e JOIN tops ON e.subject_crn = tops.crn""").fetchall():
            above_all.setdefault(r[0], []).append(r[1:])
        nparents = dict(con.execute("""SELECT subject_crn, count(DISTINCT parent_crn) FROM ctl
                                       JOIN tops ON ctl.subject_crn = tops.crn GROUP BY 1""").fetchall())
        gl_all = dict((r[0], r[1:]) for r in con.execute("""
            SELECT gl.ra_entity_id, g2.parent_lei, p.jurisdiction
            FROM glei gl JOIN tops ON gl.ra_entity_id = tops.crn AND gl.ra_id = 'RA000585'
            JOIN gl2 g2 ON g2.child_lei = gl.lei AND g2.rel_type = 'IS_ULTIMATELY_CONSOLIDATED_BY'
                       AND g2.rel_status = 'ACTIVE'
            LEFT JOIN (SELECT lei, any_value(jurisdiction) AS jurisdiction FROM glei GROUP BY 1) p
                   ON p.lei = g2.parent_lei""").fetchall())
        ceased = {}
        if fy == "2025/26":
            pairs = {(a, b) for (t, d, path) in tops.values() for a, b in zip(path[:-1], path[1:])}
            pl = sorted(pairs)
            con.register("_pairs", pa.table({"a": pa.array([x[0] for x in pl], type=pa.string()),
                                             "b": pa.array([x[1] for x in pl], type=pa.string())}))
            con.execute("CREATE OR REPLACE TEMP TABLE pairs AS SELECT * FROM _pairs")
            for a, b, c in con.execute("""SELECT pairs.a, pairs.b, min(ceased_on) FROM pairs JOIN psc_now p
                                          ON p.company_number = pairs.a
                                          AND regexp_replace(upper(p.registration_number), '\\s', '', 'g')
                                              IN (pairs.b, ltrim(pairs.b, '0'))
                                          WHERE ceased_on IS NOT NULL GROUP BY 1, 2""").fetchall():
                if str(c) >= fs:
                    ceased[(a, b)] = str(c)
        for s in sorted(start_crns):
            top, depth, path = tops[s]
            klass, where, detail, evid = classify_top(top, above_all.get(top, []), con)
            gl = gl_all.get(top) if klass in ("foreign entity", "no unit over 50%", "no PSC data held",
                                              "corporate parent without a register number",
                                              "corporate parent, no further PSC data") else None
            flag = "; ".join(f"edge {a} to {b} ceased {ceased[(a, b)]} (PSC list 8 September 2026)"
                             for a, b in zip(path[:-1], path[1:]) if (a, b) in ceased)
            out.append({
                "company_number": s, "financial_year": fy, "ultimate_company": top, "chain_length": depth,
                "chain": ">".join(path), "class": klass, "where": where, "detail": detail,
                "gleif_ultimate_parent_lei": gl[0] if gl else "",
                "gleif_ultimate_parent_jurisdiction": (gl[1] or "") if gl else "",
                "multiple_controlling_parents": nparents.get(top, 0) > 1, "cycle": ("cyc", s) in by_start,
                "flag_2025_26": flag, "bods_view_end": view_end, "evidence_statements": "|".join(evid[:4])})
    return out, cycles_total


def classify_top(top, above, con):
    """Rule 2.2 classes at the unit nobody (above 50%) controls."""
    if not above:
        return "no PSC data held", "", "no BODS statement for this company", []
    persons = [a for a in above if a[0] == "person"]
    ents = [a for a in above if a[0] == "entity"]
    stm = [a for a in above if a[0] == "statement"]
    evid = [a[11] for a in above]
    ctl_p = [a for a in persons if a[8] is not None and a[8] >= 50]
    if ctl_p:
        res = sorted({a[6] or "unknown" for a in ctl_p})
        return "individual", ",".join(res), "an individual holds more than 50% of votes (country of residence only)", evid
    ctl_e = [a for a in ents if a[8] is not None and a[8] >= 50]
    if ctl_e:
        a = ctl_e[0]
        if a[1]:
            # a register number we could not walk (not in the edge set as a subject)
            return "corporate parent, no further PSC data", "GB", f"parent {a[1]}", evid
        if a[4]:
            return "dispersed listed", a[3] or "", "parent entity has a public listing", evid
        if a[3] and a[3] != "GB":
            return "foreign entity", a[3], f"parent registered in {a[3]}", evid
        return "corporate parent without a register number", a[3] or "", "", evid
    sh = [a for a in persons + ents if a[8] is None and a[12] is not None and a[12] >= 50]
    if sh:
        a = sh[0]
        who = (f"individual ({a[6] or 'unknown'})" if a[0] == "person" else
               f"company {a[1]}" if a[1] else f"entity registered in {a[3] or 'unknown'}")
        return ("more than 50% of shares, voting rights not filed", a[6] if a[0] == "person" else (a[3] or ""),
                f"held by {who}; rule 2.1 needs voting power, so the walk stops here (proposed rule P2)", evid)
    if any(a[10] for a in above):
        return "trust or trustee", "", "trustee interest filed", evid
    reasons = {a[7] for a in stm if a[7]}
    if "subjectExemptFromDisclosure" in reasons:
        return "exempt listed", "", "company exempt from PSC disclosure", evid
    if "noBeneficialOwners" in reasons and not persons and not ents:
        return "no PSC filed", "", "statement: no registrable person or entity", evid
    if reasons and not persons and not ents:
        return "no PSC filed", "", "statement: " + ",".join(sorted(reasons)), evid
    return "no unit over 50%", "", "no person or entity holds more than 50% of votes", evid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true", help="(re)build bods_edges.parquet")
    ap.add_argument("--resolver", default=str(OUT / "resolver_proposed.csv"))
    ap.add_argument("--bodies", default="")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WORK / "ownership.duckdb"))
    con.execute("SET memory_limit='8GB'")
    man = {"pipelineGitSha": git_sha(), "bodsAsAt": BODS_AS_AT}
    if a.build or not EDGES.exists():
        man["bods"] = build_edges(con)
    else:
        man["bods"] = {"edges_sha256": sha256_file(EDGES)}
    man["gleif"] = gleif_l2(con)
    want = set(a.bodies.split(",")) if a.bodies else None
    crns = set()
    with open(a.resolver, newline="") as f:
        for r in csv.DictReader(f):
            if want and r["body_id"] not in want:
                continue
            if r["org_scheme"] == "GB-COH" and r["org_id"]:
                crns.add(r["org_id"])
            for al in (r["alias_ids"] or "").split("|"):
                if al.startswith("GB-COH-"):
                    crns.add(al[7:])
    rows, cycles = walk(con, crns, ["2024/25", "2025/26"])
    path = OUT / ("ownership_walk.csv" if not want else f"ownership_walk_{'_'.join(sorted(want))}.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["company_number"])
        w.writeheader()
        w.writerows(rows)
    from collections import Counter
    man.update(companies=len(crns), rows=len(rows), cycles=cycles,
               classes={fy: dict(Counter(r["class"] for r in rows if r["financial_year"] == fy)) for fy in ("2024/25", "2025/26")},
               output=str(path), output_sha256=sha256_file(path))
    write_json(OUT / ("ownership_manifest.json" if not want else f"ownership_manifest_{'_'.join(sorted(want))}.json"), man)
    print(json.dumps(man, indent=1, default=str))


if __name__ == "__main__":
    main()
