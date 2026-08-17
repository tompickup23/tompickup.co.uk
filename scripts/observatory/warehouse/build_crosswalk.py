#!/usr/bin/env python3
"""Deterministic crosswalk edges: migrate the production matchers (M3).

Every identification decision the live pipeline already makes lives in a dict
inside one script or in a per-matcher JSON file. This turns all of them into
rows of one table with the same columns, the same provenance and the same
vocabulary of schemes, so that a later session can add a matcher without
touching any other matcher, and so that a decision survives its input going
missing.

Input, all of it from bronze or silver, never a working directory:

  silver ch_register       the register spine and the only thing that can
                           confirm a company number is real
  bronze matcher_pound     build_pound.py supplier resolution
  bronze matcher_ocds      publisher-stated GB-COH ids on award notices
  bronze matcher_nndr      billing authority ratepayer files
  bronze matcher_websites  verified company websites
  silver gazette_notices   notice-stated company numbers
  bronze org_id_guide      the scheme codelist

Output: gold/crosswalk_edges/snapshot_date=<today>/part.parquet, one row per
(scheme, source_id, matcher) decision, with no entity_id yet. Entity ids are
minted in build_entities.py after clustering, because an id must be minted for
a resolved entity, not for an edge.

`decision_id` is the column that makes clustering possible. One matcher
decision ("this supplier ledger name IS company 01234567") produces two rows,
one per identifier it touches, and they carry the same decision_id. Connected
components over decision_ids is what build_entities.py clusters. Without it the
table would be a list of identifiers with no statement about which of them are
the same thing, which is the one thing a crosswalk exists to say. A decision
that resolves to more than one company number does NOT share a decision_id
across those numbers: an ambiguous name is not evidence that two companies are
one company.

Two columns carry the honesty:

  evidence_class  identifier-observed, when some publisher wrote the
                  identifier down and we read it, or name-rule, when we
                  inferred it from a name. Both are deterministic in the sense
                  that they are reproducible rules with no score, and they are
                  not the same kind of evidence, so they are not the same
                  column value.
  confidence      the rule's own strength. 1.0 only where an identifier was
                  observed.

Every GB-COH edge is checked against the register. A company number that no
snapshot of the register has ever held is not a company number, whatever the
source called it, and the edge is rejected with a reason rather than landed.
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import crosswalk as X  # noqa: E402
import entity_type as ET  # noqa: E402
import silver as SV  # noqa: E402

TABLE = "crosswalk_edges"

# matcher -> (evidence_class, confidence, note)
#
# The confidences are the production matchers' own stated strength, not a new
# judgment. Anything an identifier was read off gets 1.0. The name rules get
# less, in the order build_pound.py itself tries them, which is the order of
# decreasing safety its author chose.
MATCHER_RULES = {
    "ch_register": ("identifier-observed", 1.0,
                    "the register states the number"),
    "pound_override": ("identifier-observed", 1.0,
                       "hand-verified against the register"),
    "pound_alias": ("identifier-observed", 1.0,
                    "curated trading name to verified company number"),
    "ocds": ("identifier-observed", 1.0,
             "GB-COH id published by the buyer on an award notice"),
    "nndr": ("identifier-observed", 1.0,
             "company number published in the billing authority file"),
    "gazette": ("identifier-observed", 1.0,
                "company number printed on the notice"),
    "website_crn": ("identifier-observed", 1.0,
                    "company number printed on the fetched page"),
    "website_name_postcode": ("name-rule", 0.99,
                              "registered name and postcode both on the page"),
    "pound_exact_lancs": ("name-rule", 0.97,
                          "unique exact normalised name in the Lancashire register"),
    "pound_exact": ("name-rule", 0.95,
                    "unique exact normalised name in the national register"),
    "pound_prefix_unique": ("name-rule", 0.90,
                            "unique word-boundary prefix extension in the register"),
}

POUND_HOW_TO_MATCHER = {
    "override": "pound_override",
    "alias": "pound_alias",
    "ocds": "ocds",
    "exact-lancs": "pound_exact_lancs",
    "exact": "pound_exact",
    "prefix-unique": "pound_prefix_unique",
}


def read_bronze_json(source_id, name_suffix, snapshot=None):
    snap, path, manifest = SV.resolve_bronze(source_id, snapshot)
    fpath, sha = SV.bronze_file(manifest, path, name_suffix)
    return snap, sha, str(fpath), json.loads(Path(fpath).read_text())


def read_bronze_jsonl(source_id, name_suffix, snapshot=None):
    snap, path, manifest = SV.resolve_bronze(source_id, snapshot)
    fpath, sha = SV.bronze_file(manifest, path, name_suffix)
    rows = [json.loads(line) for line in Path(fpath).read_text().splitlines()
            if line.strip()]
    return snap, sha, str(fpath), rows


def edge(scheme, source_id, matcher, source_name, snapshot, sha,
         valid_from=None, valid_to=None, extra_evidence=None,
         decision_id=None):
    ec, conf, note = MATCHER_RULES[matcher]
    return {
        "decision_id": decision_id,
        "scheme": scheme,
        "source_id": source_id,
        "scheme_is_local": X.is_local(scheme),
        "method": "deterministic",
        "matcher": matcher,
        "evidence_class": ec,
        "confidence": conf,
        "match_score": None,
        "evidence": extra_evidence or note,
        "source_name": source_name,
        "source_name_norm": X.normalise(source_name) if source_name else None,
        "source_postcode": None,
        "source_outcode": None,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "source_snapshot": snapshot,
        "source_sha256": sha,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-snapshot", default=None,
                    help="partition date, defaults to today UTC")
    args = ap.parse_args()

    out_snapshot = args.out_snapshot or _dt.datetime.now(
        _dt.timezone.utc).strftime("%Y-%m-%d")

    con, dv = SV.connect()
    scheme_info = X.validate_schemes()
    SV.log(f"duckdb {dv}; org-id codelist {scheme_info['codelistSnapshot']} "
           f"({scheme_info['codelistLists']} lists)")

    inputs = [{"layer": "bronze", "source": "org_id_guide",
               "snapshot": scheme_info["codelistSnapshot"],
               "sha256": scheme_info["codelistSha256"]}]

    # --- the register spine ------------------------------------------------
    reg_snap = None
    reg_parts = sorted((SV.silver_dir() / "ch_register").glob("snapshot_date=*"))
    if not reg_parts:
        raise SystemExit("FATAL: silver ch_register is not built. Run M2 first.")
    reg_pqs = [str(p / "part.parquet") for p in reg_parts]
    reg_snap = reg_parts[-1].name.split("=", 1)[1]
    latest_pq = str(reg_parts[-1] / "part.parquet")
    SV.log(f"register spine: {len(reg_pqs)} snapshot(s), latest {reg_snap}")

    # EVERY company number the register has EVER carried, across all snapshots.
    # A dissolved company that has left the latest snapshot is still a real
    # company and its edges are still real, so validating against the latest
    # snapshot alone would reject true edges (DATA-INTEGRITY s4 vintage rule:
    # a source refresh gap is never evidence of absence).
    con.execute(f"""
        CREATE TABLE reg_all AS
        SELECT company_number,
               max(company_name) AS company_name,
               max(reg_postcode_norm) AS reg_postcode_norm,
               max(entity_type) AS entity_type,
               bool_or(companies_act_body) AS companies_act_body,
               min(incorporation_date) AS incorporation_date,
               max(dissolution_date) AS dissolution_date
        FROM read_parquet({reg_pqs})
        GROUP BY company_number
    """)
    n_reg_all = con.execute("SELECT count(*) FROM reg_all").fetchone()[0]

    con.execute(f"""
        CREATE TABLE reg_lancs AS
        SELECT * FROM read_parquet('{latest_pq}')
        WHERE reg_postcode_norm IS NOT NULL
          AND {X.lancs_postcode_sql('reg_postcode_norm')}
    """)
    n_lancs = con.execute("SELECT count(*) FROM reg_lancs").fetchone()[0]
    n_lancs_co = con.execute(
        "SELECT count(*) FROM reg_lancs WHERE companies_act_body").fetchone()[0]
    SV.log(f"register: {n_reg_all:,} numbers ever seen; Lancashire prefilter "
           f"{n_lancs:,} rows, {n_lancs_co:,} of them Companies Act bodies")
    for p in reg_parts:
        m = json.loads((p / "manifest.json").read_text())
        inputs.append({"layer": "silver", "table": "ch_register",
                       "snapshot": p.name.split("=", 1)[1], "rows": m["rows"]})

    edges = []

    # --- D1 register self-edges -------------------------------------------
    # Every Lancashire-registered Companies Act body is an entity in its own
    # right whether or not any other source has ever mentioned it. Without
    # these the crosswalk would only contain companies somebody happened to
    # match, which is a survivorship-biased register.
    rows = con.execute("""
        SELECT company_number, company_name, incorporation_date,
               dissolution_date, source_sha256, snapshot_date
        FROM reg_lancs WHERE companies_act_body
    """).fetchall()
    for cn, name, inc, dis, sha, snap in rows:
        edges.append(edge("GB-COH", cn, "ch_register", name,
                          str(snap), sha, valid_from=inc, valid_to=dis,
                          decision_id=f"ch_register:{cn}"))
    SV.log(f"D1 register self-edges: {len(rows):,}")

    # --- D2 pound supplier resolution -------------------------------------
    # Every snapshot, for the same reason as OCDS and websites below: the
    # 27 Jul edition resolved 260 suppliers through award-notice identifiers
    # that the 16 Aug edition could not, because the OCDS input is a cross-repo
    # path that exists on only one machine. Reading the latest snapshot alone
    # would throw those identifications away on the grounds that the machine
    # changed, which is not a reason for an identification to stop being true.
    #
    # Every (name, company number, rule) triple across every edition is kept,
    # each carrying the snapshot it was made in. Where two editions resolved
    # one supplier name to two DIFFERENT companies they do not share a
    # decision_id, so the disagreement is recorded without merging two
    # companies into one entity.
    pound_inputs = []
    seen_pound = {}          # (norm_name, crn, matcher) -> (snap, sha, raw, how)
    unresolved_names = {}    # norm_name -> (raw, snap, sha, how)
    for snap, path, manifest in SV.bronze_partitions("matcher_pound"):
        f = path / "pound.json"
        if not f.exists():
            continue
        sha = next(e["sha256"] for e in manifest["files"]
                   if e["name"] == "pound.json")
        r = json.loads(f.read_text()).get("resolved", {})
        pound_inputs.append({"layer": "bronze", "source": "matcher_pound",
                             "snapshot": snap, "sha256": sha,
                             "resolved": len(r)})
        for raw, rec in r.items():
            norm = X.normalise(raw)
            if not norm:
                continue
            how, crn = rec.get("matchHow"), rec.get("crn")
            matcher = POUND_HOW_TO_MATCHER.get(how)
            if crn and matcher:
                seen_pound.setdefault((norm, crn, matcher),
                                      (snap, sha, raw, how))
            else:
                unresolved_names.setdefault(norm, (raw, snap, sha, how))

    crns_per_name = {}
    for (norm, crn, _m) in seen_pound:
        crns_per_name.setdefault(norm, set()).add(crn)
    pound_ambiguous = sum(1 for v in crns_per_name.values() if len(v) > 1)

    pound_counts = {}
    for (norm, crn, matcher), (snap, sha, raw, how) in seen_pound.items():
        single = len(crns_per_name[norm]) == 1
        did = f"pound:{norm}" if single else f"pound-ambiguous:{norm}:{crn}"
        pound_counts[matcher] = pound_counts.get(matcher, 0) + 1
        edges.append(edge("GB-COH", crn, matcher, raw, snap, sha,
                          decision_id=did))
    # One LBO-SUPPLIER node per distinct supplier name, linked to the company
    # only where the editions agree on which company that is.
    keys_by_name = {}
    for k in seen_pound:
        keys_by_name.setdefault(k[0], []).append(k)
    for norm, crns in crns_per_name.items():
        key = sorted(keys_by_name[norm])[0]
        crn, matcher = key[1], key[2]
        snap, sha, raw, how = seen_pound[key]
        single = len(crns) == 1
        edges.append(edge("LBO-SUPPLIER", norm, matcher, raw, snap, sha,
                          decision_id=(f"pound:{norm}" if single
                                       else f"pound-ambiguous-name:{norm}"),
                          extra_evidence=(f"supplier ledger name resolved to "
                                          f"{crn} by {how}" if single else
                                          "supplier ledger name resolved to "
                                          f"{len(crns)} different companies "
                                          "across editions, not linked")))
    # Unmatched supplier names are entities too: a named payee that is not a
    # company is still something the council paid, and dropping it would make
    # the crosswalk silently agree with the matcher that it does not exist.
    for norm, (raw, snap, sha, how) in unresolved_names.items():
        if norm in crns_per_name:
            continue
        edges.append({
            "decision_id": f"pound:{norm}",
            "scheme": "LBO-SUPPLIER",
            "source_id": norm,
            "scheme_is_local": True,
            "method": "deterministic",
            "matcher": "pound_unresolved",
            "evidence_class": "name-rule",
            "confidence": 1.0,
            "match_score": None,
            "evidence": f"supplier ledger name, matchHow={how}",
            "source_name": raw,
            "source_name_norm": norm,
            "source_postcode": None,
            "source_outcode": None,
            "valid_from": None,
            "valid_to": None,
            "source_snapshot": snap,
            "source_sha256": sha,
        })
    pound_unmatched = sum(1 for n in unresolved_names if n not in crns_per_name)
    SV.log(f"D2 pound: {sum(pound_counts.values()):,} company edges "
           f"{pound_counts}, {pound_unmatched:,} supplier names unresolved, "
           f"across {len(pound_inputs)} snapshot(s)")
    inputs.extend(pound_inputs)

    # --- D3 OCDS publisher-stated identifiers ------------------------------
    # Read across EVERY bronze snapshot, not just the latest. The vps edition
    # of this file is empty because its input is a cross-repo path that only
    # exists on the Mac, so taking the latest snapshot alone would silently
    # drop 329 publisher-stated identifiers. An identification does not stop
    # being true because the machine that made it stopped being able to.
    ocds_seen = {}
    ocds_inputs = []
    for snap, path, manifest in SV.bronze_partitions("matcher_ocds"):
        fpath, sha = SV.bronze_file(manifest, path, ".json")
        d = json.loads(Path(fpath).read_text())
        by_name = d.get("byName", {})
        ocds_inputs.append({"layer": "bronze", "source": "matcher_ocds",
                            "snapshot": snap, "sha256": sha,
                            "names": len(by_name)})
        for name, rec in by_name.items():
            crn = rec.get("crn")
            if not crn:
                continue
            key = (name, crn)
            if key in ocds_seen:
                continue
            ocds_seen[key] = (snap, sha, rec.get("notices"))
    # A supplier name that award notices attach to two different company
    # numbers is an ambiguity, not a merger, so those names link to neither.
    ocds_name_crns = {}
    for (name, crn) in ocds_seen:
        ocds_name_crns.setdefault(X.normalise(name), set()).add(crn)
    ocds_ambiguous = sum(1 for v in ocds_name_crns.values() if len(v) > 1)
    for (name, crn), (snap, sha, notices) in ocds_seen.items():
        norm = X.normalise(name)
        did = (f"ocds:{norm}" if len(ocds_name_crns[norm]) == 1
               else f"ocds-ambiguous:{norm}:{crn}")
        edges.append(edge("GB-COH", crn, "ocds", name, snap, sha,
                          decision_id=did,
                          extra_evidence=f"GB-COH id on {notices} award notice(s)"))
        edges.append(edge("LBO-SUPPLIER", norm, "ocds", name,
                          snap, sha,
                          decision_id=(did if len(ocds_name_crns[norm]) == 1
                                       else f"ocds-ambiguous-name:{norm}"),
                          extra_evidence=f"award notice supplier name for {crn}"))
    inputs.extend(ocds_inputs)
    SV.log(f"D3 OCDS: {len(ocds_seen):,} name-to-GB-COH identifications "
           f"across {len(ocds_inputs)} snapshot(s)")

    # --- D4 NNDR ratepayer files -------------------------------------------
    n_snap, n_sha, n_path, nndr = read_bronze_json(
        "matcher_nndr", "nndr_presence.json")
    nndr_by_name = nndr.get("byName", {})
    nndr_edges = 0
    nndr_ambiguous = 0
    for name, rec in nndr_by_name.items():
        norm = X.normalise(name)
        councils = rec.get("councils") or []
        crns = rec.get("crns") or []
        # One ratepayer name carrying two company numbers across billing
        # authorities is two ratepayers who share a name, or one file with an
        # error in it. Either way it is not evidence that the two companies
        # are the same company, so the name links to neither of them.
        single = len(crns) == 1
        if len(crns) > 1:
            nndr_ambiguous += 1
        did = f"nndr:{norm}" if single else f"nndr-nolink:{norm}"
        # Premises postcodes are the only geography a ratepayer name carries
        # and they are the strongest discriminator the probabilistic tier has.
        # A single postcode is kept as a comparison key; a ratepayer with
        # premises in twenty places has no single location and gets none,
        # which is the honest answer rather than an arbitrary first element.
        pcs = sorted({p for p in (rec.get("postcodes") or []) if p})
        outcodes = sorted({p.split()[0] for p in pcs if p.split()})
        edges.append({
            "decision_id": did,
            "scheme": "LBO-NNDR",
            "source_id": norm,
            "scheme_is_local": True,
            "method": "deterministic",
            "matcher": "nndr_presence",
            "evidence_class": "identifier-observed",
            "confidence": 1.0,
            "match_score": None,
            "evidence": f"ratepayer in {len(councils)} billing authority "
                        f"file(s), {rec.get('records')} record(s)",
            "source_name": name,
            "source_name_norm": norm,
            "source_postcode": pcs[0] if len(pcs) == 1 else None,
            "source_outcode": outcodes[0] if len(outcodes) == 1 else None,
            "valid_from": None,
            "valid_to": None,
            "source_snapshot": n_snap,
            "source_sha256": n_sha,
        })
        for crn in crns:
            edges.append(edge("GB-COH", crn, "nndr", name, n_snap, n_sha,
                              decision_id=(did if single
                                           else f"nndr-nolink:{norm}:{crn}"),
                              extra_evidence="company number published in the "
                                             "billing authority ratepayer file"))
            nndr_edges += 1
    SV.log(f"D4 NNDR: {len(nndr_by_name):,} ratepayer names, "
           f"{nndr_edges:,} stated company numbers, "
           f"{nndr_ambiguous:,} names with more than one number (not linked)")
    inputs.append({"layer": "bronze", "source": "matcher_nndr",
                   "snapshot": n_snap, "sha256": n_sha,
                   "names": len(nndr_by_name)})

    # --- D5 verified websites ----------------------------------------------
    # Also read across every snapshot. The 27 Jul edition verified 342 sites
    # and the 17 Aug edition 271, and P0 established that the drop is dominated
    # by no-dns rejects that look like a resolver fault on the host rather than
    # 8,486 domains dying in three weeks. A verification that happened is a
    # fact with a date on it; a later run failing to repeat it is not evidence
    # that it was wrong. Both editions land, each carrying its own valid_from.
    web_seen = {}
    web_inputs = []
    for snap, path, manifest in SV.bronze_partitions("matcher_websites"):
        hits = [e for e in manifest["files"] if e["name"] == "websites.jsonl"]
        if not hits:
            continue
        fpath = path / "websites.jsonl"
        sha = hits[0]["sha256"]
        rows = [json.loads(l) for l in fpath.read_text().splitlines() if l.strip()]
        web_inputs.append({"layer": "bronze", "source": "matcher_websites",
                           "snapshot": snap, "sha256": sha, "verified": len(rows)})
        for r in rows:
            key = (r["crn"], r["url"])
            checked = (r.get("checkedAt") or "")[:10] or snap
            if key not in web_seen or checked < web_seen[key]["checked"]:
                web_seen[key] = {"row": r, "snap": snap, "sha": sha,
                                 "checked": checked}
    for (crn, url), v in web_seen.items():
        r = v["row"]
        matcher = ("website_crn" if r.get("matchedOn") == "crn"
                   else "website_name_postcode")
        vf = None
        try:
            vf = _dt.date.fromisoformat(v["checked"])
        except Exception:
            vf = None
        did = f"website:{crn}:{url}"
        edges.append(edge("GB-COH", crn, matcher, r.get("name"), v["snap"],
                          v["sha"], valid_from=vf, decision_id=did,
                          extra_evidence=f"verified at {r.get('evidenceUrl') or url}"))
        edges.append(edge("LBO-WEB", url, matcher, r.get("name"), v["snap"],
                          v["sha"], valid_from=vf, decision_id=did,
                          extra_evidence=f"verified website of {crn}"))
    inputs.extend(web_inputs)
    SV.log(f"D5 websites: {len(web_seen):,} distinct verified (crn, url) pairs "
           f"across {len(web_inputs)} snapshot(s)")

    # --- D6 Gazette notice-stated numbers ----------------------------------
    # One edge per company number ACROSS snapshots, keeping the earliest
    # snapshot that observed it, which is what D4 and D5 already do.
    #
    # This diverged until M5 and nothing noticed, because there was exactly one
    # gazette snapshot in silver. The moment a second landed, every company
    # appearing in both produced two edges carrying the same decision_id and
    # the crosswalk's own double-load gate fired on 20 of them. The gate was
    # right and the tier was wrong: `gazette:<crn>` says "this number appeared
    # on a notice", which is one decision however many editions of the
    # candidate file record it.
    #
    # With a single snapshot present this is byte-identical to the previous
    # behaviour, so the M3 baseline is unaffected.
    gz_parts = sorted((SV.silver_dir() / "gazette_notices").glob("snapshot_date=*"))
    gz_edges = 0
    gz_seen = {}
    for p in gz_parts:
        pq = str(p / "part.parquet")
        snap = p.name.split("=", 1)[1]
        # One company number per snapshot, not one per notice. The same
        # company appears on several notices under slightly different names,
        # and a notice name is a notice fact belonging to the gazette table.
        # The crosswalk's claim is only that the number exists on a notice.
        rows = con.execute(f"""
            SELECT company_number,
                   any_value(company_name) AS company_name,
                   any_value(source_sha256) AS source_sha256
            FROM read_parquet('{pq}')
            WHERE company_number IS NOT NULL AND company_number_is_crn
            GROUP BY company_number
        """).fetchall()
        for cn, name, sha in rows:
            if cn not in gz_seen:
                gz_seen[cn] = (name, snap, sha)
        inputs.append({"layer": "silver", "table": "gazette_notices",
                       "snapshot": snap, "companyNumbers": len(rows)})
    for cn, (name, snap, sha) in gz_seen.items():
        edges.append(edge("GB-COH", cn, "gazette", name, snap, sha,
                          decision_id=f"gazette:{cn}"))
        gz_edges += 1
    SV.log(f"D6 Gazette: {gz_edges:,} notice-stated company numbers across "
           f"{len(gz_parts)} snapshot(s)")

    # --- validate + land ----------------------------------------------------
    import pandas as pd  # noqa: E402  (arrives with splink)
    SV.log(f"assembling {len(edges):,} edges into a frame...")
    df = pd.DataFrame(edges)
    con.register("edges_in", df)
    SV.log("frame registered, validating against the register...")

    # A GB-COH source_id must be eight characters (DATA-INTEGRITY s9.3) AND
    # present in the register. The shape test alone is not enough: RS000822 is
    # a perfectly shaped society number (s9.4), and only the register can tell
    # the difference.
    # The join key is nulled for non-GB-COH rows FIRST, in its own projection.
    # Writing the scheme test into the ON clause instead makes the condition
    # non-equi, which DuckDB can only serve with a blockwise nested loop over
    # 173k x 5.8M rows. It does not error, it just never finishes.
    con.execute("""
        CREATE TABLE edges_keyed AS
        SELECT *, CASE WHEN scheme = 'GB-COH' THEN source_id END AS coh_id
        FROM edges_in
    """)
    con.execute(f"""
        CREATE TABLE edges_checked AS
        SELECT e.* EXCLUDE (coh_id),
               CASE
                 WHEN e.scheme <> 'GB-COH' THEN NULL
                 WHEN NOT regexp_matches(e.source_id, '{ET.CH_NUMBER_RE}')
                   THEN 'not-eight-characters'
                 WHEN r.company_number IS NULL THEN 'not-in-any-register-snapshot'
                 WHEN NOT r.companies_act_body THEN 'not-a-companies-act-body'
                 ELSE NULL
               END AS reject_reason,
               r.company_name AS register_name,
               r.entity_type AS register_entity_type,
               r.reg_postcode_norm AS register_postcode
        FROM edges_keyed e
        LEFT JOIN reg_all r ON r.company_number = e.coh_id
    """)

    rejects = con.execute("""
        SELECT reject_reason, matcher, count(*) AS n
        FROM edges_checked WHERE reject_reason IS NOT NULL
        GROUP BY 1, 2 ORDER BY n DESC
    """).fetchall()
    n_rej = sum(r[2] for r in rejects)
    if rejects:
        SV.log(f"rejected {n_rej:,} edges:")
        for reason, matcher, n in rejects:
            SV.log(f"    {n:>6,}  {matcher:<24} {reason}")

    out_dir = X.table_dir(TABLE, out_snapshot)
    rej_dir = out_dir
    con.execute(
        f"COPY (SELECT * FROM edges_checked WHERE reject_reason IS NOT NULL) "
        f"TO '{rej_dir / '_rejected.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    select = """
        SELECT decision_id, scheme, source_id, scheme_is_local, method, matcher,
               evidence_class, confidence, match_score, evidence,
               source_name, source_name_norm, source_postcode, source_outcode,
               CAST(valid_from AS DATE) AS valid_from,
               CAST(valid_to AS DATE) AS valid_to,
               source_snapshot, source_sha256,
               register_name, register_entity_type, register_postcode
        FROM edges_checked WHERE reject_reason IS NULL
    """
    rows, nbytes = SV.write_parquet(con, select, out_dir)

    by_matcher = dict(con.execute("""
        SELECT matcher, count(*) FROM edges_checked WHERE reject_reason IS NULL
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall())
    by_scheme = dict(con.execute("""
        SELECT scheme, count(*) FROM edges_checked WHERE reject_reason IS NULL
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall())
    distinct_crn = con.execute("""
        SELECT count(DISTINCT source_id) FROM edges_checked
        WHERE reject_reason IS NULL AND scheme = 'GB-COH'
    """).fetchone()[0]

    assertions = {
        "registerNumbersEverSeen": n_reg_all,
        "lancashirePrefilterRows": n_lancs,
        "lancashireCompaniesActBodies": n_lancs_co,
        "edgesLanded": rows,
        "edgesRejected": n_rej,
        "distinctCompanyNumbers": distinct_crn,
        "nndrNamesWithMoreThanOneNumber": nndr_ambiguous,
        "poundNamesWithMoreThanOneNumber": pound_ambiguous,
        "ocdsNamesWithMoreThanOneNumber": ocds_ambiguous,
        "byMatcher": by_matcher,
        "byScheme": by_scheme,
        "rejectReasons": {f"{m}/{r}": n for r, m, n in rejects},
    }
    X.write_manifest(
        out_dir, TABLE, out_snapshot, rows, nbytes, dv, inputs,
        assertions=assertions,
        notes=("Deterministic crosswalk edges migrated from the production "
               "matchers. No entity ids yet: ids are minted per resolved "
               "entity in build_entities.py, never per edge."),
        extra={"schemeRegistry": scheme_info,
               "matcherRules": {k: {"evidenceClass": v[0], "confidence": v[1],
                                    "note": v[2]}
                                for k, v in MATCHER_RULES.items()}})

    SV.log(f"WROTE {out_dir}/part.parquet: {rows:,} edges, {nbytes/1e6:.1f} MB")
    SV.log(f"  by scheme: {by_scheme}")
    SV.log(f"  distinct company numbers: {distinct_crn:,}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
