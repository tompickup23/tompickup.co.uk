#!/usr/bin/env python3
"""Cluster crosswalk edges into entities and mint their ids (M3).

An identifier is not an entity. `GB-COH:01847728`, `LBO-NNDR:PRIMESIGHT` and
`LBO-WEB:https://primesight.co.uk/` are three ways of pointing at one firm, and
the job here is to say so once, give the firm an id, and record which
identifiers are the firm and on whose authority.

Clustering is connected components over `decision_id`: two identifiers are in
the same component when some matcher decision named them both. Nothing here
invents a link. If build_crosswalk.py declined to link an ambiguous name to a
company number, that name stays its own entity, which is the correct answer,
not a gap.

**Mint-once, and what that actually costs.** The anchor of a cluster is its
highest-precedence identifier, so an entity anchored on a company number is
immune to churn in the weak identifiers around it. On a rebuild:

  * if any member identifier is already a registered anchor, the entity keeps
    that id, even if the cluster has since grown a better anchor. Continuity
    beats tidiness.
  * the new best anchor is BOUND to the same id, so the next rebuild is stable
    without another lookup chain.
  * if a cluster contains members carrying two different existing ids, the two
    entities have merged. The higher-precedence id wins and the other is
    recorded in entity_alias with its reason. It is never deleted and never
    reused, because it may already have been published.
  * if a cluster splits, the part holding the registered anchor keeps the id
    and the other part mints a fresh one. That is the only case where a new id
    appears for something that already existed, and it is a genuine statement
    that we were wrong before.

Outputs, all under gold/:
  entity/         one row per resolved entity
  crosswalk/      one row per (entity_id, scheme, source_id) edge
  entity_alias/   superseded ids, so a published id always resolves
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import crosswalk as X  # noqa: E402
import silver as SV  # noqa: E402


def load_accepted_linkage(gold, threshold):
    """Accepted probabilistic pairs, if build_linkage.py has run.

    Absent by design on a first run: the deterministic tier must stand on its
    own, and a probabilistic edge is only ever an addition to it.
    """
    base = gold / "linkage_pairs"
    parts = sorted(base.glob("snapshot_date=*"))
    if not parts:
        return None, None
    part = parts[-1]
    return str(part / "part.parquet"), part.name.split("=", 1)[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-snapshot", default=None)
    ap.add_argument("--threshold", type=float, default=0.95)
    ap.add_argument("--with-probabilistic", action="store_true",
                    help="include accepted Splink pairs as crosswalk edges")
    ap.add_argument("--dry-run", action="store_true",
                    help="cluster and report, mint nothing, write nothing")
    args = ap.parse_args()

    out_snapshot = args.out_snapshot or _dt.datetime.now(
        _dt.timezone.utc).strftime("%Y-%m-%d")
    gold = X.gold_dir()

    ed_parts = sorted((gold / "crosswalk_edges").glob("snapshot_date=*"))
    if not ed_parts:
        raise SystemExit("FATAL: build_crosswalk.py has not run.")
    ed_part = ed_parts[-1]
    edges_pq = str(ed_part / "part.parquet")

    con, dv = SV.connect()
    SV.log(f"duckdb {dv}; edges {ed_part.name}")

    con.execute(f"CREATE TABLE ev AS SELECT * FROM read_parquet('{edges_pq}')")
    inputs = [{"layer": "gold", "table": "crosswalk_edges",
               "snapshot": ed_part.name.split("=", 1)[1]}]

    prob_pq, prob_snap = (None, None)
    if args.with_probabilistic:
        prob_pq, prob_snap = load_accepted_linkage(gold, args.threshold)
        if not prob_pq:
            raise SystemExit(
                "FATAL: --with-probabilistic asked for, but build_linkage.py "
                "has not produced a linkage_pairs partition.")
        con.execute(f"""
            INSERT INTO ev BY NAME
            SELECT 'splink:' || name_key || ':' || crn AS decision_id,
                   'GB-COH' AS scheme, crn AS source_id,
                   false AS scheme_is_local,
                   'probabilistic' AS method, 'splink' AS matcher,
                   'score' AS evidence_class,
                   match_probability AS confidence,
                   match_probability AS match_score,
                   'Splink match, p=' || round(match_probability, 4) AS evidence,
                   name_raw AS source_name, name_key AS source_name_norm,
                   NULL::DATE AS valid_from, NULL::DATE AS valid_to,
                   '{prob_snap}' AS source_snapshot,
                   NULL::VARCHAR AS source_sha256,
                   register_name, NULL::VARCHAR AS register_entity_type,
                   register_postcode
            FROM read_parquet('{prob_pq}')
            WHERE accepted_at_operating_threshold
        """)
        con.execute(f"""
            INSERT INTO ev BY NAME
            SELECT 'splink:' || name_key || ':' || crn AS decision_id,
                   CASE WHEN name_key IN (SELECT source_id FROM ev
                                          WHERE scheme = 'LBO-NNDR')
                        THEN 'LBO-NNDR' ELSE 'LBO-SUPPLIER' END AS scheme,
                   name_key AS source_id, true AS scheme_is_local,
                   'probabilistic' AS method, 'splink' AS matcher,
                   'score' AS evidence_class,
                   match_probability AS confidence,
                   match_probability AS match_score,
                   'Splink match to ' || crn AS evidence,
                   name_raw AS source_name, name_key AS source_name_norm,
                   NULL::DATE AS valid_from, NULL::DATE AS valid_to,
                   '{prob_snap}' AS source_snapshot,
                   NULL::VARCHAR AS source_sha256,
                   register_name, NULL::VARCHAR AS register_entity_type,
                   register_postcode
            FROM read_parquet('{prob_pq}')
            WHERE accepted_at_operating_threshold
        """)
        inputs.append({"layer": "gold", "table": "linkage_pairs",
                       "snapshot": prob_snap, "threshold": args.threshold})
        SV.log(f"probabilistic edges included at p>={args.threshold}")

    # --- nodes and links ----------------------------------------------------
    nodes = con.execute("""
        SELECT DISTINCT scheme, source_id FROM ev
        WHERE source_id IS NOT NULL AND source_id <> ''
    """).fetchall()
    node_index = {(s, i): n for n, (s, i) in enumerate(nodes)}
    SV.log(f"{len(nodes):,} distinct identifiers")

    link_rows = con.execute("""
        SELECT decision_id, scheme, source_id FROM ev
        WHERE decision_id IS NOT NULL
          AND source_id IS NOT NULL AND source_id <> ''
    """).fetchall()
    by_decision = {}
    for did, s, i in link_rows:
        by_decision.setdefault(did, set()).add((s, i))

    pairs = set()
    for did, members in by_decision.items():
        ms = sorted(members)
        for a in range(1, len(ms)):
            pairs.add((node_index[ms[0]], node_index[ms[a]]))
    SV.log(f"{len(by_decision):,} matcher decisions, {len(pairs):,} identifier links")

    import igraph as ig
    g = ig.Graph(n=len(nodes), edges=sorted(pairs))
    comps = g.connected_components(mode="weak")
    SV.log(f"{len(comps):,} connected components")

    # --- anchor selection + minting ----------------------------------------
    run = X.run_id()
    minter = X.Minter(run)
    rev = {n: k for k, n in node_index.items()}

    clusters = []
    merges = []
    for members_idx in comps:
        members = sorted(
            (rev[n] for n in members_idx),
            key=lambda k: (X.SCHEME_PRECEDENCE.get(k[0], 999), k[0], k[1]))
        keys = [X.anchor_key(s, i) for s, i in members]
        known = [(k, minter.known[k]["entity_id"]) for k in keys
                 if k in minter.known]
        if known:
            eid = known[0][1]
            minted = False
            other_ids = sorted({e for _, e in known if e != eid})
            if other_ids:
                merges.append({"entityId": eid, "absorbed": other_ids,
                               "anchor": keys[0]})
        else:
            eid, minted = minter.entity_id_for(*members[0])
        if keys[0] not in minter.known:
            minter.bind(members[0][0], members[0][1], eid)
        clusters.append({
            "entity_id": eid,
            "anchor_scheme": members[0][0],
            "anchor_source_id": members[0][1],
            "members": members,
            "minted_this_run": minted,
        })

    SV.log(f"entities: {len(clusters):,} "
           f"({minter.minted_this_run:,} ids minted this run, "
           f"{minter.reused_this_run:,} reused, {len(merges):,} merges)")

    if args.dry_run:
        SV.log("dry run: nothing written, no id minted")
        con.close()
        return 0

    written = minter.flush()
    SV.log(f"registry: {written:,} rows appended to {minter.path}")

    # --- entity table -------------------------------------------------------
    import pandas as pd
    ent_rows = []
    xw_rows = []
    for c in clusters:
        schemes = sorted({s for s, _ in c["members"]})
        ent_rows.append({
            "entity_id": c["entity_id"],
            "anchor_scheme": c["anchor_scheme"],
            "anchor_source_id": c["anchor_source_id"],
            "identifier_count": len(c["members"]),
            "schemes": schemes,
            "scheme_count": len(schemes),
            "has_company_number": "GB-COH" in schemes,
            "only_local_identifiers": all(X.is_local(s) for s in schemes),
            "minted_in_run": run if c["minted_this_run"] else None,
        })
        for s, i in c["members"]:
            xw_rows.append({"entity_id": c["entity_id"], "scheme": s,
                            "source_id": i})
    con.register("ent_in", pd.DataFrame(ent_rows))
    con.register("xw_in", pd.DataFrame(xw_rows))

    # The crosswalk is the edge table with an entity_id joined on. Every
    # column that says WHO decided and HOW survives, because an edge without
    # its provenance is an assertion without a source.
    con.execute("""
        CREATE TABLE crosswalk_out AS
        SELECT x.entity_id, e.scheme, e.source_id, e.scheme_is_local,
               e.method, e.matcher, e.evidence_class, e.confidence,
               e.match_score, e.evidence, e.source_name, e.source_name_norm,
               e.decision_id,
               e.valid_from, e.valid_to, e.source_snapshot, e.source_sha256
        FROM ev e
        JOIN xw_in x ON x.scheme = e.scheme AND x.source_id = e.source_id
    """)

    # An entity's display name comes from the register where a company number
    # anchors it, and from the source name otherwise. A local name is never
    # allowed to overwrite a registered name (DATA-INTEGRITY s2: entityType and
    # identity are never upgraded by a weaker source).
    con.execute("""
        CREATE TABLE entity_out AS
        WITH reg_name AS (
            SELECT x.entity_id,
                   any_value(e.register_name) AS register_name,
                   any_value(e.register_entity_type) AS entity_type,
                   any_value(e.register_postcode) AS postcode
            FROM ev e JOIN xw_in x
              ON x.scheme = e.scheme AND x.source_id = e.source_id
            WHERE e.scheme = 'GB-COH' AND e.register_name IS NOT NULL
            GROUP BY x.entity_id
        ),
        any_name AS (
            SELECT x.entity_id, any_value(e.source_name) AS source_name
            FROM ev e JOIN xw_in x
              ON x.scheme = e.scheme AND x.source_id = e.source_id
            WHERE e.source_name IS NOT NULL
            GROUP BY x.entity_id
        ),
        best_conf AS (
            SELECT x.entity_id, max(e.confidence) AS best_confidence,
                   bool_or(e.method = 'probabilistic') AS has_probabilistic,
                   bool_or(e.method = 'deterministic') AS has_deterministic
            FROM ev e JOIN xw_in x
              ON x.scheme = e.scheme AND x.source_id = e.source_id
            GROUP BY x.entity_id
        )
        SELECT i.entity_id, i.anchor_scheme, i.anchor_source_id,
               coalesce(r.register_name, a.source_name) AS name,
               (r.register_name IS NOT NULL) AS name_from_register,
               r.entity_type, r.postcode,
               i.identifier_count, i.schemes, i.scheme_count,
               i.has_company_number, i.only_local_identifiers,
               c.best_confidence, c.has_probabilistic, c.has_deterministic,
               i.minted_in_run
        FROM ent_in i
        LEFT JOIN reg_name r USING (entity_id)
        LEFT JOIN any_name a USING (entity_id)
        LEFT JOIN best_conf c USING (entity_id)
    """)

    ent_dir = X.table_dir("entity", out_snapshot)
    xw_dir = X.table_dir("crosswalk", out_snapshot)
    n_ent, b_ent = SV.write_parquet(con, "SELECT * FROM entity_out", ent_dir)
    n_xw, b_xw = SV.write_parquet(con, "SELECT * FROM crosswalk_out", xw_dir)

    alias_dir = X.table_dir("entity_alias", out_snapshot)
    (alias_dir / "aliases.json").write_text(json.dumps({
        "$meta": {"run": run, "note": "Ids superseded by a cluster merge. "
                                      "Never reused, never deleted; a "
                                      "published id must always resolve."},
        "merges": merges,
    }, indent=2) + "\n")

    stats = dict(con.execute("""
        SELECT anchor_scheme, count(*) FROM entity_out GROUP BY 1 ORDER BY 2 DESC
    """).fetchall())
    n_multi = con.execute(
        "SELECT count(*) FROM entity_out WHERE scheme_count > 1").fetchone()[0]
    n_local_only = con.execute(
        "SELECT count(*) FROM entity_out WHERE only_local_identifiers"
    ).fetchone()[0]

    assertions = {
        "identifiers": len(nodes),
        "matcherDecisions": len(by_decision),
        "identifierLinks": len(pairs),
        "entities": n_ent,
        "crosswalkEdges": n_xw,
        "idsMintedThisRun": minter.minted_this_run,
        "idsReusedThisRun": minter.reused_this_run,
        "clusterMerges": len(merges),
        "entitiesWithMoreThanOneScheme": n_multi,
        "entitiesWithOnlyLocalIdentifiers": n_local_only,
        "byAnchorScheme": stats,
    }
    for d, table, rows, nbytes in ((ent_dir, "entity", n_ent, b_ent),
                                   (xw_dir, "crosswalk", n_xw, b_xw)):
        X.write_manifest(
            d, table, out_snapshot, rows, nbytes, dv, inputs,
            assertions=assertions,
            notes=("Entity ids are mint-once ULIDs held in an append-only "
                   "registry at gold/entity_id_registry.jsonl. That file IS "
                   "the guarantee; regenerating it from scratch would mint "
                   "new ids for entities that already have published ones."),
            extra={"runId": run,
                   "includesProbabilistic": bool(args.with_probabilistic),
                   "operatingThreshold": (args.threshold
                                          if args.with_probabilistic else None)})

    SV.log(f"WROTE entity {n_ent:,} rows ({b_ent/1e6:.1f} MB), "
           f"crosswalk {n_xw:,} rows ({b_xw/1e6:.1f} MB)")
    SV.log(f"  by anchor scheme: {stats}")
    SV.log(f"  {n_multi:,} entities carry more than one identifier scheme")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
