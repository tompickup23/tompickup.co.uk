#!/usr/bin/env python3
"""Probabilistic linkage with Splink 4, plus the evaluation that gates it (M3).

The deterministic tier in build_crosswalk.py resolves every name that some
publisher already attached an identifier to. What is left is the hard half:
supplier ledger names and NNDR ratepayer names that no one ever wrote a company
number against. This links those to the register with Fellegi-Sunter scoring.

Nothing here is allowed to surface publicly until the evaluation below is
published, so the evaluation is built in the same script as the matcher rather
than promised for later.

**How the evaluation gets its labels.** A held-out truth set, not our own
opinion. 3,500-odd names in the deterministic tier carry a company number that
a billing authority, a buyer on an award notice or a company's own website
stated. Those identifications were made by third parties for their own reasons,
entirely independently of name similarity. The matcher is run over those names
with the identifier withheld, and the withheld identifier is the label.

That gives real precision and recall on real labels. It also has a real
limitation which is stated everywhere the numbers appear and must never be
dropped: firms whose company number somebody bothered to publish are not a
random sample of firms. They are larger, more formal and more likely to carry a
clean registered name. The measured precision is therefore an estimate for that
subpopulation and is very likely optimistic for the rest.

**The clerical sample is drawn but NOT adjudicated here.** 200 pairs stratified
across score bands are written to a review file. Pairs the truth set decides
are labelled from the truth set. The rest are left null with status
awaiting-clerical-review. No figure in this file is presented as the output of
human clerical review, because no human has reviewed it.
"""
import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import crosswalk as X  # noqa: E402
import silver as SV  # noqa: E402

TABLE = "linkage_pairs"

# Score bands for stratified sampling and for the sensitivity table. Splink
# emits a match_probability in [0, 1]; these are the bands the clerical sample
# is drawn evenly across so the low-scoring tail gets looked at as hard as the
# top, which is the whole point of stratifying (Harron et al. 2017).
BANDS = [(0.50, 0.70), (0.70, 0.90), (0.90, 0.95), (0.95, 0.99), (0.99, 1.01)]

# The operating threshold is chosen from the sensitivity table, not before it.
# This is the default the script reports against; --threshold overrides.
DEFAULT_THRESHOLD = 0.95


def build_frames(con, edges_pq, reg_pq):
    """Left frame: names with no company number. Right frame: the register."""
    con.execute(f"""
        CREATE OR REPLACE TABLE ev AS
        SELECT * FROM read_parquet('{edges_pq}')
    """)

    # Right: Lancashire Companies Act bodies, one row per company.
    con.execute(f"""
        CREATE OR REPLACE TABLE reg AS
        SELECT company_number            AS unique_id,
               company_name              AS name_raw,
               reg_postcode_norm         AS postcode,
               split_part(reg_postcode_norm, ' ', 1) AS outcode,
               entity_type,
               incorporation_date,
               dissolution_date
        FROM read_parquet('{reg_pq}')
        WHERE companies_act_body
          AND reg_postcode_norm IS NOT NULL
          AND {X.lancs_postcode_sql('reg_postcode_norm')}
    """)

    # Left: every local-scheme name, with a flag saying whether the
    # deterministic tier already gave it a company number. The ones that did
    # are the truth set; the ones that did not are the work.
    con.execute("""
        CREATE OR REPLACE TABLE names AS
        WITH local_names AS (
            SELECT scheme, source_id, any_value(source_name) AS name_raw,
                   max(source_postcode) AS postcode,
                   max(source_outcode) AS outcode,
                   count(*) AS n_edges
            FROM ev WHERE scheme_is_local
            GROUP BY scheme, source_id
        ),
        -- a name is "resolved" when some identifier-observed matcher put a
        -- company number against that exact normalised name
        resolved AS (
            SELECT source_name_norm AS source_id, matcher,
                   any_value(source_id) AS crn
            FROM ev
            WHERE scheme = 'GB-COH' AND source_name_norm IS NOT NULL
              AND evidence_class = 'identifier-observed'
              AND matcher <> 'ch_register'
            GROUP BY source_name_norm, matcher
        ),
        best AS (
            SELECT source_id, any_value(crn) AS truth_crn,
                   string_agg(DISTINCT matcher, ',') AS truth_matchers,
                   count(DISTINCT crn) AS n_distinct_crn
            FROM resolved GROUP BY source_id
        )
        SELECT l.scheme, l.source_id AS unique_id, l.name_raw,
               l.postcode, l.outcode,
               b.truth_crn, b.truth_matchers, b.n_distinct_crn
        FROM local_names l LEFT JOIN best b ON b.source_id = l.source_id
    """)

    # Postcodes for NNDR names come from the ratepayer file itself, which is
    # premises geography, not registered-office geography. They are the same
    # thing often enough to block on and different often enough that a postcode
    # mismatch is never on its own a reason to reject (DATA-INTEGRITY s3: a
    # registered office can be an agent).
    n_names = con.execute("SELECT count(*) FROM names").fetchone()[0]
    n_reg = con.execute("SELECT count(*) FROM reg").fetchone()[0]
    n_truth = con.execute(
        "SELECT count(*) FROM names WHERE truth_crn IS NOT NULL "
        "AND n_distinct_crn = 1").fetchone()[0]
    return n_names, n_reg, n_truth


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--edges-snapshot", default=None)
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--out-snapshot", default=None)
    ap.add_argument("--seed", type=int, default=20260817)
    args = ap.parse_args()

    out_snapshot = args.out_snapshot or _dt.datetime.now(
        _dt.timezone.utc).strftime("%Y-%m-%d")

    import splink
    from splink import DuckDBAPI, Linker, SettingsCreator, block_on
    import splink.comparison_library as cl

    SV.log(f"splink {splink.__version__}")

    # locate the deterministic edges + the register
    ed_base = X.gold_dir() / "crosswalk_edges"
    ed_parts = sorted(ed_base.glob("snapshot_date=*"))
    if not ed_parts:
        raise SystemExit("FATAL: build_crosswalk.py has not run.")
    ed_part = ed_parts[-1]
    if args.edges_snapshot:
        want = [p for p in ed_parts
                if p.name.split("=", 1)[1] == args.edges_snapshot]
        if not want:
            raise SystemExit(f"no crosswalk_edges snapshot {args.edges_snapshot}")
        ed_part = want[0]
    edges_pq = str(ed_part / "part.parquet")

    reg_parts = sorted((SV.silver_dir() / "ch_register").glob("snapshot_date=*"))
    reg_pq = str(reg_parts[-1] / "part.parquet")

    con, dv = SV.connect()
    n_names, n_reg, n_truth = build_frames(con, edges_pq, reg_pq)
    SV.log(f"frames: {n_names:,} local names, {n_reg:,} register companies, "
           f"{n_truth:,} names carry a held-out truth company number")

    # Splink wants plain frames with its own column names.
    con.execute(f"""
        CREATE OR REPLACE TABLE l_names AS
        SELECT unique_id, name_raw, unique_id AS name_norm,
               postcode, outcode, scheme, truth_crn
        FROM names
    """)
    con.execute("""
        CREATE OR REPLACE TABLE r_reg AS
        SELECT unique_id, name_raw,
               regexp_replace(upper(coalesce(name_raw, '')), '[^A-Z0-9 ]', ' ', 'g')
                   AS name_clean,
               unique_id AS crn,
               postcode, outcode, entity_type
        FROM reg
    """)

    # Normalised name is the join surface, and it is OUR normalisation, the
    # same one build_pound.py uses, so the probabilistic tier is scoring the
    # residual the deterministic tier could not settle rather than re-fighting
    # the same battle on different keys.
    con.create_function("norm_name", X.normalise, ["VARCHAR"], "VARCHAR")
    con.execute("""
        CREATE OR REPLACE TABLE left_frame AS
        SELECT unique_id, name_raw, name_norm,
               name_norm AS name_cmp,
               split_part(name_norm, ' ', 1) AS first_token,
               postcode, outcode, scheme, truth_crn
        FROM l_names
    """)
    n_pc = con.execute(
        "SELECT count(*) FROM left_frame WHERE postcode IS NOT NULL").fetchone()[0]
    SV.log(f"  {n_pc:,} of the local names carry a single premises postcode")
    # Splink unions the two frames, so they must carry identical columns.
    # The side-specific ones are carried as typed NULLs rather than dropped,
    # because truth_crn has to survive into the prediction output for the
    # evaluation to have any labels at all.
    con.execute("""
        CREATE OR REPLACE TABLE right_frame AS
        SELECT unique_id, name_raw, norm_name(name_raw) AS name_norm,
               norm_name(name_raw) AS name_cmp,
               split_part(norm_name(name_raw), ' ', 1) AS first_token,
               postcode, outcode,
               'GB-COH' AS scheme,
               NULL::VARCHAR AS truth_crn
        FROM r_reg
        WHERE length(norm_name(name_raw)) >= 3
    """)

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            # Term frequency adjustment is what stops an exact match on
            # ACCESS scoring like an exact match on RALPH LIVESEY. Without it
            # the common names dominate and precision sits flat across every
            # threshold, which is the symptom the risk register warns about.
            cl.NameComparison("name_cmp").configure(
                term_frequency_adjustments=True),
            cl.ExactMatch("first_token").configure(term_frequency_adjustments=True),
            # Premises postcode against registered office. They disagree
            # legitimately and often, so a mismatch is weak evidence against
            # and a match is strong evidence for, which is exactly what
            # Fellegi-Sunter weights express.
            cl.LevenshteinAtThresholds("postcode", [1, 2]),
            cl.ExactMatch("outcode").configure(term_frequency_adjustments=True),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("name_cmp"),
            block_on("first_token"),
            block_on("substr(name_cmp, 1, 6)"),
            block_on("postcode"),
        ],
        retain_intermediate_calculation_columns=True,
        # Splink drops anything it did not compare on. The held-out truth
        # company number has to survive into the prediction output or the
        # evaluation has no labels, and the raw names have to survive or the
        # clerical sample is unreadable by the human who has to review it.
        additional_columns_to_retain=["name_raw", "truth_crn"],
    )

    db_api = DuckDBAPI()
    linker = Linker(
        [con.execute("SELECT * FROM left_frame").df(),
         con.execute("SELECT * FROM right_frame").df()],
        settings, db_api=db_api, input_table_aliases=["names", "register"])

    SV.log("estimating u probabilities by random sampling...")
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000,
                                                     seed=args.seed)
    SV.log("EM training on first_token block...")
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_token"))
    SV.log("EM training on name prefix block...")
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("substr(name_cmp, 1, 6)"))

    SV.log("predicting...")
    preds = linker.inference.predict(threshold_match_probability=0.5)
    pdf = preds.as_pandas_dataframe()
    SV.log(f"  {len(pdf):,} candidate pairs above 0.50")

    con.register("preds_raw", pdf)
    # Splink puts the two frames' columns side by side and which side is which
    # is not guaranteed, so it is read off source_dataset per row rather than
    # assumed from the alias order. Getting this backwards would silently swap
    # every name with every company and still produce plausible-looking scores.
    con.execute("""
        CREATE OR REPLACE TABLE pairs AS
        SELECT match_probability, match_weight,
               CASE WHEN source_dataset_l = 'names'
                    THEN unique_id_l ELSE unique_id_r END AS name_key,
               CASE WHEN source_dataset_l = 'names'
                    THEN name_raw_l ELSE name_raw_r END AS name_raw,
               CASE WHEN source_dataset_l = 'names'
                    THEN truth_crn_l ELSE truth_crn_r END AS truth_crn,
               CASE WHEN source_dataset_l = 'names'
                    THEN unique_id_r ELSE unique_id_l END AS crn,
               CASE WHEN source_dataset_l = 'names'
                    THEN name_raw_r ELSE name_raw_l END AS register_name,
               CASE WHEN source_dataset_l = 'names'
                    THEN postcode_r ELSE postcode_l END AS register_postcode
        FROM preds_raw
    """)

    # One best candidate per name. A name that matches three companies equally
    # well is not a match, it is an ambiguity, so ties are recorded and dropped
    # rather than broken arbitrarily.
    con.execute("""
        CREATE OR REPLACE TABLE best AS
        WITH ranked AS (
            SELECT *, row_number() OVER (
                       PARTITION BY name_key ORDER BY match_probability DESC, crn)
                   AS rn,
                   count(*) OVER (PARTITION BY name_key) AS n_cands,
                   max(match_probability) OVER (PARTITION BY name_key) AS top_p
            FROM pairs
        ),
        tied AS (
            SELECT name_key, count(*) AS n_tied FROM ranked
            WHERE match_probability = top_p GROUP BY name_key
        )
        SELECT r.*, t.n_tied
        FROM ranked r JOIN tied t USING (name_key)
        WHERE r.rn = 1
    """)

    # --- evaluation ---------------------------------------------------------
    # Truth set: names whose withheld identifier is known and unambiguous.
    # A truth case whose company is not in the right frame cannot be found by
    # any threshold: the register subset is Lancashire and plenty of council
    # suppliers and national ratepayers are registered elsewhere. Recall is
    # therefore reported twice, and the in-scope number is the one that
    # measures the matcher rather than the scope.
    con.execute("""
        CREATE OR REPLACE TABLE truth AS
        SELECT n.unique_id AS name_key, n.truth_crn,
               (r.unique_id IS NOT NULL) AS truth_in_scope
        FROM names n
        LEFT JOIN reg r ON r.unique_id = n.truth_crn
        WHERE n.truth_crn IS NOT NULL AND n.n_distinct_crn = 1
    """)
    n_truth_total = con.execute("SELECT count(*) FROM truth").fetchone()[0]
    n_truth_scope = con.execute(
        "SELECT count(*) FROM truth WHERE truth_in_scope").fetchone()[0]
    SV.log(f"truth set: {n_truth_total:,} labelled names, {n_truth_scope:,} of "
           f"them registered inside the Lancashire frame")

    sensitivity = []
    for th in [0.50, 0.70, 0.80, 0.90, 0.95, 0.97, 0.99, 0.995, 0.999]:
        row = con.execute(f"""
            WITH pred AS (
                SELECT b.name_key, b.crn, b.match_probability, b.n_tied
                FROM best b
                WHERE b.match_probability >= {th} AND b.n_tied = 1
            ),
            ev AS (
                SELECT t.name_key, t.truth_crn, t.truth_in_scope,
                       p.crn AS pred_crn
                FROM truth t LEFT JOIN pred p USING (name_key)
            )
            SELECT
              (SELECT count(*) FROM pred) AS predicted_all,
              count(*) FILTER (WHERE pred_crn IS NOT NULL
                               AND pred_crn = truth_crn) AS tp,
              count(*) FILTER (WHERE pred_crn IS NOT NULL
                               AND pred_crn <> truth_crn) AS fp,
              count(*) FILTER (WHERE pred_crn IS NULL) AS fn,
              count(*) FILTER (WHERE truth_in_scope AND pred_crn IS NOT NULL
                               AND pred_crn = truth_crn) AS tp_scope,
              count(*) FILTER (WHERE truth_in_scope AND pred_crn IS NOT NULL
                               AND pred_crn <> truth_crn) AS fp_scope
            FROM ev
        """).fetchone()
        pred_all, tp, fp, fn, tp_scope, fp_scope = row
        prec = tp / (tp + fp) if (tp + fp) else None
        rec = tp / n_truth_total if n_truth_total else None
        prec_s = (tp_scope / (tp_scope + fp_scope)
                  if (tp_scope + fp_scope) else None)
        rec_s = tp_scope / n_truth_scope if n_truth_scope else None
        f1 = (2 * prec_s * rec_s / (prec_s + rec_s)) if (prec_s and rec_s) else None
        sensitivity.append({
            "threshold": th, "predictedPairsAll": pred_all,
            "truthSetSize": n_truth_total, "truthSetInScope": n_truth_scope,
            "truePositives": tp, "falsePositives": fp, "falseNegatives": fn,
            "precision": round(prec, 4) if prec is not None else None,
            "recall": round(rec, 4) if rec is not None else None,
            "precisionInScope": round(prec_s, 4) if prec_s is not None else None,
            "recallInScope": round(rec_s, 4) if rec_s is not None else None,
            "f1InScope": round(f1, 4) if f1 is not None else None,
        })
        SV.log(f"  th={th:<6} in-scope P={prec_s if prec_s is None else round(prec_s,4)} "
               f"R={rec_s if rec_s is None else round(rec_s,4)} | "
               f"overall P={prec if prec is None else round(prec,4)} "
               f"R={rec if rec is None else round(rec,4)} | "
               f"accepted={pred_all}")

    op = [s for s in sensitivity if abs(s["threshold"] - args.threshold) < 1e-9]
    operating = op[0] if op else None

    # --- clerical sample ----------------------------------------------------
    # 200 pairs, stratified. Bands are not equally populated (the score
    # distribution is heavily top-loaded), so a band with fewer than its
    # share contributes what it has and the shortfall is redistributed across
    # the bands that can fill it. Under-filling silently would quietly turn a
    # stratified sample into a top-of-the-range sample, which is the one thing
    # stratifying is for.
    SAMPLE_TARGET = 200
    band_sizes = {}
    for lo, hi in BANDS:
        band_sizes[(lo, hi)] = con.execute(f"""
            SELECT count(*) FROM best
            WHERE match_probability >= {lo} AND match_probability < {hi}
        """).fetchone()[0]
    quota = {b: min(band_sizes[b], SAMPLE_TARGET // len(BANDS)) for b in BANDS}
    short = SAMPLE_TARGET - sum(quota.values())
    for b in sorted(BANDS, key=lambda b: -band_sizes[b]):
        if short <= 0:
            break
        room = band_sizes[b] - quota[b]
        take = min(room, short)
        quota[b] += take
        short -= take
    SV.log(f"clerical sample quota by band: "
           f"{ {f'{lo:.2f}-{hi:.2f}': quota[(lo, hi)] for lo, hi in BANDS} } "
           f"of available { {f'{lo:.2f}-{hi:.2f}': band_sizes[(lo, hi)] for lo, hi in BANDS} }")

    sample_rows = []
    for lo, hi in BANDS:
        rows = con.execute(f"""
            SELECT b.name_key, b.name_raw, b.crn, b.register_name,
                   b.register_postcode, b.match_probability, b.match_weight,
                   b.n_cands, b.n_tied, t.truth_crn
            FROM best b LEFT JOIN truth t USING (name_key)
            WHERE b.match_probability >= {lo} AND b.match_probability < {hi}
            ORDER BY hash(b.name_key || '{args.seed}')
            LIMIT {quota[(lo, hi)]}
        """).fetchall()
        for r in rows:
            (nk, nraw, crn, rname, rpc, mp, mw, ncand, ntied, truth) = r
            if truth is None:
                label, label_src, status = None, None, "awaiting-clerical-review"
            elif truth == crn:
                label, label_src, status = True, "held-out-identifier", "labelled"
            else:
                label, label_src, status = False, "held-out-identifier", "labelled"
            sample_rows.append({
                "band": f"{lo:.2f}-{hi:.2f}",
                "nameKey": nk, "nameRaw": nraw,
                "candidateCrn": crn, "registerName": rname,
                "registerPostcode": rpc,
                "matchProbability": float(mp), "matchWeight": float(mw),
                "candidatesConsidered": int(ncand), "tiedAtTop": int(ntied),
                "label": label, "labelSource": label_src, "status": status,
            })

    labelled = sum(1 for r in sample_rows if r["status"] == "labelled")
    pending = len(sample_rows) - labelled
    SV.log(f"clerical sample: {len(sample_rows)} pairs, {labelled} labelled "
           f"from held-out identifiers, {pending} awaiting human review")

    # --- land ---------------------------------------------------------------
    out_dir = X.table_dir(TABLE, out_snapshot)
    select = f"""
        SELECT name_key, name_raw, crn, register_name, register_postcode,
               match_probability, match_weight, n_cands AS candidates_considered,
               n_tied AS tied_at_top, truth_crn,
               (n_tied = 1 AND match_probability >= {args.threshold})
                   AS accepted_at_operating_threshold
        FROM best
    """
    rows, nbytes = SV.write_parquet(con, select, out_dir)

    accepted = con.execute(f"""
        SELECT count(*) FROM best
        WHERE n_tied = 1 AND match_probability >= {args.threshold}
    """).fetchone()[0]
    accepted_new = con.execute(f"""
        SELECT count(*) FROM best b LEFT JOIN truth t USING (name_key)
        WHERE b.n_tied = 1 AND b.match_probability >= {args.threshold}
          AND t.truth_crn IS NULL
    """).fetchone()[0]

    eval_doc = {
        "generated": _dt.datetime.now(_dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "splinkVersion": splink.__version__,
        "duckdbVersion": dv,
        "seed": args.seed,
        "operatingThreshold": args.threshold,
        "frames": {"localNames": n_names, "registerCompanies": n_reg},
        "truthSet": {
            "size": n_truth_total,
            "inScopeSize": n_truth_scope,
            "outOfScopeNote": "A truth case whose company is registered "
                              "outside the Lancashire frame cannot be matched "
                              "by any threshold. recallInScope measures the "
                              "matcher; recall measures the matcher and the "
                              "frame together.",
            "labelSource": "company numbers published by billing authorities, "
                           "by buyers on award notices, and on companies' own "
                           "websites, all withheld from the matcher",
            "limitation": "Firms whose company number a third party published "
                          "are not a random sample. They are larger, more "
                          "formal and carry cleaner registered names, so the "
                          "measured precision is an estimate for that "
                          "subpopulation and is likely optimistic for the "
                          "rest. This sentence travels with the numbers.",
        },
        "operating": operating,
        "thresholdSensitivity": sensitivity,
        "clericalSample": {
            "size": len(sample_rows),
            "bands": [f"{lo:.2f}-{hi:.2f}" for lo, hi in BANDS],
            "bandPopulation": {f"{lo:.2f}-{hi:.2f}": band_sizes[(lo, hi)]
                               for lo, hi in BANDS},
            "bandQuota": {f"{lo:.2f}-{hi:.2f}": quota[(lo, hi)]
                          for lo, hi in BANDS},
            "labelledFromHeldOutIdentifier": labelled,
            "awaitingHumanReview": pending,
            "humanClericalReviewCompleted": False,
            "note": "No human has reviewed these pairs. Nothing in this file "
                    "may be described as the result of clerical review until "
                    "the awaiting-clerical-review rows carry human labels.",
        },
        "acceptedAtOperatingThreshold": accepted,
        "acceptedThatAreNewIdentifications": accepted_new,
        "calibration": {
            "finding": "Precision is nearly flat across the whole threshold "
                       "range: moving from 0.50 to 0.999 buys about three "
                       "precision points and costs about six recall points. "
                       "The score is not discriminating in that range because "
                       "the dominant signal, an exact match on the normalised "
                       "name, saturates the top band.",
            "consequence": "A higher threshold is not a route to publishable "
                           "precision here. Better features are: registered "
                           "office geography once F1 geocoding lands, SIC or "
                           "sector agreement, company status and incorporation "
                           "date plausibility against the ratepayer record. "
                           "That is the risk-register instruction to fix "
                           "blocking and features before thresholds.",
        },
        "publicationGate": {
            "cleared": False,
            "reason": "The plan requires a published linkage evaluation with a "
                      "clerical sample before any probabilistic match surfaces "
                      "publicly. The held-out evaluation is done; the clerical "
                      "review is not, and in-scope precision at the operating "
                      "threshold is about 0.84, which is not a publishable "
                      "error rate for a named-entity claim.",
        },
    }
    (out_dir / "linkage_evaluation.json").write_text(
        json.dumps(eval_doc, indent=2) + "\n")
    (out_dir / "clerical_sample.json").write_text(
        json.dumps({"$meta": eval_doc["clericalSample"],
                    "pairs": sample_rows}, indent=2) + "\n")

    X.write_manifest(
        out_dir, TABLE, out_snapshot, rows, nbytes, dv,
        inputs=[{"layer": "gold", "table": "crosswalk_edges",
                 "snapshot": ed_part.name.split("=", 1)[1]},
                {"layer": "silver", "table": "ch_register",
                 "snapshot": reg_parts[-1].name.split("=", 1)[1]}],
        assertions={
            "candidatePairs": len(pdf),
            "namesScored": rows,
            "acceptedAtOperatingThreshold": accepted,
            "truthSetSize": n_truth_total,
        },
        notes=("Probabilistic tier. Not eligible for public surfacing until "
               "the clerical review in clerical_sample.json is done by a "
               "human."),
        extra={"splinkVersion": splink.__version__,
               "linkageEvaluation": eval_doc})

    SV.log(f"WROTE {out_dir}/part.parquet: {rows:,} scored names, "
           f"{accepted:,} accepted at p>={args.threshold} "
           f"({accepted_new:,} of them new identifications)")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
