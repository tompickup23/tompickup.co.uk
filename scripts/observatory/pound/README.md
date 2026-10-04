# Public Pound: resolution waterfall (Phase 1)

Code for the Lancashire Public Pound's payee resolution, ownership walk, verification tables, gold-set draw and coverage. The specification is in the private Reports repo: `Public_Pound_work/phase0/WATERFALL.md`, `GOLD_SET.md` and `RULES.md` (decisions 7 to 11, 4 October 2026). Nothing here publishes, deploys or writes to the warehouse. Claude never writes `resolver.csv`: the pipeline writes `resolver_proposed.csv`, and only rows a named person accepts become the resolver.

## Where it runs

On vps-main, in `/root/pp-phase1c` (outside `/opt/observatory`), reading bronze, silver and gold read-only, with `uv run` and DuckDB pinned to 1.4. `run_vps.sh` copies a clean checkout there and stamps the run with its HEAD; a dirty tree refuses to run.

Inputs copied into `/root/pp-phase1c/inputs` from the Mac: `payee_keys.jsonl.gz` and `council_supplier_spend.json` (from `aggregate_spend.py --payee-keys-out`, PR #31), `public_bodies.csv`, `central_government_bodies.csv` and `council_company_seed.csv` (Reports `phase1/sources`), `ocds_supplier_ids.json` and the fourteen `procurement_finder.json` files.

## Order

```
scripts/observatory/pound/run_vps.sh extract.py            # register extracts, extract_manifest.json
scripts/observatory/pound/run_vps.sh waterfall.py          # resolver_proposed.csv, queue.csv, payee_key_values.csv
scripts/observatory/pound/run_vps.sh ownership.py --build  # bods_edges.parquet, ownership_walk.csv
scripts/observatory/pound/run_vps.sh verify.py             # verify_<body>.csv, verify_suppliers.csv
scripts/observatory/pound/run_vps.sh tests_waterfall.py    # the five WATERFALL.md s5 tests; exit 1 on failure
scripts/observatory/pound/run_vps.sh gold_set.py           # gold_set_manifest.json, then the unlabelled pairs
scripts/observatory/pound/run_vps.sh coverage.py           # coverage.json, coverage.csv
```

No result is read before `tests_waterfall.py` passes. `--bodies burnley` runs the pilot.

## Files

| File | What it does |
|---|---|
| `common.py` | Paths, company-number normaliser, distinctive-token test, sha256 |
| `extract.py` | Register extracts: CH names (current and previous, three silver snapshots), Charity Commission, CQC HSCA, public bodies, GIAS trusts, GLEIF Level 1, OCDS (OCP Find a Tender, `ocds_supplier_ids.json`, `procurement_finder.json`), GGIS, RSH and OSCR |
| `waterfall.py` | Steps 1, 1b, 2, 2b, 2c, 3, 4, 5, 5b, 6, 7; the step 9 queue; step 10 reasons including `payment-route`. Step 8 (Splink) is not built in Phase 1 |
| `ownership.py` | BODS UK edges as time intervals; GLEIF Level 2 and exceptions; recursive walk over edges with a voting band of more than 50%; rule 2.2 classes; cycle count |
| `verify.py` | Per-body verification sets (top 80% by value per year) and a supplier-level deduplicated table |
| `tests_waterfall.py` | WATERFALL.md s5 tests 1 to 5, plus step 5b against the council-company seed list |
| `gold_set.py` | Stratified clerical sample and recall sample, manifest first |
| `coverage.py` | Rule 2.4 coverage per body and year |

## Privacy

Individual and redacted payee keys are written as a hash. No PSC, trustee, manager or contact name is read from any register; persons in BODS are carried as a count and a country of residence only. Test 5 checks every output against the individual PSC names in gold `mart_psc_lancs`.
