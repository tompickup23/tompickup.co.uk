#!/usr/bin/env python3
"""Gates V-T3, V-R3 and V-L1: the serve layer, validated against its schemas.

`validate_outputs.py` checks cross-file invariants (does the index agree with
the dossiers, do the tier percentages sum). It is unmodified and stays the last
gate before deploy. This runs one layer earlier and answers a different
question: is each file the SHAPE it is contracted to be, and does every claim
that needs a caption carry one.

Four things happen here.

1. **JSON Schema per serve family.** `schemas/*.schema.json`, one per published
   file plus one for a company dossier. The same schema files are what
   `check-jsonschema` runs in CI, so a local pass and a CI pass are the same
   assertion rather than two similar ones.

2. **V-T3, modelled fields carry their caption.** Expressed inside
   `biz-areas.schema.json` as a conditional: an `unregisteredModelled` integer
   requires an `unregisteredNote` that contains the word "modelled". A modelled
   count published as a count is failure mode 1 in the rulebook.

3. **V-R3, now enforced.** The gate wants asAt, retrievedAt and licence on
   every `$meta.sources[]` entry. The published contract carried none of the
   first and the wrong shape of the second, and two files carried no sources
   array at all. `scripts/observatory/sources_meta.py` now builds the block
   once, dating each entry from the input it describes, and all three files
   that publish one read it from there. The driver runs this with
   `--enforce-vr3`, so a source entry that loses its dates fails the build
   rather than being counted in a report nobody reads. Without the flag the gap
   is still measured and reported, which is what a bare local run does.

4. **V-L1, the restricted-licence tripwire.** No published file currently draws
   on a non-OGL source. If one ever does, the file has to carry the attribution
   or terms block that licence demands. The check is forward-looking on purpose:
   it fires the first time a restricted source appears, not after somebody
   notices on the page.

Usage:
    check_schemas.py --serve /opt/observatory/m5/run/public/data
    check_schemas.py --serve public/data --dossiers --out reports/schemas.json
    check_schemas.py --serve public/data --enforce-vr3
"""
import argparse
import datetime as _dt
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import driver as D  # noqa: E402
import staleness as ST  # noqa: E402

HERE = Path(__file__).resolve().parent
SCHEMAS = HERE / "schemas"

# file -> schema stem. A published file with no schema is itself a finding, so
# the map is explicit rather than a glob.
FAMILIES = {
    "biz-overview.json": "biz-overview",
    "biz-areas.json": "biz-areas",
    "biz-watch.json": "biz-watch",
    "biz-growth.json": "biz-growth",
    "biz-pound.json": "biz-pound",
    "biz-innovation.json": "biz-innovation",
    "biz-money.json": "biz-money",
    "biz-companies-index.json": "biz-companies-index",
    "biz-changes.json": "biz-changes",
}

# Declared serve-layer faults, the same device the mart manifests use for
# `reproducedFaults`. A schema error matching one of these is counted and named
# rather than failing the build, because fixing it changes a published file and
# that is Tom's call, not a build session's.
#
# The alternative was to weaken the schema so the fault stops being visible,
# which is how a rulebook quietly stops meaning anything.
KNOWN_SERVE_FAULTS = []

# Faults that WERE declared here and are now fixed. The record stays, with the
# commit that fixed it, because a fault that vanishes from the register without
# trace is indistinguishable from one nobody ever found. An entry here that
# still matches a live schema error is a regression, and the gate says so.
CLEARED_SERVE_FAULTS = [
    {
        "id": "F7",
        "ref": "DATA-INTEGRITY s4 rule 1 (gate V-R2), s13.3",
        "file": "biz-areas.json",
        "pathPattern": r"^(east|north|south|west)-lancashire(-unitary)?/wholeEconomy$",
        "messagePattern": r"'selfEmploymentIncomeYear' is a required property",
        "what": ("the four 2028 unitary rollups published an HMRC "
                 "self-employment income figure with no year label, while the "
                 "14 districts carried one. build_site_json.py rolls up only "
                 "numeric keys, then re-added unregisteredNote by hand and did "
                 "not re-add selfEmploymentIncomeYear."),
        "rowsAffected": 4,
        "fix": ("build_site_json.py now carries both whole-economy captions "
                "across the unitary rollup in one loop, so adding a third "
                "cannot leave one behind."),
        "clearedIn": "fix/observatory-faults-f2-f7",
    },
]


def regressions(errs):
    """A cleared fault that matches a live error again is a regression."""
    out = []
    for e in errs:
        for f in CLEARED_SERVE_FAULTS:
            if (e["file"] == f["file"]
                    and re.search(f["pathPattern"], e["path"])
                    and re.search(f["messagePattern"], e["message"])):
                out.append(dict(e, regressedFaultId=f["id"], ref=f["ref"]))
                break
    return out


def classify(errs):
    """Split schema errors into declared faults and real failures."""
    declared, hard = [], []
    for e in errs:
        for f in KNOWN_SERVE_FAULTS:
            if (e["file"] == f["file"]
                    and re.search(f["pathPattern"], e["path"])
                    and re.search(f["messagePattern"], e["message"])):
                declared.append(dict(e, faultId=f["id"], ref=f["ref"]))
                break
        else:
            hard.append(e)
    return declared, hard


# House style, verified both ways: a literal em-dash and the entity encodings
# that render identically and score zero on a literal grep.
DASH_ENTITIES = re.compile(r"&mdash;|&ndash;|&#8212;|&#8211;|&#x2014;|&#x2013;",
                           re.I)


def validate_one(validator_cls, schema, instance, label, limit=8):
    v = validator_cls(schema)
    errs = []
    for e in sorted(v.iter_errors(instance), key=lambda x: list(x.path)):
        path = "/".join(str(p) for p in e.path) or "(root)"
        errs.append({"file": label, "path": path, "message": e.message[:300]})
        if len(errs) >= limit:
            errs.append({"file": label, "path": "(truncated)",
                         "message": "further errors suppressed"})
            break
    return errs


def measure_vr3(serve, files, enforced=False):
    """Count the V-R3 gap exactly, and say whether it is a gate today."""
    total = missing_asat = missing_retrieved = missing_licence = 0
    no_sources = []
    for name in files:
        p = serve / name
        if not p.exists():
            continue
        meta = json.loads(p.read_text()).get("$meta", {})
        src = meta.get("sources")
        if not src:
            no_sources.append(name)
            continue
        for s in src:
            total += 1
            if not s.get("asAt"):
                missing_asat += 1
            if not (s.get("retrievedAt") or s.get("retrieved")):
                missing_retrieved += 1
            if not s.get("licence"):
                missing_licence += 1
    return {
        "gate": "V-R3",
        "status": "ENFORCED" if enforced else "PENDING",
        "sourceEntries": total,
        "missingAsAt": missing_asat,
        "missingRetrieved": missing_retrieved,
        "missingLicence": missing_licence,
        "filesWithNoSourcesArray": no_sources,
        "why": ("The published $meta.sources[] contract carried no asAt until "
                "the source block moved into sources_meta.py, which dates each "
                "entry from the input it describes. Bronze already satisfied "
                "V-R3 in full; the serve layer now does too, so the gate is "
                "switched on in the driver rather than counted."),
        "fix": ("scripts/observatory/sources_meta.py builds the block once, "
                "with asAt, asAtBasis, retrievedAt and licence on every entry; "
                "build_site_json, build_diff and build_dossiers all emit it, "
                "so biz-changes.json and biz-companies-index.json now carry a "
                "sources array of their own."),
    }


def check_vl1(serve, files):
    """V-L1: a restricted-licence source may not appear without its block."""
    findings = []
    restricted = ST.RESTRICTED_LICENCE
    for name in files:
        p = serve / name
        if not p.exists():
            continue
        d = json.loads(p.read_text())
        meta = d.get("$meta", {})
        blob = json.dumps(meta.get("sources") or []).lower()
        notes = " ".join(meta.get("notes") or []).lower()
        for sid, requirement in restricted.items():
            token = sid.replace("_", " ")
            if token in blob or sid in blob:
                # The attribution test is deliberately weak (does the file say
                # anything about the licence at all) because the exact wording
                # lives in LEGAL.md and belongs on the page, not in a regex.
                # A weak positive test beats no test: today it fires zero times
                # and the day a restricted source lands it fires once.
                if "licence" not in notes and "attribution" not in notes:
                    findings.append({
                        "file": name, "source": sid,
                        "requirement": requirement,
                        "message": "restricted-licence source present with no "
                                   "licence or attribution note in $meta.notes",
                    })
    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", required=True,
                    help="a public/data directory")
    ap.add_argument("--dossiers", action="store_true",
                    help="also validate every public/data/company/*.json")
    ap.add_argument("--out", default=None)
    ap.add_argument("--enforce-vr3", action="store_true",
                    help="make V-R3 a hard gate. Flip this the day the "
                         "$meta.sources contract gains asAt.")
    args = ap.parse_args()

    from jsonschema import Draft202012Validator as V

    serve = Path(args.serve)
    if not serve.exists():
        raise SystemExit(f"FATAL: no serve directory at {serve}")

    errors, declared, checked = [], [], []
    for name, stem in FAMILIES.items():
        sp = SCHEMAS / f"{stem}.schema.json"
        if not sp.exists():
            errors.append({"file": name, "path": "(schema)",
                           "message": f"no schema file {sp.name}"})
            continue
        fp = serve / name
        if not fp.exists():
            errors.append({"file": name, "path": "(file)",
                           "message": "published file missing from the serve "
                                      "directory"})
            continue
        schema = json.loads(sp.read_text())
        errs = validate_one(V, schema, json.loads(fp.read_text()), name)
        dec, hard_errs = classify(errs)
        errors.extend(hard_errs)
        declared.extend(dec)
        checked.append(name)
        mark = "ok  " if not hard_errs else "FAIL"
        extra = f", {len(dec)} declared fault(s)" if dec else ""
        print(f"  {mark} {name} ({len(hard_errs)} error(s), "
              f"schema {sp.name}{extra})")

    dossier_errors, n_dossiers = [], 0
    if args.dossiers:
        schema = json.loads((SCHEMAS / "company-dossier.schema.json").read_text())
        for p in sorted((serve / "company").glob("*.json")):
            n_dossiers += 1
            e = validate_one(V, schema, json.loads(p.read_text()),
                             f"company/{p.name}", limit=3)
            dossier_errors.extend(e)
        bad = len({e["file"] for e in dossier_errors})
        print(f"  {'ok  ' if not dossier_errors else 'FAIL'} "
              f"{n_dossiers} dossiers ({bad} with errors)")

    # House style, over the data files themselves. Both forms, because an
    # entity-encoded dash renders identically and scores zero on a literal grep.
    dash = []
    for p in sorted(serve.glob("biz-*.json")):
        t = p.read_text()
        n_lit = t.count("—") + t.count("–")
        n_ent = len(DASH_ENTITIES.findall(t))
        if n_lit or n_ent:
            dash.append({"file": p.name, "literal": n_lit, "entity": n_ent})
    print(f"  {'ok  ' if not dash else 'FAIL'} house style: "
          f"{len(dash)} file(s) carry a dash")

    vr3 = measure_vr3(serve, FAMILIES, args.enforce_vr3)
    vl1 = check_vl1(serve, FAMILIES)
    vr3_ok = not (vr3["missingAsAt"] or vr3["missingRetrieved"]
                  or vr3["filesWithNoSourcesArray"])
    print(f"  {'ok  ' if vr3_ok else 'FAIL'} V-R3 {vr3['status']}: "
          f"{vr3['missingAsAt']} of {vr3['sourceEntries']} source entries "
          f"carry no asAt, {vr3['missingRetrieved']} no retrieved date, "
          f"{len(vr3['filesWithNoSourcesArray'])} file(s) carry no sources "
          "array at all")
    print(f"  {'ok  ' if not vl1 else 'FAIL'} V-L1: "
          f"{len(vl1)} restricted-licence finding(s)")
    for f in KNOWN_SERVE_FAULTS:
        hit = [d for d in declared if d["faultId"] == f["id"]]
        print(f"  declared {f['id']} ({f['ref']}): {len(hit)} of "
              f"{f['rowsAffected']} expected occurrence(s)")
    regressed = regressions(errors)
    for f in CLEARED_SERVE_FAULTS:
        hit = [r for r in regressed if r["regressedFaultId"] == f["id"]]
        print(f"  {'ok  ' if not hit else 'FAIL'} cleared {f['id']} "
              f"({f['ref']}): {len(hit)} occurrence(s), expected 0")

    report = {
        "gates": ["V-T3", "V-R3", "V-L1"],
        "runId": D.run_id(),
        "pipelineGitSha": D.pipeline_git_sha(),
        "generated": _dt.datetime.now(_dt.timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "serve": str(serve),
        "filesChecked": checked,
        "dossiersChecked": n_dossiers,
        "schemaErrors": errors,
        "declaredServeFaults": declared,
        "knownServeFaults": KNOWN_SERVE_FAULTS,
        "clearedServeFaults": CLEARED_SERVE_FAULTS,
        "regressedServeFaults": regressed,
        "dossierErrors": dossier_errors[:50],
        "dossierErrorCount": len(dossier_errors),
        "houseStyleFindings": dash,
        "pendingGates": [vr3],
        "vl1Findings": vl1,
    }
    out = Path(args.out) if args.out else D.report_dir() / "schemas.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"  written to {out}")

    hard = errors + dossier_errors + dash + vl1
    if args.enforce_vr3 and (vr3["missingAsAt"] or vr3["missingRetrieved"]
                             or vr3["filesWithNoSourcesArray"]):
        hard.append({"file": "(all)", "path": "$meta.sources",
                     "message": "V-R3 enforced and not satisfied"})
    if hard:
        for e in (errors + dossier_errors)[:20]:
            print(f"    {e['file']} {e['path']}: {e['message']}")
        print(f"\nSCHEMA GATES FAILED: {len(hard)} finding(s)")
        return 1
    print(f"\nSCHEMA GATES GREEN (V-R3 {vr3['status'].lower()})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
