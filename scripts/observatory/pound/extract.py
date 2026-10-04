#!/usr/bin/env python3
"""Build the waterfall's register extracts from bronze, silver and gold.

Each extract is a Parquet file in $POUND_WORK/extract with one row per
(normalised name, register record), plus the attributes its step needs.
Nothing personal is carried: CQC rows whose provider is an individual keep
their Provider ID only (name blanked); no trustee, manager, nominated
individual, contact or PSC name is read. extract_manifest.json lists every
input file with its sha256 and the row count of every extract.

Run on vps-main:
  uv run --no-project --with 'duckdb>=1.4,<1.5' --with python-calamine \
      python scripts/observatory/pound/extract.py [--only reg,charity,...]
"""
import argparse
import csv
import glob
import gzip
import io
import json
import zipfile
from collections import defaultdict
from pathlib import Path

import duckdb

from common import (BRONZE, EXTRACT, GOLD, INPUTS, SILVER, WORK, norm_crn,
                    norm_pc, normalise, sha256_file, write_json, git_sha)

MANIFEST = {"inputs": [], "extracts": {}}


def note_input(path, role):
    p = Path(path)
    MANIFEST["inputs"].append({"role": role, "path": str(p),
                               "bytes": p.stat().st_size,
                               "sha256": sha256_file(p)})


PA_TYPES = {"VARCHAR": "string", "BOOLEAN": "bool_", "INTEGER": "int64"}


def write(con, name, rows, schema):
    """rows: list of tuples matching schema [(col, duckdb type)], written
    through an Arrow table (DuckDB's executemany is row by row and slow)."""
    import pyarrow as pa
    EXTRACT.mkdir(parents=True, exist_ok=True)
    out = EXTRACT / f"{name}.parquet"
    cols = list(zip(*rows)) if rows else [[] for _ in schema]
    tbl = pa.table({c: pa.array(list(v), type=getattr(pa, PA_TYPES[t])())
                    for (c, t), v in zip(schema, cols)})
    con.register("_x", tbl)
    con.execute(f"COPY (SELECT * FROM _x) TO '{out}' (FORMAT parquet)")
    con.unregister("_x")
    MANIFEST["extracts"][name] = {"rows": len(rows), "sha256": sha256_file(out)}
    print(f"  {name}: {len(rows)} rows")


def bronze_files(source, pattern="*"):
    parts = sorted(glob.glob(str(BRONZE / f"source={source}" / "snapshot_date=*")))
    if not parts:
        raise SystemExit(f"no bronze partition for {source}")
    files = sorted(glob.glob(str(Path(parts[-1]) / pattern)))
    return [f for f in files if not f.endswith("manifest.json")]


# ------------------------------------------------------------ CH register --
def ex_register(con):
    """Every name (current and previous) of every company in every silver
    ch_register snapshot. Live companies only: the monthly basic company file
    lists no dissolved company, so a supplier dissolved before July 2026 is
    in none of these snapshots."""
    parts = sorted(glob.glob(str(SILVER / "ch_register/snapshot_date=*/part.parquet")))
    for p in parts:
        note_input(p, "silver ch_register")
    lancs = sorted(glob.glob(str(GOLD / "mart_register_lancs/snapshot_date=*/part.parquet")))[-1]
    note_input(lancs, "gold mart_register_lancs (Lancashire frame)")
    con.execute(f"CREATE OR REPLACE TEMP TABLE lancs AS SELECT DISTINCT company_number FROM read_parquet('{lancs}')")
    q = f"""
      WITH r AS (SELECT company_number, company_name, company_status, company_category,
                        reg_postcode_norm, snapshot_date, previous_names, incorporation_date
                 FROM read_parquet({parts!r}))
      SELECT company_number, company_name AS name, 'current' AS kind, company_status,
             company_category, reg_postcode_norm, snapshot_date, incorporation_date
      FROM r
      UNION ALL
      SELECT company_number, pn.name, 'previous', company_status, company_category,
             reg_postcode_norm, snapshot_date, incorporation_date
      FROM (SELECT *, unnest(previous_names) AS pn FROM r)
    """
    con.execute(f"CREATE OR REPLACE TEMP TABLE regall AS {q}")
    print(f"  register name rows read: {con.execute('SELECT count(*) FROM regall').fetchone()[0]}")
    # normalise each distinct name once, in Python, then join back in DuckDB
    import pyarrow as pa
    names = [r[0] for r in con.execute("SELECT DISTINCT name FROM regall WHERE name IS NOT NULL").fetchall()]
    norms = [normalise(n) for n in names]
    con.register("_nm", pa.table({"name": pa.array(names, type=pa.string()),
                                  "name_norm": pa.array(norms, type=pa.string())}))
    del names, norms
    out = EXTRACT / "reg_names.parquet"
    con.execute(f"""COPY (
        SELECT name_norm, company_number, kind, arg_max(r.name, snapshot_date) AS name,
               arg_max(company_status, snapshot_date) AS status,
               arg_max(company_status, snapshot_date) = 'Active' AS active,
               arg_max(reg_postcode_norm, snapshot_date) AS postcode_norm,
               max(snapshot_date)::VARCHAR AS snapshot_date,
               arg_max(company_category, snapshot_date) AS category,
               company_number IN (SELECT company_number FROM lancs) AS in_lancs_frame
        FROM regall r JOIN _nm USING (name)
        WHERE name_norm <> ''
        GROUP BY name_norm, company_number, kind) TO '{out}' (FORMAT parquet)""")
    con.unregister("_nm")
    con.execute("DROP TABLE regall")
    MANIFEST["extracts"]["reg_names"] = {
        "rows": con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0], "sha256": sha256_file(out)}
    print(f"  reg_names: {MANIFEST['extracts']['reg_names']['rows']} rows")
    con.execute(f"""COPY (SELECT company_number, max(snapshot_date)::VARCHAR AS last_snapshot,
                         count(DISTINCT snapshot_date) AS snapshots
                  FROM read_parquet({parts!r}) GROUP BY 1)
                  TO '{EXTRACT / "reg_numbers.parquet"}' (FORMAT parquet)""")
    MANIFEST["extracts"]["reg_numbers"] = {
        "rows": con.execute(f"SELECT count(*) FROM '{EXTRACT / 'reg_numbers.parquet'}'").fetchone()[0],
        "sha256": sha256_file(EXTRACT / "reg_numbers.parquet")}
    print(f"  reg_numbers: {MANIFEST['extracts']['reg_numbers']['rows']} rows")


# -------------------------------------------------------- Charity Commission --
def ex_charity(con):
    main = bronze_files("charity_commission", "charity.zip")[0]
    other = bronze_files("charity_commission", "charity_other_names.zip")[0]
    note_input(main, "bronze charity_commission charity.zip")
    note_input(other, "bronze charity_commission charity_other_names.zip")
    tmp = WORK / "tmp"
    tmp.mkdir(exist_ok=True)
    for z in (main, other):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(tmp)
    def load(path, cols):
        with open(path, encoding="utf-8-sig") as fh:
            data = json.load(fh)
        return [tuple(d.get(c) for c in cols) for d in data]
    c = load(tmp / "publicextract.charity.json",
             ["registered_charity_number", "linked_charity_number", "charity_name",
              "charity_registration_status", "charity_company_registration_number",
              "date_of_removal", "charity_contact_postcode"])
    o = load(tmp / "publicextract.charity_other_names.json",
             ["registered_charity_number", "linked_charity_number", "charity_name", "charity_name_type"])
    info = {}
    rows = []
    for num, link, name, status, crn, removed, pc in c:
        cn, ok = norm_crn(crn) if crn else (None, False)
        info[(num, link)] = (status, cn if ok else None, str(removed) if removed else None)
        k = normalise(name or "")
        if k:
            rows.append((k, str(num), int(link or 0), "registered", name, status,
                         cn if ok else None, str(removed) if removed else None, norm_pc(pc)))
    for num, link, name, typ in o:
        st, cn, removed = info.get((num, link), (None, None, None))
        k = normalise(name or "")
        if k:
            rows.append((k, str(num), int(link or 0), (typ or "other").lower(), name, st, cn, removed, None))
    write(con, "charity", rows,
          [("name_norm", "VARCHAR"), ("charity_number", "VARCHAR"), ("linked", "INTEGER"),
           ("kind", "VARCHAR"), ("name", "VARCHAR"), ("status", "VARCHAR"),
           ("company_number", "VARCHAR"), ("removed", "VARCHAR"), ("postcode_norm", "VARCHAR")])


# ------------------------------------------------------------------- CQC --
def read_ods(path, sheet):
    import python_calamine as pc
    rows = pc.CalamineWorkbook.from_path(path).get_sheet_by_name(sheet).to_python()
    head = [str(h).strip() for h in rows[0]]
    return [dict(zip(head, r)) for r in rows[1:]]


def ex_cqc(con):
    rows = []
    for src, sheet in (("cqc_hsca_active", "HSCA_Active_Locations"),
                       ("cqc_hsca_deactivated", None)):
        f = bronze_files(src, "*.ods")[0]
        note_input(f, f"bronze {src}")
        if sheet is None:
            import python_calamine as pc
            names = pc.CalamineWorkbook.from_path(f).sheet_names
            sheet = next(s for s in names if s.lower() != "readme")
        seen = set()
        for r in read_ods(f, sheet):
            pid = str(r.get("Provider ID") or "").strip()
            if not pid or pid in seen and src == "cqc_hsca_active":
                continue
            seen.add(pid)
            own = str(r.get("Provider Ownership Type") or "").strip()
            name = str(r.get("Provider Name") or "").strip()
            individual = own.lower() == "individual"
            k = "" if individual else normalise(name)
            cn, ok = norm_crn(r.get("Provider Companies House Number")) \
                if str(r.get("Provider Companies House Number") or "").strip() else (None, False)
            chn = str(r.get("Provider Charity Number") or "").strip() or None
            if chn and chn.endswith(".0"):
                chn = chn[:-2]
            rows.append((k, pid, src, None if individual else name, own,
                         cn if cn else None, ok, chn,
                         norm_pc(r.get("Provider Postal Code")),
                         str(r.get("Location Local Authority") or "").strip()))
    # one row per provider and file; the local authority kept is the first seen
    write(con, "cqc", rows,
          [("name_norm", "VARCHAR"), ("provider_id", "VARCHAR"), ("file", "VARCHAR"),
           ("provider_name", "VARCHAR"), ("ownership_type", "VARCHAR"),
           ("company_number", "VARCHAR"), ("company_number_valid", "BOOLEAN"),
           ("charity_number", "VARCHAR"), ("provider_postcode_norm", "VARCHAR"),
           ("location_la", "VARCHAR")])


# ------------------------------------------------------- public bodies, GIAS --
def ex_public(con):
    rows = []
    for fname, role in (("public_bodies.csv", "Reports Public_Pound_work/phase1/sources/public_bodies.csv"),
                        ("central_government_bodies.csv",
                         "Reports Public_Pound_work/phase1/sources/central_government_bodies.csv "
                         "(GOV.UK organisations register, 4 October 2026)")):
        f = INPUTS / fname
        note_input(f, role)
        with open(f, newline="") as fh:
            for r in csv.DictReader(fh):
                names = [r["name"]] + [v for v in (r.get("name_variants") or "").split("|") if v.strip()]
                for i, n in enumerate(names):
                    k = normalise(n)
                    if k:
                        rows.append((k, r["body_id"], r["org_scheme"], r["org_id"], r.get("alias_ids") or "",
                                     r.get("gss_code") or "", r["body_type"], r["step5_class_proposed"],
                                     r["status"], r.get("in_lancashire") or "", n, "name" if i == 0 else "variant"))
    write(con, "public_bodies", rows,
          [("name_norm", "VARCHAR"), ("body_id", "VARCHAR"), ("org_scheme", "VARCHAR"),
           ("org_id", "VARCHAR"), ("alias_ids", "VARCHAR"), ("gss_code", "VARCHAR"),
           ("body_type", "VARCHAR"), ("step5_class_proposed", "VARCHAR"), ("status", "VARCHAR"),
           ("in_lancashire", "VARCHAR"), ("name", "VARCHAR"), ("kind", "VARCHAR")])


def ex_gias(con):
    f = bronze_files("gias_groups", "gias_allgroupsdata.csv")[0]
    note_input(f, "bronze gias_groups")
    rows = []
    with open(f, newline="", encoding="utf-8-sig", errors="replace") as fh:
        for r in csv.DictReader(fh):
            if "trust" not in (r["Group Type"] or "").lower():
                continue
            k = normalise(r["Group Name"] or "")
            cn, ok = norm_crn(r["Companies House Number"]) if (r["Companies House Number"] or "").strip() else (None, False)
            if k:
                rows.append((k, r["Group UID"], r["Group Name"], r["Group Type"], r["Group Status"],
                             cn if ok else None, norm_pc(r["Group Postcode"])))
    write(con, "gias_groups", rows,
          [("name_norm", "VARCHAR"), ("group_uid", "VARCHAR"), ("name", "VARCHAR"),
           ("group_type", "VARCHAR"), ("status", "VARCHAR"), ("company_number", "VARCHAR"),
           ("postcode_norm", "VARCHAR")])


# ------------------------------------------------------------------ GLEIF --
def ex_gleif(con):
    z = bronze_files("gleif_lei2_full", "*.zip")[0]
    note_input(z, "bronze gleif_lei2_full")
    tmp = WORK / "tmp"
    tmp.mkdir(exist_ok=True)
    with zipfile.ZipFile(z) as zf:
        name = zf.namelist()[0]
        if not (tmp / name).exists():
            zf.extract(name, tmp)
    csvp = tmp / name
    cols = ['"LEI"', '"Entity.LegalName"'] + \
        [f'"Entity.OtherEntityNames.OtherEntityName.{i}"' for i in range(1, 6)] + \
        ['"Entity.RegistrationAuthority.RegistrationAuthorityID"',
         '"Entity.RegistrationAuthority.RegistrationAuthorityEntityID"',
         '"Entity.LegalJurisdiction"', '"Entity.EntityStatus"',
         '"Registration.RegistrationStatus"', '"Entity.LegalAddress.PostalCode"',
         '"Entity.LegalAddress.Country"']
    res = con.execute(f"""SELECT {', '.join(cols)} FROM read_csv('{csvp}', header=true,
                          all_varchar=true, quote='"', escape='"', parallel=true)""").fetchall()
    rows = []
    for r in res:
        lei, legal = r[0], r[1]
        ra, raid, jur, est, rst, pc, ctry = r[7:14]
        for i, n in enumerate([legal] + list(r[2:7])):
            if not n:
                continue
            k = normalise(n)
            if k:
                rows.append((k, lei, "legal" if i == 0 else "other", n, ra, raid, jur, est, rst,
                             norm_pc(pc), ctry))
    write(con, "gleif", rows,
          [("name_norm", "VARCHAR"), ("lei", "VARCHAR"), ("kind", "VARCHAR"), ("name", "VARCHAR"),
           ("ra_id", "VARCHAR"), ("ra_entity_id", "VARCHAR"), ("jurisdiction", "VARCHAR"),
           ("entity_status", "VARCHAR"), ("registration_status", "VARCHAR"),
           ("postcode_norm", "VARCHAR"), ("country", "VARCHAR")])


# ----------------------------------------------- OCDS, OCP FTS, GGIS (step 7) --
OCDS_SCHEMES = {"GB-COH", "GB-CHC", "GB-SC", "GB-NIC"}


def ex_ocds(con):
    rows = []
    f = bronze_files("ocp_fts_bulk", "*.jsonl.gz")[0]
    note_input(f, "bronze ocp_fts_bulk")
    with gzip.open(f, "rt") as fh:
        for line in fh:
            rel = json.loads(line)
            for p in rel.get("parties") or []:
                roles = set(p.get("roles") or [])
                if not roles & {"supplier", "tenderer", "payee", "buyer", "procuringEntity"}:
                    continue
                name = (p.get("name") or "").strip()
                ids = [p.get("identifier") or {}] + (p.get("additionalIdentifiers") or [])
                for i in ids:
                    sch = (i.get("scheme") or "").strip()
                    v = str(i.get("id") or "").strip()
                    if sch not in OCDS_SCHEMES or not v:
                        continue
                    if sch == "GB-COH":
                        v, ok = norm_crn(v.replace("GB-COH-", ""))
                        if not ok:
                            continue
                    for n in {name, (i.get("legalName") or "").strip()}:
                        k = normalise(n) if n else ""
                        if k:
                            rows.append((k, sch, v, "ocp_fts", rel.get("ocid"), n))
    ocds_ids = INPUTS / "ocds_supplier_ids.json"
    note_input(ocds_ids, "ocds_supplier_ids.json (vps processed, Contracts Finder awards)")
    for k, v in json.loads(ocds_ids.read_text()).get("byName", {}).items():
        cn, ok = norm_crn(v.get("crn"))
        if ok:
            rows.append((k, "GB-COH", cn, "ocds_supplier_ids", None, k))
    for pf in sorted(INPUTS.glob("procurement_finder/*.json")):
        note_input(pf, f"clawd procurement_finder {pf.stem}")
        for n in json.loads(pf.read_text()).get("notices", []):
            ident = str(n.get("supplier_identifier") or "")
            if ident in ("", "None"):
                continue
            sch, _, v = ident.rpartition("-")
            if sch in OCDS_SCHEMES:
                k = normalise(n.get("supplier_name") or "")
                if k:
                    rows.append((k, sch, v, f"procurement_finder:{pf.stem}", n.get("notice_id"),
                                 n.get("supplier_name")))
    write(con, "ocds", rows,
          [("name_norm", "VARCHAR"), ("scheme", "VARCHAR"), ("id", "VARCHAR"),
           ("source", "VARCHAR"), ("ref", "VARCHAR"), ("name", "VARCHAR")])

    g = bronze_files("ggis_grants_register", "*.ods")[0]
    note_input(g, "bronze ggis_grants_register")
    grows = []
    for r in read_ods(g, "#Awards"):
        name = str(r.get("Recipient Org:Name") or "").strip()
        k = normalise(name)
        if not k:
            continue
        cn_raw = str(r.get("Recipient Org:Company Number") or "").strip()
        ch_raw = str(r.get("Recipient Org:Charity Number") or "").strip()
        if cn_raw:
            cn, ok = norm_crn(cn_raw[:-2] if cn_raw.endswith(".0") else cn_raw)
            if ok:
                grows.append((k, "GB-COH", cn, "ggis", str(r.get("Identifier") or ""), name))
        if ch_raw:
            ch = ch_raw[:-2] if ch_raw.endswith(".0") else ch_raw
            sch = "GB-SC" if ch.upper().startswith("SC") else "GB-CHC"
            grows.append((k, sch, ch.upper(), "ggis", str(r.get("Identifier") or ""), name))
    write(con, "ggis", grows,
          [("name_norm", "VARCHAR"), ("scheme", "VARCHAR"), ("id", "VARCHAR"),
           ("source", "VARCHAR"), ("ref", "VARCHAR"), ("name", "VARCHAR")])


# ---------------------------------------------------- RSH and OSCR (by name) --
def ex_rsh_oscr(con):
    import python_calamine as pc
    f = bronze_files("rsh_registered_providers", "*.xlsx")[0]
    note_input(f, "bronze rsh_registered_providers")
    wb = pc.CalamineWorkbook.from_path(f)
    rows = []
    for sn in wb.sheet_names:
        data = wb.get_sheet_by_name(sn).to_python()
        hi = next((i for i, r in enumerate(data[:20])
                   if any(str(c).strip().lower() == "organisation name" for c in r)), None)
        if hi is None:
            continue
        head = [str(h).strip() for h in data[hi]]
        for r in data[hi + 1:]:
            d = dict(zip(head, r))
            k = normalise(str(d.get("Organisation name") or ""))
            if k:
                rows.append((k, str(d.get("Registration number") or "").strip(), "rsh",
                             str(d.get("Organisation name")).strip(),
                             str(d.get("Designation") or "").strip(),
                             str(d.get("Corporate form") or "").strip()))
    z = bronze_files("oscr_register", "*.zip")[0]
    note_input(z, "bronze oscr_register")
    with zipfile.ZipFile(z) as zf:
        member = zf.namelist()[0]
        text = zf.read(member).decode("utf-8-sig", errors="replace")
    for d in csv.DictReader(io.StringIO(text)):
        k = normalise(d.get("Charity Name") or "")
        if k:
            rows.append((k, (d.get("Charity Number") or "").strip(), "oscr",
                         (d.get("Charity Name") or "").strip(),
                         (d.get("Charity Status") or "").strip(),
                         (d.get("Constitutional Form") or "").strip()))
    write(con, "rsh_oscr", rows,
          [("name_norm", "VARCHAR"), ("id", "VARCHAR"), ("register", "VARCHAR"),
           ("name", "VARCHAR"), ("status", "VARCHAR"), ("form", "VARCHAR")])


STEPS = {"reg": ex_register, "charity": ex_charity, "cqc": ex_cqc, "public": ex_public,
         "gias": ex_gias, "gleif": ex_gleif, "ocds": ex_ocds, "rsh_oscr": ex_rsh_oscr}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=",".join(STEPS))
    a = ap.parse_args()
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    mf = EXTRACT / "extract_manifest.json"
    if mf.exists():
        old = json.loads(mf.read_text())
        MANIFEST["extracts"].update(old.get("extracts", {}))
        keep = [i for i in old.get("inputs", [])]
    else:
        keep = []
    for s in a.only.split(","):
        print(f"== {s}")
        before = len(MANIFEST["inputs"])
        STEPS[s](con)
        roles = {i["role"] for i in MANIFEST["inputs"][before:]}
        keep = [i for i in keep if i["role"] not in roles]
    MANIFEST["inputs"] = keep + MANIFEST["inputs"]
    MANIFEST["pipelineGitSha"] = git_sha()
    MANIFEST["duckdb"] = duckdb.__version__
    write_json(mf, MANIFEST)


if __name__ == "__main__":
    main()
