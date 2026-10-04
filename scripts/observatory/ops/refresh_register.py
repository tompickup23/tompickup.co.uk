#!/usr/bin/env python3
"""Monthly refresh: Lancashire register (postcode->LAD) + national slim index.

Resolves the latest CH BasicCompanyDataAsOneFile snapshot from the CH index page,
freshness-skips if the on-disk build already reflects that date, otherwise
downloads + funzip-filters it (same method as etl_register.py: ONSPD pcds->lad25cd
lookup, keep only the 14 Lancashire LADs) and regenerates:
  lancs_register.csv(.gz), register_index.tsv.gz, lancs_crns.txt, register_summary.json

Disk guard: abort the download if free space < 40 GB. Deletes the zip after use.
Exit codes: 0 refreshed, 3 already fresh (skipped), 2 error.
"""
import csv, subprocess, sys, gzip, os, json, io, shutil, re, glob, urllib.request, time

WORK = "/opt/observatory/work"
OUT = "/opt/observatory/out"
ONSPD_DIR = "/opt/observatory/onspd"
INDEX_URL = "http://download.companieshouse.gov.uk/en_output.html"
BASE_URL = "http://download.companieshouse.gov.uk/"
MIN_FREE_GB = 40

TARGET_LADS = {
    "E07000121": "Lancaster", "E07000123": "Preston", "E07000124": "Ribble Valley",
    "E07000119": "Fylde", "E07000128": "Wyre", "E06000009": "Blackpool",
    "E06000008": "Blackburn with Darwen", "E07000120": "Hyndburn",
    "E07000125": "Rossendale", "E07000122": "Pendle", "E07000117": "Burnley",
    "E07000118": "Chorley", "E07000126": "South Ribble", "E07000127": "West Lancashire",
}
ONSPD_AREAS = ["BB", "PR", "FY", "LA", "L", "OL", "WN", "BL"]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] register: {m}", flush=True)


def free_gb():
    return shutil.disk_usage("/")[2] / 1e9


def norm_pc(pc):
    return (pc or "").replace(" ", "").upper().strip()


def latest_snapshot():
    html = urllib.request.urlopen(INDEX_URL, timeout=90).read().decode("utf-8", "replace")
    dates = sorted(set(re.findall(r"BasicCompanyDataAsOneFile-(\d{4}-\d{2}-\d{2})\.zip", html)))
    return dates[-1] if dates else None


def recorded_date():
    p = os.path.join(OUT, "register_summary.json")
    if os.path.exists(p):
        try:
            return json.load(open(p)).get("snapshot_date")
        except Exception:
            return None
    return None


def load_onspd():
    log("loading ONSPD candidate-area lookup...")
    pc2lad = {}
    for a in ONSPD_AREAS:
        matches = sorted(glob.glob(os.path.join(ONSPD_DIR, f"ONSPD_*_UK_{a}.csv")))
        if not matches:
            log(f"WARNING no ONSPD file for area {a}")
            continue
        with open(matches[-1], newline="", encoding="utf-8", errors="replace") as f:
            r = csv.DictReader(f)
            for row in r:
                pcds = row.get("pcds") or ""
                lad = row.get("lad25cd") or ""
                if pcds and lad:
                    pc2lad[norm_pc(pcds)] = lad
    log(f"ONSPD candidate postcodes loaded: {len(pc2lad):,}")
    if not pc2lad:
        raise SystemExit("no ONSPD postcodes loaded; cannot build register")
    return pc2lad


def build(zip_path, snapshot_date, pc2lad):
    p = subprocess.Popen(["funzip", zip_path], stdout=subprocess.PIPE)
    stream = io.TextIOWrapper(p.stdout, encoding="utf-8", errors="replace")
    reader = csv.reader(stream)
    header = next(reader)
    cols = {name.strip(): i for i, name in enumerate(header)}

    def g(row, name):
        i = cols.get(name)
        return row[i].strip() if (i is not None and i < len(row)) else ""

    lancs_out = open(os.path.join(OUT, "lancs_register.csv"), "w", newline="", encoding="utf-8")
    lw = csv.writer(lancs_out)
    lw.writerow(["CompanyNumber", "CompanyName", "RegAddress.PostCode", "lad_code", "lad_name",
                 "CompanyCategory", "CompanyStatus", "DissolutionDate", "IncorporationDate",
                 "Accounts.AccountCategory", "Accounts.NextDueDate", "Accounts.LastMadeUpDate",
                 "SICCode.SicText_1", "SICCode.SicText_2", "SICCode.SicText_3", "SICCode.SicText_4", "cic"])
    idx_out = gzip.open(os.path.join(OUT, "register_index.tsv.gz"), "wt", encoding="utf-8")
    idx_out.write("CompanyNumber\tCompanyName\tPostCode\tCompanyStatus\n")
    crns_out = open(os.path.join(OUT, "lancs_crns.txt"), "w")

    total = lancs = cic_count = 0
    per_lad = {k: 0 for k in TARGET_LADS}
    cic_strings = {}
    for row in reader:
        total += 1
        if total % 1000000 == 0:
            log(f"  processed {total:,} rows, lancs {lancs:,}")
        crn = g(row, "CompanyNumber")
        name = g(row, "CompanyName")
        pc = g(row, "RegAddress.PostCode")
        status = g(row, "CompanyStatus")
        idx_out.write(f"{crn}\t{name.replace(chr(9),' ').replace(chr(10),' ')}\t{pc}\t{status}\n")
        lad = pc2lad.get(norm_pc(pc))
        if not lad or lad not in TARGET_LADS:
            continue
        lancs += 1
        per_lad[lad] += 1
        cat = g(row, "CompanyCategory")
        is_cic = "community interest company" in cat.lower()
        if is_cic:
            cic_count += 1
            cic_strings[cat] = cic_strings.get(cat, 0) + 1
        lw.writerow([crn, name, pc, lad, TARGET_LADS[lad], cat, status,
                     g(row, "DissolutionDate"), g(row, "IncorporationDate"),
                     g(row, "Accounts.AccountCategory"), g(row, "Accounts.NextDueDate"),
                     g(row, "Accounts.LastMadeUpDate"), g(row, "SICCode.SicText_1"),
                     g(row, "SICCode.SicText_2"), g(row, "SICCode.SicText_3"),
                     g(row, "SICCode.SicText_4"), "true" if is_cic else "false"])
        crns_out.write(crn + "\n")

    lancs_out.close(); idx_out.close(); crns_out.close()
    p.wait()

    # gzip the lancs register (consumed by build_master)
    with open(os.path.join(OUT, "lancs_register.csv"), "rb") as fi, \
         gzip.open(os.path.join(OUT, "lancs_register.csv.gz"), "wb") as fo:
        shutil.copyfileobj(fi, fo)

    summary = {
        "snapshot_date": snapshot_date,
        "total_uk_companies": total,
        "lancs_companies": lancs,
        "per_lad": {TARGET_LADS[k]: v for k, v in sorted(per_lad.items(), key=lambda x: -x[1])},
        "cic_count": cic_count,
        "cic_category_strings": cic_strings,
    }
    with open(os.path.join(OUT, "register_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    log(f"DONE register {snapshot_date}: {lancs:,} lancs / {total:,} UK")


def main():
    os.makedirs(WORK, exist_ok=True)
    latest = latest_snapshot()
    if not latest:
        log("ERROR could not resolve latest snapshot from CH index")
        sys.exit(2)
    if latest == recorded_date():
        log(f"already fresh (snapshot {latest} == on-disk build); skipping")
        sys.exit(3)
    log(f"new snapshot available: {latest} (on-disk: {recorded_date()})")
    zip_path = os.path.join(WORK, f"BasicCompanyDataAsOneFile-{latest}.zip")
    if not os.path.exists(zip_path):
        fg = free_gb()
        if fg < MIN_FREE_GB:
            log(f"ABORT: free {fg:.1f}GB below {MIN_FREE_GB}GB guard")
            sys.exit(2)
        log(f"downloading BasicCompanyDataAsOneFile-{latest}.zip (free {fg:.1f}GB)...")
        rc = subprocess.call(["curl", "-sSL", "-o", zip_path, BASE_URL + f"BasicCompanyDataAsOneFile-{latest}.zip"])
        if rc != 0 or not os.path.exists(zip_path):
            log(f"download FAILED rc={rc}")
            sys.exit(2)
    pc2lad = load_onspd()
    build(zip_path, latest, pc2lad)
    try:
        os.remove(zip_path)
        log(f"removed zip, free {free_gb():.1f}GB")
    except OSError:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
