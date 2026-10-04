#!/usr/bin/env python3
"""The Public Pound resolution waterfall (Reports Public_Pound_work/phase0/WATERFALL.md).

Unit: the payee key, (body_id, payee_key), where payee_key is
upper(strip(supplier_canonical or supplier)) as the spend input writes it
(the fuel register's key, so a classification overlay joins on it).

Steps implemented: 1 (bank supplier_company_number, labelled id-bank-inferred
because it is the AI DOGE pipeline's own name match), 1b (council-published
charity numbers: Pendle's Charity Number column), 2 (name and postcode; the
banks carry no postcode, so it can close nothing today and says so), 2b, 2c,
3 (Charity Commission), 4 (CQC HSCA: name to Provider ID, Provider ID to
Provider Companies House Number), 5 (public_bodies.csv, academy trusts from
GIAS; RSH and OSCR by name, for evidence only: they publish no company
number), 5b (council companies, from gold mart_psc_lancs, method rule 2.1),
6 (GLEIF Level 1), 7 (OCP Find a Tender, ocds_supplier_ids.json, the clawd
procurement_finder files, GGIS), the step 9 queue and step 10 with reasons.
Step 8 (Splink) is not built in Phase 1 (RULES.md decision 10).

Every step is run for every key. The proposal is the first step that
resolves the key; if any other step proposes a different organisation the
key goes to the step 9 queue with all candidates and is not resolved (a
conflict is never settled by order, WATERFALL.md s1).

Outputs in $POUND_WORK/out:
  resolver_proposed.csv  one row per payee key; decided_by pipeline:<sha>.
                         Claude never writes resolver.csv: only rows a named
                         person accepts become it (RULES.md stop rule 5).
  queue.csv              step 9: conflicts and ambiguous pools
  payee_key_values.csv   value and rows per key and financial year
  run_manifest.json      inputs with sha256, counts, git sha
Individual and redacted keys are written as a hash, never by name.
"""
import argparse
import bisect
import csv
import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

import duckdb

from common import (EXTRACT, GOLD, INPUTS, NAMELESS_KEY, OUT, WITHHELD_KEY,
                    distinctive_tokens, git_sha, norm_crn, normalise,
                    sha256_file, supplier_variants, write_json)

# Provisional routing confidences from WATERFALL.md s2. Never published.
CONF = {"id-bank-inferred": 0.90, "id-council": 0.99, "name-postcode": 0.97,
        "name-exact": 0.93, "name-exact-out": 0.90, "name-prefix": 0.85,
        "charity": 0.95, "cqc-provider": 0.95, "public-body": 0.95,
        "gleif": 0.93, "ocds": 0.92, "ggis": 0.92}
STEP_OF = {"id-bank-inferred": "1", "id-council": "1b", "name-postcode": "2",
           "name-exact": "2b", "name-exact-out": "2b", "name-prefix": "2c",
           "charity": "3", "cqc-provider": "4", "public-body": "5",
           "gleif": "6", "ocds": "7", "ggis": "7"}

# Hand-verified bad prefix matches, carried from build_pound.PREFIX_DENY.
PREFIX_DENY = {"BROTHERS OF CHARITY SERVICES", "RED ROSE SCHOOL", "QUEENS LODGE"}

# Payee keys that name a payment route (an account, clearing line or
# direct-debit batch), not an organisation. Matched on the normalised key.
PAYMENT_ROUTE_RE = re.compile(
    r"\bDD PAYMENT\b|\bDIRECT DEBITS?\b|\bMAIN ACCOUNT\b|\bSUSPENSE\b|"
    r"\bCLEARING ACCOUNT\b|\bCONTROL ACCOUNT\b|\bPETTY CASH\b|\bIMPREST\b|"
    r"\bBACS\b|\bSUNDRY (CREDITORS?|SUPPLIERS?|PAYEES?)\b|\bONE OFF PAYMENTS?\b|"
    r"\bVARIOUS (SUPPLIERS?|PAYEES?|CREDITORS?)\b|\bPAYMENT CARD\b|\bPROCUREMENT CARDS?\b")
# A key that names no payee at all.
NO_PAYEE_RE = re.compile(r"^(UNKNOWN|VARIOUS|SUNDRY|MISCELLANEOUS|NOT KNOWN|N A|NONE|TBC)$")
REDACT_RE = re.compile(r"^REDACT|^CONFIDENTIAL|^NAME (WITHHELD|REDACTED)|^PERSONAL\b|"
                       r"^WITHHELD\b|^PRIVATE INDIVIDUAL|^PERSONAL DATA")
TITLE_RE = re.compile(r"^(MR|MRS|MISS|MS|DR|MSTR|MASTER|REV|PROF)\b")
# Words that make a name an organisation's, for the individual-payee test
# (WATERFALL.md s2: no organisational token and no register match).
ORG_WORDS = set("""
LTD LIMITED PLC LLP LP CIC CIO INC CO COMPANY GROUP HOLDINGS UK SERVICES SERVICE
SOLUTIONS SYSTEMS INTERNATIONAL NATIONAL TRUST ASSOCIATION CENTRE CENTER COUNCIL
CLUB SCHOOL SCHOOLS COLLEGE UNIVERSITY ACADEMY CHURCH PARISH SOCIETY FOUNDATION
CHARITY HOUSING HOMES HOME CARE NURSING HEALTH NHS HOSPITAL HOSPICE PRACTICE SURGERY
MEDICAL DENTAL PHARMACY ENERGY POWER WATER GAS ELECTRIC ELECTRICAL BANK INSURANCE
PARTNERSHIP PARTNERS ASSOCIATES CONSULTING CONSULTANTS CONSULTANCY ENGINEERING
CONSTRUCTION BUILDERS BUILDING ROOFING PLUMBING HEATING MOTORS GARAGE TRANSPORT TRAVEL
COACHES TAXIS TAXI CARS HIRE PLANT SUPPLIES SUPPLY STORES SHOP RETAIL WHOLESALE FOODS
FOOD CATERING CAFE RESTAURANT HOTEL INN PUB BAR LEISURE SPORTS SPORT FITNESS ARTS
THEATRE MUSIC MEDIA PRINT PRINTING DESIGN SIGNS DIGITAL SOFTWARE TECHNOLOGY TECH
COMPUTERS IT DATA NETWORKS TELECOM COMMUNICATIONS SECURITY CLEANING CLEANERS WASTE
RECYCLING ENVIRONMENTAL LANDSCAPES LANDSCAPING GARDENS GARDENING TREE TREES FARM FARMS
ESTATES ESTATE PROPERTY PROPERTIES LETTINGS DEVELOPMENTS DEVELOPMENT INVESTMENTS
CAPITAL FINANCE FINANCIAL ACCOUNTANTS SOLICITORS LAW LEGAL CHAMBERS BARRISTERS
AUTHORITY AGENCY DEPARTMENT MINISTRY OFFICE BOARD COMMISSION COMMISSIONER POLICE FIRE
TRADING ENTERPRISES ENTERPRISE VENTURES INDUSTRIES MANUFACTURING PRODUCTS EQUIPMENT
TRAINING EDUCATION LEARNING NURSERY NURSERIES PRESCHOOL PLAYGROUP CHILDCARE YOUTH
COMMUNITY PROJECT PROJECTS NETWORK FORUM ALLIANCE FEDERATION UNION GUILD INSTITUTE
FUND FUNDS PENSION PENSIONS AND THE OF FOR
""".split())


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def key_hash(body, key):
    return hashlib.sha1(f"{body}\x1f{key}".encode()).hexdigest()[:12]


# ------------------------------------------------------------------ input --
def load_keys(path):
    keys = {}
    for line in gzip.open(path, "rt"):
        d = json.loads(line)
        k = (d["body_id"], d["payee_key"])
        e = keys.setdefault(k, {"body_id": d["body_id"], "payee_key": d["payee_key"],
                                "value": Counter(), "rows": Counter(), "raw": Counter(),
                                "canon": Counter(), "cn": defaultdict(lambda: [0, 0]),
                                "chn": defaultdict(lambda: [0, 0]), "rc": Counter()})
        fy = d["financial_year"]
        e["value"][fy] += d["value_pence"]
        e["rows"][fy] += d["rows"]
        e["raw"].update(d["raw_names"])
        e["canon"].update(d["supplier_canonical"])
        for c, v in d["bank_company_numbers"].items():
            e["cn"][c][0] += v["rows"]
            e["cn"][c][1] += v["value_pence"]
        for c, v in d["bank_charity_numbers"].items():
            e["chn"][c][0] += v["rows"]
            e["chn"][c][1] += v["value_pence"]
        e["rc"].update(d["record_class_pence"])
    for e in keys.values():
        e["total"] = sum(e["value"].values())
        variants, seen = [], set()
        names = [e["payee_key"]] + [n for n, _ in e["canon"].most_common()] + \
                [n for n, _ in e["raw"].most_common()]
        for n in names:
            for v in supplier_variants(n):
                if v not in seen:
                    seen.add(v)
                    variants.append(v)
        e["variants"] = variants
    return keys


# -------------------------------------------------------------- registers --
class Registers:
    def __init__(self, con, variants):
        self.con = con
        con.execute("CREATE TEMP TABLE v (name_norm VARCHAR)")
        con.executemany("INSERT INTO v VALUES (?)", [(x,) for x in variants])
        self.reg = defaultdict(list)
        for r in con.execute(f"""SELECT r.name_norm, company_number, kind, name, status, active,
                                        postcode_norm, snapshot_date, in_lancs_frame
                                 FROM '{EXTRACT}/reg_names.parquet' r JOIN v USING (name_norm)""").fetchall():
            self.reg[r[0]].append(dict(crn=r[1], kind=r[2], name=r[3], status=r[4], active=r[5],
                                       pc=r[6], snap=r[7], lancs=r[8]))
        self.numbers = {r[0]: r[1] for r in con.execute(
            f"SELECT company_number, last_snapshot FROM '{EXTRACT}/reg_numbers.parquet'").fetchall()}
        self.sorted_current = None
        self.by_name_current = None
        self.tables = {}
        for t in ("charity", "cqc", "public_bodies", "gias_groups", "gleif", "ocds", "ggis", "rsh_oscr"):
            cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{EXTRACT}/{t}.parquet'").fetchall()]
            d = defaultdict(list)
            for r in con.execute(f"SELECT t.* FROM '{EXTRACT}/{t}.parquet' t JOIN v USING (name_norm)").fetchall():
                d[r[0]].append(dict(zip(cols, r)))
            self.tables[t] = d

    def names_for(self, crn):
        """Register names (current and previous) for a company number."""
        if not hasattr(self, "_names_cache"):
            self._names_cache = {}
        if crn not in self._names_cache:
            rows = self.con.execute(f"""SELECT DISTINCT name_norm, name, kind FROM '{EXTRACT}/reg_names.parquet'
                                        WHERE company_number = ?""", [crn]).fetchall()
            self._names_cache[crn] = rows
        return self._names_cache[crn]

    def load_prefix_index(self):
        rows = self.con.execute(f"""SELECT name_norm, company_number, active FROM '{EXTRACT}/reg_names.parquet'
                                    WHERE kind = 'current'""").fetchall()
        by = defaultdict(list)
        for n, c, a in rows:
            by[n].append((c, a))
        self.by_name_current = by
        self.sorted_current = sorted(by)


# ------------------------------------------------------------------ steps --
def cand(scheme, org_id, method, evidence, alias=None, payee_class="supplier"):
    return {"org_scheme": scheme, "org_id": org_id, "method": method,
            "alias_ids": alias or [], "evidence": evidence, "payee_class": payee_class}


def step1(e, R, counts):
    if not e["cn"]:
        return None, None
    total = e["total"] if e["total"] > 0 else sum(v[1] for v in e["cn"].values())
    best, best_v, invalid = None, 0, 0
    for c, (rows, v) in e["cn"].items():
        n, ok = norm_crn(c)
        if not ok or n not in R.numbers:
            invalid += 1
            counts["step1_invalid_numbers"] += 1
            continue
        if v > best_v:
            best, best_v = n, v
    if best is None:
        return None, "invalid-identifier"
    if total <= 0 or best_v * 2 < total:
        counts["step1_below_half_value"] += 1
        return None, None
    reg_tokens = set()
    reg_names = R.names_for(best)
    for nn, _, _ in reg_names:
        reg_tokens |= distinctive_tokens(nn)
    pay_tokens = set()
    for v in e["variants"]:
        pay_tokens |= distinctive_tokens(v)
    ev = {"scheme": "GB-COH", "id": best, "source": "bank supplier_company_number",
          "share_of_value": round(best_v / total, 4),
          "register_names": sorted({n for _, n, _ in reg_names})[:6],
          "register_last_snapshot": R.numbers[best]}
    if reg_tokens & pay_tokens:
        ev["shared_tokens"] = sorted(reg_tokens & pay_tokens)[:6]
        return cand("GB-COH", best, "id-bank-inferred", ev), None
    ev["conflict"] = "identifier and name conflict: no distinctive token shared"
    return {"conflict": True, **cand("GB-COH", best, "id-bank-inferred", ev)}, None


def step1b(e, R):
    """Pendle publishes a Charity Number column; the bank carries it."""
    if not e["chn"] or e["body_id"] != "pendle":
        return None
    total = e["total"] or 1
    c, (rows, v) = max(e["chn"].items(), key=lambda kv: kv[1][1])
    num = re.sub(r"\D", "", str(c))
    if not num or v * 2 < total:
        return None
    for vname in e["variants"]:
        for r in R.tables["charity"].get(vname, []):
            if r["charity_number"] == num:
                ev = {"scheme": "GB-CHC", "id": num, "source": "Pendle Charity Number column (bank charity_number)",
                      "register_name": r["name"], "status": r["status"]}
                if r["company_number"] and r["company_number"] in R.numbers:
                    return cand("GB-COH", r["company_number"], "id-council", ev, [f"GB-CHC-{num}"])
                return cand("GB-CHC", num, "id-council", ev)
    return None


def pick_pool(cands):
    active = [c for c in cands if c["active"]]
    pool = active or cands
    crns = {c["crn"] for c in pool}
    lancs = {c["crn"] for c in pool if c["lancs"]}
    return pool, crns, lancs


def step2_2b(e, R):
    """-> (candidate or None, ambiguous pool or None). Step 2 needs payment
    postcodes, which the banks do not carry, so only 2b can close a key."""
    amb = None
    for kind in ("current", "previous"):
        for v in e["variants"]:
            cands = [c for c in R.reg.get(v, []) if c["kind"] == kind]
            if not cands:
                continue
            pool, crns, lancs = pick_pool(cands)
            ev = {"scheme": "GB-COH", "matched_variant": v, "name_kind": kind,
                  "register": "silver ch_register, snapshots 2026-07-01, 2026-08-01, 2026-10-01"}
            if len(lancs) == 1:
                crn = next(iter(lancs))
                c = next(x for x in pool if x["crn"] == crn)
                ev.update(id=crn, register_name=c["name"], status=c["status"], snapshot=c["snap"],
                          rule="one candidate in the Lancashire frame")
                return cand("GB-COH", crn, "name-exact", ev), None
            if len(crns) == 1:
                crn = next(iter(crns))
                c = pool[0]
                ev.update(id=crn, register_name=c["name"], status=c["status"], snapshot=c["snap"],
                          rule="unique among active" if c["active"] else "unique, none active")
                return cand("GB-COH", crn, "name-exact-out", ev), None
            if amb is None:
                srt = sorted(pool, key=lambda c: (not c["lancs"], c["crn"]))
                amb = {"matched_variant": v, "name_kind": kind,
                       "candidates": [{"crn": c["crn"], "name": c["name"], "status": c["status"],
                                       "lancs_frame": c["lancs"]} for c in
                                      {c["crn"]: c for c in srt}.values()][:6],
                       "pool_size": len(crns)}
        if amb:
            break
    return None, amb


def step2c(e, R):
    if R.sorted_current is None:
        R.load_prefix_index()
    keys = R.sorted_current
    for k in e["variants"]:
        if k in PREFIX_DENY or len(k) < 12:
            continue
        i = bisect.bisect_left(keys, k)
        hits = []
        while i < len(keys) and keys[i].startswith(k):
            if keys[i] == k or keys[i].startswith(k + " "):
                hits.append(keys[i])
                if len(hits) > 2:
                    break
            i += 1
        if len(hits) == 1:
            cands = R.by_name_current[hits[0]]
            active = [c for c in cands if c[1]]
            pool = {c[0] for c in (active or cands)}
            if len(pool) == 1:
                crn = next(iter(pool))
                return cand("GB-COH", crn, "name-prefix",
                            {"scheme": "GB-COH", "id": crn, "matched_variant": k,
                             "register_name_norm": hits[0], "rule": "prefix_unique, 12+ characters, word boundary"})
    return None


def unique_by(rows, key, prefer=None):
    if prefer:
        pref = [r for r in rows if prefer(r)]
        if pref:
            rows = pref
    ids = {r[key] for r in rows}
    return (rows, ids)


def step3(e, R):
    for v in e["variants"]:
        rows = [r for r in R.tables["charity"].get(v, []) if r["linked"] == 0]
        if not rows:
            continue
        rows, ids = unique_by(rows, "charity_number", lambda r: r["status"] == "Registered")
        if len(ids) != 1:
            return None
        r = rows[0]
        num = r["charity_number"]
        ev = {"scheme": "GB-CHC", "id": num, "matched_variant": v, "register_name": r["name"],
              "name_kind": r["kind"], "status": r["status"], "removed": r["removed"],
              "register": "Charity Commission extract 2026-10-04"}
        if r["company_number"] and r["company_number"] in R.numbers:
            return cand("GB-COH", r["company_number"], "charity", ev, [f"GB-CHC-{num}"])
        return cand("GB-CHC", num, "charity", ev)
    return None


def step4(e, R):
    for v in e["variants"]:
        rows = [r for r in R.tables["cqc"].get(v, []) if r["name_norm"]]
        if not rows:
            continue
        rows, ids = unique_by(rows, "provider_id", lambda r: r["file"] == "cqc_hsca_active")
        if len(ids) != 1:
            return None
        r = rows[0]
        ev = {"scheme": "CQC provider", "provider_id": r["provider_id"], "matched_variant": v,
              "register_name": r["provider_name"], "ownership_type": r["ownership_type"],
              "file": r["file"], "provider_companies_house_number": r["company_number"],
              "rule": "name to Provider ID; Provider ID to Provider Companies House Number"}
        alias = [f"GB-CHC-{r['charity_number']}"] if r["charity_number"] else []
        cn = r["company_number"]
        if cn and r["company_number_valid"] and cn in R.numbers:
            return cand("GB-COH", cn, "cqc-provider", ev, alias)
        if cn and cn.startswith("IP"):
            ev["note"] = "society number in the CQC company number field (Mutuals Public Register style)"
            return {"out_of_scope": True, **cand("GB-MPR", cn, "cqc-provider", ev)}
        if r["charity_number"]:
            return cand("GB-CHC", r["charity_number"], "cqc-provider", ev)
        ev["note"] = "CQC provider with no company or charity number; not resolved by this step"
        return {"evidence_only": True, **cand("UNRESOLVED", None, "cqc-provider", ev)}
    return None


def step5(e, R):
    for v in e["variants"]:
        rows = R.tables["public_bodies"].get(v, [])
        if rows:
            rows2, ids = unique_by(rows, "body_id", lambda r: r["status"] in ("open", "active", "ACTIVE", "Open", "live", ""))
            if len(ids) == 1:
                r = rows2[0]
                ev = {"scheme": r["org_scheme"], "id": r["org_id"], "body_id": r["body_id"],
                      "body_type": r["body_type"], "matched_variant": v, "register_name": r["name"],
                      "step5_class_proposed": r["step5_class_proposed"], "gss_code": r["gss_code"],
                      "source": "public_bodies.csv (Reports, 4 October 2026)"}
                klass = r["step5_class_proposed"]
                alias = [a for a in (r["alias_ids"] or "").split("|") if a]
                if klass == "public body":
                    return cand(r["org_scheme"] or "UNRESOLVED", r["org_id"] or r["body_id"], "public-body",
                                ev, alias, payee_class="public body")
                if klass == "gate decision":
                    sch, oid = r["org_scheme"], r["org_id"]
                    return cand(sch, oid, "public-body", ev, alias, payee_class="gate decision: " + r["body_type"])
                ev["note"] = f"public_bodies.csv class '{klass}': evidence only"
                return {"evidence_only": True, **cand("UNRESOLVED", None, "public-body", ev)}
        g = R.tables["gias_groups"].get(v, [])
        if g:
            g2, ids = unique_by(g, "group_uid", lambda r: r["status"] == "Open")
            if len(ids) == 1 and g2[0]["company_number"] in R.numbers:
                r = g2[0]
                ev = {"scheme": "GB-COH", "id": r["company_number"], "gias_group_uid": r["group_uid"],
                      "group_type": r["group_type"], "status": r["status"], "matched_variant": v,
                      "register_name": r["name"], "source": "GIAS all groups, bronze 2026-10-04"}
                return cand("GB-COH", r["company_number"], "public-body", ev,
                            payee_class="gate decision: academy trust")
    return None


def rsh_oscr_evidence(e, R):
    out = []
    for v in e["variants"]:
        for r in R.tables["rsh_oscr"].get(v, []):
            out.append({"register": r["register"], "id": r["id"], "name": r["name"],
                        "status": r["status"], "form": r["form"]})
    return out[:4]


GLEIF_CH_RA = {"RA000585"}  # Companies House, as recorded in GLEIF's RA list


def step6(e, R):
    for v in e["variants"]:
        rows = [r for r in R.tables["gleif"].get(v, [])
                if (r["entity_status"] or "").upper() == "ACTIVE"]
        if not rows:
            continue
        ids = {r["lei"] for r in rows}
        if len(ids) != 1:
            return None
        r = rows[0]
        ev = {"scheme": "XI-LEI", "id": r["lei"], "matched_variant": v, "register_name": r["name"],
              "name_kind": r["kind"], "ra_id": r["ra_id"], "ra_entity_id": r["ra_entity_id"],
              "jurisdiction": r["jurisdiction"], "registration_status": r["registration_status"],
              "source": "GLEIF Level 1 golden copy 2026-10-04 08:00"}
        if r["ra_id"] in GLEIF_CH_RA and r["ra_entity_id"]:
            cn, ok = norm_crn(r["ra_entity_id"])
            if ok and cn in R.numbers:
                return cand("GB-COH", cn, "gleif", ev, [f"XI-LEI-{r['lei']}"])
        return cand("XI-LEI", r["lei"], "gleif", ev)
    return None


def step7(e, R, table, method):
    for v in e["variants"]:
        rows = R.tables[table].get(v, [])
        if not rows:
            continue
        coh = {r["id"] for r in rows if r["scheme"] == "GB-COH"}
        chc = {(r["scheme"], r["id"]) for r in rows if r["scheme"] != "GB-COH"}
        srcs = sorted({r["source"] for r in rows})
        ev = {"matched_variant": v, "sources": srcs, "records": len(rows)}
        if len(coh) > 1:
            ev.update(ambiguous_identifiers=sorted(coh)[:6],
                      note="name carries more than one company number across notices or grants; linked to nothing")
            return {"evidence_only": True, "ambiguous_ids": True, **cand("UNRESOLVED", None, method, ev)}
        if len(coh) == 1:
            cn = next(iter(coh))
            if cn not in R.numbers:
                ev["note"] = f"company number {cn} is in no register snapshot held"
                return {"evidence_only": True, **cand("UNRESOLVED", None, method, ev)}
            ev.update(scheme="GB-COH", id=cn)
            return cand("GB-COH", cn, method, ev, [f"{s}-{i}" for s, i in sorted(chc)][:3])
        if len(chc) == 1:
            s, i = next(iter(chc))
            ev.update(scheme=s, id=i)
            return cand(s, i, method, ev)
        if len(chc) > 1:
            ev["note"] = "more than one charity number; linked to nothing"
            return {"evidence_only": True, "ambiguous_ids": True, **cand("UNRESOLVED", None, method, ev)}
    return None


def same_org(a, b):
    ida = {f"{a['org_scheme']}-{a['org_id']}"} | set(a["alias_ids"])
    idb = {f"{b['org_scheme']}-{b['org_id']}"} | set(b["alias_ids"])
    return bool(ida & idb)


def looks_individual(e):
    k = normalise(e["payee_key"])
    if not k:
        return False
    if TITLE_RE.search(k):
        return True
    toks = k.split()
    if not 2 <= len(toks) <= 4:
        return False
    if any(t in ORG_WORDS or any(ch.isdigit() for ch in t) for t in toks):
        return False
    return all(t.isalpha() and len(t) >= 2 for t in toks) or (
        len(toks[0]) == 1 and all(t.isalpha() for t in toks))


# ------------------------------------------------------------------- 5b --
def council_companies(con, crns, R):
    """Method rule 2.1 over gold mart_psc_lancs: a company is a council
    company when an active corporate PSC that is a local authority holds a
    voting-rights band whose lower bound is 50% or more (the 50 to 75 band
    means more than 50%), directly, or through one council-owned parent that
    is itself in the mart. Returns {crn: evidence}."""
    mart = sorted((GOLD / "mart_psc_lancs").glob("snapshot_date=*/part.parquet"))[-1]
    rows = con.execute(f"""SELECT company_number, name, natures_of_control, ceased_on, active
                           FROM read_parquet('{mart}')
                           WHERE NOT coalesce(is_individual, false)""").fetchall()
    la_names = set()
    for v in R.tables["public_bodies"].values():
        for r in v:
            if r["body_type"].startswith("council") or r["body_type"] in (
                    "police_and_crime_commissioner", "fire_and_rescue_authority",
                    "combined_county_authority"):
                la_names.add(r["name_norm"])
    allpb = con.execute(f"""SELECT name_norm, body_type FROM '{EXTRACT}/public_bodies.parquet'""").fetchall()
    for n, t in allpb:
        if t.startswith("council"):
            la_names.add(n)

    def band_ok(natures):
        for n in natures or []:
            if n.startswith("voting-rights-50-to-75") or n.startswith("voting-rights-75-to-100"):
                return n
        return None
    direct = {}
    corp_edges = defaultdict(list)
    for crn, name, natures, ceased, active in rows:
        if not active:
            continue
        nn = normalise(name or "")
        b = band_ok(natures)
        if not b:
            continue
        if nn in la_names or re.search(r"\b(BOROUGH|COUNTY|CITY|DISTRICT) COUNCIL\b|^COUNCIL OF\b", nn):
            direct[crn] = {"psc": name, "band": b, "route": "direct",
                           "source": f"gold mart_psc_lancs {mart.parent.name}"}
        corp_edges[crn].append((nn, name, b))
    out = dict(direct)
    # one step up: a parent named as corporate PSC whose own register name is a council company
    name_to_crn = {}
    for c in direct:
        for nn, _, _ in R.names_for(c):
            name_to_crn[nn] = c
    for crn, edges in corp_edges.items():
        if crn in out:
            continue
        for nn, name, b in edges:
            if nn in name_to_crn:
                out[crn] = {"psc": name, "band": b, "route": f"through {name_to_crn[nn]}",
                            "source": f"gold mart_psc_lancs {mart.parent.name}"}
                break
    return out, mart


# ------------------------------------------------------------------- run --
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", default=str(INPUTS / "payee_keys.jsonl.gz"))
    ap.add_argument("--bodies", default="")
    a = ap.parse_args()
    sha = git_sha()
    decided_by = f"pipeline:{sha}"
    OUT.mkdir(parents=True, exist_ok=True)
    keys = load_keys(a.keys)
    if a.bodies:
        want = set(a.bodies.split(","))
        keys = {k: v for k, v in keys.items() if k[0] in want}
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    allv = sorted({v for e in keys.values() for v in e["variants"]})
    R = Registers(con, allv)
    counts = Counter()
    results = {}
    for k, e in keys.items():
        pk = e["payee_key"]
        nk = normalise(pk)
        res = {"cands": [], "queue": None, "reason": None, "detail": None, "evidence_extra": {}}
        if pk == WITHHELD_KEY or (nk and REDACT_RE.search(nk)) or pk.strip().upper() in ("REDACTED", "REDACT"):
            res["reason"], res["detail"] = "redacted", "withheld or redacted by the council"
            results[k] = res
            continue
        if pk == NAMELESS_KEY:
            res["reason"], res["detail"] = "no-candidate", "no supplier text on the rows"
            results[k] = res
            continue
        # The route test reads the key with bracketed notes removed, so that
        # "LANCASHIRE COUNTY COUNCIL (DIRECT DEBIT PAYMENTS)" stays a payee.
        core = normalise(re.sub(r"[\(\[].*?[\)\]]", " ", pk))
        if nk and NO_PAYEE_RE.search(nk):
            res["reason"], res["detail"] = "no-candidate", "the council's file names no payee"
            results[k] = res
            continue
        if core and PAYMENT_ROUTE_RE.search(core):
            res["reason"], res["detail"] = "payment-route", "names a payment account or route, not an organisation"
            results[k] = res
            continue
        c1, inv = step1(e, R, counts)
        amb_pool = None
        found = []
        if c1:
            found.append(c1)
        c = step1b(e, R)
        if c:
            found.append(c)
        c, amb_pool = step2_2b(e, R)
        if c:
            found.append(c)
        if not c and not amb_pool:
            c = step2c(e, R)
            if c:
                found.append(c)
        for fn in (step3, step4, step5, step6):
            c = fn(e, R)
            if c:
                found.append(c)
        for table, method in (("ocds", "ocds"), ("ggis", "ggis")):
            c = step7(e, R, table, method)
            if c:
                found.append(c)
        rsh = rsh_oscr_evidence(e, R)
        if rsh:
            res["evidence_extra"]["rsh_oscr_by_name"] = rsh
        real = [c for c in found if not c.get("evidence_only") and not c.get("conflict")
                and not c.get("out_of_scope")]
        res["cands"] = found
        conflicts = [c for c in found if c.get("conflict")]
        distinct = []
        for c in real:
            if not any(same_org(c, d) for d in distinct):
                distinct.append(c)
        if conflicts or len(distinct) > 1 or (amb_pool and not distinct):
            why = []
            if conflicts:
                why.append("identifier and name conflict")
            if len(distinct) > 1:
                why.append("steps propose different organisations")
            if amb_pool and not distinct:
                why.append("ambiguous name pool")
            res["queue"] = {"why": why, "pool": amb_pool}
            res["reason"] = "ambiguous"
            res["detail"] = "; ".join(why)
        elif distinct:
            # first step in waterfall order that resolved it
            order = list(STEP_OF)
            res["proposal"] = min(real, key=lambda c: order.index(c["method"]))
            if amb_pool:
                res["evidence_extra"]["name_pool_also_ambiguous"] = amb_pool
        else:
            if any(c.get("out_of_scope") for c in found):
                res["reason"], res["detail"] = "out-of-scope-identifier", "society number, not a Companies House number"
            elif inv == "invalid-identifier" and not found:
                res["reason"], res["detail"] = "invalid-identifier", "bank company number not in any register snapshot held"
            elif looks_individual(e):
                res["reason"], res["detail"] = "individual-payee", "no organisational token and no register match"
            elif any(c.get("ambiguous_ids") for c in found):
                res["reason"], res["detail"] = "ambiguous", "name carries several identifiers in notices or grants"
            else:
                res["reason"], res["detail"] = "no-candidate", None
        results[k] = res

    # step 5b: council companies among resolved GB-COH keys
    coh = {r["proposal"]["org_id"] for r in results.values() if r.get("proposal")
           and r["proposal"]["org_scheme"] == "GB-COH"}
    cc, mart = council_companies(con, coh, R)
    for r in results.values():
        p = r.get("proposal")
        if p and p["org_scheme"] == "GB-COH" and p["org_id"] in cc:
            p["payee_class"] = "council company"
            p["evidence"]["council_company"] = cc[p["org_id"]]

    # ---- write
    inputs = [{"role": "payee keys (spend input)", "path": a.keys, "sha256": sha256_file(a.keys)},
              {"role": "extract manifest", "path": str(EXTRACT / "extract_manifest.json"),
               "sha256": sha256_file(EXTRACT / "extract_manifest.json")},
              {"role": "gold mart_psc_lancs", "path": str(mart), "sha256": sha256_file(mart)}]
    em = json.loads((EXTRACT / "extract_manifest.json").read_text())
    at = now()
    fields = ["decision_id", "body_id", "payee_key", "payee_class", "org_scheme", "org_id", "alias_ids",
              "step", "method", "confidence", "match_weight", "evidence", "reason", "reason_detail",
              "decided_by", "decided_at", "status", "supersedes"]
    vf = open(OUT / "payee_key_values.csv", "w", newline="")
    vw = csv.writer(vf)
    vw.writerow(["body_id", "payee_key_out", "financial_year", "value_pence", "rows"])
    qf = open(OUT / "queue.csv", "w", newline="")
    qw = csv.writer(qf)
    qw.writerow(["body_id", "payee_key", "value_pence_total", "why", "candidates"])
    with open(OUT / "resolver_proposed.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for (body, pk), r in sorted(results.items(), key=lambda kv: (kv[0][0], -keys[kv[0]]["total"])):
            e = keys[(body, pk)]
            masked = r["reason"] in ("redacted", "individual-payee")
            pk_out = f"{'INDIVIDUAL' if r['reason'] == 'individual-payee' else 'REDACTED'}:{key_hash(body, pk)}" \
                if masked else pk
            for fy in sorted(e["value"]):
                vw.writerow([body, pk_out, fy, e["value"][fy], e["rows"][fy]])
            p = r.get("proposal")
            if p:
                ev = dict(p["evidence"], **r["evidence_extra"])
                ev["inputs"] = "extract_manifest.json sha256 " + inputs[1]["sha256"][:16]
                row = {"body_id": body, "payee_key": pk_out, "payee_class": p["payee_class"],
                       "org_scheme": p["org_scheme"], "org_id": p["org_id"],
                       "alias_ids": "|".join(p["alias_ids"]), "step": STEP_OF[p["method"]],
                       "method": p["method"], "confidence": CONF[p["method"]], "match_weight": "",
                       "evidence": json.dumps(ev, ensure_ascii=False, sort_keys=True),
                       "reason": "", "reason_detail": ""}
                counts[f"resolved:{p['method']}"] += 1
            else:
                cls = "individual" if r["reason"] == "individual-payee" else "unclassified"
                ev = {} if masked else {"candidates": [
                    {k2: c[k2] for k2 in ("org_scheme", "org_id", "method")} | {"evidence": c["evidence"]}
                    for c in r["cands"]], **r["evidence_extra"]}
                row = {"body_id": body, "payee_key": pk_out, "payee_class": cls,
                       "org_scheme": "INDIVIDUAL" if cls == "individual" else "UNRESOLVED", "org_id": "",
                       "alias_ids": "", "step": "9" if r["queue"] else "10",
                       "method": "queue" if r["queue"] else "unclassified", "confidence": "",
                       "match_weight": "", "evidence": json.dumps(ev, ensure_ascii=False, sort_keys=True) if ev else "",
                       "reason": r["reason"], "reason_detail": r["detail"] or ""}
                counts[f"unclassified:{r['reason']}"] += 1
                if r["queue"]:
                    qw.writerow([body, pk, e["total"], "; ".join(r["queue"]["why"]),
                                 json.dumps({"steps": [{k2: c[k2] for k2 in ("org_scheme", "org_id", "method")}
                                                       | {"evidence": c["evidence"]} for c in r["cands"]],
                                             "name_pool": r["queue"]["pool"]}, ensure_ascii=False)])
            row["decision_id"] = "pp1-" + hashlib.sha1(
                f"{body}|{pk}|{row['org_scheme']}|{row['org_id']}|{row['method']}".encode()).hexdigest()[:16]
            row.update(decided_by=decided_by, decided_at=at, status="active", supersedes="")
            w.writerow(row)
    vf.close()
    qf.close()
    write_json(OUT / "run_manifest.json", {
        "pipelineGitSha": sha, "decidedBy": decided_by, "runAt": at, "duckdb": duckdb.__version__,
        "keys": len(keys), "bodies": sorted({k[0] for k in keys}), "inputs": inputs,
        "extractInputs": em.get("inputs", []), "counts": dict(sorted(counts.items())),
        "councilCompanies": len(cc), "notes": [
            "Step 2 (name and postcode) closes nothing: the banks carry no postcode.",
            "Step 8 (Splink) not built in Phase 1 (RULES.md decision 10).",
            "Every step runs for every key; a key with two different proposals goes to the queue."]})
    print(json.dumps(dict(sorted(counts.items())), indent=1))


if __name__ == "__main__":
    main()
