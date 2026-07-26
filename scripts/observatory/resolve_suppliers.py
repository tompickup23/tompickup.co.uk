#!/usr/bin/env python3
"""Supplier resolution for the Lancashire Pound.

Stage 1 (this file, runnable now): normalise supplier names, classify
public-sector / excluded payees, produce the private-supplier universe with
per-council coverage stats.
Stage 2 (needs vps register_index): exact + fuzzy name -> company number.
Stage 3 (needs PSC files): ownership chain walk -> tier.

Tier rules and safe-language constraints: briefing pack METHODS.md s2 +
LEGAL.md. Public-to-public transfers are excluded from the retention
denominator (they are precepts/levies/transfers, not procurement) and
disclosed separately.
"""
import json, re, gzip, csv, sys
from pathlib import Path

PROC = Path.home() / "observatory-data/processed"
VPS = Path.home() / "observatory-data/vps"

LEGAL_SUFFIX = re.compile(
    r"\b(LTD|LIMITED|PLC|LLP|LP|CIC|CIO|INC|CO|COMPANY|GROUP|HOLDINGS|UK)\b\.?$")
VAT_TAG = re.compile(r"\s*-\s*(NET|GROSS)\s*$", re.I)

def normalise(name: str) -> str:
    n = VAT_TAG.sub("", name.upper().strip())
    n = re.sub(r"\bT/A\b.*$", " ", n)          # drop trading-as tails
    n = re.sub(r"[^A-Z0-9& ]+", " ", n)
    n = n.replace("&", " AND ")                # canonical AND
    n = re.sub(r"\s+", " ", n).strip()
    prev = None
    while prev != n:          # strip stacked suffixes: "X GROUP LTD" -> "X"
        prev = n
        n = LEGAL_SUFFIX.sub("", n).strip()
    return n

def supplier_variants(raw: str):
    """Ordered match keys for a spend-side supplier name (registered names use
    plain normalise). Handles bracketed alternates, former-name notes, and
    source truncation."""
    out, seen = [], set()
    def add(s):
        k = normalise(s)
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    add(raw)
    add(re.sub(r"[\(\[].*?[\)\]]", " ", raw))          # drop (…) [...] segments
    base = normalise(raw)
    toks = base.split()
    if toks and len(toks[-1]) <= 2 and len(base) >= 24:  # truncated feed tail
        add(" ".join(toks[:-1]))
    return out

PUBLIC_PATTERNS = [
    r"\bCOUNCIL\b", r"\bBOROUGH OF\b", r"\bCITY OF\b", r"\bCOUNTY OF\b",
    r"\bNHS\b", r"INTEGRATED CARE", r"\bICB\b", r"FOUNDATION TRUST",
    r"\bHMRC\b", r"HM REVENUE", r"\bDWP\b", r"DEPARTMENT FOR", r"DEPARTMENT OF",
    r"HOME OFFICE", r"MINISTRY OF", r"\bHM COURTS?\b", r"\bDVLA\b",
    r"POLICE", r"CONSTABULARY", r"CROWN COMMISSIONER", r"FIRE (AND|&) RESCUE",
    r"ENVIRONMENT AGENCY", r"HIGHWAYS ENGLAND", r"NATIONAL HIGHWAYS",
    r"PENSION FUND", r"SUPERANNUATION", r"VALUATION OFFICE",
    r"^TEACHERS PENSIONS?\b", r"^NEST\b",
    r"^GOVERNMENT\b", r"PUBLIC WORKS LOAN", r"\bPWLB\b", r"HM TREASURY",
    r"PARISH COUNCIL", r"TOWN COUNCIL", r"COMBINED AUTHORITY",
    r"\bMBC\b", r"\bBC$", r"\bCC$", r"\bCCG\b", r"\bICS\b",
    r"DEBT MANAGEMENT OFFICE", r"\bOFSTED\b", r"\bUKPN\b",
    r"UNIVERSITY(?! OF LAW)", r"\bUCLAN\b", r"COLLEGE OF", r"\bCOLLEGE\b",
    r"^THE COUNCIL", r"ROYAL MAIL", r"POST OFFICE",
    r"TRANSPORT FOR", r"ARMS LENGTH MANAGEMENT",
]
PUBLIC_RE = re.compile("|".join(PUBLIC_PATTERNS))

EXCLUDED_PATTERNS = [
    r"^REDACT", r"^VARIOUS\b", r"^CONFIDENTIAL", r"^PERSONAL\b",
    r"^NAME (WITHHELD|REDACTED)", r"^INDIVIDUAL\b", r"^EMPLOYEE\b",
    r"^UNKNOWN\b", r"\bINTERNAL\b", r"^THIRD PARTY SUPPLIER",
    r"^MR\b", r"^MRS\b", r"^MISS\b", r"^MS\b", r"^DR\b",
    r"^BANK OF\b", r"BUILDING SOCIETY$",   # treasury counterparties, not suppliers
    r"^BARCLAYS\b", r"^LLOYDS\b", r"^NATWEST\b", r"^HSBC\b", r"^SANTANDER\b",
    r"LIQUIDITY FUND", r"^LONDON TREASURY", r"MONEY MARKET FUND",
]
EXCLUDED_RE = re.compile("|".join(EXCLUDED_PATTERNS))

# Council/public-owned trading companies (bootstrap; the PSC chain walk also
# detects these when the corporate PSC is a local authority). Normalised keys.
COUNCIL_OWNED = {
    "LANCASHIRE RENEWABLES",          # LCC-owned waste operator
    "ENVECO BLACKPOOL WASTE SERVICES",
    "BLACKPOOL COASTAL HOUSING",      # ALMO
    "BLACKPOOL TRANSPORT SERVICES",
    "BLACKPOOL OPERATING",            # winter gardens etc
    "ONE FYLDE",
    "BURNLEY LEISURE",                # leisure trust (charitable, council-linked)
    "LIBERATA UK" if False else "",   # NOT council-owned; placeholder guard
} - {""}

def classify(name: str) -> str:
    """public | excluded | councilOwned | supplier"""
    n = normalise(name)
    if not n or EXCLUDED_RE.search(n):
        return "excluded"
    if n in COUNCIL_OWNED:
        return "councilOwned"
    if PUBLIC_RE.search(n):
        return "public"
    return "supplier"

def stage1():
    spend = json.loads((PROC / "council_supplier_spend.json").read_text())
    out = {"$meta": {"note": "stage1 classification of supplier universe"},
           "bodies": {}}
    universe = {}
    for body, d in spend["bodies"].items():
        buckets = {"public": 0.0, "excluded": 0.0, "supplier": 0.0,
                   "councilOwned": 0.0}
        sup_rows = {}
        for name, amt in d["suppliers"].items():
            c = classify(name)
            buckets[c] += amt
            if c == "supplier":
                key = normalise(name)
                sup_rows[name] = amt
                u = universe.setdefault(key, {"names": set(), "total": 0.0,
                                              "bodies": set()})
                u["names"].add(name)
                u["total"] += amt
                u["bodies"].add(body)
        total = d["total"]
        out["bodies"][body] = {
            "total": total, "tail": d["tail"],
            "publicTransfers": round(buckets["public"], 2),
            "councilOwned": round(buckets["councilOwned"], 2),
            "excludedPayees": round(buckets["excluded"], 2),
            "privateSupplierSpend": round(buckets["supplier"], 2),
            "privateSupplierCount": len(sup_rows),
        }
        print(f"{body}: private £{buckets['supplier']/1e6:.1f}m | "
              f"public-transfer £{buckets['public']/1e6:.1f}m | "
              f"excluded £{buckets['excluded']/1e6:.1f}m of £{total/1e6:.1f}m")
    uni = [{"key": k, "names": sorted(v["names"]), "total": round(v["total"], 2),
            "bodies": sorted(v["bodies"])}
           for k, v in sorted(universe.items(), key=lambda kv: -kv[1]["total"])]
    out["universe"] = uni
    (PROC / "supplier_universe.json").write_text(json.dumps(out))
    print(f"universe: {len(uni)} distinct private suppliers; "
          f"top100 = £{sum(u['total'] for u in uni[:100])/1e6:.0f}m")

if __name__ == "__main__":
    stage1()
