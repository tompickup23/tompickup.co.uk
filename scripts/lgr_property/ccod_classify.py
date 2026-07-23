#!/usr/bin/env python3
"""Classify CCOD Lancashire-14 titles into public-sector owner buckets.
Writes ccod_public.csv: title,tenure,address,district,postcode,owner,body."""
import csv, re

def norm(s):
    s = (s or "").upper()
    if s.startswith("THE "): s = s[4:]
    return re.sub(r"[^A-Z0-9]", "", s)

# district councils: token that must appear + council-type marker
DISTRICT_TOKENS = [
    ("SOUTHRIBBLE", "South Ribble Borough Council"),
    ("RIBBLEVALLEY", "Ribble Valley Borough Council"),
    ("WESTLANCASHIRE", "West Lancashire Borough Council"),
    ("BLACKBURNWITHDARWEN", "Blackburn with Darwen Borough Council"),
    ("BLACKBURN", "Blackburn with Darwen Borough Council"),
    ("BLACKPOOL", "Blackpool Council"),
    ("BURNLEY", "Burnley Borough Council"),
    ("HYNDBURN", "Hyndburn Borough Council"),
    ("PENDLE", "Pendle Borough Council"),
    ("ROSSENDALE", "Rossendale Borough Council"),
    ("PRESTON", "Preston City Council"),
    ("CHORLEY", "Chorley Borough Council"),
    ("FYLDE", "Fylde Borough Council"),
    ("WYRE", "Wyre Borough Council"),
    ("LANCASTER", "Lancaster City Council"),
]
COUNCIL_MARK = ("BOROUGHCOUNCIL", "CITYCOUNCIL", "DISTRICTCOUNCIL",
                "COUNCILOFTHEBOROUGH", "COUNCILOFTHEBOROUGHOF",
                "MAYORALDERMEN", "BOROUGHCOUNCILOF")

def classify(name):
    n = norm(name)
    if not n:
        return None, None
    # town / parish councils (lowest tier)
    if "PARISHCOUNCIL" in n or "TOWNCOUNCIL" in n:
        return "parish", name.strip().title()
    # county
    if "COUNTYCOUNCIL" in n and "LANCASHIRE" in n:
        return "county", "Lancashire County Council"
    if "PALATINEOFLANCASTER" in n:
        return "county", "Lancashire County Council"
    # ambulance (route NWAS here, before generic NHS)
    if "NORTHWESTAMBULANCE" in n or ("AMBULANCE" in n and "NHS" in n):
        return "ambulance", "North West Ambulance Service"
    # NHS
    if ("NHS" in n or "FOUNDATIONTRUST" in n or "NATIONALHEALTHSERVICE" in n
            or "COMMUNITYHEALTHPARTNERSHIP" in n or "PRIMARYCARE" in n
            or "INTEGRATEDCARE" in n or "HEALTHPROPERTIES" in n
            or "SECRETARYOFSTATEFORHEALTH" in n):
        return "nhs", name.strip().title()
    # police
    if "POLICE" in n or "CRIMECOMMISSIONER" in n or "CHIEFCONSTABLE" in n:
        return "police", "Lancashire Constabulary / PCC"
    # fire
    if "FIREAUTHORITY" in n or ("FIRE" in n and "LANCASHIRE" in n):
        return "fire", "Lancashire Combined Fire Authority"
    # education -> skip (GIAS covers schools/colleges/universities)
    if ("UNIVERSITY" in n or "COLLEGE" in n or "ACADEMY" in n
            or "SCHOOL" in n or "EDUCATIONCOUNCIL" in n
            or "EDUCATIONTRUST" in n or "MULTIACADEMY" in n):
        return None, None
    # district / borough / city councils
    if any(m in n for m in COUNCIL_MARK):
        for tok, body in DISTRICT_TOKENS:
            if tok in n:
                return "district", body
    # central government + arms-length public bodies
    if ("NATIONALHIGHWAYS" in n or "HIGHWAYSENGLAND" in n or "HIGHWAYSAGENCY" in n):
        return "gov", "National Highways"
    if "NETWORKRAIL" in n:
        return "gov", "Network Rail"
    if "ENVIRONMENTAGENCY" in n:
        return "gov", "Environment Agency"
    if "HOMESENGLAND" in n or "HOMESANDCOMMUNITIESAGENCY" in n:
        return "gov", "Homes England"
    if "CANAL" in n and "RIVER" in n:
        return "gov", "Canal & River Trust"
    if "COALAUTHORITY" in n:
        return "gov", "The Coal Authority"
    if "SECRETARYOFSTATE" in n or "MINISTRYOF" in n:
        return "gov", name.strip().title()
    return None, None

rows = csv.DictReader(open("ccod_lancs14.csv"))
out = csv.writer(open("ccod_public.csv", "w"))
out.writerow(["title","tenure","address","district","postcode","owner","body"])
from collections import Counter
by_owner = Counter(); by_body = Counter(); no_pc = 0
seen = set(); n = 0
for row in rows:
    owner = body = None
    for slot in ("p1name","p2name","p3name","p4name"):
        o, b = classify(row[slot])
        if o:
            owner, body = o, b
            break
    if not owner:
        continue
    if row["title"] in seen:
        continue
    seen.add(row["title"])
    if not (row["postcode"] or "").strip():
        no_pc += 1
    out.writerow([row["title"], row["tenure"], row["address"], row["district"],
                  row["postcode"], owner, body])
    by_owner[owner] += 1; by_body[body] += 1; n += 1

print("public titles classified:", n)
print("without postcode (will drop):", no_pc)
print("\n=== by owner ===")
for o, c in by_owner.most_common():
    print(f"{c:6d}  {o}")
print("\n=== top bodies ===")
for b, c in by_body.most_common(30):
    print(f"{c:6d}  {b}")
