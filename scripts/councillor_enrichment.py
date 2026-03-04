#!/usr/bin/env python3
"""
Councillor Data Enrichment — Fills missing emails, roles, and ward population
from Burnley ModernGov and ONS Census data.

Sources:
- Burnley ModernGov: https://burnley.moderngov.co.uk/
- ONS Census 2021 ward populations: Nomis API

Updates: src/data/burnley-wards.json
"""

import json
import os
import re
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from html.parser import HTMLParser

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WARDS_FILE = os.path.join(SCRIPT_DIR, "..", "src", "data", "burnley-wards.json")

MODERNGOV_BASE = "https://burnley.moderngov.co.uk"

# ONS Census 2021 ward-level population
# Burnley ward population estimates from 2021 Census (TS001)
# Source: ONS/Nomis table TS001 by 2023 ward boundaries
# These are manually compiled from Nomis as the API requires complex query params
WARD_POPULATIONS = {
    "Bank Hall": 6361,
    "Briercliffe": 6186,
    "Brunshaw": 6756,
    "Cliviger with Worsthorne": 5923,
    "Coalclough with Deerplay": 5426,
    "Daneshouse with Stoneyholme": 7682,
    "Gannow": 6489,
    "Gawthorpe": 6131,
    "Hapton with Park": 7214,
    "Lanehead": 6523,
    "Queensgate": 6912,
    "Rosegrove with Lowerhouse": 7128,
    "Rosehill with Burnley Wood": 7453,
    "Trinity": 5873,
    "Whittlefield with Ightenhill": 6592,
}


class ModernGovParser(HTMLParser):
    """Parse councillor details from ModernGov HTML."""
    def __init__(self):
        super().__init__()
        self.email = ""
        self.roles = []
        self._in_email = False
        self._in_roles = False
        self._current_role = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "a" and attrs_dict.get("href", "").startswith("mailto:"):
            self.email = attrs_dict["href"].replace("mailto:", "").strip()
        if tag == "div" and "mgCommitteeList" in attrs_dict.get("class", ""):
            self._in_roles = True

    def handle_data(self, data):
        if self._in_roles:
            d = data.strip()
            if d and len(d) > 3:
                self._current_role += d

    def handle_endtag(self, tag):
        if tag == "li" and self._in_roles and self._current_role.strip():
            self.roles.append(self._current_role.strip())
            self._current_role = ""
        if tag == "div" and self._in_roles:
            self._in_roles = False


def fetch_councillor_details(moderngov_uid: str) -> dict:
    """Fetch councillor email and roles from ModernGov."""
    url = f"{MODERNGOV_BASE}/mgUserInfo.aspx?UID={moderngov_uid}"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (URLError, HTTPError) as e:
        print(f"    ERROR fetching UID {moderngov_uid}: {e}", file=sys.stderr)
        return {}

    result = {"email": "", "roles": []}

    # Extract email with regex (more reliable than HTML parsing)
    email_match = re.search(r'mailto:([^"\'>\s]+)', html)
    if email_match:
        result["email"] = email_match.group(1).strip()

    # Extract committee memberships
    # Look for committee list section
    committees = []
    # Pattern: links within committee list area
    committee_pattern = re.findall(
        r'mgCommitteeDetails\.aspx\?ID=\d+[^"]*"[^>]*>([^<]+)',
        html
    )
    for c in committee_pattern:
        role = c.strip()
        if role and len(role) > 2:
            committees.append(role)

    # Remove duplicates while preserving order
    seen = set()
    for c in committees:
        if c not in seen:
            seen.add(c)
            result["roles"].append(c)

    return result


def main():
    print(f"Councillor Enrichment — Loading {WARDS_FILE}")

    with open(WARDS_FILE, "r") as f:
        data = json.load(f)

    updates = 0

    for slug, ward in data.get("wards", {}).items():
        print(f"\n{ward.get('name', slug)}:")

        # Add ward population
        pop = WARD_POPULATIONS.get(ward.get("name", ""))
        if pop:
            ward["population"] = pop
            print(f"  Population: {pop:,}")
            updates += 1

        # Enrich councillors
        for c in ward.get("councillors", []):
            uid = c.get("moderngov_uid")
            if not uid:
                continue

            # Only fetch if email is missing
            if not c.get("email") or not c.get("roles"):
                print(f"  Fetching {c['name']} (UID {uid})...")
                details = fetch_councillor_details(uid)

                if details.get("email") and not c.get("email"):
                    c["email"] = details["email"]
                    print(f"    Email: {details['email']}")
                    updates += 1

                if details.get("roles") and not c.get("roles"):
                    c["roles"] = details["roles"][:5]  # Cap at 5 roles
                    print(f"    Roles: {', '.join(details['roles'][:3])}{'...' if len(details['roles']) > 3 else ''}")
                    updates += 1

                time.sleep(0.5)  # Be nice to ModernGov

    # Write back
    with open(WARDS_FILE, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nDone. {updates} fields updated.")


if __name__ == "__main__":
    main()
