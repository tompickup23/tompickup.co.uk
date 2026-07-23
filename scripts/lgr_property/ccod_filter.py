#!/usr/bin/env python3
"""Stream-filter CCOD FULL to the 14 Lancashire districts. Reads stdin (the
single CSV inside the zip, via funzip), writes a small filtered CSV to stdout.
Keeps title/tenure/address/district/postcode + all 4 proprietor name+category."""
import csv, sys

DISTRICTS = {
    "BURNLEY","HYNDBURN","PENDLE","ROSSENDALE","RIBBLE VALLEY","PRESTON",
    "CHORLEY","SOUTH RIBBLE","WEST LANCASHIRE","FYLDE","WYRE","LANCASTER",
    "BLACKBURN WITH DARWEN","BLACKPOOL",
}
r = csv.reader(sys.stdin)
w = csv.writer(sys.stdout)
header = next(r)
# column indices
TITLE, TEN, ADDR, DIST, PC = 0, 1, 2, 3, 6
P1N, P1C = 9, 11
P2N, P2C = 15, 17
P3N, P3C = 21, 23
P4N, P4C = 27, 29
w.writerow(["title","tenure","address","district","postcode",
            "p1name","p1cat","p2name","p2cat","p3name","p3cat","p4name","p4cat"])
n = 0
for row in r:
    if len(row) < 30:
        continue
    if row[DIST] not in DISTRICTS:
        continue
    n += 1
    w.writerow([row[TITLE], row[TEN], row[ADDR], row[DIST], row[PC],
                row[P1N], row[P1C], row[P2N], row[P2C],
                row[P3N], row[P3C], row[P4N], row[P4C]])
sys.stderr.write(f"filtered rows: {n}\n")
