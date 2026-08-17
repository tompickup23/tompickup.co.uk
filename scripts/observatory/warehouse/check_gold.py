#!/usr/bin/env python3
"""Run every checks_gold/<table>.sql against the built gold partitions.

Same blunt contract as check_silver.py: any row returned by any check is a
failure, so a clean table returns nothing at all and there is no threshold to
argue about.

Two things here are not SQL and cannot be, so they run first:

  * **the port check.** crosswalk.normalise is a hand port of
    resolve_suppliers.normalise. A port that drifts silently is worse than no
    port, because the crosswalk would be keyed on names the production matcher
    would never produce. If the original is importable, both are run over a
    fixture and any disagreement fails.
  * **the registry check.** gold/entity_id_registry.jsonl is the mint-once
    guarantee. It is checked for duplicate anchors, duplicate ids and for
    agreement with the entity table it is supposed to explain.

Usage:
    check_gold.py                    # every table, latest partition each
    check_gold.py --table entity
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import crosswalk as X  # noqa: E402
import silver as SV  # noqa: E402

CHECKS = Path(__file__).resolve().parent / "checks_gold"

# Names chosen to exercise every branch of normalise: the AND canonicalisation,
# the stacked-suffix strip, the trading-as tail, the VAT tag and the punctuation
# class. If the port drifts on any of these it drifts on real data.
PORT_FIXTURE = [
    "J & B SMITH (BURNLEY) LTD",
    "ACME GROUP HOLDINGS LIMITED",
    "BLOGGS LTD T/A THE CORNER SHOP",
    "SOMETHING LTD TA OTHER THING",
    "WIDGETS UK PLC - NET",
    "O'REILLY & SONS CO.",
    "RED ROSE SCHOOL - SALES LEDGER",
    "PRIMESIGHT",
    "",
]


def check_normalise_port():
    """Compare the warehouse port against the production original."""
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    try:
        from resolve_suppliers import normalise as orig  # noqa: E402
    except Exception as e:
        print(f"  skip [normalise-port] original not importable here ({e}); "
              "the port is unverified on this host")
        return 0, 1
    bad = []
    for s in PORT_FIXTURE:
        a, b = X.normalise(s), orig(s)
        if a != b:
            bad.append((s, a, b))
    if bad:
        print(f"  FAIL [normalise-port] {len(bad)} disagreement(s):")
        for s, a, b in bad:
            print(f"        {s!r}: port {a!r} vs original {b!r}")
        return 1, 0
    print(f"  ok   [normalise-port] {len(PORT_FIXTURE)} fixtures agree")
    return 0, 0


def check_registry(entity_pq, con):
    """The append-only id registry is the mint-once guarantee. Test it."""
    p = X.registry_path()
    if not p.exists():
        print("  FAIL [registry] gold/entity_id_registry.jsonl is missing. "
              "Without it the next build mints new ids for everything.")
        return 1
    recs = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    fails = 0

    anchors = Counter(r["anchor"] for r in recs)
    dup_anchor = [a for a, n in anchors.items() if n > 1]
    if dup_anchor:
        print(f"  FAIL [registry-anchor-unique] {len(dup_anchor)} anchor(s) "
              f"appear twice, e.g. {dup_anchor[:3]}. An anchor resolving to "
              "two ids means an id was reassigned.")
        fails += 1
    else:
        print(f"  ok   [registry-anchor-unique] {len(recs):,} rows, "
              f"{len(anchors):,} distinct anchors")

    ids = Counter(r["entity_id"] for r in recs)
    collided = [i for i, n in ids.items() if n > 1]
    bound = {r["entity_id"] for r in recs if r.get("bound")}
    unexplained = [i for i in collided if i not in bound]
    if unexplained:
        print(f"  FAIL [registry-id-unique] {len(unexplained)} id(s) minted "
              f"more than once without a binding, e.g. {unexplained[:3]}")
        fails += 1
    else:
        print(f"  ok   [registry-id-unique] {len(ids):,} distinct ids, "
              f"{len(collided):,} carry a bound second anchor")

    malformed = [r["entity_id"] for r in recs
                 if len(r["entity_id"]) != 26
                 or set(r["entity_id"]) - set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")]
    if malformed:
        print(f"  FAIL [registry-ulid] {len(malformed)} malformed id(s)")
        fails += 1
    else:
        print(f"  ok   [registry-ulid] every id is a well formed ULID")

    orphans = con.execute(f"""
        SELECT count(*) FROM read_parquet('{entity_pq}') e
        ANTI JOIN (SELECT unnest($ids) AS entity_id) r USING (entity_id)
    """, {"ids": sorted(ids)}).fetchone()[0]
    if orphans:
        print(f"  FAIL [registry-covers-entities] {orphans:,} entities carry "
              "an id the registry has never issued")
        fails += 1
    else:
        print("  ok   [registry-covers-entities] every entity id was issued "
              "by the registry")
    return fails


def split_checks(text):
    out, name, buf = [], None, []
    for line in text.splitlines():
        if line.strip().lower().startswith("-- check:"):
            if name:
                out.append((name, "\n".join(buf)))
            name = line.split(":", 1)[1].strip()
            buf = []
        elif name is not None:
            buf.append(line)
    if name:
        out.append((name, "\n".join(buf)))
    return [(n, s.strip()) for n, s in out if s.strip()]


def latest(table):
    base = X.gold_dir() / table
    parts = [p for p in sorted(base.glob("snapshot_date=*"))
             if (p / "part.parquet").exists()] if base.exists() else []
    return parts[-1] if parts else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table")
    args = ap.parse_args()

    con, dv = SV.connect()
    SV.log(f"duckdb {dv}, gold root {X.gold_dir()}")

    total = failures = 0

    ent_part = latest("entity")
    xw_part = latest("crosswalk")
    if not ent_part or not xw_part:
        raise SystemExit("FATAL: entity or crosswalk partition missing. "
                         "Run build_entities.py first.")
    entity_pq = str(ent_part / "part.parquet")
    crosswalk_pq = str(xw_part / "part.parquet")

    if not args.table:
        SV.log("code and registry checks")
        f, skipped = check_normalise_port()
        total += 1
        failures += f
        f = check_registry(entity_pq, con)
        total += 4
        failures += f

    files = sorted(CHECKS.glob("*.sql"))
    if args.table:
        files = [f for f in files if f.stem == args.table]
        if not files:
            raise SystemExit(f"no checks file for gold table {args.table}")

    for f in files:
        table = f.stem
        part = latest(table)
        if not part:
            SV.log(f"SKIP {table}: no built partition")
            continue
        snap = part.name.split("=", 1)[1]
        pq = str(part / "part.parquet")
        manifest = json.loads((part / "manifest.json").read_text())
        checks = split_checks(f.read_text())
        SV.log(f"{table} snapshot_date={snap} "
               f"({manifest['rows']:,} rows, {len(checks)} checks)")
        for name, sql in checks:
            body = (sql.replace("{pq}", pq)
                       .replace("{entity_pq}", entity_pq)
                       .replace("{crosswalk_pq}", crosswalk_pq)
                       .replace("{snapshot}", snap)
                       .replace("{rows}", str(manifest["rows"])))
            total += 1
            try:
                rows = con.execute(body).fetchall()
            except Exception as e:
                failures += 1
                print(f"  FAIL [{name}] check did not run: {e}")
                continue
            if rows:
                failures += 1
                print(f"  FAIL [{name}] {len(rows)} row(s) returned:")
                for r in rows[:5]:
                    print(f"        {r}")
            else:
                print(f"  ok   [{name}]")

    con.close()
    print()
    SV.log(f"{total} checks run, {failures} failed")
    if failures:
        print("GOLD CHECKS FAILED")
        return 1
    print("GOLD CHECKS GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
