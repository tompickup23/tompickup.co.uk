#!/usr/bin/env python3
"""Agent review (RULES.md decisions 12 and 13). Not verification: no person
labels these rows, and the labels never enter the resolver.

Items, each with an evidence block built from the register extracts:
  gold    every pair in gold_set_pairs.csv (the pipeline's proposal)
  recall  every distinct key in recall_sample.csv, with up to five candidate
          records from a blocked search (distinctive name tokens over the
          Companies House, Charity Commission, CQC, public-body and GLEIF
          extracts) plus any candidates the step 9 queue holds
  verify  every resolved key in the verification sets (verify_<body>.csv)
          that is not corroborated
A key whose name has no organisational form and contains a forename and
surname from the individual PSC list is withheld from review (rule P1's
test), so no likely personal name is sent to the model.

The evidence block: the council's name for the payee (raw and canonical
names), the council's own descriptions (department, service area and
expenditure category, from descriptions.py; never the free-text
description), the payment years, and for each register record its
record id, register, name, previous names, status, dates, post town and
outward postcode, and SIC descriptions (Companies House) or the
register's equivalent. No person's name is read from any register.

Two independent passes through the Message Batches API on the current
Sonnet model, with different prompts and one fixed JSON schema: label
(same, different, cannot tell), the record ids relied on, and a reason.
A pass's "same" counts only if it cites a record id from the block. The
agreed label is "same" or "different" only when both passes give it (for
recall, citing the same record); otherwise "cannot tell" (decision 13).
Every row carries the model id, prompt hash and batch id of each pass.

  agent_review.py build                 agent_items.jsonl, agent_review_manifest.json
  agent_review.py submit [--pilot N]    two batches (N items each for a pilot)
  agent_review.py collect               waits, then agent_labels.csv and costs
The API key is read from ANTHROPIC_API_KEY or, with --key-file, from the
ANTHROPIC_API_KEY line of that file; it is never printed or written.
"""
import argparse
import csv
import glob
import gzip
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import duckdb

from common import (EXTRACT, INPUTS, OUT, SILVER, distinctive_tokens, git_sha, normalise,
                    sha256_file, write_json)

csv.field_size_limit(sys.maxsize)

MODEL = "claude-sonnet-5-5"
EFFORT = "low"
# Claude Sonnet 5.5 Message Batches prices, US dollars per million tokens:
# half the standard $2 input and $10 output (claude-api skill, model table
# cached 25 September 2026; batches at 50% of standard prices).
PRICE_IN, PRICE_OUT = 1.00, 5.00
ITEMS = OUT / "agent_items.jsonl"
MANIFEST = OUT / "agent_review_manifest.json"
LABELS = OUT / "agent_labels.csv"

BODY_NAME = {"blackburn": "Blackburn with Darwen Borough Council", "blackpool": "Blackpool Council",
             "burnley": "Burnley Borough Council", "fylde": "Fylde Borough Council",
             "hyndburn": "Hyndburn Borough Council", "lancashire_cc": "Lancashire County Council",
             "lancaster": "Lancaster City Council", "pendle": "Pendle Borough Council",
             "preston": "Preston City Council", "ribble_valley": "Ribble Valley Borough Council",
             "rossendale": "Rossendale Borough Council", "south_ribble": "South Ribble Borough Council",
             "west_lancashire": "West Lancashire Borough Council", "wyre": "Wyre Council"}

SCHEMA = {"type": "object",
          "properties": {"label": {"type": "string", "enum": ["same", "different", "cannot tell"]},
                         "record_ids": {"type": "array", "items": {"type": "string"}},
                         "reason": {"type": "string"}},
          "required": ["label", "record_ids", "reason"], "additionalProperties": False}

COMMON_RULES = (
    "Use only the evidence given. Judge identity, not quality, size or conduct. Payments are from an English "
    "council's published spending file in financial years 2024/25 and 2025/26. Payee names are often "
    "abbreviated, truncated, upper-cased or carry a site, branch or ledger note. A company keeps its number "
    "when it changes its name, so a previous name counts. A company dissolved before the payments, or "
    "incorporated after them, is unlikely to be the payee. Give record_ids only from the record_id values "
    "in the evidence. Write the reason in one plain sentence, and never name a person.")
PROMPTS = {
    "A": {"system": "You match payees in council spending files to public register records. " + COMMON_RULES,
          "resolved": "Is the payee the same organisation as the proposed organisation described by the records? "
                      "Answer same, different or cannot tell, and cite the record ids you relied on.",
          "recall": "Is the payee the same organisation as any one of the candidate records? If one is, answer "
                    "same and cite that record id only. If none is, answer different. If the evidence does not "
                    "decide it, answer cannot tell."},
    "B": {"system": "You audit name matches between council payment lines and register records. Look first for "
                    "anything that would make the match wrong: a different trade or activity, a different place, "
                    "dates that rule it out, a common name shared by unrelated bodies, or a group company when "
                    "the payee is another member of the group. " + COMMON_RULES,
          "resolved": "An automatic matcher proposed the organisation in the records for this payee. Decide whether "
                      "the proposal is right (same), wrong (different) or not decidable from this evidence (cannot "
                      "tell). Cite the record ids that support your answer.",
          "recall": "No automatic match was made for this payee. Decide whether one of the candidate records is the "
                    "payee (same, citing that one record id), none of them is (different), or the evidence cannot "
                    "decide (cannot tell)."},
}


def prompt_hash(p):
    blob = json.dumps({"model": MODEL, "effort": EFFORT, "schema": SCHEMA, "prompt": PROMPTS[p]}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


# --------------------------------------------------------------- evidence --
class Records:
    """Register records by id, read in bulk from the extracts and silver."""

    def __init__(self, con):
        self.con = con

    def _in(self, vals):
        import pyarrow as pa
        self.con.register("_ids", pa.table({"id": pa.array(sorted(set(vals)), type=pa.string())}))
        self.con.execute("CREATE OR REPLACE TEMP TABLE ids AS SELECT * FROM _ids")

    def companies(self, crns):
        if not crns:
            return {}
        self._in(crns)
        parts = sorted(glob.glob(str(SILVER / "ch_register/snapshot_date=*/part.parquet")))
        out = {}
        for r in self.con.execute(f"""
                SELECT company_number, arg_max(company_name, snapshot_date), arg_max(company_status, snapshot_date),
                       arg_max(company_category, snapshot_date), min(incorporation_date)::VARCHAR,
                       arg_max(dissolution_date, snapshot_date)::VARCHAR, arg_max(reg_post_town, snapshot_date),
                       arg_max(reg_postcode_norm, snapshot_date),
                       arg_max(list_filter([sic_text_1, sic_text_2, sic_text_3, sic_text_4], lambda x: x IS NOT NULL),
                               snapshot_date),
                       arg_max(previous_names, snapshot_date), max(snapshot_date)::VARCHAR
                FROM read_parquet({parts!r}) JOIN ids ON company_number = ids.id
                GROUP BY 1 ORDER BY 1""").fetchall():
            pc = (r[7] or "")
            out[r[0]] = {"record_id": f"CH:{r[0]}", "register": "Companies House", "name": r[1], "status": r[2],
                         "category": r[3], "incorporated": r[4], "dissolved": r[5], "post_town": r[6],
                         "postcode_district": pc[:-3] if len(pc) > 4 else pc, "sic": r[8] or [],
                         "previous_names": [{"name": p["name"], "changed": str(p["condate"])}
                                            for p in (r[9] or [])][:6],
                         "register_snapshot": r[10]}
        return out

    def by_table(self, table, key, vals, build):
        if not vals:
            return {}
        self._in(vals)
        rows = defaultdict(list)
        cur = self.con.execute(f"SELECT t.* FROM '{EXTRACT}/{table}.parquet' t JOIN ids ON t.{key} = ids.id "
                               f"ORDER BY ALL")
        cols = [d[0] for d in cur.description]
        for t in cur.fetchall():
            r = dict(zip(cols, t))
            rows[r[key]].append(r)
        return {k: build(k, v) for k, v in rows.items()}


def charity_rec(num, rows):
    main = [r for r in rows if r["kind"] == "name"] or rows
    pc = main[0]["postcode_norm"] or ""
    return {"record_id": f"CC:{num}", "register": "Charity Commission", "name": main[0]["name"],
            "other_names": sorted({r["name"] for r in rows} - {main[0]["name"]})[:6], "status": main[0]["status"],
            "removed": str(main[0]["removed"] or ""), "company_number": main[0]["company_number"] or "",
            "postcode_district": pc[:-3] if len(pc) > 4 else pc}


def cqc_rec(pid, rows):
    r = rows[0]
    pc = r["provider_postcode_norm"] or ""
    return {"record_id": f"CQC:{pid}", "register": "CQC", "name": r["provider_name"] or "(individual provider)",
            "ownership_type": r["ownership_type"], "register_file": r["file"],
            "company_number": r["company_number"] or "", "charity_number": r["charity_number"] or "",
            "postcode_district": pc[:-3] if len(pc) > 4 else pc, "local_authority": r["location_la"]}


def pb_rec(oid, rows):
    main = [r for r in rows if r["kind"] == "name"] or rows
    return {"record_id": f"PB:{oid}", "register": "public-body lists", "name": main[0]["name"],
            "other_names": sorted({r["name"] for r in rows} - {main[0]["name"]})[:6],
            "body_type": main[0]["body_type"], "status": main[0]["status"], "scheme": main[0]["org_scheme"]}


def lei_rec(lei, rows):
    legal = [r for r in rows if r["kind"] == "legal"] or rows
    pc = legal[0]["postcode_norm"] or ""
    return {"record_id": f"LEI:{lei}", "register": "GLEIF", "name": legal[0]["name"],
            "other_names": sorted({r["name"] for r in rows} - {legal[0]["name"]})[:6],
            "jurisdiction": legal[0]["jurisdiction"], "status": legal[0]["entity_status"],
            "registration_authority_entity_id": legal[0]["ra_entity_id"] or "",
            "postcode_district": pc[:-3] if len(pc) > 4 else pc}


def ids_of(scheme, oid, alias):
    """Register record ids for a proposal (organisation and aliases)."""
    out = []
    for s, i in [(scheme, oid)] + [(a.split("-", 2)[0] + "-" + a.split("-", 2)[1], a.split("-", 2)[2])
                                   for a in alias if a.count("-") >= 2]:
        if s == "GB-COH":
            out.append(("CH", i))
        elif s == "GB-CHC":
            out.append(("CC", i))
        elif s == "XI-LEI":
            out.append(("LEI", i))
        elif s in ("GB-GOVUK", "GB-LAE") or s.startswith("GB-"):
            out.append(("PB", i))
        elif s == "UNRESOLVED" and i:
            out.append(("PBB", i))  # a public body held without a scheme, by its body_id
    return out


def recall_candidates(con, items):
    """Blocked search: records that share the payee key's rarest distinctive
    name token (fewest register names across the five extracts) and at least
    two of its distinctive tokens (one when it has one), ranked by tokens
    shared, then by the fewest extra words, then by record id; the top five
    across registers."""
    import pyarrow as pa
    src = [("CH", "reg_names", "company_number"), ("CC", "charity", "charity_number"),
           ("CQC", "cqc", "provider_id"), ("PB", "public_bodies", "org_id"), ("LEI", "gleif", "lei")]
    toks = {it["item_id"]: sorted(distinctive_tokens(normalise(it["payee_key"]))) for it in items}
    alltok = sorted({t for v in toks.values() for t in v})
    if not alltok:
        return {}
    con.register("_at", pa.table({"tok": pa.array(alltok)}))
    con.execute("CREATE OR REPLACE TEMP TABLE tokset AS SELECT * FROM _at")
    df = defaultdict(int)
    for pre, table, idc in src:
        for t, n in con.execute(f"""
                WITH n AS (SELECT DISTINCT name_norm FROM '{EXTRACT}/{table}.parquet' WHERE name_norm <> ''),
                     t AS (SELECT unnest(string_split(name_norm, ' ')) AS tok FROM n)
                SELECT tok, count(*) FROM t JOIN tokset USING (tok) GROUP BY 1""").fetchall():
            df[t] += n
    kt = []
    for i, ts in toks.items():
        if not ts:
            continue
        rare = min(ts, key=lambda t: (df[t], t))
        kt += [(i, t, min(2, len(ts)), t == rare) for t in ts]
    con.register("_kt", pa.table({"item": pa.array([x[0] for x in kt]), "tok": pa.array([x[1] for x in kt]),
                                  "need": pa.array([x[2] for x in kt], type=pa.int64()),
                                  "rare": pa.array([x[3] for x in kt], type=pa.bool_())}))
    con.execute("CREATE OR REPLACE TEMP TABLE kt AS SELECT * FROM _kt")
    hits = defaultdict(list)
    for pre, table, idc in src:
        for item, rid, shared, extra in con.execute(f"""
                WITH n AS (SELECT DISTINCT name_norm, {idc} AS rid FROM '{EXTRACT}/{table}.parquet'
                           WHERE {idc} IS NOT NULL AND name_norm <> ''),
                     t AS (SELECT name_norm, rid, unnest(string_split(name_norm, ' ')) AS tok FROM n),
                     m AS (SELECT kt.item, t.rid, t.name_norm, count(DISTINCT kt.tok) AS shared,
                                  len(string_split(t.name_norm, ' ')) - count(DISTINCT kt.tok) AS extra
                           FROM t JOIN kt USING (tok) GROUP BY 1, 2, 3
                           HAVING count(DISTINCT kt.tok) >= any_value(kt.need) AND bool_or(kt.rare)),
                     b AS (SELECT item, rid, max(shared) AS shared, min(extra) AS extra FROM m GROUP BY 1, 2)
                SELECT item, rid, shared, extra FROM b
                QUALIFY row_number() OVER (PARTITION BY item ORDER BY shared DESC, extra, rid) <= 5""").fetchall():
            hits[item].append((-shared, extra, pre, rid))
    return {i: [(p, r) for _, _, p, r in sorted(h)[:5]] for i, h in hits.items()}


def build():
    from waterfall import contains_person_name, has_legal_form, psc_person_names
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    persons = psc_person_names(con)
    res = {}
    with open(OUT / "resolver_proposed.csv", newline="") as f:
        for r in csv.DictReader(f):
            res[(r["body_id"], r["payee_key"])] = r
    raw = defaultdict(lambda: {"raw": defaultdict(int), "canon": defaultdict(int), "years": set()})
    for line in gzip.open(INPUTS / "payee_keys.jsonl.gz", "rt"):
        d = json.loads(line)
        e = raw[(d["body_id"], d["payee_key"])]
        for n, c in d["raw_names"].items():
            e["raw"][n] += c
        for n, c in d["supplier_canonical"].items():
            e["canon"][n] += c
        e["years"].add(d["financial_year"])
    desc = {}
    for line in gzip.open(INPUTS / "payee_descriptions.jsonl.gz", "rt"):
        d = json.loads(line)
        desc[(d["body_id"], d["payee_key"])] = d["descriptions"]
    items, seen = [], set()

    def add(kind, body, key, extra):
        if (kind, body, key) in seen:
            return
        seen.add((kind, body, key))
        r = res[(body, key)]
        e = raw[(body, key)]
        names = [key] + sorted(e["raw"], key=lambda n: (-e["raw"][n], n)) + sorted(e["canon"])
        withheld = key.startswith(("INDIVIDUAL:", "REDACTED:")) or (
            not any(has_legal_form(n) for n in names) and any(contains_person_name(n, persons) for n in names))
        items.append({"item_id": f"{kind}-{hashlib.sha1(f'{body}|{key}'.encode()).hexdigest()[:16]}",
                      "item_set": kind, "body_id": body, "payee_key": key, "decision_id": r["decision_id"],
                      "org_scheme": r["org_scheme"], "org_id": r["org_id"], "method": r["method"],
                      "reason": r["reason"], "corroborated": r["corroborated"], "withheld": withheld,
                      "names": list(dict.fromkeys(names))[:6], "years": sorted(e["years"]),
                      "descriptions": [{k: v for k, v in x.items() if k != "value_pence"}
                                       for x in desc.get((body, key), [])], **extra})
    with open(OUT / "gold_set_pairs.csv", newline="") as f:
        for r in csv.DictReader(f):
            add("gold", r["body_id"], r["payee_key"], {"stratum": r["stratum"], "value_band": r["value_band"],
                                                       "draw_no": r["draw_no"]})
    with open(OUT / "recall_sample.csv", newline="") as f:
        for r in csv.DictReader(f):
            add("recall", r["body_id"], r["payee_key"], {})
    for p in sorted(OUT.glob("verify_*.csv")):
        if p.name.startswith(("verify_suppliers", "verify_summary")):
            continue
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                x = res[(r["body_id"], r["payee_key"])]
                if x["org_id"] and x["corroborated"] != "True":
                    add("verify", r["body_id"], r["payee_key"], {})
    # register records
    want = defaultdict(set)
    props = {}
    for it in items:
        if it["item_set"] != "recall":
            x = res[(it["body_id"], it["payee_key"])]
            ids = ids_of(x["org_scheme"], x["org_id"], [a for a in x["alias_ids"].split("|") if a])
            ev = json.loads(x["evidence"]) if x["evidence"] else {}
            if ev.get("provider_id"):
                ids.append(("CQC", ev["provider_id"]))
            props[it["item_id"]] = ids
    cands = recall_candidates(con, [it for it in items if it["item_set"] == "recall" and not it["withheld"]])
    pools = {}
    with open(OUT / "queue.csv", newline="") as f:
        for r in csv.DictReader(f):
            q = json.loads(r["candidates"])
            pools[(r["body_id"], r["payee_key"])] = [("CH", c["crn"]) for c in (q.get("name_pool") or {}).get("candidates", [])]
    for it in items:
        if it["item_set"] == "recall":
            x = res[(it["body_id"], it["payee_key"])]
            ev = json.loads(x["evidence"]) if x["evidence"] else {}
            q = list(pools.get((it["body_id"], it["payee_key"]), []))
            for c in ev.get("candidates", []):
                if c.get("org_id"):
                    q += ids_of(c["org_scheme"], c["org_id"], [])
            props[it["item_id"]] = list(dict.fromkeys(q + cands.get(it["item_id"], [])))[:6]
        for pre, i in props[it["item_id"]]:
            want[pre].add(i)
    R = Records(con)
    recs = {"CH": R.companies(want["CH"]),
            "CC": R.by_table("charity", "charity_number", want["CC"], charity_rec),
            "CQC": R.by_table("cqc", "provider_id", want["CQC"], cqc_rec),
            "PB": R.by_table("public_bodies", "org_id", want["PB"], pb_rec),
            "PBB": R.by_table("public_bodies", "body_id", want["PBB"], pb_rec),
            "LEI": R.by_table("gleif", "lei", want["LEI"], lei_rec)}
    with open(ITEMS, "w") as f:
        for it in items:
            it["records"] = [recs[p][i] for p, i in props[it["item_id"]] if i in recs[p]]
            it["evidence_sha256"] = hashlib.sha256(json.dumps(evidence(it), sort_keys=True).encode()).hexdigest()
            f.write(json.dumps(it, ensure_ascii=False, sort_keys=True) + "\n")
    count = defaultdict(int)
    for it in items:
        count[f"{it['item_set']}{' withheld' if it['withheld'] else ''}"] += 1
        if not it["records"] and not it["withheld"]:
            count[f"{it['item_set']} without records"] += 1
    write_json(MANIFEST, {"pipelineGitSha": git_sha(), "builtAt": now(), "model": MODEL, "effort": EFFORT,
                          "promptHashes": {p: prompt_hash(p) for p in PROMPTS}, "items": dict(sorted(count.items())),
                          "inputs": [{"file": n, "sha256": sha256_file(OUT / n)} for n in
                                     ("resolver_proposed.csv", "gold_set_pairs.csv", "recall_sample.csv")] +
                                    [{"file": "payee_descriptions.jsonl.gz",
                                      "sha256": sha256_file(INPUTS / "payee_descriptions.jsonl.gz")}],
                          "batches": {}})
    print(json.dumps(dict(sorted(count.items())), indent=1))


def evidence(it):
    ev = {"payment": {"council": BODY_NAME[it["body_id"]], "payee_names_as_published": it["names"],
                      "council_descriptions": it["descriptions"], "financial_years": it["years"]},
          "records": it["records"]}
    if it["item_set"] != "recall":
        ev["proposal"] = {"organisation": f"{it['org_scheme']}-{it['org_id']}", "matched_by": it["method"]}
    return ev


def request(it, p):
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request
    q = PROMPTS[p]["recall" if it["item_set"] == "recall" else "resolved"]
    return Request(custom_id=f"{p}-{it['item_id']}", params=MessageCreateParamsNonStreaming(
        model=MODEL, max_tokens=4000, system=PROMPTS[p]["system"],
        output_config={"effort": EFFORT, "format": {"type": "json_schema", "schema": SCHEMA}},
        messages=[{"role": "user", "content": "Evidence:\n" + json.dumps(evidence(it), ensure_ascii=False, indent=1)
                   + "\n\n" + q}]))


def client(key_file):
    import anthropic
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key and key_file:
        for line in open(key_file):
            if line.startswith("ANTHROPIC_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        raise SystemExit("no API key")
    return anthropic.Anthropic(api_key=key)


def load_items():
    return [json.loads(line) for line in open(ITEMS)]


def submit(a):
    c = client(a.key_file)
    items = [it for it in load_items() if not it["withheld"] and it["records"]]
    man = json.loads(MANIFEST.read_text())
    tag = "pilot" if a.pilot else "full"
    if a.pilot:
        items = sorted(items, key=lambda it: it["item_id"])[::max(1, len(items) // a.pilot)][:a.pilot]
    if tag in man["batches"] and not a.force:
        raise SystemExit(f"{tag} batches already submitted: {man['batches'][tag]}")
    out = {}
    for p in PROMPTS:
        b = c.messages.batches.create(requests=[request(it, p) for it in items])
        out[p] = {"batch_id": b.id, "requests": len(items), "prompt_hash": prompt_hash(p), "submitted_at": now()}
        print(p, b.id, len(items))
    man["batches"][tag] = out
    write_json(MANIFEST, man)


def parse(result):
    if result.result.type != "succeeded":
        return {"label": "cannot tell", "record_ids": [], "reason": f"no answer ({result.result.type})"}, None
    m = result.result.message
    if m.stop_reason == "refusal":
        return {"label": "cannot tell", "record_ids": [], "reason": "refused"}, m.usage
    text = next((b.text for b in m.content if b.type == "text"), "")
    try:
        d = json.loads(text)
    except ValueError:
        return {"label": "cannot tell", "record_ids": [], "reason": f"unparsed answer ({m.stop_reason})"}, m.usage
    return d, m.usage


def collect(a):
    from waterfall import contains_person_name, psc_person_names
    persons = psc_person_names(duckdb.connect())
    c = client(a.key_file)
    man = json.loads(MANIFEST.read_text())
    tag = "pilot" if a.pilot else "full"
    got, usage = {}, {}
    for p, b in man["batches"][tag].items():
        while c.messages.batches.retrieve(b["batch_id"]).processing_status != "ended":
            time.sleep(30)
        tin = tout = 0
        for r in c.messages.batches.results(b["batch_id"]):
            d, u = parse(r)
            got[r.custom_id] = d
            if u:
                tin += u.input_tokens + (u.cache_read_input_tokens or 0) + (u.cache_creation_input_tokens or 0)
                tout += u.output_tokens
        usage[p] = {"input_tokens": tin, "output_tokens": tout,
                    "cost_usd": round(tin / 1e6 * PRICE_IN + tout / 1e6 * PRICE_OUT, 2)}
    man.setdefault("usage", {})[tag] = usage
    man["usage"][tag]["total_cost_usd"] = round(sum(u["cost_usd"] for u in usage.values()), 2)
    man["usage"][tag]["price_basis"] = (f"{MODEL} Message Batches: ${PRICE_IN} input and ${PRICE_OUT} output per "
                                        "million tokens (half the standard rate)")
    rows = []
    for it in load_items():
        recs = {r["record_id"] for r in it["records"]}
        row = {k: it[k] for k in ("item_set", "body_id", "payee_key", "decision_id", "org_scheme", "org_id",
                                  "method", "reason", "corroborated")}
        row.update(stratum=it.get("stratum", ""), value_band=it.get("value_band", ""),
                   evidence_sha256=it["evidence_sha256"], model=MODEL, effort=EFFORT)
        labels = []
        for p in PROMPTS:
            d = got.get(f"{p}-{it['item_id']}")
            b = man["batches"][tag][p]
            row[f"batch_id_{p.lower()}"] = b["batch_id"]
            row[f"prompt_hash_{p.lower()}"] = b["prompt_hash"]
            if it["withheld"] or not it["records"] or d is None:
                lab, cited, why = "", [], ("withheld: name contains a personal name" if it["withheld"] else
                                           "no register record to show" if not it["records"] else "not in this batch")
            else:
                cited = [x for x in d["record_ids"] if x in recs]
                lab, why = d["label"], d["reason"]
                if contains_person_name(why, persons):
                    why = "(reason withheld: it contains a forename and surname from the individual PSC list)"
                if lab == "same" and not cited:
                    lab, why = "cannot tell", "said same without citing a record in the evidence"
            row[f"pass_{p.lower()}_label"] = lab
            row[f"pass_{p.lower()}_records"] = "|".join(cited)
            row[f"pass_{p.lower()}_reason"] = why
            labels.append((lab, tuple(sorted(cited))))
        (la, ca), (lb, cb) = labels
        if not la or not lb:
            agreed = "not reviewed"
        elif la == lb and la in ("same", "different") and (it["item_set"] != "recall" or la != "same" or ca == cb):
            agreed = la
        else:
            agreed = "cannot tell"
        row["agreed_label"] = agreed
        row["labelled_at"] = now()
        if not a.pilot or any(row[f"pass_{p.lower()}_label"] for p in PROMPTS):
            rows.append(row)
    path = LABELS if not a.pilot else OUT / "agent_labels_pilot.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    man["labels"] = {tag: {"file": path.name, "rows": len(rows),
                           "agreed": {k: sum(1 for r in rows if r["agreed_label"] == k)
                                      for k in ("same", "different", "cannot tell", "not reviewed")}}}
    write_json(MANIFEST, man)
    print(json.dumps({"usage": man["usage"][tag], "labels": man["labels"][tag]}, indent=1))


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["build", "submit", "collect"])
    ap.add_argument("--pilot", type=int, default=0)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--key-file", default="")
    a = ap.parse_args()
    if a.action == "build":
        build()
    elif a.action == "submit":
        submit(a)
    else:
        collect(a)


if __name__ == "__main__":
    main()
