#!/usr/bin/env python3
"""How a filed-accounts stream resolves to one filing per accounting period.

Both accounts consumers (build_growth.py, build_dossiers.py) read the same
`lancs_accounts.jsonl.gz` stream, and until now both resolved a repeated
(crn, period_end) by keeping the LAST line they met. That is not a rule, it is
whatever order the extractor happened to append its archives in, and the
extractor appended a thirteen-month backfill in reverse chronological order, so
for every period the backfill covered the last line was the OLDEST archive and
the published figure was the ORIGINAL filing rather than the restatement.

The rule this module states instead, and the only one either consumer uses:

    For an accounting period, the published filing is the one filed most
    recently. Where two filings carry the same filing month, the later
    position in the stream wins, which is the archive's own order.

`filed_zip` is the only filing-date evidence a record carries. It is either a
monthly archive name (`Accounts_Monthly_Data-August2026.zip`) or the literal
`api-backfill` for the 1,377 rows pulled from the Companies House REST API at
snapshot time. An API pull returns the filing that is current on the day it
runs, so it outranks every dated archive; that is also what the old
last-line-wins rule did with those rows, because they were appended last.

Sorting on the archive NAME does not work and was live for months: it is
alphabetical on a month word, so September2025 beats August2026.

Bounds. A section 411 average headcount cannot be negative and cannot plausibly
exceed 500,000 in this dataset. 269 filings in the 2026-08-08 extract report a
negative average and two report over 700,000 for the same company; all are
iXBRL parse or scale artefacts rather than filings. Silver already nulls the
derived `employees` column at the same bounds and keeps `employees_as_filed`
verbatim, so this module is the same rule stated once more at the point the
legacy stream is read, and the two paths agree by construction.
"""

MIN_EMPLOYEES = 0
MAX_EMPLOYEES = 500000

_MONTHS = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
           "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
           "november": 11, "december": 12}

# An API pull is current as at the day it ran, so it outranks every dated
# monthly archive. 9999 rather than a real year because there is no date to
# read: the claim is ordering, not a date, and a fake date would read as one.
_API_RANK = (9999, 12)
_UNKNOWN_RANK = (0, 0)


def filed_rank(filed_zip):
    """A sortable filing vintage for one accounts record.

    Returns (year, month). An unparseable value ranks oldest rather than
    newest, so a name we do not understand can never displace a filing we do.
    """
    s = (filed_zip or "").strip()
    if not s:
        return _UNKNOWN_RANK
    if s.lower().startswith("api"):
        return _API_RANK
    stem = s.rsplit("/", 1)[-1]
    if stem.lower().endswith(".zip"):
        stem = stem[:-4]
    tail = stem.rsplit("-", 1)[-1]
    for name, num in _MONTHS.items():
        if tail.lower().startswith(name):
            year = tail[len(name):]
            if year.isdigit() and len(year) == 4:
                return (int(year), num)
            return _UNKNOWN_RANK
    return _UNKNOWN_RANK


def clean_employees(value):
    """The filed s411 average, or None where it cannot be a headcount."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v < MIN_EMPLOYEES or v > MAX_EMPLOYEES:
        return None
    return value


def resolve_latest(records):
    """Collapse an accounts stream to the latest filing per (crn, period_end).

    `records` is an iterable of decoded rows in the order the file lists them.
    Returns {crn: {period_end: record}}, each record carrying a cleaned
    `employees` value. The stream position is the tie-break, so two filings
    from the same archive month resolve exactly as the file orders them.
    """
    best = {}
    for position, r in enumerate(records):
        crn, period_end = r.get("crn"), r.get("period_end")
        if not crn or not period_end:
            continue
        key = (filed_rank(r.get("filed_zip")), position)
        held = best.get((crn, period_end))
        if held is None or key > held[0]:
            r = dict(r)
            r["employees"] = clean_employees(r.get("employees"))
            best[(crn, period_end)] = (key, r)
    out = {}
    for (crn, period_end), (_, r) in best.items():
        out.setdefault(crn, {})[period_end] = r
    return out
