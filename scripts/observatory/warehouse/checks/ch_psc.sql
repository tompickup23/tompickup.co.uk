-- Silver gates for ch_psc (Lancashire, every PSC kind).

-- check: row count matches the manifest
SELECT 'row count drift' AS problem, count(*) AS actual, {rows} AS claimed
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: company_number is never null
SELECT 'null company_number' AS problem, count(*) AS n
FROM read_parquet('{pq}')
WHERE company_number IS NULL OR company_number = ''
HAVING count(*) > 0;

-- check: company_number is an eight-character Companies House number (V-T1)
SELECT company_number
FROM read_parquet('{pq}')
WHERE NOT regexp_matches(company_number, '^[0-9A-Z]{8}$')
LIMIT 20;

-- check: no complete row repeats. This is the Northants double-load gate, and
-- it is the strongest claim the data supports: (company, kind, name) is NOT a
-- key, because a person can hold two notified interests in the same company at
-- once and 3,009 people in this snapshot do. GROUP BY on a column subset would
-- fail on those legitimate rows, so the comparison is total rows against
-- distinct rows over every column.
SELECT 'duplicate complete rows' AS problem,
       count(*) AS total_rows,
       (SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet('{pq}'))) AS distinct_rows
FROM read_parquet('{pq}')
HAVING count(*) <> (SELECT count(*)
                    FROM (SELECT DISTINCT * FROM read_parquet('{pq}')));

-- check: kind is one of the values the extractor reports
SELECT kind, count(*) AS n
FROM read_parquet('{pq}')
WHERE kind NOT IN (
  'individual-person-with-significant-control',
  'corporate-entity-person-with-significant-control',
  'legal-person-person-with-significant-control',
  'super-secure-person-with-significant-control',
  'individual-beneficial-owner', 'corporate-entity-beneficial-owner',
  'super-secure-beneficial-owner', 'legal-person-beneficial-owner',
  'persons-with-significant-control-statement', 'exemptions',
  'totals#persons-of-significant-control-snapshot')
GROUP BY kind;

-- check: no ceased date in the future
SELECT company_number, ceased_on
FROM read_parquet('{pq}')
WHERE ceased_on > DATE '{snapshot}'
LIMIT 20;

-- check: active is the inverse of a ceased date, with no third state
SELECT company_number, ceased_on, active
FROM read_parquet('{pq}')
WHERE active <> (ceased_on IS NULL)
LIMIT 20;

-- check: natures_of_control is stored sorted, so two rows that differ only in
-- list order can never masquerade as two interests
SELECT company_number, name, natures_of_control
FROM read_parquet('{pq}')
WHERE natures_of_control IS DISTINCT FROM list_sort(natures_of_control)
LIMIT 20;

-- check: the partition column agrees with the partition path
SELECT DISTINCT snapshot_date
FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
