-- Silver gates for ch_accounts (iXBRL accounts, as filed).

-- check: row count matches the manifest
SELECT 'row count drift' AS problem, count(*) AS actual, {rows} AS claimed
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: (crn, period_end) is unique after the superseded-filing collapse
SELECT crn, period_end, count(*) AS n
FROM read_parquet('{pq}')
GROUP BY 1, 2 HAVING count(*) > 1
LIMIT 20;

-- check: crn is an eight-character Companies House number (V-T2: an accounts
-- row is a company-only fact and may not hang off anything without one)
SELECT crn
FROM read_parquet('{pq}')
WHERE crn IS NULL
   OR NOT regexp_matches(crn, '^[0-9A-Z]{8}$')
LIMIT 20;

-- check: every row has the period end that is its record-level asAt
SELECT 'missing period_end' AS problem, count(*) AS n
FROM read_parquet('{pq}')
WHERE period_end IS NULL
HAVING count(*) > 0;

-- check: no accounting period ends in the future
SELECT crn, period_end
FROM read_parquet('{pq}')
WHERE period_end > DATE '{snapshot}'
LIMIT 20;

-- check: no accounting period predates 1990. Two rows in the 2026-08-08
-- extract carry 0001-01-01, an iXBRL parse artefact; they belong in
-- _rejected.parquet, not in the table.
SELECT crn, period_end
FROM read_parquet('{pq}')
WHERE period_end < DATE '1990-01-01'
LIMIT 20;

-- check: employees is an s411 average headcount, so a value that cannot be one
-- has been nulled. 269 filings in this extract report a negative average and
-- two report over 700,000.
SELECT crn, period_end, employees
FROM read_parquet('{pq}')
WHERE employees < 0 OR employees > 500000
LIMIT 20;

-- check: nothing filed is lost. employees_suspect marks exactly the rows where
-- the filed value could not be a headcount, and employees_as_filed still
-- carries it.
SELECT crn, period_end, employees_as_filed, employees, employees_suspect
FROM read_parquet('{pq}')
WHERE employees_suspect <> (employees_as_filed IS NOT NULL
                            AND (employees_as_filed < 0
                                 OR employees_as_filed > 500000))
   OR (NOT employees_suspect
       AND employees IS DISTINCT FROM employees_as_filed)
LIMIT 20;

-- check: as_at mirrors period_end, which is what makes the record-level
-- recency rule enforceable downstream
SELECT crn, period_end, as_at
FROM read_parquet('{pq}')
WHERE as_at IS DISTINCT FROM period_end
LIMIT 20;

-- check: the partition column agrees with the partition path
SELECT DISTINCT snapshot_date
FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
