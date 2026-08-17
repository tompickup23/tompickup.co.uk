-- Silver gates for ch_strikeoff (a projection of the register status).

-- check: row count matches the manifest
SELECT 'row count drift' AS problem, count(*) AS actual, {rows} AS claimed
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: company_number is unique
SELECT company_number, count(*) AS n
FROM read_parquet('{pq}')
GROUP BY company_number HAVING count(*) > 1
LIMIT 20;

-- check: every row really does carry the strike-off status. This table asserts
-- register status and nothing more, so a row that does not carry that status
-- has no evidence behind it at all.
SELECT company_number, company_status
FROM read_parquet('{pq}')
WHERE company_status <> 'Active - Proposal to Strike off'
LIMIT 20;

-- check: the evidence basis is stated on every row
SELECT DISTINCT evidence_basis
FROM read_parquet('{pq}')
WHERE evidence_basis IS DISTINCT FROM 'register-status';

-- check: company_number is an eight-character Companies House number
SELECT company_number
FROM read_parquet('{pq}')
WHERE NOT regexp_matches(company_number, '^[0-9A-Z]{8}$')
LIMIT 20;

-- check: no incorporation date in the future and no negative age
SELECT company_number, incorporation_date, age_days
FROM read_parquet('{pq}')
WHERE incorporation_date > DATE '{snapshot}' OR age_days < 0
LIMIT 20;

-- check: the overdue flags are derived from the dates and cannot drift from them
SELECT company_number, accounts_next_due_date, accounts_overdue
FROM read_parquet('{pq}')
WHERE accounts_overdue <> (accounts_next_due_date IS NOT NULL
                           AND accounts_next_due_date < DATE '{snapshot}')
LIMIT 20;

-- check: the partition column agrees with the partition path
SELECT DISTINCT snapshot_date
FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
