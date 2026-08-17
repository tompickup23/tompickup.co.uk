-- Silver gates for gazette_notices.

-- check: row count matches the manifest
SELECT 'row count drift' AS problem, count(*) AS actual, {rows} AS claimed
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: notice_id is unique
SELECT notice_id, count(*) AS n
FROM read_parquet('{pq}')
GROUP BY notice_id HAVING count(*) > 1
LIMIT 20;

-- check: THE legal gate. Nothing outside category 24 exists in this table.
-- Personal insolvency is excluded entirely, and the candidate file does carry
-- it because the feed ignores category-code=24 alongside the location
-- parameters.
SELECT notice_category, count(*) AS n
FROM read_parquet('{pq}')
WHERE notice_category <> '24'
GROUP BY notice_category;

-- check: no bankruptcy, IVA or debt relief notice type survived the filter
SELECT notice_id, insolvency_type
FROM read_parquet('{pq}')
WHERE insolvency_type IN ('BankruptcyOrderNotice', 'DischargeOrderNotice',
                          'IndividualVoluntaryArrangementNotice',
                          'DebtReliefOrderNotice', 'DebtReliefRestrictionsNotice')
LIMIT 20;

-- check: every notice carries its own code and date, since the only thing a
-- row may assert is the notice fact, verbatim and dated
SELECT notice_id, notice_code, notice_date
FROM read_parquet('{pq}')
WHERE notice_code IS NULL OR notice_date IS NULL
LIMIT 20;

-- check: no notice published in the future
SELECT notice_id, notice_date
FROM read_parquet('{pq}')
WHERE notice_date > DATE '{snapshot}'
LIMIT 20;

-- check: no notice predates the fetch window the source declares
SELECT notice_id, notice_date
FROM read_parquet('{pq}')
WHERE notice_date < DATE '2024-01-01'
LIMIT 20;

-- check: a company number that failed the CRN format test is never flagged as
-- one (V-T4)
SELECT notice_id, company_number, company_number_is_crn
FROM read_parquet('{pq}')
WHERE company_number_is_crn
      <> regexp_matches(upper(replace(coalesce(company_number, ''), ' ', '')),
                        '^([0-9]{8}|[A-Z]{2}[0-9]{6}|R[0-9]{7})$')
LIMIT 20;

-- check: as_at mirrors the notice date
SELECT notice_id, notice_date, as_at
FROM read_parquet('{pq}')
WHERE as_at IS DISTINCT FROM notice_date
LIMIT 20;

-- check: the partition column agrees with the partition path
SELECT DISTINCT snapshot_date
FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
