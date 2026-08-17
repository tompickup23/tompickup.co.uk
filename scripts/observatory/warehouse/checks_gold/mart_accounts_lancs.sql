-- Every filing, in the order the site reads them, with both winners flagged.
-- Any row returned is a build failure.

-- check: row count matches the manifest
SELECT 'rows' AS what, count(*) AS got, {rows} AS want
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: file_ordinal is the key, because the table is a stream and a repeated
-- ordinal would mean two filings collapsed into one position
SELECT file_ordinal, count(*) FROM read_parquet('{pq}')
GROUP BY 1 HAVING count(*) > 1;

-- check: exactly one site winner and one latest winner per period. Both rules
-- must resolve, or a consumer reading the mart would see a period twice.
SELECT crn, period_end,
       count(*) FILTER (WHERE site_winner)   AS site,
       count(*) FILTER (WHERE latest_winner) AS latest
FROM read_parquet('{pq}')
GROUP BY 1, 2
HAVING count(*) FILTER (WHERE site_winner) <> 1
    OR count(*) FILTER (WHERE latest_winner) <> 1;

-- check: V-T2. Every row carries a company number, because an accounts figure
-- is a company-only field and a CRN-less record may never hold one.
SELECT period_end FROM read_parquet('{pq}')
WHERE crn IS NULL OR length(crn) <> 8;

-- check: no accounting period ends in the future, and the only rows before
-- 1990 are the ones explicitly flagged as impossible iXBRL artefacts (s9.6)
SELECT crn, period_end, impossible_period FROM read_parquet('{pq}')
WHERE period_end > current_date
   OR (period_end < DATE '1990-01-01' AND NOT impossible_period);

-- check: employees is the s411 average and is nulled where it cannot be a
-- headcount, while employees_as_filed keeps exactly what was parsed (s9.6).
-- The two may only differ where the filed value is impossible.
SELECT crn, period_end, employees_as_filed, employees FROM read_parquet('{pq}')
WHERE employees IS DISTINCT FROM employees_as_filed
  AND NOT (employees_as_filed < 0 OR employees_as_filed > 500000);

-- check: filings_for_period agrees with the rows actually present
SELECT crn, period_end, any_value(filings_for_period) AS claimed, count(*) AS got
FROM read_parquet('{pq}')
GROUP BY 1, 2 HAVING any_value(filings_for_period) <> count(*);

-- check: the two feeds are the ones we registered, and nothing else
SELECT DISTINCT feed FROM read_parquet('{pq}')
WHERE feed NOT IN ('monthly-archive', 'api-backfill');
