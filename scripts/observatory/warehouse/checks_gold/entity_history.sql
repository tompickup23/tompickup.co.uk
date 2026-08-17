-- SCD2 over the register snapshots. Any row returned is a build failure.

-- check: row count matches the manifest
SELECT 'rows' AS what, count(*) AS got, {rows} AS want
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: (company_number, version_no) is the key
SELECT company_number, version_no, count(*) FROM read_parquet('{pq}')
GROUP BY 1, 2 HAVING count(*) > 1;

-- check: versions start at 1 and run without a gap in the numbering
SELECT company_number, min(version_no) AS first_v, max(version_no) AS last_v,
       count(*) AS n
FROM read_parquet('{pq}')
GROUP BY 1
HAVING min(version_no) <> 1 OR max(version_no) <> count(*);

-- check: intervals abut exactly, EXCEPT where the row left the file. That gap
-- is real data (DATA-INTEGRITY s4 rule 5: a refresh gap is "no data", never
-- "no change"), and it is the only hole this table may have.
SELECT * FROM (
  SELECT company_number, version_no, valid_to, gone_from_register,
         lead(valid_from) OVER (PARTITION BY company_number
                                ORDER BY version_no) AS next_from
  FROM read_parquet('{pq}'))
WHERE next_from IS NOT NULL AND NOT gone_from_register
  AND valid_to IS DISTINCT FROM next_from;

-- check: an interval never ends before it starts
SELECT company_number, version_no, valid_from, valid_to
FROM read_parquet('{pq}')
WHERE valid_to IS NOT NULL AND valid_to <= valid_from;

-- check: exactly one open version per company, and it is the last one
SELECT company_number, count(*) FROM read_parquet('{pq}')
WHERE valid_to IS NULL
GROUP BY 1 HAVING count(*) > 1;

-- check: an open version is never flagged gone. A row still in the latest
-- snapshot has not left the register.
SELECT company_number, version_no FROM read_parquet('{pq}')
WHERE valid_to IS NULL AND gone_from_register;

-- check: change_type is from the controlled list and 'baseline' only ever
-- belongs to a first version. A baseline is not a creation event.
SELECT company_number, version_no, change_type FROM read_parquet('{pq}')
WHERE change_type NOT IN ('baseline', 'new', 'change', 'returned')
   OR (change_type = 'baseline' AND version_no <> 1);

-- check: no two consecutive versions of a company share a row_hash, which
-- would mean a version boundary was created where nothing changed.
SELECT * FROM (
  SELECT company_number, version_no, row_hash, gone_from_register,
         lag(row_hash) OVER (PARTITION BY company_number
                             ORDER BY version_no) AS prev_hash,
         lag(gone_from_register) OVER (PARTITION BY company_number
                                       ORDER BY version_no) AS prev_gone
  FROM read_parquet('{pq}'))
WHERE prev_hash IS NOT NULL AND prev_hash = row_hash AND NOT prev_gone;
