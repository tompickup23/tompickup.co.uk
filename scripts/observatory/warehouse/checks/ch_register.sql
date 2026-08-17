-- Silver gates for ch_register. Any row returned by any check is a build
-- failure. Placeholders: {pq} the parquet path, {snapshot} the partition date,
-- {rows} the row count the manifest claims.
--
-- Maps to the DATA-INTEGRITY s6 gates: V-T1 (entityType from the enum with a
-- matching id scheme), V-T4 (society numbers never in a CRN field), and the
-- no-future-dates half of V-R1.

-- check: company_number is unique
SELECT company_number, count(*) AS n
FROM read_parquet('{pq}')
GROUP BY company_number HAVING count(*) > 1;

-- check: company_number is never null or blank
SELECT 'null company_number' AS problem, count(*) AS n
FROM read_parquet('{pq}')
WHERE company_number IS NULL OR company_number = ''
HAVING count(*) > 0;

-- check: row count matches the manifest
SELECT 'row count drift' AS problem, count(*) AS actual, {rows} AS claimed
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: every row carries an entityType from the controlled list (V-T1)
SELECT entity_type, count(*) AS n
FROM read_parquet('{pq}')
WHERE entity_type IS NULL OR entity_type NOT IN (
  'ltd', 'plc', 'llp', 'lp', 'cic', 'cio', 'registered-society',
  'overseas-establishment', 'overseas-entity', 'other-corporate-body')
GROUP BY entity_type;

-- check: every registered number is eight characters of uppercase alphanumeric,
-- which is the one universal rule across all the registers in this file (V-T1)
SELECT company_number
FROM read_parquet('{pq}')
WHERE NOT regexp_matches(company_number, '^[0-9A-Z]{8}$')
LIMIT 20;

-- check: a Companies Act body's number takes one of four verified shapes.
-- Society numbers take six further shapes and must never reach this branch,
-- which is what makes the exclusion rule testable rather than assumed.
SELECT company_number, entity_type
FROM read_parquet('{pq}')
WHERE companies_act_body
  AND NOT regexp_matches(company_number,
        '^([0-9]{8}|[A-Z]{2}[0-9]{6}|R[0-9]{7}|[A-Z]{2}[0-9]{5}[A-Z])$')
LIMIT 20;

-- check: no society, CIO or overseas row is counted as a company (V-T4)
SELECT entity_type, count(*) AS n
FROM read_parquet('{pq}')
WHERE companies_act_body
  AND entity_type IN ('cio', 'registered-society', 'overseas-entity',
                      'overseas-establishment', 'other-corporate-body')
GROUP BY entity_type;

-- check: the exclusion rule is by prefix, not by the blank-postcode accident
-- (DATA-INTEGRITY s7.8). Every CE, CS, IP, RS, SP, NO, NP, FC and OE row is
-- out of the company count whatever its postcode says.
SELECT number_prefix, count(*) AS n
FROM read_parquet('{pq}')
WHERE number_prefix IN ('CE', 'CS', 'IP', 'RS', 'SP', 'NO', 'NP', 'FC', 'OE')
  AND companies_act_body
GROUP BY number_prefix;

-- check: entityType for the overseas families comes from the prefix, never
-- from CompanyCategory, which reads "Other company type" for FC, AC, NF and SF
-- alike (DATA-INTEGRITY s7.1)
SELECT company_number, company_category, entity_type
FROM read_parquet('{pq}')
WHERE (number_prefix = 'FC' AND entity_type <> 'overseas-establishment')
   OR (number_prefix = 'OE' AND entity_type <> 'overseas-entity')
   OR (number_prefix IN ('AC', 'NF', 'SF') AND entity_type <> 'other-corporate-body')
LIMIT 20;

-- check: CIC detection is the exact category string and nothing else (s7.3)
SELECT company_number, company_category, entity_type, is_cic
FROM read_parquet('{pq}')
WHERE (company_category = 'Community Interest Company') <> is_cic
   OR (is_cic AND entity_type <> 'cic')
LIMIT 20;

-- check: BR overseas-establishment rows do not exist in this file (s7.1)
SELECT 'BR rows present' AS problem, count(*) AS n
FROM read_parquet('{pq}')
WHERE number_prefix = 'BR'
HAVING count(*) > 0;

-- check: no incorporation or dissolution date in the future
SELECT company_number, incorporation_date, dissolution_date
FROM read_parquet('{pq}')
WHERE incorporation_date > DATE '{snapshot}'
   OR dissolution_date > DATE '{snapshot}'
LIMIT 20;

-- check: a dissolution date never precedes incorporation
SELECT company_number, incorporation_date, dissolution_date
FROM read_parquet('{pq}')
WHERE dissolution_date IS NOT NULL
  AND incorporation_date IS NOT NULL
  AND dissolution_date < incorporation_date
LIMIT 20;

-- check: the partition column agrees with the partition path
SELECT DISTINCT snapshot_date
FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
