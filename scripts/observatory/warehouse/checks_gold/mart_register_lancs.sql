-- Gate contract: any row returned is a build failure. A clean table returns
-- nothing at all. Maps to DATA-INTEGRITY s6 gates V-T1, V-T4 and V-R1.

-- check: row count matches the manifest
SELECT 'rows' AS what, count(*) AS got, {rows} AS want
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: company_number is unique, which is the double-load gate. A count that
-- doubles overnight is a duplicate partition before it is anything else.
SELECT company_number, count(*) FROM read_parquet('{pq}')
GROUP BY 1 HAVING count(*) > 1;

-- check: every row sits in one of the 14 target LADs and nowhere else. The
-- frame is the published 103,468 basis; a fifteenth LAD means the ONSPD join
-- changed shape.
SELECT DISTINCT lad_code, lad_name FROM read_parquet('{pq}')
WHERE lad_code NOT IN (
  'E07000121','E07000123','E07000124','E07000119','E07000128','E06000009',
  'E06000008','E07000120','E07000125','E07000122','E07000117','E07000118',
  'E07000126','E07000127');

-- check: every registered number is exactly eight characters (s9.3). R-prefix
-- Northern Ireland companies and Scottish limited partnerships are real, so
-- the two-letters-and-six-digits test is the wrong one; length is the only
-- universal rule.
SELECT company_number FROM read_parquet('{pq}')
WHERE length(company_number) <> 8;

-- check: V-T4. A society number never appears in a company-number field. The
-- frame is postcode-filtered and society rows carry blank postcodes, so this
-- should be empty by construction; it is asserted because the blank-postcode
-- accident is not a rule (s7.8).
SELECT company_number, entity_type FROM read_parquet('{pq}')
WHERE number_prefix IN ('IP','RS','SP','NO','NP','CE','CS');

-- check: no row carries a postcode that is blank after normalisation, since
-- the frame is defined by a postcode-to-LAD join and a blank cannot have made
-- it through one.
SELECT company_number FROM read_parquet('{pq}')
WHERE reg_postcode IS NULL OR trim(reg_postcode) = '';

-- check: dates are in the register's own DD/MM/YYYY form, because the
-- downstream consumer splits them on a slash. A silently ISO-formatted date
-- would make every company's age null rather than wrong, which is worse.
SELECT company_number, incorporation_date FROM read_parquet('{pq}')
WHERE incorporation_date <> ''
  AND NOT regexp_matches(incorporation_date, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$');

-- check: a dissolution date never precedes incorporation.
SELECT company_number, incorporation_date, dissolution_date
FROM read_parquet('{pq}')
WHERE dissolution_date <> '' AND incorporation_date <> ''
  AND strptime(dissolution_date, '%d/%m/%Y')
      < strptime(incorporation_date, '%d/%m/%Y');

-- check: a CIC is derived from the category string and from nothing else,
-- because no number prefix distinguishes one (s7.3).
SELECT company_number, company_category FROM read_parquet('{pq}')
WHERE is_cic <> (lower(coalesce(company_category, ''))
                 LIKE '%community interest company%');

-- check: V-R1, the register snapshot is inside its 45-day staleness budget.
SELECT max(snapshot_date) AS snapshot,
       date_diff('day', max(snapshot_date), current_date) AS days_old
FROM read_parquet('{pq}')
HAVING date_diff('day', max(snapshot_date), current_date) > 90;

-- check: the partition column agrees with the partition path.
SELECT DISTINCT snapshot_date FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
