-- Silver gates for ch_psc_corporate (national corporate-entity PSC rows).

-- check: row count matches the manifest
SELECT 'row count drift' AS problem, count(*) AS actual, {rows} AS claimed
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: the controlled company number is an eight-character CH number. The
-- 61 rows shaped R plus seven digits are pre-1922 Northern Ireland companies
-- and are real, which is why the gate is the eight-character rule and not a
-- guess at two letters and six digits.
SELECT company_number
FROM read_parquet('{pq}')
WHERE company_number IS NULL
   OR NOT regexp_matches(company_number, '^[0-9A-Z]{8}$')
LIMIT 20;

-- check: registration_is_crn_shaped is exactly the shape test and nothing
-- looser. It narrows join candidates; it never proves a company, because a
-- registered society number matches the same pattern (gate V-T4).
SELECT registration_number_norm, registration_is_crn_shaped
FROM read_parquet('{pq}')
WHERE registration_is_crn_shaped
      <> regexp_matches(coalesce(registration_number_norm, ''),
                        '^([0-9]{8}|[A-Z]{2}[0-9]{6}|R[0-9]{7})$')
LIMIT 20;

-- check: a company filed as its own PSC is flagged, not silently carried. 284
-- rows in this snapshot do it. They are a source error and must be visible
-- before anything walks the ownership graph in M3.
SELECT company_number, registration_number_norm, self_reference
FROM read_parquet('{pq}')
WHERE self_reference <> (registration_is_crn_shaped
                         AND registration_number_norm = company_number)
LIMIT 20;

-- check: no ceased date in the future
SELECT company_number, ceased_on
FROM read_parquet('{pq}')
WHERE ceased_on > DATE '{snapshot}'
LIMIT 20;

-- check: the partition column agrees with the partition path
SELECT DISTINCT snapshot_date
FROM read_parquet('{pq}')
WHERE snapshot_date <> DATE '{snapshot}';
