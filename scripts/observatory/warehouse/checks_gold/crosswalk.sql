-- Gates on the crosswalk edge table. Any returned row is a build failure.
-- Placeholders: {pq} this partition's part.parquet, {entity_pq} the matching
-- entity partition, {snapshot} the partition date, {rows} the manifest count.

-- check: row count matches the manifest
SELECT 'rows' AS gate, count(*) AS got, {rows} AS expected
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: every entity_id is a well formed ULID
SELECT entity_id FROM read_parquet('{pq}')
WHERE entity_id IS NULL
   OR NOT regexp_matches(entity_id, '^[0-9ABCDEFGHJKMNPQRSTVWXYZ]{26}$')
LIMIT 20;

-- check: no scheme outside the declared registry
SELECT DISTINCT scheme FROM read_parquet('{pq}')
WHERE scheme NOT IN ('GB-COH', 'GB-CHC', 'GB-SC', 'GB-NIC', 'GB-MPR',
                     'GB-NHS', 'GB-EDU', 'GB-UKPRN',
                     'LBO-NNDR', 'LBO-SUPPLIER', 'LBO-WEB');

-- check: scheme_is_local agrees with the scheme code
SELECT DISTINCT scheme, scheme_is_local FROM read_parquet('{pq}')
WHERE scheme_is_local <> (scheme LIKE 'LBO-%');

-- check: V-T1 every company number is exactly eight uppercase alphanumerics
-- The eight-character rule, not a two-letters-and-six-digits regex, because
-- 93 pre-1922 Northern Ireland companies carry R plus seven digits and 6
-- Scottish limited partnerships end in a letter (DATA-INTEGRITY s9.3).
SELECT DISTINCT source_id FROM read_parquet('{pq}')
WHERE scheme = 'GB-COH' AND NOT regexp_matches(source_id, '^[0-9A-Z]{8}$')
LIMIT 20;

-- check: V-T4 no society or CIO number is carried in a GB-COH field
-- A society number can be CRN-shaped (RS000822 matches the two-letters-and-
-- six-digits pattern perfectly), so only the prefix rule separates them.
SELECT DISTINCT source_id FROM read_parquet('{pq}')
WHERE scheme = 'GB-COH'
  AND substr(source_id, 1, 2) IN ('IP', 'RS', 'SP', 'NO', 'NP', 'CE', 'CS',
                                  'OE', 'FC')
LIMIT 20;

-- check: no empty identifier
SELECT scheme, count(*) FROM read_parquet('{pq}')
WHERE source_id IS NULL OR trim(source_id) = ''
GROUP BY 1;

-- check: confidence is a probability and is never zero
SELECT DISTINCT matcher, confidence FROM read_parquet('{pq}')
WHERE confidence IS NULL OR confidence <= 0 OR confidence > 1
LIMIT 20;

-- check: a deterministic edge carries no match score
SELECT matcher, count(*) FROM read_parquet('{pq}')
WHERE method = 'deterministic' AND match_score IS NOT NULL
GROUP BY 1;

-- check: a probabilistic edge carries a match score
SELECT matcher, count(*) FROM read_parquet('{pq}')
WHERE method = 'probabilistic' AND match_score IS NULL
GROUP BY 1;

-- check: method is one of the two declared values
SELECT DISTINCT method FROM read_parquet('{pq}')
WHERE method NOT IN ('deterministic', 'probabilistic');

-- check: identifier-observed evidence is never a probabilistic score
SELECT DISTINCT method, evidence_class FROM read_parquet('{pq}')
WHERE evidence_class = 'identifier-observed' AND method <> 'deterministic';

-- check: no future validity dates
SELECT matcher, count(*) FROM read_parquet('{pq}')
WHERE valid_from > DATE '{snapshot}' OR valid_to > DATE '{snapshot}'
GROUP BY 1;

-- check: validity intervals are ordered
SELECT scheme, source_id, valid_from, valid_to FROM read_parquet('{pq}')
WHERE valid_from IS NOT NULL AND valid_to IS NOT NULL AND valid_to < valid_from
LIMIT 20;

-- check: no exact duplicate edge (the double-load gate)
SELECT entity_id, scheme, source_id, matcher, decision_id, count(*) AS n
FROM read_parquet('{pq}')
GROUP BY 1, 2, 3, 4, 5 HAVING count(*) > 1
LIMIT 20;

-- check: one identifier never belongs to two entities
SELECT scheme, source_id, count(DISTINCT entity_id) AS n
FROM read_parquet('{pq}')
GROUP BY 1, 2 HAVING count(DISTINCT entity_id) > 1
LIMIT 20;

-- check: every crosswalk entity_id exists in the entity table
SELECT DISTINCT c.entity_id
FROM read_parquet('{pq}') c
ANTI JOIN read_parquet('{entity_pq}') e USING (entity_id)
LIMIT 20;

-- check: every matcher decision that links two identifiers lands them in one
-- entity. This is the clustering contract: if it can fail, the crosswalk is
-- asserting a link it did not act on.
SELECT decision_id, count(DISTINCT entity_id) AS n
FROM read_parquet('{pq}')
WHERE decision_id IS NOT NULL
GROUP BY 1 HAVING count(DISTINCT entity_id) > 1
LIMIT 20;
