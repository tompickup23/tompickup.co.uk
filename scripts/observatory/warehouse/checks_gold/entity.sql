-- Gates on the entity table. Any returned row is a build failure.
-- Placeholders: {pq} this partition's part.parquet, {crosswalk_pq} the
-- matching crosswalk partition, {snapshot} the partition date, {rows} the
-- manifest count.

-- check: row count matches the manifest
SELECT 'rows' AS gate, count(*) AS got, {rows} AS expected
FROM read_parquet('{pq}')
HAVING count(*) <> {rows};

-- check: every entity_id is a well formed ULID
SELECT entity_id FROM read_parquet('{pq}')
WHERE entity_id IS NULL
   OR NOT regexp_matches(entity_id, '^[0-9ABCDEFGHJKMNPQRSTVWXYZ]{26}$')
LIMIT 20;

-- check: entity_id is unique
SELECT entity_id, count(*) AS n FROM read_parquet('{pq}')
GROUP BY 1 HAVING count(*) > 1 LIMIT 20;

-- check: the anchor is unique across entities
SELECT anchor_scheme, anchor_source_id, count(*) AS n
FROM read_parquet('{pq}')
GROUP BY 1, 2 HAVING count(*) > 1 LIMIT 20;

-- check: anchor scheme is in the declared registry
SELECT DISTINCT anchor_scheme FROM read_parquet('{pq}')
WHERE anchor_scheme NOT IN ('GB-COH', 'GB-CHC', 'GB-SC', 'GB-NIC', 'GB-MPR',
                            'GB-NHS', 'GB-EDU', 'GB-UKPRN',
                            'LBO-NNDR', 'LBO-SUPPLIER', 'LBO-WEB');

-- check: the anchor is always the entity's highest precedence identifier.
-- If a company-numbered entity is anchored on a ratepayer name, its id will
-- move the next time the name changes, which breaks mint-once in practice
-- even though nothing re-mints.
SELECT entity_id, anchor_scheme, schemes FROM read_parquet('{pq}')
WHERE has_company_number AND anchor_scheme <> 'GB-COH'
LIMIT 20;

-- check: every entity holds at least one identifier
SELECT entity_id FROM read_parquet('{pq}')
WHERE identifier_count < 1 OR scheme_count < 1 LIMIT 20;

-- check: has_company_number agrees with the scheme list
SELECT entity_id, has_company_number, schemes FROM read_parquet('{pq}')
WHERE has_company_number <> list_contains(schemes, 'GB-COH') LIMIT 20;

-- check: only_local_identifiers agrees with the scheme list
SELECT entity_id, only_local_identifiers, schemes FROM read_parquet('{pq}')
WHERE only_local_identifiers <> NOT EXISTS (
    SELECT 1 FROM unnest(schemes) AS t(s) WHERE s NOT LIKE 'LBO-%')
LIMIT 20;

-- check: an entity with a company number carries the registered name.
-- A company-anchored entity displaying a supplier ledger name would be a
-- weaker source overwriting a register fact (DATA-INTEGRITY s2).
SELECT entity_id, anchor_source_id, name FROM read_parquet('{pq}')
WHERE has_company_number AND (name IS NULL OR NOT name_from_register)
LIMIT 20;

-- check: an entity with no company number claims no register facts
SELECT entity_id, anchor_scheme, entity_type FROM read_parquet('{pq}')
WHERE NOT has_company_number
  AND (name_from_register OR entity_type IS NOT NULL OR postcode IS NOT NULL)
LIMIT 20;

-- check: V-T2 no CRN-less entity carries a Companies Act entity type
SELECT entity_id, entity_type FROM read_parquet('{pq}')
WHERE NOT has_company_number
  AND entity_type IN ('ltd', 'plc', 'llp', 'lp', 'cic')
LIMIT 20;

-- check: no entity typed as a family the register spine excludes
SELECT DISTINCT entity_type FROM read_parquet('{pq}')
WHERE entity_type IN ('cio', 'registered-society', 'overseas-entity',
                      'overseas-establishment', 'other-corporate-body');

-- check: best_confidence is a probability
SELECT entity_id, best_confidence FROM read_parquet('{pq}')
WHERE best_confidence IS NULL OR best_confidence <= 0 OR best_confidence > 1
LIMIT 20;

-- check: every entity has at least one deterministic edge.
-- A probabilistic score may add identifiers to an entity; it may never be the
-- only reason an entity exists.
SELECT entity_id FROM read_parquet('{pq}')
WHERE NOT has_deterministic LIMIT 20;

-- check: identifier_count matches the crosswalk
SELECT e.entity_id, e.identifier_count, c.n
FROM read_parquet('{pq}') e
JOIN (SELECT entity_id, count(DISTINCT (scheme, source_id)) AS n
      FROM read_parquet('{crosswalk_pq}') GROUP BY 1) c USING (entity_id)
WHERE e.identifier_count <> c.n
LIMIT 20;

-- check: every entity appears in the crosswalk
SELECT e.entity_id FROM read_parquet('{pq}') e
ANTI JOIN read_parquet('{crosswalk_pq}') c USING (entity_id)
LIMIT 20;
