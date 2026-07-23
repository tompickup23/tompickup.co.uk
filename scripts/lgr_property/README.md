# LGR public-property map data

Builds `public/data/lgr-public-property.json` for `/lgr/property/` (v2, 23 Jul 2026).

## Pipeline
1. **CCOD (councils + NHS + police + gov):** stream-filter HM Land Registry CCOD FULL
   to the 14 Lancashire districts, classify proprietors, geocode.
   - Get a fresh signed URL (key in `secrets/landregistry.env`, header `Authorization: <key>`):
     `GET https://use-land-property-data.service.gov.uk/api/v1/datasets/ccod/CCOD_FULL_YYYY_MM.zip`
     -> `.result.download_url` (time-limited: stream immediately, never save the 1.57GB file).
   - `curl -sS "$URL" | funzip | python3 ccod_filter.py > ccod_lancs14.csv`  (14 districts)
   - `python3 ccod_classify.py`  -> `ccod_public.csv` (public-sector proprietors only)
   - `python3 geocode_ccod.py`   -> `ccod_features.json` (postcodes.io bulk + /places, Nominatim
     fallback for localities; caches in postcode_cache.json / locality_cache.json)
   - Precision: postcoded titles = precise (`pr:pc`); council land parcels without a postcode
     = locality-approximate (`pr:loc`, jittered, shown faded). No-postcode gov (linear road/rail
     land) and CCOD fire/ambulance are dropped (curated station layers cover the latter).
2. **Schools (education):** DfE GIAS all-establishment CSV -> `gias_all.csv` (save alongside);
   `edubasealldata<YYYYMMDD>.csv`, HEAD 500s use GET; Easting/Northing 27700 -> 4326 via pyproj.
3. **Fire / ambulance:** curated station lists `fire.json` / `ambulance.json`.
4. `python3 build_lgr_property.py` merges all -> `lgr-public-property.json`; copy to `../../public/data/`.

## Owner types (frontend OWNERS map in src/pages/lgr/property/index.astro)
county, district (both **transfer** to the new unitaries in 2028) | parish, education, nhs,
police, fire, ambulance, gov (all **stay** / separate). Parish councils are NOT abolished.

## Future unitary tagging (confirmed 4-model, 16 Jul 2026)
North = Lancaster+Preston+Ribble Valley; East = Blackburn w Darwen+Burnley+Hyndburn+Pendle+Rossendale;
South = Chorley+South Ribble+West Lancashire; West/Fylde Coast = Blackpool+Fylde+Wyre.

## Not yet included
Retained council housing as per-area aggregates (not house pins); the ~4,500 council parcels whose
locality could not be geocoded; street-level precision for the ~14.7k locality-approximate parcels.
