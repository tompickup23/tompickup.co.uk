# LGR public-property map data

Builds `public/data/lgr-public-property.json` for `/lgr/property/`.

## Sources (v1, 23 Jul 2026)
- **County estate** (owner `county`): `burnley-council/data/lancashire_cc/property_assets.json`
  in the AI DOGE repo (~/clawd). LCC Local Authority Land List, geocoded. 923 in-county assets.
- **Schools** (owner `education`): DfE Get Information About Schools (GIAS) all-establishment CSV,
  `GET https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata<YYYYMMDD>.csv`
  (HEAD 500s, use GET; today's file may not be ready, use a recent weekday). Filter LA in
  {Lancashire, Blackburn with Darwen, Blackpool}, status Open, state-funded groups. Easting/Northing
  (EPSG:27700) -> lat/lng (EPSG:4326) via pyproj. Save the CSV next to this script as `gias_all.csv`.
- **Fire** (owner `fire`): `fire.json` here (39 LFRS stations, postcode-geocoded via postcodes.io).
- **Ambulance** (owner `ambulance`): `ambulance.json` here (26 NWAS sites in the Lancashire-14).

## Future unitary tagging (confirmed 4-model, 16 Jul 2026)
North = Lancaster+Preston+Ribble Valley; East = Blackburn w Darwen+Burnley+Hyndburn+Pendle+Rossendale;
South = Chorley+South Ribble+West Lancashire; West/Fylde Coast = Blackpool+Fylde+Wyre.

## Fast-follow layers (not yet built)
District-council titles (HM Land Registry CCOD, stream-filter per council proprietor names +
geocode), wider NHS estate (NHS Property Services + trusts + GP estate), retained council housing
(aggregate per area, NOT individual house pins). Add each as a new owner type in the OWNERS map in
`src/pages/lgr/property/index.astro` (colours already reserved: nhs #f472b6, housing #60a5fa).

## Run
    python3 build_lgr_property.py   # needs gias_all.csv alongside; writes lgr-public-property.json
    cp lgr-public-property.json ../../public/data/
