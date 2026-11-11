# The Padiham Job: assessment and research, 11 November 2026

Assessment of the game at commit `54d739c6` (branch `feat/straight-outta-padiham`, local only). Nothing in the
game was changed in this session. Read with `docs/PADIHAM-GAME-HANDOVER.md`.

How it was assessed: the code was read, the game was played in the dev server on desktop and at phone size
(emulated, not a real phone), `npm run build` was run (passes), and three research passes were made on the web:
local history sources, comparable projects with press and legal risk, and open elevation data. Web claims below
that were not re-checked against a primary source in this session are marked "not checked".

## 1. Verdict

The base is sound: real streets, a believable town centre, working jobs, two pub interiors, ten sourced plaques,
60 frames a second on a desktop. Today it is still a novelty, for four reasons:

1. The history is thin and vanishes. Ten plaques, shown only while you stand beside them, with no record kept
   in the game of what you have read.
2. The town is flat and, outside the centre, generic. 825 of the 853 buildings carry no type and no real
   height. Padiham's hills are its character and they are missing.
3. There is nothing to share and no reason to return once the eight scripted jobs are done.
4. Two content choices (the MP figure, and to a lesser degree the shirt) decide who will share it. See section 6.

## 2. What was measured

| Thing | Figure | Note |
|---|---|---|
| Triangles drawn per frame | about 640,000 desktop, about 616,000 at phone settings | `renderer.info`, standing by the Town Hall and the Starkie Arms |
| Draw calls per frame | 297 desktop | same |
| Frame rate | 61 per second | this Mac only; no real phone tested |
| Game script | 583,738 bytes, 157,287 gzipped | built file in `dist/_astro` |
| World file | 156,632 bytes, 49,494 gzipped | `public/games/the-padiham-job/world.json` |
| Buildings | 853, of which 825 typed "yes", 25 named, 11 with photo overrides | world file |
| Named places | 38 | world file |

The estate building blocks ledger (Reports, `Website_Audits/building_blocks.csv`, B021) lists three.js as
"avoid" on weight grounds. The game is a deliberate exception, and the script loads on this one page only.

## 3. Faults found by playing

| # | Fault | Where |
|---|---|---|
| F1 | Floating place labels are a fixed 20 m wide, so close up they fill the screen and cover the job panel (seen at Padiham Bridge) | `label()` in `game.js` |
| F2 | On entering a pub the camera starts inside the player's back | `enterPub()` |
| F3 | On a phone the four panels cover roughly the top 45 per cent of the view, and the distance wraps on to two lines | page styles, 640 px rule |
| F4 | Double yellow lines are drawn straight across junction mouths | `city.js` road markings |
| F5 | Padiham Bridge has loose parapet pieces that read as a maze on the deck | `city.js` |
| F6 | Away from the centre the houses are plain untextured boxes on flat grass (Padiham FC approach, the sheds by Boyes) | generator defaults |
| F7 | No pause, no Escape key, no settings, no reduced motion option | `game.js` |
| F8 | A plaque's words disappear when you walk away and cannot be read again in the game | `updatePlaques()` |
| F9 | The street name pill goes stale when no named road is within 14 m | `step()` |
| F10 | Timer text "Bonus if done by 0:38 (0:22)" is hard to read at a glance | `step()` |
| F11 | Random deliveries can send the player to three primary schools and the medical centre, and one cargo is "a box of leaflets". In a councillor's game that is an avoidable tone risk | `makeJob()`, `CARGO` |
| F12 | The edge of the map says "More of Padiham is coming soon", which is a promise | `step()` |
| F13 | Shadows use the costliest setting and are redrawn every frame although the sun never moves | renderer set up |
| F14 | The Town Hall plaque and the Town Hall job marker share a spot (already in the handover) | `placePlaques()` |

Not found: no console errors from the game itself, and the header no longer overlaps the game after Play.

## 4. Quick wins (each about a day or less)

1. Fix F1, F2, F3, F9, F10, F12 and F14. All are small.
2. Plaque journal: a button that lists every plaque, read ones in full with their source, unread ones as
   "not found yet". Fixes F8 and gives the history a home.
3. Double the plaques from the verified pool in section 8 (the two banks, Gawthorpe Street, the Oddfellows'
   Hall, the 1758 preaching house, the Town Hall keystone, the 1606 market, the 1754 turnpike, coal in 1434).
4. One brag number and a share card: "I found 14 of 20 plaques in Padiham". Kept on the device, no account.
5. Postcard mode: at a landmark, hide the panels and save the view as an image with the place name on it.
6. Credits on the start screen: "© OpenStreetMap contributors" linked to openstreetmap.org/copyright, and
   "Contains OS data © Crown copyright and database right 2026". Both are on the page now but not in the game.
7. Take schools and the medical centre out of random jobs and drop the leaflets (F11).
8. Cheaper shadows (F13) and a pause on Escape and on tab switch (F7).
9. Set the page to `noindex` until launch day.

## 5. Larger features, in the order I would build them

| # | Feature | Why | Size | Depends on |
|---|---|---|---|---|
| A | Real hills and real building heights from Environment Agency LIDAR | The biggest single gain in realism. The data is proven to work (section 9): 56 m of relief across the map | Several days: terrain mesh, roads laid on it, player height, bridges, collision check | Nothing |
| B | Building realism pass: height and roof per outline from A, stone or brick by street, real frontage for about 20 key buildings from Tom's own straight-on photos | Makes "that's our street" true in a clip | Days, plus a photo walk | A |
| C | Heritage walk mode: no clock, 20 to 30 plaques, journal, a printable trail sheet for the real town and a one page school worksheet | This is the "more than a gimmick" answer, and the thing a teacher or the Archives can use | Days | Plaque checks by a local history body |
| D | Daily job: one fixed route each day, same for everyone, result shared as a line of text | Reason to come back; no data collected | 1 to 2 days | Share card (quick win 4) |
| E | Then and now: at a few spots, switch the view to an earlier Padiham (Bridge End Mill where the Town Hall stands, the market hall on the fire station site, the 22 mills of 1900) | Strong for press and schools | Large; needs old maps and photographs with clear rights | C; sources not yet gathered |
| F | Playable darts and pool, Kings Arms interior, Flying Dutchman beer garden | Depth in what is already there | Days | Landlords' agreement |
| G | Seasons and events: Christmas lights, match day at the Arbories, Padiham on Parade bunting | Launch pegs that repeat every year | 1 to 2 days each | Dates confirmed with the Town Council |
| H | The Greenway and Gawthorpe Hall | Gawthorpe is outside the current map (about 600 m east of its edge) | Large; needs a wider world file | A, and phone performance |
| I | Phone performance: merge static buildings into chunks, thin distant detail | Needed before any wide share | 1 to 2 days | A real phone to test on |

Left out on purpose: shared leaderboards, nicknames, other players. They bring data protection duties and the
Children's Code for little gain (section 7).

## 6. Decisions for Tom

These are his to make. The research below is the evidence, not a ruling.

1. **The MP figure.** Tom chose to include it on 11 November 2026. Against the aim of this review (press and
   public benefit beyond a gimmick) it is the one feature most likely to turn "councillor builds his town"
   into "Reform councillor mocks MP in game", and to stop schools, the Town Council, Gawthorpe and the
   football club from sharing it. Legal risk is low while it carries no words that imply conduct. Options:
   keep it; move it to a later update after the launch coverage; or swap it for something neutral.
2. **PAYTON 10.** No free-standing image right exists in English law, but a named shirt in a politician's game
   could be read as his backing. Asking Andy Payton removes the risk and, if he says yes, gives the best
   launch photograph.
3. **Launch timing.** A soft launch at the Christmas lights switch on is possible but tight; a full launch in
   spring must clear the pre-election period.
4. **Who checks the history.** Asking Padiham Archives to check and be credited on the plaques gives the game
   a non-party co-owner.

## 7. Press, comparables and legal notes

From the web research pass. Items marked "not checked" were reported by the research agent and not re-read here.

**Comparables.** New Milton and Wisbech were rebuilt in Minecraft with schools, heritage groups and lottery
money (Microsoft UK Stories, 16 July 2018; Collusion project page, 2019). "GTA Paisley", one self-taught
designer's 3D Paisley, spread on a short clip with almost 400,000 views (STV News, 13 May 2025). Metro Memory,
a name-the-stations game, passed 100,000 players in days on one shareable score (Time Out, 24 October 2023).
No comparable published a measured footfall effect, so do not claim one.

**What made them last, in rough order:** one shareable score that tests local knowledge; real history from
named local sources; a teacher pack; contributions from residents; a 15 to 30 second clip; named non-party
partners; a gentle tone.

**The hostile frame (not checked).** The agent reports national coverage in September 2026 of a Reform UK
game inside a party app that was dominated by criticism. If so, a reporter searching "Reform game" will find
it first. Check before launch.

**Imprint.** The page already carries the site imprint in the footer. Draft statutory guidance (gov.uk, July
2023) ties the duty to material that can reasonably be seen as promoting electoral success. An opponent's
caricature pushes a town game towards that line. The Electoral Commission's final text was not reached.

**Pre-election period (not checked).** The agent reports May 2027 local elections on existing boundaries and
a notice of election in late March. From then councils and schools will not share a politician's work.

**Rights.** Real pub and shop names in a free game: low risk; tell each business first and offer removal.
Title: low risk; avoid the film's cars, music and catchphrases; the UK trade mark register was not reached.
Geograph photographs are not reproduced in the game, which is the right position.

**Data.** Keep it as it is: no accounts, no chat, scores on the device. The Children's Code (ICO) applies to
services children are likely to use. Note the site's Cloudflare analytics beacon loads on this page as on
every other.

**Calendar.** Christmas lights switch on: the agent read Friday 27 November 2026 from a garbled Town Council
listing; confirm with the Town Clerk. Padiham on Parade ran 27 and 28 June 2026 (Armed Forces Day listing).
Heritage Open Days ran 11 to 20 September 2026.

## 8. Verified history pool

Read on 11 November 2026 from the raw text of each source. Short names: **VCH** is Farrer and Brownbill
(eds), A History of the County of Lancaster, volume 6 (1911), "Townships: Padiham", pp. 492 to 496, British
History Online. **LEUS** is Lancashire County Council and Egerton Lea, Lancashire Historic Town Survey
Programme: Padiham Historic Town Assessment Report, May 2005 (printed page numbers). **CAA** is Burnley
Borough Council, Padiham Conservation Area Appraisal, June 2020. **HE** is Historic England, National Heritage
List for England, list entry number given.

Caution: historicengland.org.uk refused automated reading. Grades and dates came from Historic England's open
data service and the description text from a mirror of the list text. Open each HE entry in a browser before
a plaque is published.

### Ready to use (two sources agree)

| Fact | Where | Sources |
|---|---|---|
| The corner bank with the round tower was built in 1893 for the Craven Bank, by William Dent of Nelson; later Barclays | 15 to 17 Burnley Road, Mill Street corner | HE 1238196; LEUS p. 25; CAA para 5.39 |
| The bank at 47 Burnley Road was built in 1901 for the Manchester and County Bank, by Mills and Murgatroyd; later NatWest | 41 to 47 Burnley Road | HE 1274553; LEUS p. 25 |
| The model houses on Gawthorpe Street were built in 1842 for the Shuttleworths; the design is "supposedly" by Sir Charles Barry. Keep the hedge | 1 to 21 Gawthorpe Street | HE 1274185; CAA para 5.20 |
| The Oddfellows' Hall was built in 1845 and carries its dated plaque | 2 Bank Street | HE 1238155; LEUS p. 25 (CAA caption says c.1846) |
| 29 Mill Street was built in 1854 as the house of the National School's headmaster | 29 Mill Street | HE 1237658; CAA para 5.34 |
| 70 to 72 West Street was built in 1758 as the Wesleyan Methodists' first preaching house, made to look like two cottages | West Street | CAA para 5.108; LEUS p. 26 |
| Arbory Lodge, about 1800, was the gateway to the Starkies' Huntroyde estate (now a private house) | Arbory Drive | HE 1274552; CAA para 5.105 |
| St Leonard's keeps a stone font of 1525, said to have been given by John Paslew, last Abbot of Whalley | St Leonard's | VCH p. 495; HE 1238303 ("said to") |
| The first chapel here was a chantry founded about 1455 by John Marshall | St Leonard's | VCH p. 494; LEUS p. 15 |
| The Town Hall's door keystone shows a woman with a distaff and a shuttle; the hall stands on the site of Bridge End Mill | Town Hall | HE 1237652; CAA para 4.31 |
| The road through Padiham became a turnpike in 1754 to 1755, the earliest in east Lancashire | Church Street, Mill Street | LEUS p. 38; CAA para 4.13 |
| A local board was formed in 1873; the urban district council had fifteen members from five wards | Town | VCH p. 492; CAA paras 4.28 and 4.29 |
| Stockbridge House, mid seventeenth century, Grade II*, has a sundial dated 1829 | Victoria Road | HE 1274125; LEUS pp. 16 to 17 |

### Usable, one source (say so, or find a second)

| Fact | Source |
|---|---|
| A mine of sea coal at Padiham is recorded in an account of 1434 | VCH p. 492 |
| The earliest evidence of Padiham market is butter bought there in the Shuttleworth accounts for 1606 | LEUS p. 17 |
| A Monday market was re-established in 1797 | LEUS p. 19 |
| Padiham's first cotton factory, Clay Bank Mill, went up about 1790 | LEUS p. 20 |
| By 1900 twenty-two cotton mills were working in Padiham | LEUS p. 24 |
| Victoria Mill was built in 1852 to 1853 as a mule spinning factory | CAA para 5.56 |
| In 1647 Padiham Bridge, in danger of collapse, was repaired for £10 | LEUS p. 38 |
| The market hall closed in 1956 and a fire station was built on the site | LEUS p. 24 |
| Padiham's first fire brigade formed in 1859 | Lancashire County Council, Red Rose Collections, item 309343 |
| The former Co-operative store on Burnley Road dates from 1906 | CAA para 5.39 |
| Thirteen fried fish sellers were listed in Padiham by 1902 | LEUS p. 25 |
| The cemetery at Padiham Green was laid out in 1852 | VCH p. 492 |
| Alfred Tysoe, born in Padiham on 21 March 1874, won two gold medals at the 1900 Paris Olympics | Olympedia, athlete 69547 |
| Harry Hill, born in Padiham on 8 May 1916, won cycling bronze at the 1936 Berlin Olympics | Olympedia, athlete 14749 |
| The war memorial inscription begins "Erected by the women of Padiham" | War Memorials Online, record 153903 |
| The 1947 meeting that revived the football club was held in the Town Hall on 3 September 1947 | North West Counties Football League, "Emblematically Speaking: Padiham", 27 December 2017 |
| Gawthorpe Hall, 1600 to 1605, for the Rev. Lawrence Shuttleworth, altered by Barry about 1850 to 1860, Grade I (outside the current map) | HE 1237626 |

### Do not use yet (sources disagree or could not be read)

- **St Leonard's architect.** Historic England names Angelo Waddington; LEUS p. 26 and CAA para 5.16 name
  William Waddington. Say "the Waddington practice of Burnley" or leave out.
- **Railway dates.** Opening is given as 1875, 1876, 1877 and 1879 in different sources; passenger closure as
  1957 and 1966. Only "the 1870s" and "closed completely in 1968" are safe.
- **Liberal Club.** 1897 in the CAA text, 1898 in its caption.
- **The 1633 witchcraft hearing.** One source, hedged, and the same paragraph also says 1634.
- **17 to 19 Mill Street as council offices in 1874.** The body in 1874 was the local board, not the urban
  district council.
- **The 2015 flood.** The Environment Agency gives 3.45 m at Station Road Bridge and 149 properties reported
  flooded on 26 December 2015. It is one source, recent, affects residents, and the flood scheme is live and
  contested. Treat as sensitive.
- **Power station, Padiham on Parade's first year, the Greenway's opening day, the Town Council's creation,
  the war memorial's unveiling, the 1 April 1974 abolition date.** Not confirmed from a primary source.

### Existing plaques: one to check

**Memorial Park.** The plaque gives 18 June 1921 from the Burnley Council management plan, which would not
load today. The CAA supports 1921, but LEUS says 1922 twice (pp. 32 and 39). This is a prompt to re-read the
management plan and find a second source for the exact day. Do not change the plaque on this note alone.
The other nine plaques matched their sources.

## 9. Elevation data: the route that works

The Environment Agency LIDAR Composite is open by WCS with no key. Tested 11 November 2026; 1 m terrain (DTM)
and first-return surface (DSM) files for the whole game area were downloaded and are valid GeoTIFFs
(1500 by 1020 cells, EPSG:27700, 6,291,923 bytes each). Terrain runs from 71.51 m to 127.44 m.

Pattern: `https://environment.data.gov.uk/spatialdata/<slug>/wcs?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=<id>&format=image/tiff&subset=E(378600,380100)&subset=N(433500,434520)&SUBSETTINGCRS=http://www.opengis.net/def/crs/EPSG/0/27700`

| Product | Slug | CoverageId |
|---|---|---|
| DTM 1 m | `lidar-composite-digital-terrain-model-dtm-1m` | `13787b9a-26a4-4775-8523-806d13af58fc__Lidar_Composite_Elevation_DTM_1m` |
| DSM first return 1 m | `lidar-composite-digital-surface-model-first-return-dsm-1m` | `df4e3ec3-315e-48aa-aaaf-b5ae74d7b2bb__Lidar_Composite_Elevation_FZ_DSM_1m` |
| DSM last return 1 m | `lidar-composite-digital-surface-model-last-return-dsm-1m` | `9ba4d5ac-d596-445a-9056-dae3ddec0178__Lidar_Composite_Elevation_LZ_DSM_1m` |

Notes for the build script (`scripts/padiham-game/build_terrain.py`, not yet written):

- One request stalled and worked on retry: use a timeout and retries.
- The game's local metres are not the National Grid. Convert each sample through latitude and longitude.
- Sample terrain at 5 m (221 by 153 points) into a separate `terrain.json`; let the game read heights from
  the grid directly instead of casting rays.
- Building height: the middle value of surface minus terrain inside each outline shrunk by 1 m; fall back to
  the present guess when under 2 m or too few cells. Trees over small roofs inflate it.
- Bridges are cut out of the terrain model: join the two ends of any road tagged as a bridge.
- Flatten the river to one level.
- Licence: Open Government Licence. Add "© Environment Agency copyright and/or database right 2022. All
  rights reserved." to the attribution.
- The survey is from about 2020; `overrides.json` stays the last word.
