# The Padiham Job: handover

Written 11 November 2026 at the end of the session that built the game.

## Where it stands

- Branch `feat/straight-outta-padiham`, commit `ec7f7305` plus this note. Local only: not pushed, no pull request, not on `main`, so **not live**.
- Page: `/the-padiham-job/`. `npm run build` passes every guard and the built output has no em-dashes.
- Checked in the built site on desktop and at phone size in a desktop browser. Not tried on a real phone. The pub jobs were not played end to end.

## What the game is

A third-person walking game on the real street layout of Padiham town centre, from the Town Hall and Padiham Bridge up to Church Street. Seven scripted jobs, then random deliveries. Pedestrians, pigeons that scatter, Union Jack bunting on Burnley Road. The Starkie Arms and the Flying Dutchman can be entered; the Kings Arms is exterior only.

## Where things are

| What | Where |
|---|---|
| Page, markup and styles | `src/pages/the-padiham-job/index.astro` |
| Game loop, jobs, input, pub doors | `src/games/padiham/game.js` |
| Town generator: roads, buildings, roofs, street furniture | `src/games/padiham/city.js` |
| Facade texture atlas, signs, flags | `src/games/padiham/textures.js` |
| Pub interiors | `src/games/padiham/interior.js` |
| People and pigeons | `src/games/padiham/life.js` |
| Sound, all synthesised | `src/games/padiham/audio.js` |
| World file the game loads | `public/games/the-padiham-job/world.json` |
| World builder and its inputs | `scripts/padiham-game/` |
| Per-building details from photos | `scripts/padiham-game/data/overrides.json` |
| Photo credits | `scripts/padiham-game/data/photo-credits.json` |
| Share card and its script | `public/images/share/the-padiham-job.png`, `scripts/generate_padiham_game_card.mjs` |

To change a building, edit `overrides.json` and rebuild the world file with the command in the header of `scripts/padiham-game/build_world.py`. Do not edit `world.json` by hand.

## Sources and licences

- Roads, river, green space and named places: OpenStreetMap, Open Database Licence.
- Building outlines: OS OpenMap Local, Open Government Licence. The 103 MB download is not in the repo; `extract_os_buildings.py` says how to fetch it.
- Building details: read from photographs on Geograph and Wikimedia Commons (Creative Commons licences, listed in `photo-credits.json`) and from photographs of the Starkie Arms and Flying Dutchman supplied by Tom. No photograph is reproduced in the game.
- Google Street View was not used: Google's terms bar building models from it.

## Decisions already made

- Name: The Padiham Job, chosen by Tom on 11 November 2026 in place of "Straight Outta Padiham". The game is all jobs, and the name nods to a film title. The branch keeps the old name.
- Stylised look built on real data, not photo-realism.
- No "find your house" sharing. Sharing should centre on public places.
- Pigeons, not seagulls.
- Pubs are quizzes, pies, pool and darts. No drinking, no drink brands.
- Bar staff are generic figures, not real people.
- Named pubs, shops and landmarks from OpenStreetMap keep their real names on signs and in jobs (Tom, 11 November 2026: real business names are fine). Unnamed shop fronts carry generic trades. The page says the jobs are made up and nothing is endorsed by any business shown.

## Before it goes live

1. Play it on a real phone. The scene is heavy; parked cars may need thinning.
2. Ask the landlords of the Starkie Arms, Flying Dutchman and Kings Arms for their OK.
3. Decide whether the page is `noindex` until launch, as Lancashire Voice is. It is indexable as written.
4. Check the name is acceptable: it plays on a well-known film and album title.
5. Done 11 November 2026: the game panel is sized to the screen less the fixed site header, and pressing Play parks it just below the header.
6. The Mill Street bank corner and the White Horse block were matched to outlines by reading photos against a plan. Confirm on the ground.

## Known gaps

- Flat ground: Padiham's hills are missing. Environment Agency LIDAR would give terrain and true building heights.
- The player is a built figure, not an animated model.
- Several pubs and shops sit inside larger merged outlines, so they are fronts on a longer row.
- Kings Arms has no interior. The Flying Dutchman beer garden is left out.
- Buildings use repeating facade tiles. Straight-on photos of each frontage would allow real textures.

## Ideas not yet built

Daily challenge with a shareable result, postcard photo mode at landmarks, hidden collectibles with local history cards, playable darts or pool, team leaderboards with preset names, other players as anonymous figures with no chat.

## Added 11 November 2026 (second session)

Committed to the branch, still local only: not pushed, no pull request, not live. The page is now `/the-padiham-job/`.

For the next review, start here:

- Run `npm run dev` and open `/the-padiham-job/`. In the dev server only, `window.__padiham` gives the player, camera, state and scene for testing (teleport by setting `player.x` and `player.z`). It is not in the built site.
- Not yet looked at closely: the boots (not visible in the screenshots taken), the selfie figure's face, the plaque positions other than the Town Hall one, the edges of the wider map, and the whole game on a real phone.
- Known rough spots: the Town Hall plaque stands on the same spot as the Town Hall job marker; the floating "Padiham Football Club" label is large from the pitch; random deliveries are capped at 380 m but the Match day job is a long walk.
- Building realism was not changed in this session.

- **Player:** Burnley colours (claret, sky blue sleeves, no badge) with PAYTON 10 on the back, and black boots with three white stripes. Andy Payton wore 10 in 2000-01 (season squad list); he was born in Whalley, so nothing in the game says he was born in Padiham.
- **Start:** facing the Town Hall from across its forecourt.
- **Selfie taker:** a caricature of Oliver Ryan MP stands by the Town Hall portico taking a selfie, with a red rosette. Parliament lists him as Labour (Co-op) today (Members API, member 5135, read 11 November 2026), so the rosette is accurate. The page says the figure is a caricature of the town's MP. No photograph is used.
- **Playable area:** widened from the town centre to nearly the whole mapped area (`slice` in `build_world.py`). Bunting stays in the centre.
- **Padiham FC:** the mapped pitch has markings, goals, floodlights, a rail and a blue stand. The stand's side of the pitch is a guess, not checked. There is a Match day job to the ground.
- **History plaques:** ten, in `src/games/padiham/history.js`, each with its source; the page lists them under the game. Add one only with a named source. Left out because sources disagree or the site is unconfirmed: the church architect, railway dates, the two bank buildings, the fire station site.
- **Position check:** 15 pubs and food shops were compared with the Food Standards Agency's published locations (FHRS API, 11 November 2026). All are within 3 to 17 metres, including the three pubs placed by hand.
- **Fixes:** dartboard trigger, phone layout below the fixed header, off-screen pause, a message when WebGL is missing.

Still open: the name (Tom wants a new one), real building heights from LIDAR (the Environment Agency service address was not found), hills, phone performance on a real device.

## Added 11 November 2026 (third session: assessment and quick wins)

Not committed when written. The full assessment, the verified history pool and the working LIDAR route are in
`docs/PADIHAM-GAME-ASSESSMENT-2026-11-11.md`.

- **Plaques:** eighteen, up from ten. The eight new ones were checked against Historic England's own pages
  (opened in a browser, 11 November 2026) and the raw text of the town survey, the conservation area appraisal
  and the county history. A plaque can now be placed by `at` (a named place), `ll` (latitude and longitude of
  a building) or `road` (a street name). Every plaque settles off the carriageway and at least 4.4 m from a
  job marker.
- **Menu:** the Menu button, Esc, or leaving the tab pauses the game. The menu holds the plaque journal, Share
  my score (text only, nothing is sent anywhere by the game), Take a postcard (the view saved as an image with
  the credits on it), sound and full screen.
- **Fixes:** place labels shrink close up; the camera no longer starts inside the player in a pub; phone
  panels take about a quarter of the view; kerb lines break at side roads and forks; bridge parapets stand
  only on the outside of the deck; the timer and map edge wording; the street name falls back to "Padiham".
- **Jobs:** random deliveries no longer go to schools or the medical centre, and the box of leaflets is gone.
- **Credits:** OpenStreetMap and OS credits are on the start screen, in the menu and on every postcard.
- **Search:** the page is `noindex` and out of the sitemap (`noindex` on the Layout call, and the filter in
  `astro.config.mjs`). Remove both together on launch day.
- **Shadows:** the cheaper shadow type. Not measured on a real phone.
- **Testing:** in the dev server, set `window.__noAutoPause = true` to stop the pause on tab switch.

Not done: the Memorial Park date check (the town survey says 1922), hills and real heights, a real phone test.
The four decisions in section 6 of the assessment are still open.

## Added 11 November 2026 (fourth session: landmarks)

Not committed when written. Live check the same day: the branch is still local only and the page returns 404.

- **St Leonard's:** the tower now stands at the south-west corner with four stages, clock faces, battlements,
  corner turrets and pinnacles, and the church has a clerestory nave under a steep roof and a south porch.
  All from the list description (Historic England, list entry 1238303). The tower height is not in the
  listing and is unchanged. Features `nave` and `porch` in `overrides.json`.
- **War memorial:** a wheel cross on a pedestal at the Church Street and Blackburn Road corner of Memorial
  Park, built from the War Memorials Online description (record 153903), with a nineteenth plaque quoting
  the inscription. The position was placed by hand and the height is a guess: check both on the ground.
- **Bandstand:** at the spot OpenStreetMap maps it (way 973585469). No source describes it, so it is a plain
  open bandstand. A photograph would let it be drawn properly.
- **Landmarks list:** things with no building outline go in `overrides.json` under `landmarks`, each with its
  source, and reach the game as `landmarks` in the world file.
- **Boyes, Tesco and Lidl** carry larger name boards.
- **Murals:** not built. "Splendour" (Gavin Renshaw, Station Road corner) and "Up the Hill" (Chris Butcher,
  Church Street) are painted works, so copying them needs each artist's permission. Whether one shows the
  horse Tom remembers was not confirmed.

- **Player look:** the start screen has a chooser for hair (short, ponytail, long, cap), hair colour, skin
  and jacket colour, with no "boy" or "girl" label. The player is a courier in a plain jacket with a
  backpack. The choice is kept in the browser under `padiham-look-1`. Code: `LOOKS` and `setLook` in `game.js`.
- **PAYTON 10:** off the player. About one passer-by in six wears a claret and blue shirt with the name and
  number on the back (`Pedestrians` in `life.js`). The page wording under the game was changed to match.
  The question of asking Andy Payton is still open.

## Added 11 November 2026 (fifth session: hills and real heights)

Not committed when written. Still local only and not live.

- **Hills:** the town now sits on real ground from the Environment Agency LIDAR Composite 1 m terrain model:
  54.8 m of relief across the mapped area, on a 12 m grid. `scripts/padiham-game/build_terrain.py` downloads
  the data (kept out of git in `scripts/padiham-game/data/lidar/`) and writes
  `public/games/the-padiham-job/terrain.json`. Run it after `build_world.py`, every time: the building heights
  in it follow the order of the buildings in the world file. The exact command is in the script header.
- **How the game uses it:** `src/games/padiham/terrain.js` gives `groundH(x, z)`. Every batch of town geometry
  is lifted on to the ground when its mesh is made (`drape` in `city.js`): roads and other flat things are cut
  along the terrain grid so they sit on it exactly, and everything else moves up by the ground height under
  each point, so walls stay upright and a row follows the slope of its street. The player, passers-by, pigeons,
  plaques, markers, signs and trees all read `groundH`. With no terrain file the town is built flat as before.
- **Bridges:** the terrain model cuts bridges out, so each mapped bridge is a deck laid from bank to bank over
  the river, with a slab under the road. Thirteen decks after joining the pieces each bridge is mapped in.
- **Building heights:** 704 of 853 buildings carry the middle value of surface minus terrain over their
  outline. Houses take their number of floors from it and sheds their wall height. Buildings with details in
  `overrides.json`, churches, schools and civic buildings keep what they had.
- **Credits:** the Environment Agency line is on the start screen, in the menu, on postcards and under Sources.
- **Cost:** about 870,000 triangles a frame by the Town Hall, up from about 640,000, and 56 to 60 frames a
  second on this Mac. Roughly 230,000 of the rise is roads and ground cut along the grid. The largest single
  mesh is still street furniture (about 378,000). Not tried on a real phone: do the phone pass before launch.
- **Not checked:** the far edges of the map on foot, every bridge, and steep streets where a long row of
  houses leans with the slope instead of stepping. Small ground pieces (round road joins, short dashes) are
  not cut along the grid and could flicker on a sharp change of slope; none was seen.

## Launch, 11 November 2026

Tom asked for the game to go live. Done for launch: a "Disclaimer and removal requests" section at the foot of
the page (linked from the start screen and the menu), `noindex` removed and the page added to the sitemap.
Checked the same day: Parliament's Members API lists Oliver Ryan (member 5135) as Labour (Co-op), a current
member; the war memorial inscription was read from the War Memorials Online page itself.

Launched with these still open: no real phone test; the three landlords and Andy Payton not asked; Padiham
Archives not asked to check the plaques; the Memorial Park opening date (1921 on the plaque, 1922 in the town
survey); the mural permissions; the war memorial's position and height.

## Phone performance pass, 11 November 2026 (after launch)

On branch `perf/padiham-phone-pass`. Measured in the dev server on this Mac; still not tried on a real phone.

- **Culling:** every batch of town geometry and the trees are now built in squares 230 m across (`CHUNK` in
  `city.js`), so the renderer skips what is out of view, and squares beyond the haze are hidden
  (`cullChunks` in `game.js`). Triangles drawn per frame by the Town Hall fell from about 870,000 to about
  250,000; the widest view tested, from the north side, draws about 390,000. Draw calls are 225 to 444.
- **Lighter shapes:** chimney pots and lamp posts are five-sided open tubes, and a parked car's four wheels are
  two drums. That took roughly 160,000 triangles out of the street furniture with no visible change.
- **Phones:** half the parked cars, plainer trees with one crown, no dark kerb edge, and a nearer horizon
  (600 m against 900 m). About 480,000 triangles are built in all, against about 700,000 on a desktop.
- **Automatic step-down:** if play runs under 27 frames a second for three seconds the game lowers the
  resolution, then switches shadows off, then brings the horizon in to 380 m (`watchSpeed` and `stepDown` in
  `game.js`). It never steps back up. In the dev server `window.__padiham.stepDown()` forces a step.
- **Not checked:** the look of the game after each step-down (the preview pane was hidden, so nothing was
  drawn), and real frame rates on any phone.

## Comfort and access settings, 11 November 2026

On branch `feat/padiham-access`. Checked in the dev server's preview pane at desktop and phone width; the site
build passes. Not tried with a real screen reader, a real game controller or a real phone.

- **Settings** (`S`, `applySettings` and `setSetting` in `game.js`): calm, less movement, stronger contrast,
  messages that stay, camera follow, thumbstick side and three text sizes. The block sits in a `details` on the
  start screen and is moved into the menu when play starts. Only the settings the player has changed are stored
  (`padiham-settings-1`); the rest follow the device's reduced motion and contrast settings. `?calm=1` still
  works and is not stored unless the player changes the switch.
- **Calm** now also removes the clock, the speed bonus, the pigeon and selfie messages and the selfie flash, and
  thins the passers-by without a reload (`Pedestrians.setCount` in `life.js`).
- **Keys:** Q and E turn the view, Page Up and Page Down tilt it, R switches running, G (or the Where next
  button) says the way, Enter closes a message that is set to stay.
- **The way in words:** the job panel shows the distance and a direction ("ahead, to your left"), measured from
  where the view faces. A hidden status line (`#sop-say`) reads it out when it changes, at most every eight
  seconds.
- **Controller** (`readPad`): left stick moves, right stick looks, A switches running, Y says the way, Start
  opens the menu. Untested on hardware.
- **Text size** is one CSS variable, `--sop-fs`; panel sizes and positions are written against it. Messages now
  last at least two and a half seconds, and the menu shows the last one.
- **Menu:** `aria-modal`, the Tab key stays inside it, and focus returns to the Menu button on closing.
- The page has a "Playing with a disability" section that says what works and what does not yet.

Ideas and evidence: rows I128 to I179 in the Reports ideas register (Reports PR 688) and
`Tompickup_Games_work/Padiham_Job_Access_Local_Play_2026-11-11.md`.

## Four more buildings, 11 November 2026

On branch `feat/padiham-four-buildings` (cut from main, separate from `feat/padiham-access`). Checked in the
dev server's preview pane; the site build passes.

- **Reference only.** Tom saved nine screenshots on his Desktop (Google Street View, visitor photographs on
  Google Maps, one Facebook image, one from padiham.org.uk). They were read for shapes and colours. None is in
  the repo and none is reproduced.
- **Positions** came from the Food Standards Agency food hygiene register's geocodes (API, header
  `x-api-version: 2`, no key): 61-63 and 65 Burnley Road, 46 Church Street, 90 Burnley Road.
- **A fault found on the way.** The "Former bank, Burnley Road" override sat on the outline that is really
  57 to 65 Burnley Road. Wikimedia Commons gives that photo's object position as game metres (69, 132), the
  outline OpenStreetMap names The Bank. The override was moved there.
- **Padiham Business Centre** (`gablefront`): three storeys of stone over shops, central gable, two white bay
  windows, arched doorway with a balcony, two planters.
- **Padiham Leisure Centre** (`sportshall`; OpenStreetMap calls it Padiham Pool, and that is the name shown):
  brown brick ranges, a taller grey hall with a window band, a canopy.
- **Corner of Church Street and Alma Street** (`cornershop`): white painted splayed corner, dark red pilasters
  and fascia, wooden door. No name is shown, because the business there is not in OpenStreetMap and the page
  says names come from it.
- **Jaipur** (`blackfront`, with new override keys `spot` and `sign`): a black shop front on one bay of the long
  terrace outline, white lettering. `spot` picks the wall nearest a point, so a single shop in a terrace can be
  dressed without touching the rest.
- `build_world.py` is deterministic: outlines and their order did not change, so `terrain.json` was not rebuilt.
- **Easy next step:** the register lists about fifteen more food businesses inside the playable area that are
  not in OpenStreetMap (cafes, takeaways, the Bridge Inn). Adding them to OpenStreetMap would bring their real
  names into the game on the next rebuild.

## The white horse, the Bridge Inn and more named businesses, 11 November 2026

Same branch, `feat/padiham-four-buildings`. Checked in the dev server's preview pane; the site build passes.

- **The white horse** (the "Padiham horse" earlier notes could not identify): on a dark brick block above the
  shops at 42 Burnley Road, next to the Town Hall. That outline was dressed as rendered and gabled; it is now
  brown brick with a flat roof. Built as a `spots` entry of kind `horse`. No plaque yet: there is no sourced
  fact about the horse, so ask Padiham Archives or the Town Council before writing one.
- **Bridge Inn**, 68 Burnley Road: a stone pub front (`spots` kind `pub`) at the west end of the long terrace
  that also holds Jaipur. No interior.
- **`spots` replaces `spot`:** a list on a building override, each `{at, k, sign}` with `k` one of `black`,
  `pub` or `horse`. Each picks the wall nearest its point.
- **Serrano's Deli-Pizza is now named**, on Tom's say (11 November 2026).
- **32 more businesses are named**, from the Food Standards Agency food hygiene register (API, no key, Open
  Government Licence). They are in the `pois` list in `overrides.json`, each with its establishment number and
  the date read. Left out: care settings, schools, the foodbank, anything marked former, and anything not
  inspected since the start of 2022. No rating is stored or shown. The page's disclaimer now names the register
  as a second source of names. The register lags, so a closed business may linger: removal requests still apply.
- **Not built, noted:** Tom also saved three pictures of Padiham Cricket Club's pavilion (white walls, brown
  tiled roof, a central gable with a clock, a veranda). The cricket pitch is in the map data; the pavilion is
  not dressed. Children appear in one picture: reference for the building only.
- **Seen when comparing the bridge view with Tom's picture:** the real approach to Padiham Bridge is heavily
  treed on both banks and has stone parapets with railings; the game's is bare. Riverbank trees are an easy win.

## Cricket pavilion, White Horse plaque and riverbank trees, 11 November 2026

Same branch. Checked in the dev server's preview pane.

- **Padiham Cricket Club pavilion** (`pavilion`): the outline on the far side of the cricket ground from the
  football ground, as Tom said, and where the food hygiene register places the club. White walls, brown tiled
  roof, a central gable with a clock, a veranda with benches and baskets, the club's name on the veranda. A new
  landmark kind `wicket` draws a mown square and strip at the centre of the mapped cricket pitch; its direction
  is a guess.
- **Plaque 20, The White Horse.** Tom said the horse was the sign of the White Horse pub. Two sources read in
  raw text: Lancashire County Council's Red Rose Collections item EBU20150112002 (built with the surrounding
  buildings in the 1960s, closed June 2008) and CAMRA pub record 175582 (permanently closed 1 July 2008). The
  plaque says 2008 without a month. Burnley's licensing register lists the premises at 52 Burnley Road.
- **Riverbank trees:** `build_world.py` now plants a tree about every 16 m on each bank of the wide river
  lines inside the playable area, clear of roads, buildings and the water outlines (78 trees). They use their
  own random stream, so nothing else moved.

## Today's job, a first version, 11 November 2026

Ideas I148 and I149 in the Reports register, as a prototype. In `game.js` under "Today's job".

- The menu has **Today's job**: three stops picked from the date alone (`seeded('padiham ' + date)` over the
  job places sorted by name), so everyone playing the same published version gets the same job. It starts
  everyone outside the Town Hall. Adding or renaming places changes the day's job for everyone at once.
- On finishing, the result (date, seconds, par, calm or not) is kept in the browser under `padiham-daily-1`,
  best time of the day, and the menu shows **Share today's result**: the date, three squares and the time (or
  "done at a stroll" in calm), the address of the page, and nothing about the route. Phones get the share
  sheet; elsewhere it is copied.
- No streaks, no reminders, nothing sent. Checked in the preview pane by moving the figure to each stop;
  **not played through on foot, and the copy step could not be exercised in the pane.**
- Next, if it earns its place: the route drawn on the map as the share card (I150) and a beat my round link
  (I151).

## Play, pace and phone controls, 11 November 2026 (evening)

Branch `feat/padiham-play`. Asked for: more to do in play, a faster run, and a game that works fully on a phone.

- **Pace.** Three paces in `game.js` (`PACE`: walk 4.6, run 9.5, sprint 14 metres a second). R, the pace button or
  the controller's A button steps through them; Shift goes one faster while held. Indoors the top pace is a run.
  Movement is taken in steps of 0.3 m so a sprint cannot pass through a wall. The bonus time (`parFor`) was
  tightened to suit. The view opens out a little at a sprint unless less movement is set.
- **Jump.** Space, JUMP on a phone, B on a controller. It changes nothing about where the player can go.
- **Say hello.** H, HI on a phone, X on a controller. The nearest passer-by within 7 m stops and waves back
  (`Pedestrians.greet` in `life.js`).
- **Litter.** 34 pieces on the pavements (22 on a phone), shown as pale green dots on the map. Walking over one
  picks it up, and it turns up elsewhere after 45 to 90 seconds.
- **Good turns.** Every third random delivery goes to "a neighbour on" a named street and pays nothing but
  thanks. Only the street is named, never a house, which keeps the no find your house decision.
- **Stamps.** Thirteen in the menu (twelve in the plain version, which has no pubs), worked out from the saved
  counts. The menu also says how far the player has gone on foot. Save key unchanged (`padiham-errands-2`), with
  new fields added; older saves load as before and their stamps are marked quietly.
- **Phone.** A thumb put down in the lower half on the stick's side becomes the thumbstick, and the view can be
  turned with the other thumb at the same time. Full screen falls back to filling the window where the browser
  cannot do it (iPhone). A layout for a phone held sideways (`max-height: 520px`). Short buzzes on Android for
  pick-ups, unless less movement is set.

Checked in the dev server at desktop, 375 by 812 and 812 by 375: no panels or controls overlap at standard text,
in the left-handed layout, or sideways at the largest text. Known and not fixed: at the largest text size in
portrait, the job card runs under Where next and the street name (it did before this change). Still untested:
a real phone, a controller, a screen reader. Frame timing cannot be judged in the preview pane.

## Practice routes, a first cut, 11 November 2026 (night)

Branch `feat/padiham-practice-route`. Ideas I057 (route rehearsal) and I067 (supporter sets the route), without
the photographs of I069.

- **The link.** `?route=114_182.230_217.34_136`: a start and up to eleven markers, each a pair of whole metres
  on the game's grid, checked against a strict pattern in `game.js` (`ROUTE`). No free text, on purpose, so a
  link cannot put words on the screen. Points are held inside the play area.
- **The mode.** Always calm and plain. No cash panel, plaques, litter, pigeons, stamps, pubs, selfie taker or
  today's job; walk and run only. One marker at a time, named from the nearest place or street (`spotName`),
  with the rest of the route as a dotted line on the map. "Walk it again" returns to the start. The count of
  finished walks is kept in the browser under `padiham-route-<route>`. The ordinary save is not changed.
- **The maker.** In the menu of the ordinary game: Add where I am standing, Start again, Copy the link.
- **What the screen says.** The start box states that it does not teach road safety and that the game has no
  traffic. Keep that wording unless traffic and crossings are built and tested.
- **Not done.** Photographs at decision points, any trial with learners, routes beyond the town centre. After a
  route is finished the map and street name stop updating until Walk it again is pressed.

The article `src/content/news/the-padiham-job-what-it-does-and-who-it-could-help.md` describes the game and
this use. It quotes no research: the council-uses note in Reports was read through summaries only.
