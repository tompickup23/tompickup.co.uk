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
