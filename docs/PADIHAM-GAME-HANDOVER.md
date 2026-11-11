# Straight Outta Padiham: handover

Written 11 November 2026 at the end of the session that built the game.

## Where it stands

- Branch `feat/straight-outta-padiham`, commit `ec7f7305` plus this note. Local only: not pushed, no pull request, not on `main`, so **not live**.
- Page: `/straight-outta-padiham/`. `npm run build` passes every guard and the built output has no em-dashes.
- Checked in the built site on desktop and at phone size in a desktop browser. Not tried on a real phone. The pub jobs were not played end to end.

## What the game is

A third-person walking game on the real street layout of Padiham town centre, from the Town Hall and Padiham Bridge up to Church Street. Seven scripted jobs, then random deliveries. Pedestrians, pigeons that scatter, Union Jack bunting on Burnley Road. The Starkie Arms and the Flying Dutchman can be entered; the Kings Arms is exterior only.

## Where things are

| What | Where |
|---|---|
| Page, markup and styles | `src/pages/straight-outta-padiham/index.astro` |
| Game loop, jobs, input, pub doors | `src/games/padiham/game.js` |
| Town generator: roads, buildings, roofs, street furniture | `src/games/padiham/city.js` |
| Facade texture atlas, signs, flags | `src/games/padiham/textures.js` |
| Pub interiors | `src/games/padiham/interior.js` |
| People and pigeons | `src/games/padiham/life.js` |
| Sound, all synthesised | `src/games/padiham/audio.js` |
| World file the game loads | `public/games/straight-outta-padiham/world.json` |
| World builder and its inputs | `scripts/padiham-game/` |
| Per-building details from photos | `scripts/padiham-game/data/overrides.json` |
| Photo credits | `scripts/padiham-game/data/photo-credits.json` |
| Share card and its script | `public/images/share/straight-outta-padiham.png`, `scripts/generate_padiham_game_card.mjs` |

To change a building, edit `overrides.json` and rebuild the world file with the command in the header of `scripts/padiham-game/build_world.py`. Do not edit `world.json` by hand.

## Sources and licences

- Roads, river, green space and named places: OpenStreetMap, Open Database Licence.
- Building outlines: OS OpenMap Local, Open Government Licence. The 103 MB download is not in the repo; `extract_os_buildings.py` says how to fetch it.
- Building details: read from photographs on Geograph and Wikimedia Commons (Creative Commons licences, listed in `photo-credits.json`) and from photographs of the Starkie Arms and Flying Dutchman supplied by Tom. No photograph is reproduced in the game.
- Google Street View was not used: Google's terms bar building models from it.

## Decisions already made

- Name: Straight Outta Padiham.
- Stylised look built on real data, not photo-realism.
- No "find your house" sharing. Sharing should centre on public places.
- Pigeons, not seagulls.
- Pubs are quizzes, pies, pool and darts. No drinking, no drink brands.
- Bar staff are generic figures, not real people.
- Shop fascias carry generic trades, not real business names, and the page says shop names and jobs are made up.

## Before it goes live

1. Play it on a real phone. The scene is heavy; parked cars may need thinning.
2. Ask the landlords of the Starkie Arms, Flying Dutchman and Kings Arms for their OK.
3. Decide whether the page is `noindex` until launch, as Lancashire Voice is. It is indexable as written.
4. Check the name is acceptable: it plays on a well-known film and album title.
5. The sticky site header covers the top strip of the game panel when scrolled. Full screen avoids it.
6. The Mill Street bank corner and the White Horse block were matched to outlines by reading photos against a plan. Confirm on the ground.

## Known gaps

- Flat ground: Padiham's hills are missing. Environment Agency LIDAR would give terrain and true building heights.
- The player is a built figure, not an animated model.
- Several pubs and shops sit inside larger merged outlines, so they are fronts on a longer row.
- Kings Arms has no interior. The Flying Dutchman beer garden is left out.
- Buildings use repeating facade tiles. Straight-on photos of each frontage would allow real textures.

## Ideas not yet built

Daily challenge with a shareable result, postcard photo mode at landmarks, hidden collectibles with local history cards, playable darts or pool, team leaderboards with preset names, other players as anonymous figures with no chat.
