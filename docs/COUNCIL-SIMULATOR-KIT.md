# Council chamber simulators: the kit, and how to add a council

Written 16 November 2026, when Budget Council (Lancashire) and Burnley Budget Council moved onto
one shared look. Read this before building a simulator for another council, or before moving
Young Lancashire Voice onto the same base.

## The four layers

| Layer | Where | What changes per council |
|---|---|---|
| Look | `src/games/chamber-kit/` (`kit.css`, `icons.js`, `sheet.js`) | Nothing. A council sets colours only, through `ui.theme` |
| Room | `src/games/ylv/chamber.js` (3D), `src/games/rooms/plans.js` (seats), `src/games/rooms/<council>.js` (walls, top table) | A seat plan in `plans.js`, and an architecture file unless County Hall's will do |
| Game | `src/games/budget-council/engine.js` and `app.js` | Nothing. The engine is seeded and reads the pack |
| Pack | `src/games/budget-council/<council>/` (`content.js`, `ui.js`) and `src/data/budget-council/<council>/` (five JSON files) | Everything a council says, counts or cites |

The page is `src/pages/budget-council/<council>/index.astro`: about 30 lines that hand the pack to
`Shell.astro` and `mount()`. Copy `burnley/index.astro`.

## What the look gives you

A dark stage the width of the page with the room behind it, held in view while the words scroll;
the agenda as numbered steps along the top; the speech set like a subtitle over the room; choices
as drawn tiles with an icon guessed from their words (`iconFor`, first match wins); a menu and
papers in sheets that rise from the bottom on a phone; where you stand as small bars on glass; a
recorded vote as a bar with the half way line; scores as rings. The menus and sheets follow the
Virtual Cat Cafe (`~/virtual-cat-cafe/site/src/pages/[venue]/index.astro`), the bold stage title
the Lancashire Budget Challenge. On a phone the room sits on top and the words follow it.

Class prefix `ck-`. Nothing in `kit.css` knows about budgets, so the youth game can use it.

## Theme

`ui.theme` sets the kit's tokens without their `--ck-` prefix. Take the colours from the room as
modelled, never from the council's logo or house style: the simulator is not the council's.

| Council | stage | stage-2 | accent | primary | From |
|---|---|---|---|---|---|
| Lancashire | `#0f1d19` | `#1b3029` | `#e2be5a` | `#2f5247` | County Hall's green leather benches and gold |
| Burnley | `#1c0d0f` | `#371418` | `#e0bd6a` | `#7a2430` | the Town Hall chamber's red walls and gold cornice |

White text sits on `primary`, so keep it dark enough for that.

## Adding a council, in order

1. **Research note** in `docs/research/<council>-budget-council-facts-<date>.md`: seats by group
   from the council's own site (with the date read), the constitution's meeting rules (budget
   meeting, amendments, recorded votes, the chair's casting vote, time limits), last year's
   Budget meeting minutes and votes, and the current budget gap. Every figure needs publisher,
   document, table or page, and date. Mark anything unresolved as a conflict, not a guess.
2. **Data** in `src/data/budget-council/<council>/`, copying Burnley's shapes:
   `chamber.json` (seats, groups with `colour` for the game only, administration, chair, priors),
   `minutes.json` (last year's recorded votes), `amendments.json` (costed parts and real
   amendments), `papers.json` (figures and rules, each with `src` and `at`), `sources.json`
   (Harvard `cite` and full `text` with URL).
3. **Words** in `src/games/budget-council/<council>/content.js` (beats, endings, debrief, role
   cards) and `ui.js` (start screen, terms, agenda, theme, labels such as `chairTitle`). Groups and
   offices only, never a named councillor; every speech is labelled as written for the simulator.
4. **Room**: add the seat plan to `ROOMS` in `plans.js` (rows of seats, arcs, where the officers
   sit) and, if the chamber is not County Hall's shape, an architecture file in
   `src/games/rooms/`. Say in the page that the seating is invented unless the council has
   supplied a plan.
5. **Walker test**: copy `scripts/budget-council-burnley.test.mjs`, point it at the new pack and
   add it to the `node --test` line in `.github/workflows/deploy.yml`. It fails on dead ends,
   unreachable endings, choices that change nothing, runs that do not replay, seats that do not
   add up, figures without a source, and dashes.
6. **Page and listing**: the page (noindex, prototype notice kept), a line in
   `src/data/projects.ts`, and links from the other versions' `startFooter`.
7. **Checks** before the PR: the walker, `npm run build`, `/design-check` at phone and desktop,
   `/present-check` on the words, `/fact-check` on every figure.

## Keep in every version

The prototype notice (`src/data/prototype-notice.ts`) and the sentence above it in `Shell.astro`;
the credit to Wayne Walsh for the idea of a virtual council meeting; "words written for the
simulator" on every speech; no accounts, no text boxes, nothing stored or sent.

## Not done yet

- Young Lancashire Voice still has its own look (`src/games/ylv/`). Moving it onto `ck-` classes
  is the next step towards one base for adult and youth versions.
- A meeting other than the Budget (an ordinary Full Council, a committee, a motion debate) needs
  the engine's beats to be lifted out of budget words; the look and the room are ready for it.
- Safari, Firefox and printing the role cards are untested on the new look.
