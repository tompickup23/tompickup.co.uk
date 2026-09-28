# Archive

Files taken out of `public/` on 28 September 2026 because no published page linked
to them, so they were being deployed and served to anyone who guessed the URL.

`archive/public/` mirrors the `public/` paths exactly. To use a file again, move it
back to the same path under `public/` and link it from a page; the build's
unreferenced-media guard (`scripts/guard-unreferenced.mjs`) fails if a file under
`public/videos/`, `public/images/` or `public/data/` ships without a page using it.

What is here:

- `videos/`: MP4s, voiceover scripts and frame grabs from archived drafts.
  The Burnley attendance video, its script and frames, and the images
  `burnley-elections-all15.jpg` and `burnley-elections-worst3.jpg` are NOT kept here:
  they name individual councillors beside attendance percentages, which the house
  rules do not allow in one asset, so they cannot be reused. They remain in git
  history (last present in `public/` at commit c7e5cc9).
- `images/councillors/`: councillor photographs no page used.
- `images/share/reform-lancashire-9-months/` and loose cover images from archived drafts.
- `data/`: the roadworks, FixMyStreet, LCC highways, highways assets and traffic
  feeds. No page read them. The ETL that rebuilt them twice a day
  (`.github/workflows/data-etl.yml`) now runs only when started by hand.
- `source-backups/`: old page templates (`.bak` files and `src/_pages-backup/`), kept out of `src/`.
