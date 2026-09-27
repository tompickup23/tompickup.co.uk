# Light Foundation debates

Researched 27 September 2026. Three retrospective articles added locally under the
Public debates topic. At Tom's request, the displayed article dates were subsequently
set close to the events: Preston 6 October 2025 (Blog Preston report date),
Burnley 14 November 2025 (day after the debate), and Blackburn 5 April 2026
(Lancashire Telegraph report date). These are editorially assigned archive dates;
the articles were created locally on 27 September 2026, not verified as published
on those earlier dates. Event dates remain explicit in the opening paragraphs.
Publication authorised by Tom on 27 September 2026 after a factual and political wording review. See docs/releases/2026-09-27-public-debates.md for the release checks.

## Verified event details

| Event | Date | Venue | Actual panel besides Tom |
| --- | --- | --- | --- |
| Is Hostile Politics Endangering Refugees? | 2 October 2025 | Preston Quaker Meeting House, St George's Road | Adnan Hussain MP; Matthew Brown |
| Is Britain Better Off Because of Immigration? | 13 November 2025 | Burnley Boys and Girls Club, Barden Playing Fields, Barden Lane | Adnan Hussain MP; Gordon Birtwistle |
| Blackburn in Question: Community or Parallel Lives? | 27 March 2026 | Wesley Hall Methodist Church, Feilden Street | Adnan Hussain MP; Elaine Whittingham |

The articles link their primary sources and contemporary coverage. Organiser reports
were read on lightfoundation.org.uk; the attached posters were visually inspected in
the browser for event dates and venues. The organiser's WordPress publication dates
alone were not treated as proof of the event dates.

## Source discrepancies resolved

- Preston's original poster names Simon Evans. Blog Preston's 6 October 2025 report
  confirms Tom stood in for him. Use the actual panel, not the poster's panel.
- Blackburn's poster names Phil Riley and Tommy Temperley. Light Foundation's
  retrospective report names Tom and Elaine Whittingham. The Lancashire Telegraph's
  5 April 2026 report confirms the substitutions and the approximately 80 attendance.
- Elaine Whittingham was Independent at the event, formerly Labour. Both reports
  confirm this; Blackburn council's 26 March 2026 minutes also record her resignation
  from Labour: https://blackburn.moderngov.co.uk/documents/s33778/2026%2003%2026%20Cl%20Forum%20Mins.pdf

## Photos and remaining material

- Preston: eight AirDropped photos received on 27 September 2026, with event
  assignment confirmed by Tom. Tom subsequently selected one lead image and three gallery images, converted to
  WebP without cropping; originals remain in Downloads. Source mapping is recorded in
  preston-debate-photos.json. Tom confirmed the photographs are from the Light Foundation press release;
  the displayed credit reflects that source. Blackburn now has a lead image and five gallery photos selected from ten supplied
  images; source mapping is in blackburn-debate-photos.json. Burnley now has five photographs and the original event poster, received from Tom; source mapping is in burnley-debate-photos.json.
- No borrowed press or organiser photos were copied into the site. No fabricated
  photo paths or stock substitutes were added.
- Preston and Blackburn press reports were read in the browser after the web fetch
  tool failed. Facebook returned a temporary-block screen through web fetch.
- No contemporary Burnley press report was located in this pass. Its article uses
  the organiser's report and poster and makes no claims about individual speeches.
- Final editorial review removed individual speech paraphrases and broad consensus wording.
  The articles record participation, topics and public questions without attributing policy
  positions, inventing quotations or naming a debate winner.

## Files

- src/content/news/preston-light-foundation-debate.md
- src/content/news/burnley-light-foundation-immigration-debate.md
- src/content/news/blackburn-light-foundation-debate.md
- src/lib/news-categories.ts: Public debates topic and classification

Existing unrelated working-tree changes were present before this task and preserved.

## Public debates category page

The dedicated category page is /news/public-debates/, linked from the news index
and the category label on debate articles. It automatically includes non-draft
articles classified under Public debates, across all geographic areas.

For future LCC, Combined Authority, Reform or other speech/appearance articles,
use subcategory: "Public debates" and an appropriate geographic category. Keep
each event as its own article with the event date in the text and accurate source
links. YouTube links or embeds can be added when the actual videos are supplied;
no recordings or placeholder video entries have been invented.
