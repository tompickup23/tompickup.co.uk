import fs from 'node:fs';
import path from 'node:path';

/* Stocks Massey figures for /stocks-massey/. Every number on the page comes from
   the two CSVs in public/data/stocks-massey/, which are byte-identical to
   allocations.csv and allocation-summary.csv in the Zenodo deposit
   (doi:10.5281/zenodo.23124513). Change them only by publishing a new version
   there first. The CSVs are read at build time; nothing is fetched in the browser. */

const DATA_DIR = path.join(process.cwd(), 'public', 'data', 'stocks-massey');
export const ALLOCATIONS_CSV = '/data/stocks-massey/allocations-2023-24-to-2026-27.csv';
export const SUMMARY_CSV = '/data/stocks-massey/allocation-summary-2023-24-to-2026-27.csv';

function parseCsv(text: string): Record<string, string>[] {
  const rows: string[][] = [];
  let row: string[] = [], field = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') quoted = false;
      else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      row.push(field); field = '';
      if (row.some((v) => v !== '')) rows.push(row);
      row = [];
    } else field += c;
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row); }
  const [head, ...body] = rows;
  return body.map((r) => Object.fromEntries(head.map((h, i) => [h, r[i] ?? ''])));
}

const read = (file: string) => parseCsv(fs.readFileSync(path.join(DATA_DIR, path.basename(file)), 'utf-8'));
const allocationRows = read(ALLOCATIONS_CSV);
const summaryRows = read(SUMMARY_CSV);

/* Fixed order and colour, so a category keeps its colour on every chart. The
   four grant categories use the site's validated chart palette; scholarships
   are grey because they sit outside the Appendix A schedule and are chosen by a
   separate process. */
export const CATEGORIES = [
  { key: 'LCC', label: 'County council projects', short: 'County', colour: 'var(--chart-1)' },
  { key: 'BBC', label: 'Borough council projects', short: 'Borough', colour: 'var(--chart-2)' },
  { key: 'Mechanics', label: 'Mechanics Theatre', short: 'Mechanics', colour: 'var(--chart-3)' },
  { key: 'Voluntary', label: 'Individuals and voluntary groups', short: 'Voluntary', colour: 'var(--chart-4)' },
  { key: 'Scholarships', label: 'Student scholarships', short: 'Scholarships', colour: 'var(--chart-neutral)' },
] as const;

export type CategoryKey = (typeof CATEGORIES)[number]['key'];

export interface AllocationYear {
  year: string;
  approvalDate: string;
  approvedBy: string;
  amounts: Record<CategoryKey, number>;
  total: number;
  funded: number;
  notFunded: number;
}

const APPROVER: Record<string, string> = {
  '2023/24': 'Cabinet', '2024/25': 'Cabinet', '2025/26': 'Cabinet', '2026/27': 'Director of Finance',
};

export const allocationYears: AllocationYear[] = summaryRows.map((s) => {
  const rows = allocationRows.filter((r) => r.financial_year === s.financial_year);
  const sum = (key: string) => rows.filter((r) => r.category === key).reduce((t, r) => t + Number(r.allocation_gbp), 0);
  const amounts = {
    LCC: sum('LCC'), BBC: sum('BBC'), Mechanics: sum('Mechanics'), Voluntary: sum('Voluntary'),
    Scholarships: Number(s.scholarship_category_gbp),
  };
  const total = Object.values(amounts).reduce((a, b) => a + b, 0);
  // The page must agree with the deposit's own summary, or the build stops.
  if (total !== Number(s.reported_total_gbp)) throw new Error(`Stocks Massey ${s.financial_year}: ${total} != ${s.reported_total_gbp}`);
  return {
    year: s.financial_year,
    approvalDate: s.approval_date,
    approvedBy: APPROVER[s.financial_year] ?? '',
    amounts,
    total,
    funded: Number(s.positive_applicant_rows),
    notFunded: Number(s.zero_applicant_rows),
  };
});

export const gbp = (n: number) => '£' + n.toLocaleString('en-GB');

/* One gift, three figures. Not reconciled: each measures something different. */
export const bequestFigures = [
  {
    value: '£135,000',
    measure: 'The gift, as later council histories give it',
    source: 'Burnley Borough Council, Stocks Massey Bequest (undated)',
    url: 'https://burnley.gov.uk/council-democracy/stocks-massey/',
  },
  {
    value: '£123,563',
    measure: 'Gross value of the estate, as reported in 1910',
    source: 'Auckland Star, 30 April 1910, p. 15',
    url: 'https://paperspast.natlib.govt.nz/newspapers/AS19100430.2.93',
  },
  {
    value: 'about £90,000',
    measure: 'Left for Burnley after his widow and other legacies, in the same report',
    source: 'Auckland Star, 30 April 1910, p. 15',
    url: 'https://paperspast.natlib.govt.nz/newspapers/AS19100430.2.93',
  },
];

/* The kind of evidence behind each date, as classified in the working paper's
   source register. Shape and colour both carry the kind, so it never rests on
   colour alone. */
export const EVIDENCE = {
  record: { label: 'Official or institutional record', shape: 'circle', colour: 'var(--chart-1)' },
  report: { label: 'Report at the time', shape: 'square', colour: 'var(--chart-2)' },
  catalogue: { label: 'Archive catalogue entry; original not yet read', shape: 'ring', colour: 'var(--chart-3)' },
  later: { label: 'Later account', shape: 'diamond', colour: 'var(--chart-4)' },
} as const;

export type EvidenceKind = keyof typeof EVIDENCE;

export interface ChronologyEvent { year: number; text: string; kind: EvidenceKind; source: string; url: string }

export const chronology: ChronologyEvent[] = [
  { year: 1850, kind: 'record', text: 'Birth registered in Burnley in the last quarter of the year.', source: 'General Register Office index, via FreeBMD', url: 'https://www.freebmd.org.uk/cgi/information.pl?cite=rlmDh59g6BZBtQnrNlfrUA&scan=1' },
  { year: 1883, kind: 'later', text: 'Becomes a partner in the family brewery.', source: 'Burnley Borough Council', url: 'https://burnley.gov.uk/council-democracy/stocks-massey/' },
  { year: 1887, kind: 'catalogue', text: 'Agrees to provide £850 for an organ at St Luke’s, Brierfield. Twenty residents guarantee to repay it with interest.', source: 'Lancashire Archives, PR3171/4/2', url: 'https://archivecat.lancashire.gov.uk/records/PARISH/120/4/2' },
  { year: 1909, kind: 'record', text: 'Buried at Burnley Cemetery on 31 December.', source: 'Burnley Cemetery register, transcribed by Lancashire Online Parish Clerks', url: 'https://lan-opc.org.uk/Burnley/Burnley/cemetery/graves_11000-11999.html' },
  { year: 1910, kind: 'record', text: 'Will proved on 3 March.', source: 'Charity Commission register', url: 'https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/526516/governing-document' },
  { year: 1910, kind: 'report', text: 'Newspaper reports a gross estate of £123,563, about £90,000 for Burnley, and the pub-licence condition.', source: 'Auckland Star, 30 April 1910', url: 'https://paperspast.natlib.govt.nz/newspapers/AS19100430.2.93' },
  { year: 1921, kind: 'later', text: 'Bequest income reported as becoming available to Towneley’s gallery.', source: 'Public Catalogue Foundation, 2012', url: 'https://www.carc.ox.ac.uk/news%20archive/Your%20Paintings%20Press%20Release%20December%2013th%202012.pdf' },
  { year: 1923, kind: 'later', text: 'The Edward Stocks Massey Gallery opens at Towneley in April, at first for watercolours.', source: 'Tony Kitto, Changes to Towneley Hall after 1902', url: 'https://tonykitto.github.io/collections/atoz/changes.html' },
  { year: 1924, kind: 'later', text: 'The music library is formed with a grant from the bequest.', source: 'Karen Attar, Directory of Rare Book and Special Collections, 2016', url: 'https://api.pageplace.de/preview/DT0400.9781783301485_A29002696/preview-9781783301485_A29002696.pdf' },
  { year: 1929, kind: 'catalogue', text: 'Opening programme for the Stocks Massey Music Pavilion in Towneley Park, dated Sunday 30 June.', source: 'Burnley Library catalogue, T65/BUR', url: 'https://archivecat.lancashire.gov.uk/books/3b640c2d-1852-4ec1-98d6-a7be8795a6e3' },
  { year: 1939, kind: 'record', text: 'Zoffany’s painting of Charles Townley and his friends is bought for Towneley with Art Fund support. The bequest is named as a co-funder.', source: 'Art Fund; National Heritage Memorial Fund', url: 'https://www.artfund.org/our-purpose/art-funded-by-you/charles-towneley-and-his-friends-in-the-park-street-gallery-westminster' },
  { year: 1975, kind: 'catalogue', text: 'The catalogued Joint Committee minutes begin on 24 November.', source: 'Lancashire Archives, LCC/7/7/1', url: 'https://archivecat.lancashire.gov.uk/records/LCC/DEM/DM/CCM/4/10' },
  { year: 1977, kind: 'record', text: 'Charity Commission scheme dated 13 September.', source: 'Charity Commission register', url: 'https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/526516/governing-document' },
  { year: 1989, kind: 'later', text: 'The Burnley Mechanics Institution Trust Fund is brought into the bequest’s administration.', source: 'Trustees’ report and accounts, 2025', url: 'https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/526516/accounts-and-annual-returns' },
  { year: 2008, kind: 'record', text: 'Nollekens’s bust of Charles Townley is bought with Art Fund support. The bequest is among the contributors.', source: 'Art Fund; National Heritage Memorial Fund', url: 'https://www.artfund.org/our-purpose/art-funded-by-you/bust-of-charles-townley-1' },
  { year: 2026, kind: 'record', text: 'The music library opens in a refurbished room at Burnley Central Library on 2 February.', source: 'Lancashire County Council', url: 'https://news.lancashire.gov.uk/news/burnley-library-hits-the-right-note-with-stocks-massey-music-library' },
];

export const CHRONO_START = 1850;
export const CHRONO_END = 2026;

/* Place each date on a true time scale. Dates closer than MIN_GAP years share
   no lane, so clustered years (1909 and 1910, the 1920s) stack instead of
   overlapping. */
export function chronologyLanes(events: ChronologyEvent[], minGap = 6) {
  const laneEnds: number[] = [];
  return events.map((e, i) => {
    let lane = laneEnds.findIndex((end) => e.year - end >= minGap);
    if (lane === -1) { lane = laneEnds.length; laneEnds.push(e.year); } else laneEnds[lane] = e.year;
    return { ...e, index: i, lane, x: ((e.year - CHRONO_START) / (CHRONO_END - CHRONO_START)) * 100 };
  });
}
