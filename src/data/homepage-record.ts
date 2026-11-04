/* The homepage record: each footnoted claim line on /, with a last-reviewed
 * date and a permalink id. Wording stays in the consuming templates; this file
 * holds identity, the displayed figure, cites and the review stamp.
 *
 * reviewed (YYYY-MM-DD): when the line was last checked as still accurate.
 * Method: if git shows the line unchanged since its previous reviewed date,
 * set today; otherwise set the date of the line's last content commit. First
 * population (this file) reviewed every line on 2026-11-04 with no wording
 * change; git blame of the figure text last moved on 2026-09-28.
 */

export interface RecordLine {
  /** Permalink fragment on the homepage, without the leading #. */
  id: string;
  /** Short label shown with the figure. */
  label: string;
  /** The lead figure as plain text (templates apply markup). */
  value: string;
  /** ISO date the line was last reviewed. */
  reviewed: string;
  /** Source numbers matching the homepage Sources list. */
  cites: number[];
}

export const HERO_FIGURES: RecordLine[] = [
  {
    id: 'record-council-tax',
    label: 'Lowest council tax rise since 2015/16',
    value: '3.80%',
    reviewed: '2026-11-04',
    cites: [3],
  },
  {
    id: 'record-overspend',
    label: '2025/26 overspend, forecast to outturn',
    value: '£27.9M→£2.99M',
    reviewed: '2026-11-04',
    cites: [1],
  },
  {
    id: 'record-reform-seats',
    label: 'Reform UK seats at LCC, September 2026',
    value: '51/84',
    reviewed: '2026-11-04',
    cites: [4],
  },
];

export interface AchievementLine extends RecordLine {
  description: string;
  before: string;
}

export const ACHIEVEMENTS: AchievementLine[] = [
  {
    id: 'record-lowest-rise',
    label: 'Lowest council tax rise since 2015/16',
    value: '3.80%',
    reviewed: '2026-11-04',
    cites: [2, 3],
    description:
      'Every county council budget from 2016/17 to 2025/26 raised council tax by 3.99% or more, peaking at 5.99% in 2018/19. The 2026/27 budget set 3.80%.',
    before: '2025/26: 4.99%',
  },
  {
    id: 'record-overspend-closed',
    label: 'Overspend nearly eliminated',
    value: '£27.9M → £2.99M',
    reviewed: '2026-11-04',
    cites: [1],
    description:
      'At the first quarter of 2025/26 the council forecast a £27.9 million overspend. The year closed £2.99 million over on a £1.24 billion budget: an 89% reduction, and 0.24% off balance.',
    before: 'Quarter 1 forecast: £27.9M overspend',
  },
  {
    id: 'record-bonds',
    label: 'The cost of selling the bonds',
    value: '£350M',
    reviewed: '2026-11-04',
    cites: [5, 6],
    description:
      'The council has £519M invested in government and commercial bonds. Its 2026/27 treasury strategy says an immediate sale of all of them would realise a loss of some £350M.',
    before: 'Treasury Management Strategy 2026/27, para 29',
  },
];

export interface ComparisonLine extends RecordLine {
  reformLabel: string;
  reformValue: string;
  reformPercent: number;
  conLabel: string;
  conValue: string;
  conPercent: number;
  annotation: string;
}

export const SAVINGS_COMPARISON: ComparisonLine = {
  id: 'record-savings',
  label: 'Savings Delivery Rate',
  value: '98%',
  reviewed: '2026-11-04',
  cites: [7, 8],
  reformLabel: '2025/26',
  reformValue: '98%',
  reformPercent: 98,
  conLabel: '2024/25',
  conValue: '48%',
  conPercent: 48,
  annotation:
    '2025/26: savings delivered, including mitigations where the original saving was delayed (80% delivered on a lasting basis). 2024/25: savings delivered against the original budget; the report gives no figure including mitigations.',
};

/** Every dated line on the homepage record. */
export const RECORD_LINES: RecordLine[] = [
  ...HERO_FIGURES,
  ...ACHIEVEMENTS,
  SAVINGS_COMPARISON,
];

export function formatReviewed(iso: string): string {
  const [y, m, d] = iso.split('-').map(Number);
  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
  ];
  return `${d} ${months[m - 1]} ${y}`;
}
