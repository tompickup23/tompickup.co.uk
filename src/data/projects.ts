/* Single source of truth for the projects list.
 *
 * The Projects page and the footer's Projects column both read this, so the two
 * cannot drift. Before this existed the footer listed a section the page did not
 * and vice versa.
 *
 * Every project carries a `figure`. That is not decoration: these are statistical
 * publications, and the first thing anyone checking one wants to know is what it
 * covers. The figure is the scope the project itself leads with, and `source`
 * names the primary record it is built from, because provenance is the whole
 * claim these projects make.
 *
 * House style: no em-dashes anywhere user-visible. The build gate greps dist/.
 */

export interface Project {
  href: string;
  title: string;
  /** One terse line: the subject, then what it holds. No claim wider than the
      project's own coverage page will support. */
  desc: string;
  /** The scope this project leads with. Set large, in tabular figures. */
  figure: string;
  /** What the figure counts. Reads as a continuation of it. */
  unit: string;
  /** The primary record behind the numbers. */
  source: string;
  accent: string;
  /** External sites open in a new tab and get the arrow glyph. */
  external?: boolean;
  /** Shown in the footer's Projects column. */
  inFooter?: boolean;
}

/* Built into this site: part of the councillor work, not a separate publication.
   The Burnley spending explorer used to live here too. It was retired on
   11 Sept 2026: the same payments are on AI DOGE at /councils/burnley/, checked
   by hand against the council's source files, so keeping a second copy here only
   split the record in two. */
export const ON_THIS_SITE: Project[] = [
  {
    href: '/lgr/',
    accent: '#ff9f0a',
    title: "Lancashire's new councils",
    desc: 'Fifteen councils into four: who merges, the budget, the need each one inherits.',
    figure: '15→4',
    unit: 'councils by 2028',
    source: 'MHCLG decision, LCC budget books, Contracts Finder',
    inFooter: true,
  },
];

/* Separate publications, each on its own domain, each with its own method and
   corrections route. Figures are the ones those sites lead with. */
export const DATA_PROJECTS: Project[] = [
  {
    href: 'https://aidoge.co.uk',
    external: true,
    accent: '#ffd60a',
    title: 'AI DOGE',
    desc: 'Public spending, transaction by transaction: councils, police, fire, Whitehall.',
    figure: '22m',
    unit: 'spending transactions',
    source: 'The files those bodies publish themselves',
    inFooter: true,
  },
  {
    href: 'https://ukdemographics.co.uk',
    external: true,
    accent: '#bf5af2',
    title: 'UK Demographics',
    desc: 'Population: ethnicity, schools, housing, health and tenure, by area and seat.',
    figure: '318',
    unit: 'local authorities',
    source: 'ONS Census 2021, DWP Stat-Xplore, DfE School Census',
    inFooter: true,
  },
  {
    href: 'https://ukelections.co.uk',
    external: true,
    accent: '#30d158',
    title: 'UK Elections',
    desc: 'Every seat, every English council: forecast, then graded on the result.',
    figure: '650',
    unit: 'constituencies',
    source: 'Named pollsters, declared results, Democracy Club',
    inFooter: true,
  },
  {
    href: 'https://asylumstats.co.uk',
    external: true,
    accent: '#64d2ff',
    title: 'Asylum Stats',
    desc: 'Asylum accommodation: the cost, the contracts, the companies paid.',
    figure: '£2.1bn',
    unit: 'on hotels in 2024/25',
    source: 'Home Office statistics, Companies House, council evidence',
    inFooter: true,
  },
];

export const ALL_PROJECTS: Project[] = [...DATA_PROJECTS, ...ON_THIS_SITE];
export const FOOTER_PROJECTS: Project[] = ALL_PROJECTS.filter((p) => p.inFooter);
