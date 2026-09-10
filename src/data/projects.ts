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
  /** Shown in the footer's Projects column, which has room for four. */
  inFooter?: boolean;
}

/* Built into this site: part of the councillor work, not separate publications. */
export const ON_THIS_SITE: Project[] = [
  {
    href: '/lgr/',
    accent: '#ff9f0a',
    title: "Lancashire's new councils",
    desc: 'Fifteen councils become four in 2028. An interactive model of each new authority: who merges, the budget it runs, the need it has to meet.',
    figure: '15→4',
    unit: 'councils by 2028',
    source: 'MHCLG decision, LCC budget books, Contracts Finder',
    inFooter: true,
  },
  {
    href: '/doge/',
    accent: '#12b6cf',
    title: 'Council spending investigations',
    desc: 'Where the money actually goes, traced line by line through the spending files councils publish. Includes the contracts register and property map.',
    figure: '2023-26',
    unit: 'of Burnley spending',
    source: "Councils' own published transparency files",
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
    desc: 'What every public body in England publishes about the money it spends. Councils, police and fire bodies, searchable by supplier, category and body.',
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
    desc: 'Population data for every community: ethnic projections to 2061, schools, housing demand, health and tenure, for local authorities and constituencies alike.',
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
    desc: 'A rolling general election forecast for every seat in the Commons, published alongside an honest backtest of every prediction it has ever made.',
    figure: '650',
    unit: 'constituencies',
    source: 'Published polling and the 2024 result',
  },
  {
    href: 'https://asylumstats.co.uk',
    external: true,
    accent: '#64d2ff',
    title: 'Asylum Stats',
    desc: 'What asylum accommodation costs and who is paid for it, built from Home Office statistics, Companies House filings and council evidence.',
    figure: '£2.1bn',
    unit: 'on hotels in 2024/25',
    source: 'Home Office statistics, Companies House, council evidence',
  },
];

export const ALL_PROJECTS: Project[] = [...ON_THIS_SITE, ...DATA_PROJECTS];
export const FOOTER_PROJECTS: Project[] = ALL_PROJECTS.filter((p) => p.inFooter);
