/* Single source of truth for the projects list.
 *
 * The Projects page and the footer's Projects column both read this, so the two
 * cannot drift. Before this existed the footer listed a section the page did not
 * and vice versa.
 *
 * House style: no em-dashes anywhere user-visible. The build gate greps dist/.
 */

export interface Project {
  href: string;
  title: string;
  desc: string;
  meta: string;
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
    meta: 'Includes the contracts register and property map',
    inFooter: true,
  },
  {
    href: '/doge/',
    accent: '#12b6cf',
    title: 'Council spending investigations',
    desc: 'Where the money actually goes, traced line by line through the spending files councils publish.',
    meta: 'Burnley spending 2023 to 2026',
    inFooter: true,
  },
];

/* Separate publications, each on its own domain, each with its own method and
   corrections route. Figures here are the ones those sites lead with. */
export const DATA_PROJECTS: Project[] = [
  {
    href: 'https://aidoge.co.uk',
    external: true,
    accent: '#ffd60a',
    title: 'AI DOGE',
    desc: 'What every public body in England publishes about the money it spends. Councils, police and fire bodies, searchable by supplier, category and body.',
    meta: '22 million spending transactions',
    inFooter: true,
  },
  {
    href: 'https://ukdemographics.co.uk',
    external: true,
    accent: '#bf5af2',
    title: 'UK Demographics',
    desc: 'Population data for every community: ethnic projections to 2061, schools, housing demand, health and tenure, for local authorities and constituencies alike.',
    meta: '318 local authorities, 631 constituencies',
    inFooter: true,
  },
  {
    href: 'https://ukelections.co.uk',
    external: true,
    accent: '#30d158',
    title: 'UK Elections',
    desc: 'A rolling general election forecast for every seat in the Commons, published alongside an honest backtest of every prediction it has ever made.',
    meta: '650 constituencies, forecast and backtested',
  },
  {
    href: 'https://asylumstats.co.uk',
    external: true,
    accent: '#64d2ff',
    title: 'Asylum Stats',
    desc: 'What asylum accommodation costs and who is paid for it, built from Home Office statistics, Companies House filings and council evidence.',
    meta: 'Every figure traced to its source',
  },
];

export const ALL_PROJECTS: Project[] = [...ON_THIS_SITE, ...DATA_PROJECTS];
export const FOOTER_PROJECTS: Project[] = ALL_PROJECTS.filter((p) => p.inFooter);
