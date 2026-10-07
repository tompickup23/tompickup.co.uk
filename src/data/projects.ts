/* Single source of truth for the projects list.
 *
 * The Projects page and the footer's Projects column both read this, so the two
 * cannot drift. Before this existed the footer listed a section the page did not
 * and vice versa.
 *
 * Data projects name their sources and may carry a verified scope figure.
 * Other websites use a description without a statistic.
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
  figure?: string;
  /** What the figure counts. Reads as a continuation of it. */
  unit?: string;
  /** The primary record behind the numbers. */
  source?: string;
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
    href: '/stocks-massey/',
    accent: '#d9bd83',
    title: 'Stocks Massey',
    desc: 'The history of the Edward Stocks Massey Bequest Fund and its continuing support for education, music and the arts in Burnley.',
    inFooter: true,
  },
  {
    href: '/lgr/',
    accent: '#ff9f0a',
    title: "Lancashire's new councils",
    desc: 'An interactive model of Lancashire’s council reorganisation, exploring the budgets, needs and obligations of the four councils the government chose in July 2026, a plan paused in September.',
    figure: '15→4',
    unit: 'councils in the July proposal',
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
    desc: 'Searchable public spending records showing who public bodies pay, how much and what the published files reveal.',
    figure: '22m',
    unit: 'spending transactions',
    source: 'The files those bodies publish themselves',
    inFooter: false,
  },
  {
    href: 'https://ukdemographics.co.uk',
    external: true,
    accent: '#bf5af2',
    title: 'UK Demographics',
    desc: 'Population data and projections alongside local statistics on housing, schools, health and living conditions.',
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
    desc: 'Election results, forecasts and council control, with transparent methods and a record of how predictions performed.',
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
    desc: 'Official asylum statistics, accommodation costs and contractor records, organised nationally and by local area.',
    figure: '£2.1bn',
    unit: 'on hotels in 2024/25',
    source: 'Home Office statistics, Companies House, council evidence',
    inFooter: false,
  },
  {
    href: 'https://ukcouncils.co.uk',
    external: true,
    accent: '#8cc4ed',
    title: 'UK Councils',
    desc: 'Council tax by band and year, with links to local election results, public spending and other council information.',
    source: 'Official council tax tables and linked local records',
    inFooter: true,
  },
  {
    href: 'https://ukplaces.co.uk',
    external: true,
    accent: '#f5c77e',
    title: 'UK Places',
    desc: 'A postcode and place directory bringing together local spending, demographics, elections and asylum data.',
    source: 'ONS geography and linked specialist data publications',
    inFooter: true,
  },
  {
    href: 'https://ukfoodhygiene.co.uk',
    external: true,
    accent: '#9bdaab',
    title: 'UK Food Hygiene',
    desc: 'Food hygiene ratings by business and area, drawn from Food Standards Agency records.',
    source: 'Food Standards Agency food hygiene ratings',
    inFooter: true,
  },
  {
    href: 'https://ukschoolholidaydates.co.uk',
    external: true,
    accent: '#ffb69e',
    title: 'UK School Holiday Dates',
    desc: 'School term dates, half terms and holidays by local authority, with source links and downloadable calendars.',
    source: 'Local authorities’ published term dates',
    inFooter: true,
  },
];

export const WEBSITE_PROJECTS: Project[] = [
  {
    href: 'https://reformukburnley.co.uk',
    external: true,
    accent: '#12b6cf',
    title: 'Reform UK Burnley',
    desc: 'The Burnley and Padiham branch website, bringing together councillor profiles, local news, priorities and ways to get involved.',
    inFooter: false,
  },
];

export const ALL_PROJECTS: Project[] = [...DATA_PROJECTS, ...ON_THIS_SITE, ...WEBSITE_PROJECTS];
export const FOOTER_PROJECTS: Project[] = ALL_PROJECTS.filter((p) => p.inFooter);

/** Portfolio categories also drive the page's jump navigation. */
function projectsNamed(...titles: string[]): Project[] {
  return titles.map(title => {
    const project = ALL_PROJECTS.find(project => project.title === title);
    if (!project) throw new Error(`Unknown portfolio project: ${title}`);
    return project;
  });
}

export const PROJECT_GROUPS = [
  { id: 'public-data', title: 'Public money & democracy', shortTitle: 'Public data', description: 'Follow the money, understand the decisions and check the record.', projects: projectsNamed('AI DOGE', 'UK Councils', 'UK Elections', 'Asylum Stats') },
  { id: 'everyday-tools', title: 'Places & everyday life', shortTitle: 'Everyday tools', description: 'Find your area, understand its population and look up practical local information.', projects: projectsNamed('UK Places', 'UK Demographics', 'UK Food Hygiene', 'UK School Holiday Dates') },
  { id: 'burnley', title: 'Burnley', shortTitle: 'Burnley', description: 'Local history, charitable work, politics and investigations into the life of the town.', projects: projectsNamed('Stocks Massey', 'Reform UK Burnley') },
  { id: 'lancashire', title: 'Lancashire', shortTitle: 'Lancashire', description: 'The county’s new councils, public services and the decisions shaping its future.', projects: projectsNamed("Lancashire's new councils") },
];

/** Related reading is labelled as such, rather than presented as a launch article. */
export const PROJECT_DETAILS: Record<string, {
  mark: string;
  subject: string;
  article?: { slug: string; label: string };
  section?: { href: string; label: string };
}> = {
  'Stocks Massey': { mark: 'SM', subject: 'History & charitable legacy', section: { href: '/stocks-massey/#articles', label: 'Read the award articles' } },
  'AI DOGE': { mark: 'DG', subject: 'Public spending', article: { slug: 'where-burnley-councils-money-goes', label: 'Where Burnley Council’s money goes' }, section: { href: '/aidoge/', label: 'How AI DOGE works' } },
  'UK Councils': { mark: 'UC', subject: 'Local government' },
  'UK Elections': { mark: 'UE', subject: 'Elections & forecasts' },
  'Asylum Stats': { mark: 'AS', subject: 'Asylum & accommodation', article: { slug: 'temporary-accommodation-asylum-burnley', label: 'Burnley’s temporary accommodation bill' } },
  'UK Places': { mark: 'UP', subject: 'Local data directory' },
  'UK Demographics': { mark: 'UD', subject: 'Population & society', article: { slug: 'the-burnley-trap', label: 'Health, work and deprivation in Burnley' } },
  'UK Food Hygiene': { mark: 'FH', subject: 'Food hygiene ratings' },
  'UK School Holiday Dates': { mark: 'SH', subject: 'School calendars' },
  "Lancashire's new councils": { mark: 'LC', subject: 'Interactive council model', article: { slug: 'lancashire-four-unitaries-model', label: 'Read the introduction to the model' }, section: { href: '/lgr/contracts/', label: 'Explore the contracts register' } },
  'Reform UK Burnley': { mark: 'RB', subject: 'Political website' },
};

export const LOCAL_READING: Record<string, string[]> = {
  burnley: ['where-burnley-councils-money-goes', 'who-owns-burnley', 'the-burnley-trap'],
  lancashire: ['two-wind-farms-one-view', 'lytham-supported-living', 'lancashire-crime-divide'],
};
