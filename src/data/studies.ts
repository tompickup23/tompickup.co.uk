/* Deposited papers, newest first. One entry per Zenodo record.

   Title, date and version are as recorded on Zenodo. The summary is a plain
   description of what each paper does and carries no figures: readers who want
   the numbers follow the DOI to the paper, which is checked against its own
   reproduce.py output. The interest line restates the paper's own declaration. */

export interface Study {
  doi: string;
  title: string;
  date: string; // ISO, the Zenodo publication date
  version: string;
  summary: string;
  interest: string;
  article: { href: string; label: string };
}

export const STUDIES: Study[] = [
  {
    doi: '10.5281/zenodo.23138011',
    title: 'A reproducible audit of a cumulative zone of theoretical visibility with open data: Scout Moor II and Calderdale Energy Park',
    date: '2026-10-04',
    version: '1.0',
    summary:
      'Builds the area from which both proposed South Pennines wind farms could be seen, entirely from open data, and checks it against the applicant’s own combined map and software. It then tests the result against the Environment Agency’s laser-scanned surface, which includes buildings and trees, and reports what went against the author, including the errors in the first-round objection and their correction.',
    interest:
      'The author objected to the Scout Moor II applications in a personal capacity and as county councillor. The paper describes a method and its checks and takes no position on the decision.',
    article: { href: '/news/scout-moor-ii-round-two/', label: 'Scout Moor II is cut to 12 turbines' },
  },
  {
    doi: '10.5281/zenodo.23127077',
    title: 'Waiting for Vesting Day: What the Pause in Lancashire’s Reorganisation Costs, Through Which Channels, and to Whom',
    date: '2026-10-03',
    version: '1.0',
    summary:
      'Asks what the September 2026 pause in Lancashire’s reorganisation, and a possible slip of vesting day, costs and who bears it. Three scenarios are tested across eight channels, from the fifteen councils’ budgets and contract registers to deferred savings and the May 2027 elections, under a rule that every figure is read from a published document or computed from one with the parameter shown.',
    interest:
      'The author is a Lancashire County Council Cabinet member and sat on the Cabinet that took the 1 October 2026 decision the paper describes.',
    article: { href: '/news/lgr-delay-year-did-not-exist/', label: 'The year the pause handed back to Lancashire’s councils' },
  },
  {
    doi: '10.5281/zenodo.23124513',
    title: 'The Price of a Civic Legacy: Edward Stocks Massey, brewing wealth and the making of public culture in Burnley',
    date: '2026-10-03',
    version: '1.0',
    summary:
      'Examines how a Burnley brewer’s benefaction became a continuing public resource. It separates the donor’s reported intentions from later allocation decisions, payments and public benefit, drawing on contemporary newspaper reporting, museum and library records, recipient testimony and four transcribed allocation schedules.',
    interest:
      'The author chairs the Edward Stocks Massey Bequest Fund Joint Advisory Committee. The paper is personal research and claims no endorsement by the committee, the trustees or either council.',
    article: { href: '/stocks-massey/', label: 'Stocks Massey: a legacy for Burnley' },
  },
  {
    doi: '10.5281/zenodo.23016761',
    title: 'The Financial Case for Local Government Reorganisation in Lancashire: A Transparent Component Model of Five Structural Proposals, with a Postscript on the 2026 Decisions',
    date: '2026-09-28',
    version: '3.1',
    summary:
      'Lancashire’s fifteen councils put forward five competing proposals in November 2025 to replace the two-tier system with between two and five unitary councils. The paper sets out a fully disclosed model of the recurring structural costs and savings of each configuration, built from the councils’ own outturn returns and published pay, contracts, unit-cost and population data, and reports the specifications not chosen beside the central case.',
    interest:
      'The author is a Lancashire County Council Cabinet member; the county council proposed two unitaries.',
    article: { href: '/lgr/#paper', label: 'The paused plan for Lancashire’s new councils' },
  },
];
