// Local history plaques. One fact each, with the source it was read from (all read 11 November 2026).
// 'at' is the name of a place in the world file; 'short' is the source line shown in the game;
// 'source' and 'url' are the full reference listed on the page under the game.
// Add a plaque only with a named source, and keep to what that source says.
const HE = (n) => `https://historicengland.org.uk/listing/the-list/list-entry/${n}?section=official-list-entry`;
const CAA = 'https://burnley.gov.uk/wp-content/uploads/2022/02/Padiham-Conservation-Area-Appraisal-2020.pdf';
const LEUS = 'https://burnley.gov.uk/wp-content/uploads/2022/02/Lancashire-Extensive-Urban-Survey-Historic-Town-Assessment-Report-Padiham.pdf';

export const PLAQUES = [
  {
    id: 'town-hall', at: 'Padiham Town Hall', title: 'Padiham Town Hall',
    text: 'The Town Hall dates from 1938 and was designed by Bradshaw Gass and Hope. It was built to house Padiham Urban District Council.',
    short: 'Historic England, list entry 1237652',
    source: 'Historic England, National Heritage List for England, "Town Hall", list entry 1237652, Grade II, first listed 12 February 1985; Burnley Borough Council, Padiham Conservation Area Appraisal, June 2020, para. 4.31', url: HE(1237652),
  },
  {
    id: 'st-leonards', at: "St Leonard's", title: "St Leonard's Church",
    text: "The present St Leonard's was built between 1866 and 1869, replacing an older church on the same site.",
    short: 'Victoria County History of Lancashire, volume 6 (1911)',
    source: 'Farrer and Brownbill (eds), A History of the County of Lancaster: Volume 6 (1911), "Townships: Padiham", pp. 492 to 496, British History Online; Historic England, "Church of St Leonard", list entry 1238303', url: 'https://www.british-history.ac.uk/vch/lancs/vol6/pp492-496',
  },
  {
    id: 'bridge', at: 'Padiham Bridge', title: 'Padiham Bridge',
    text: 'A bridge over the Calder here is first recorded around 1530 and was probably older still. The town grew up around this crossing.',
    short: 'Lancashire Historic Town Survey, Padiham (2005)',
    source: 'Lancashire County Council and Egerton Lea Consultancy, Lancashire Historic Town Survey Programme: Padiham Historic Town Assessment Report, May 2005, section 4.4, p. 16', url: LEUS,
  },
  {
    id: 'helm-mill', at: 'Helm Mill', title: "Helm's Mill",
    text: "Helm's Mill is Padiham's oldest surviving mill. Its datestone reads H. Helm 1807, and it began as a cotton spinning mill.",
    short: 'Padiham Conservation Area Appraisal (2020)',
    source: 'Burnley Borough Council, Padiham Conservation Area Appraisal, June 2020, para. 5.51, p. 45; Historic England, "2 Factory Lane", list entry 1237655', url: CAA,
  },
  {
    id: 'starkie-arms', at: 'Starkie Arms', title: 'The Starkie Arms',
    text: 'The Starkie Arms, a former coaching inn, takes its name from the Starkie family of Huntroyde. It was already listed in a trade directory of 1818.',
    short: 'Padiham Conservation Area Appraisal (2020)',
    source: 'Burnley Borough Council, Padiham Conservation Area Appraisal, June 2020, para. 5.19, pp. 32 to 33; Lancashire Historic Town Survey Programme: Padiham Historic Town Assessment Report, May 2005, p. 25', url: CAA,
  },
  {
    id: 'kings-arms', at: 'Kings Arms', title: 'The Kings Arms',
    text: 'The Kings Arms dates from about 1820. It was one of nine inns and taverns listed in Padiham in 1824.',
    short: 'Padiham Conservation Area Appraisal (2020)',
    source: 'Burnley Borough Council, Padiham Conservation Area Appraisal, June 2020, para. 5.19; Lancashire Historic Town Survey Programme: Padiham Historic Town Assessment Report, May 2005, p. 25', url: CAA,
  },
  {
    id: 'black-bull', at: 'The Cellar', title: 'The Old Black Bull',
    text: 'This was the Old Black Bull inn. The building is seventeenth century, with a later front.',
    short: 'Historic England, list entry 1274563',
    source: 'Historic England, National Heritage List for England, "Cellar Restaurant", list entry 1274563, Grade II, first listed 12 February 1985', url: HE(1274563),
  },
  {
    id: 'nazareth', at: 'Nazareth Unitarian Chapel', title: 'Nazareth Unitarian Chapel',
    text: 'This chapel was designed by Virgil Anderton, a member of its own congregation. Historic England dates the building to 1872.',
    short: 'Historic England, list entry 1238377',
    source: 'Historic England, National Heritage List for England, "Nazareth Unitarian Church", list entry 1238377, Grade II, first listed 12 February 1985', url: HE(1238377),
  },
  {
    id: 'memorial-park', at: 'Padiham Memorial Park', title: 'Memorial Park',
    text: 'Memorial Park opened on 18 June 1921 in memory of the Padiham men who died in the First World War. Thomas Mawson drew up the plans.',
    short: 'Burnley Council, Memorial Park Management Plan',
    source: 'Burnley Borough Council, Memorial Park, Padiham, Management Plan 2024 to 2029 (January 2026 update), "The History of Memorial Park", pp. 6 to 7', url: 'https://burnley.gov.uk/wp-content/uploads/2026/04/Memorial-Park-Management-Plan-2024-2029-for-2026.pdf',
  },
  {
    id: 'padiham-fc', at: 'Padiham Football Club', title: 'Padiham Football Club',
    text: 'A club called Padiham FC has existed since 1878, with a break between 1916 and 1949. Its stork badge comes from the Starkie family coat of arms.',
    short: 'North West Counties Football League (2017)',
    source: 'North West Counties Football League, "Emblematically Speaking: Padiham", Stewart Taylor, 27 December 2017', url: 'https://www.nwcfl.com/news-articles.php?id=6419',
  },
];
