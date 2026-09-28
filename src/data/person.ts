/* The Person entity behind the whole site.

   One definition, emitted in full on the home page and as the mainEntity of
   /about/, and referenced by @id from every NewsArticle, so every page describes
   the same person the same way. Change it here, not in a template.

   The career domains live in `description` in the past tense on purpose:
   hasOccupation is for the roles held now. */

const PERSON_ID = 'https://tompickup.co.uk/#person';

/* Profiles still to be filled in by Tom. An empty string is dropped from sameAs,
   so nothing half-made is published. */
const TODO_PROFILES = {
  linkedin: '', // TODO(Tom): LinkedIn profile URL
  orcid: 'https://orcid.org/0009-0004-6726-0744', // as registered on the Zenodo record 23016761
  zenodo: '', // TODO(Tom): Zenodo author or community URL
  wikidata: '', // TODO(Tom): Wikidata item URL, https://www.wikidata.org/wiki/Q...
};

const LCC = {
  '@type': 'GovernmentOrganization',
  name: 'Lancashire County Council',
  url: 'https://www.lancashire.gov.uk/',
};

export const person = {
  '@type': 'Person',
  '@id': PERSON_ID,
  name: 'Tom Pickup',
  alternateName: 'Thomas Pickup',
  url: 'https://tompickup.co.uk/about/',
  image: 'https://tompickup.co.uk/images/headshot.jpg',
  jobTitle: 'Cabinet Member for Adult Social Care, Lancashire County Council',
  description:
    'County Councillor for Padiham and Burnley West on Lancashire County Council since 2025, Cabinet Member for Adult Social Care for 2026/27, ' +
    'and a parish councillor for Dunnockshaw & Clowbridge. Lead Member for Finance and Resources in 2025/26. ' +
    'Before elected office he was a consultant to businesses, helping them cut costs and raise profits through software, energy, procurement and more, ' +
    'and worked in public sector procurement at Lancashire County Council. ' +
    'He builds open public data sites and has written an independent working paper on the finances of local government reorganisation in Lancashire.',
  worksFor: LCC,
  hasOccupation: [
    {
      '@type': 'Occupation',
      name: 'County Councillor',
      description: 'Elected member for the Padiham and Burnley West division.',
      occupationLocation: { '@type': 'AdministrativeArea', name: 'Lancashire' },
    },
    {
      '@type': 'Occupation',
      name: 'Cabinet Member for Adult Social Care',
      description: 'Cabinet portfolio holder at Lancashire County Council.',
      occupationLocation: { '@type': 'AdministrativeArea', name: 'Lancashire' },
    },
    {
      '@type': 'Occupation',
      name: 'Parish Councillor',
      description: 'Member of Dunnockshaw & Clowbridge Parish Council.',
      occupationLocation: { '@type': 'AdministrativeArea', name: 'Dunnockshaw & Clowbridge, Burnley' },
    },
  ],
  memberOf: { '@type': 'PoliticalParty', name: 'Reform UK' },
  knowsAbout: [
    'Public finance',
    'Local government reorganisation',
    'Adult social care',
    'Open data',
    'Energy systems',
    'Public procurement',
    'Statistics',
    'Software engineering',
    'Lancashire local history',
  ],
  sameAs: [
    'https://council.lancashire.gov.uk/mgUserInfo.aspx?UID=33781',
    'https://github.com/tompickup23',
    'https://x.com/tompickup',
    'https://www.facebook.com/tompickupburnley',
    'https://www.instagram.com/tompickupburnley',
    ...Object.values(TODO_PROFILES),
  ].filter(Boolean),
};

/* For NewsArticle author and publisher: the full node lives on / and /about/. */
export const personRef = { '@id': PERSON_ID };

export { PERSON_ID };
