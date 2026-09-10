/* UK digital imprint (Elections Act 2022, Part 6).
 *
 * ON permanently, not just in a regulated period. The organic-material limb
 * applies year round to material promoting a registered party, and this site
 * carries it: the hero badge, the party affiliation on /about/, and the link to
 * the Burnley branch site in the footer. As a holder of elected office Tom is in
 * scope as promoter.
 *
 * Same field names and the same rendered sentence as the Reform UK Burnley site
 * (`src/data/site.ts` in the reform-burnley repo), so the two imprints read as
 * one house style rather than two inventions. Promoter is Tom, acting on behalf
 * of Reform UK at the party's registered office (Electoral Commission PP7931).
 * `party` is intentionally empty: promoter and party share the one address, so
 * the trailing "and by ..." clause is suppressed.
 *
 * The address is the party's, not a personal one. That is a deliberate choice
 * and it depends on the party accepting correspondence at Millbank on Tom's
 * behalf. If that ever stops being true the imprint stops being accurate, and
 * the address here has to change with it.
 */
export const imprint = {
  show: true,
  promoter: 'Tom Pickup',
  onBehalfOf: 'Reform UK',
  address: 'Millbank Tower, 21-24 Millbank, London, SW1P 4QP',
  party: '',
} as const;
