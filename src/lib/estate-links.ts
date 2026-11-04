/* Links to the estate's own sites keep noopener (new-tab safety) but drop
 * noreferrer so those visits arrive as referrals on the sister sites, not as
 * direct. Every other external host still gets both. */

const ESTATE_HOSTS = new Set([
  'aidoge.co.uk',
  'www.aidoge.co.uk',
  'ukdemographics.co.uk',
  'www.ukdemographics.co.uk',
  'ukelections.co.uk',
  'www.ukelections.co.uk',
  'asylumstats.co.uk',
  'www.asylumstats.co.uk',
  'ukcouncils.co.uk',
  'www.ukcouncils.co.uk',
  'ukplaces.co.uk',
  'www.ukplaces.co.uk',
  'ukfoodhygiene.co.uk',
  'www.ukfoodhygiene.co.uk',
  'ukschoolholidaydates.co.uk',
  'www.ukschoolholidaydates.co.uk',
  'ukcounciltax.co.uk',
  'www.ukcounciltax.co.uk',
  'reformukburnley.co.uk',
  'www.reformukburnley.co.uk',
  'reformwestlancs.co.uk',
  'www.reformwestlancs.co.uk',
  'tompickup.co.uk',
  'www.tompickup.co.uk',
]);

export function isEstateUrl(href: string): boolean {
  try {
    const host = new URL(href, 'https://tompickup.co.uk').hostname.toLowerCase();
    return ESTATE_HOSTS.has(host);
  } catch {
    return false;
  }
}

/** Rel attribute for an external link. Undefined for same-site paths. */
export function externalRel(href: string): string | undefined {
  if (!href.startsWith('http://') && !href.startsWith('https://')) return undefined;
  return isEstateUrl(href) ? 'noopener' : 'noopener noreferrer';
}
