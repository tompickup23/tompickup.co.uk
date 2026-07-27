"""Matching rules for the observatory's verified-website layer.

The rule is deliberately narrow. A candidate site is only accepted when the
site itself proves the match:

  crn            the company's registration number appears on the page, in a
                 form a UK company would use to comply with the Companies
                 (Trading Disclosures) Regulations
  name-postcode  the exact registered company name AND the registered-office
                 postcode both appear on the same page

Anything else is rejected however plausible it looks. False negatives are
expected and fine. A false positive puts a wrong website on a named company's
public dossier, which is a publishable error, so the bar sits here.

No external dependencies beyond the stdlib so the rules can be unit-checked
anywhere. House style: no em-dashes in any output text.
"""
import html
import re
import unicodedata

# --- name handling ---------------------------------------------------------

# Legal-form suffixes stripped before domain guessing and compared loosely
# when matching a registered name on a page.
SUFFIX_ALTS = {
    "LIMITED": r"(?:limited|ltd\.?)",
    "LTD": r"(?:limited|ltd\.?)",
    "PLC": r"(?:p\.?l\.?c\.?|public limited company)",
    "LLP": r"(?:l\.?l\.?p\.?)",
    "CIC": r"(?:c\.?i\.?c\.?|community interest company)",
    "LP": r"(?:l\.?p\.?)",
    "CIO": r"(?:c\.?i\.?o\.?)",
}
STRIP_SUFFIXES = [
    "LIMITED", "LTD", "PLC", "LLP", "LP", "CIC", "CIO", "L.L.P.", "LTD.",
    "COMPANY", "CO", "THE",
]
# Words too generic to be a domain on their own.
GENERIC_FIRST_WORDS = {
    "the", "a", "uk", "new", "north", "south", "east", "west", "great",
    "british", "national", "first", "one", "my", "our", "all", "best",
    "group", "holdings", "services", "solutions", "trading", "company",
    "international", "global", "central", "royal", "united", "and",
}
# Place names are never used as a one-word domain guess. "blackpool.co.uk" is
# a town portal, not Blackpool Football Club, and a portal page can carry a
# company name and postcode without being that company's site.
PLACE_WORDS = {
    "lancashire", "blackpool", "blackburn", "burnley", "preston", "chorley",
    "lancaster", "pendle", "hyndburn", "rossendale", "fylde", "wyre",
    "accrington", "nelson", "colne", "clitheroe", "morecambe", "fleetwood",
    "ormskirk", "skelmersdale", "leyland", "darwen", "bacup", "haslingden",
    "padiham", "kirkham", "poulton", "garstang", "longridge", "lytham",
    "thornton", "carnforth", "barnoldswick", "great harwood", "manchester",
    "liverpool", "yorkshire", "cumbria", "london", "england", "britain",
    "northwest", "northern", "pennine", "ribble", "wyresdale", "bowland",
}

# Hosts that can never be a company's own website. Directories, registries,
# social networks, marketplaces and site builders on shared domains.
BLOCKED_HOST_PARTS = {
    "facebook.com", "linkedin.com", "twitter.com", "x.com", "instagram.com",
    "tiktok.com", "youtube.com", "pinterest.com", "threads.net",
    "yell.com", "yelp.com", "yelp.co.uk", "192.com", "thomsonlocal.com",
    "freeindex.co.uk", "cylex-uk.co.uk", "scoot.co.uk", "hotfrog.co.uk",
    "endole.co.uk", "opencorporates.com", "companycheck.co.uk",
    "checkcompany.co.uk", "duedil.com", "bizdb.co.uk", "companiesintheuk.co.uk",
    "globaldatabase.com", "bizzdirectory.com", "companydirectorcheck.com",
    "companies-house.uk", "company-information.service.gov.uk",
    "find-and-update.company-information.service.gov.uk", "gov.uk",
    "charitycommission.gov.uk", "register-of-charities.charitycommission.gov.uk",
    "mutuals.fca.org.uk", "gtr.ukri.org", "ukri.org",
    "wikipedia.org", "wikidata.org", "crunchbase.com", "bloomberg.com",
    "indeed.com", "glassdoor.co.uk", "totaljobs.com", "reed.co.uk",
    "trustpilot.com", "checkatrade.com", "ratedpeople.com", "mybuilder.com",
    "tripadvisor.co.uk", "tripadvisor.com", "just-eat.co.uk", "deliveroo.co.uk",
    "ubereats.com", "booking.com", "amazon.co.uk", "ebay.co.uk", "etsy.com",
    "wordpress.com", "wixsite.com", "blogspot.com", "sites.google.com",
    "weebly.com", "squarespace.com", "godaddysites.com", "business.site",
    "google.com", "bing.com", "duckduckgo.com", "archive.org",
    "gazette.co.uk", "thegazette.co.uk", "ratings.food.gov.uk", "cqc.org.uk",
    "get-information-schools.service.gov.uk", "companieslist.co.uk",
    "bizstats.co.uk", "companiesinfo.co.uk", "sic-code.co.uk",
    # News, trade press and membership bodies. These write about companies and
    # quote their addresses, which is exactly what the name-postcode rule
    # looks for, so they must never be candidates.
    "lancashiretelegraph.co.uk", "lep.co.uk", "burnleyexpress.net",
    "blackpoolgazette.co.uk", "lancasterguardian.co.uk", "pendletoday.co.uk",
    "chorley-guardian.co.uk", "wigantoday.net", "bbc.co.uk", "bbc.com",
    "insidermedia.com", "thebusinessdesk.com", "prolificnorth.co.uk",
    "lancashirebusinessview.co.uk", "businesslancashire.co.uk",
    "lancashire.gov.uk", "lancashirelep.co.uk", "boostbusinesslancashire.co.uk",
    "chamber-uk.com", "britishchambers.org.uk", "fsb.org.uk",
    "constructionenquirer.com", "theconstructionindex.co.uk",
    "companies-house.gov.uk", "nationalarchives.gov.uk",
}


def _ascii(s):
    """Fold accents so 'Cafe' matches 'Cafe' regardless of the source form."""
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()


def name_tokens(name):
    """Alphanumeric tokens of a registered name, legal suffixes removed."""
    s = _ascii(name or "").upper()
    s = s.replace("&", " AND ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t]
    while toks and toks[0] in ("THE",):
        toks = toks[1:]
    while toks and toks[-1] in STRIP_SUFFIXES:
        toks = toks[:-1]
    return toks


def domain_bases(name, extra_names=()):
    """Domain labels worth guessing for a company, best first.

    "ALTHAMS TRAVEL SERVICES LIMITED" gives althamstravelservices,
    althams-travel-services, althamstravel, althams.
    """
    bases = []

    def add(b):
        b = re.sub(r"[^a-z0-9-]", "", (b or "").lower())
        b = b.strip("-")
        if 3 <= len(b) <= 63 and b not in bases:
            bases.append(b)

    for n in [name] + list(extra_names):
        toks = name_tokens(n)
        if not toks:
            continue
        low = [t.lower() for t in toks]
        add("".join(low))
        if len(low) > 1:
            add("-".join(low))
            add("".join(low[:2]))
            # Trailing filler ("group", "holdings", "uk") is often not in the
            # domain: "sustainable building services (uk)" trades as
            # sustainablebuildinguk, "leach holdings" as leach.
            if low[-1] in TRAILING_FILLER and len(low) > 2:
                add("".join(low[:-1]))
            if len(low) > 2:
                add("".join([low[0], low[-1]]))
        if (low[0] not in GENERIC_FIRST_WORDS and low[0] not in PLACE_WORDS
                and len(low[0]) >= 5):
            add(low[0])
    return bases


TRAILING_FILLER = {"group", "holdings", "holding", "uk", "international",
                   "services", "solutions", "trading", "developments",
                   "properties", "investments", "enterprises"}

TLDS = (".co.uk", ".com", ".uk", ".org.uk")


def guess_domains(name, extra_names=(), limit=12):
    """Candidate hostnames from name permutations. Every one still has to
    pass verification, so a wrong guess costs a request, not accuracy."""
    out = []
    for b in domain_bases(name, extra_names):
        for tld in TLDS:
            host = b + tld
            if host not in out and not is_blocked_host(host):
                out.append(host)
            if len(out) >= limit:
                return out
    return out


def is_blocked_host(host):
    h = (host or "").lower().lstrip(".")
    if h.startswith("www."):
        h = h[4:]
    for part in BLOCKED_HOST_PARTS:
        if h == part or h.endswith("." + part):
            return True
    return False


# --- page text -------------------------------------------------------------

_SCRIPT_RE = re.compile(r"(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>")
_TAG_RE = re.compile(r"(?s)<[^>]+>")
_WS_RE = re.compile(r"[\s ]+")


def page_text(raw_html):
    """HTML to flat text. Deliberately crude: we only need the strings a
    company writes about itself, not a faithful DOM."""
    s = _SCRIPT_RE.sub(" ", raw_html or "")
    s = _TAG_RE.sub(" ", s)
    s = html.unescape(s)
    s = _ascii(s)
    return _WS_RE.sub(" ", s).strip()


def snippet(text, start, end, width=200):
    """Evidence snippet around a match, capped at `width` characters."""
    pad = max(0, (width - (end - start)) // 2)
    a = max(0, start - pad)
    b = min(len(text), end + pad)
    s = text[a:b].strip()
    if a > 0:
        s = "..." + s
    if b < len(text):
        s = s + "..."
    return s[:width]


# --- rule 1: registration number on the page -------------------------------

# Words a compliant UK site puts next to the number. Used for the shortened
# form of a number that has leading zeros, where the digits alone are too
# short to be self-evidently a CRN.
_CTX = (r"(?:compan(?:y|ies)|registration|registered|reg\.?|regd\.?|"
        r"incorporat\w*|no\.?|number|#)")
_CTX_RE = re.compile(_CTX, re.I)


def crn_evidence(text, crn):
    """Snippet proving the CRN is on the page, or None.

    Full form (8 characters, as Companies House issues it) is accepted on
    sight: an exact 8-digit run is not a coincidence. A zero-stripped form
    ("1417048" for "01417048") is accepted only with registration wording
    within the 40 characters before it, because a bare 6 or 7 digit run
    could be anything.
    """
    if not text or not crn:
        return None
    crn = crn.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{6,10}", crn):
        return None

    # Full form, allowing the spacing and punctuation sites insert.
    spaced = r"[\s.\-]*".join(re.escape(c) for c in crn)
    m = re.search(r"(?<![A-Z0-9])" + spaced + r"(?![A-Z0-9])", text, re.I)
    if m:
        return snippet(text, m.start(), m.end())

    # Zero-stripped numeric form, registration wording required nearby.
    if crn.isdigit():
        short = crn.lstrip("0")
        if 5 <= len(short) < len(crn):
            for m in re.finditer(r"(?<!\d)" + re.escape(short) + r"(?!\d)", text):
                before = text[max(0, m.start() - 40):m.start()]
                if _CTX_RE.search(before):
                    return snippet(text, m.start(), m.end())
    return None


# --- rule 2: exact registered name plus registered postcode ----------------

def _name_regex(name):
    """Regex for the registered name as written by a real site: any spacing
    or punctuation between words, and the standard abbreviation of the legal
    suffix ('Ltd' for 'LIMITED'). Nothing looser than that."""
    s = _ascii(name or "").upper()
    s = re.sub(r"[^A-Z0-9&]+", " ", s).strip()
    words = [w for w in s.split() if w]
    if not words:
        return None
    parts = []
    for w in words:
        if w == "&":
            parts.append(r"(?:&|and)")
        elif w in SUFFIX_ALTS:
            parts.append(SUFFIX_ALTS[w])
        else:
            parts.append(re.escape(w.lower()))
    core = [p for p in parts]
    if len(core) < 2:
        return None
    return re.compile(r"(?<![a-z0-9])" + r"[\s\W]{0,3}".join(core) +
                      r"(?![a-z0-9])", re.I)


def _postcode_regex(pc):
    n = re.sub(r"[^A-Z0-9]", "", (pc or "").upper())
    if not re.fullmatch(r"[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}", n):
        return None
    return re.compile(r"(?<![A-Z0-9])" + re.escape(n[:-3]) + r"\s*" +
                      re.escape(n[-3:]) + r"(?![A-Z0-9])", re.I)


def name_postcode_evidence(text, name, postcode):
    """Snippet proving both the registered name and the registered-office
    postcode appear on the page, or None."""
    if not text:
        return None
    nre = _name_regex(name)
    pre = _postcode_regex(postcode)
    if not nre or not pre:
        return None
    nm = nre.search(text)
    if not nm:
        return None
    pm = pre.search(text)
    if not pm:
        return None
    # Prefer one snippet covering both when they sit together (a footer).
    if abs(pm.start() - nm.start()) < 300:
        return snippet(text, min(nm.start(), pm.start()),
                       max(nm.end(), pm.end()), width=200)
    return (snippet(text, nm.start(), nm.end(), width=110) + " | " +
            snippet(text, pm.start(), pm.end(), width=80))


# Path segments every site has. None of these identifies a company, even when
# they happen to contain one of its name words ("careers" holds "care").
COMMON_PATH_SEGMENTS = {
    "home", "index", "about", "aboutus", "contact", "contactus", "careers",
    "career", "jobs", "news", "blog", "shop", "store", "services", "service",
    "products", "product", "team", "people", "privacy", "privacypolicy",
    "terms", "termsandconditions", "legal", "cookies", "sitemap", "search",
    "login", "account", "basket", "cart", "help", "support", "faq", "media",
    "gallery", "portfolio", "projects", "work", "pages", "page", "site",
    "content", "main", "default", "welcome", "landing", "enus", "engb",
    "uken", "gben", "english",
}


def path_segment_names(segment, name, extra_names=()):
    """Does this first path segment name the company?

    Some organisations live on a path of a shared host: a local Age UK sits at
    ageuk.org.uk/<place>/. Publishing the bare domain there would credit the
    national body's site to a local company. Everywhere else the bare domain
    is the right link, so this only holds on to a path that carries the
    company's own name.
    """
    seg = re.sub(r"[^a-z0-9]", "", (segment or "").lower())
    if len(seg) < 4 or seg in COMMON_PATH_SEGMENTS:
        return False
    for n in [name] + list(extra_names):
        toks = [t.lower() for t in name_tokens(n)]
        if not toks:
            continue
        flat = "".join(toks)
        # The segment has to BE the name, or a run of it. A name word merely
        # occurring inside a longer segment is not enough: "legal" appears in
        # both BB LEGAL LIMITED and every firm's /legal-regulatory-information.
        if seg == flat or seg in flat or (flat in seg and len(flat) >= 6):
            return True
        if seg in toks:
            return True
    return False


def verify(text, crn, name, postcode, allow_name_postcode=True):
    """Apply both rules. Returns (matchedOn, evidence) or (None, None).

    `allow_name_postcode=False` drops to registration number only. Callers use
    that for candidates that arrived from a web search, where the address the
    page quotes might be the company's while the page belongs to somebody else
    (a trade directory entry, a member list, a news piece). Register-published
    URLs and name-derived domain guesses do not have that problem: the first
    is the company's own declaration, the second is already its name.
    """
    ev = crn_evidence(text, crn)
    if ev:
        return "crn", ev
    if allow_name_postcode:
        ev = name_postcode_evidence(text, name, postcode)
        if ev:
            return "name-postcode", ev
    return None, None
