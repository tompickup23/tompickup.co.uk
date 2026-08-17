"""entityType derivation for the Companies House register, per DATA-INTEGRITY s2.

Two rules, in this order, and the order is the whole point:

1. **Prefix first.** The two-letter prefix of CompanyNumber is what actually
   defines the legal family. CompanyCategory does not: it collapses FC, AC, NF
   and SF into the single string "Other company type" (s7.1), so anything
   derived from the category for those families is wrong by construction.
2. **Category second**, and only for the plain-numeric, SC and NI populations
   where the prefix carries no form information. A CIC has no distinguishing
   number prefix, so the category string is the only route to it (s7.3), the
   exact mirror image of rule 1.

The bulk file is not a register of Companies Act companies. It also carries
CIOs, Scottish CIOs and registered societies (s7.8). Our published counts are
clean today only because every one of those rows happens to have a blank
postcode and the postcode-to-LAD filter drops them. That is an accident, not a
rule. Everything here excludes them by prefix and asserts the counts.

Verified against BasicCompanyDataAsOneFile-2026-08-01, 5,695,466 rows, on
17 Aug 2026. PREFIX_CLASSES is exhaustive over that edition: a prefix that is
not listed makes the build fail rather than silently landing as a company.
"""

# prefix -> (entityType, isCompaniesActBody)
#
# isCompaniesActBody drives the headline company count. False means the row is
# a real registration of something, just not a company, so it is out of any
# "companies in Lancashire" figure.
PREFIX_CLASSES = {
    # Excluded families. Counts are the 2026-08-01 national totals.
    "OE": ("overseas-entity", False),          # 30,199 land ownership only
    "FC": ("overseas-establishment", False),   # 13,824 BR rows do not exist
    "CE": ("cio", False),                      # 40,160 not a CA company
    "CS": ("cio", False),                      # 7,872 Scottish CIO
    "IP": ("registered-society", False),       # 6,277
    "RS": ("registered-society", False),       # 3,758
    "SP": ("registered-society", False),       # 636
    "NO": ("registered-society", False),       # 146
    "NP": ("registered-society", False),       # 133

    # Companies Act forms carried by prefix.
    "OC": ("llp", True),                       # 47,135
    "SO": ("llp", True),                       # 2,897 Scottish LLP
    "NC": ("llp", True),                       # 588 NI LLP
    "LP": ("lp", True),                        # 21,877
    "SL": ("lp", True),                        # 38,113 Scottish LP
    "NL": ("lp", True),                        # 925 NI LP

    # Corporate bodies that are neither Companies Act companies nor any of the
    # s2 named families: royal charter bodies, assurance companies, open-ended
    # investment companies, EEIGs, Scottish qualifying partnerships, a UK
    # Societas, protected cell companies and two FE college corporations.
    # Out of the company count, kept in the table, typed honestly.
    "RC": ("other-corporate-body", False),     # 907 Royal Charter
    "SR": ("other-corporate-body", False),     # 1
    "AC": ("other-corporate-body", False),     # 878 assurance company
    "NF": ("other-corporate-body", False),     # 508
    "SF": ("other-corporate-body", False),     # 182
    "SA": ("other-corporate-body", False),     # 43
    "ZC": ("other-corporate-body", False),     # 35
    "SZ": ("other-corporate-body", False),     # 8
    "IC": ("other-corporate-body", False),     # 703 ICVC
    "SI": ("other-corporate-body", False),     # 10 ICVC umbrella
    "GE": ("other-corporate-body", False),     # 266 EEIG
    "GS": ("other-corporate-body", False),     # 6 EEIG
    "SG": ("other-corporate-body", False),     # 241 Scottish partnership
    "SE": ("other-corporate-body", False),     # 6 UK Societas
    "PC": ("other-corporate-body", False),     # 3 protected cell
    "FE": ("other-corporate-body", False),     # 2 FE college corporation

    # Ordinary companies that merely carry a jurisdiction prefix. These fall
    # through to the category rule, exactly like the plain-numeric population.
    "SC": (None, True),                        # 275,971 Scotland
    "NI": (None, True),                        # 89,071 Northern Ireland
}

# --- Number formats -------------------------------------------------------
# Every registered number in the bulk file is EXACTLY eight characters. That is
# the only universal rule, and it is the id-scheme gate (V-T1).
CH_NUMBER_RE = "^[0-9A-Z]{8}$"

# The shapes a Companies Act body's number actually takes, verified against the
# 2026-08-01 edition. The third is the giveaway that a bare two-letter prefix
# rule is not enough: 93 Northern Ireland companies registered before 1922
# carry R plus seven digits, and one of them is a plc.
COMPANIES_ACT_NUMBER_RE = (
    "^([0-9]{8}"            # 5,112,084 England and Wales
    "|[A-Z]{2}[0-9]{6}"     # SC, NI, OC, SO, NC, LP, SL, NL
    "|R[0-9]{7}"            # 93 pre-1922 Northern Ireland
    "|[A-Z]{2}[0-9]{5}[A-Z])$"  # 6 Scottish limited partnerships
)

# Used to decide whether a free-text registration number from another source
# could be a Companies House number at all. It is a SHAPE test and nothing
# more. RS000822 is a registered society and matches it perfectly, which is
# gate V-T4 in one line: format can never separate a society number from a
# company number, only the register can. Anything relying on this flag as
# proof of a company is wrong.
CRN_SHAPE_RE = "^([0-9]{8}|[A-Z]{2}[0-9]{6}|R[0-9]{7})$"

# Prefixes whose form is settled by the prefix alone.
PREFIX_DECIDED = {k: v for k, v in PREFIX_CLASSES.items() if v[0] is not None}
# Prefixes that defer to CompanyCategory.
PREFIX_DEFERRED = sorted(k for k, v in PREFIX_CLASSES.items() if v[0] is None)

# The one string that identifies a CIC. No number prefix distinguishes one.
CIC_CATEGORY = "Community Interest Company"
PLC_CATEGORIES = ("Public Limited Company", "Old Public Company")

# Expected CompanyCategory values per decided prefix. A build asserts these,
# because a future edition that starts filing CIOs under a different prefix, or
# societies under a company prefix, must fail loudly rather than inflate a count.
PREFIX_EXPECTED_CATEGORIES = {
    "OE": {"Overseas Entity"},
    "CE": {"Charitable Incorporated Organisation"},
    "CS": {"Scottish Charitable Incorporated Organisation"},
    "OC": {"Limited Liability Partnership"},
    "SO": {"Limited Liability Partnership"},
    "NC": {"Limited Liability Partnership"},
    "LP": {"Limited Partnership"},
    "SL": {"Limited Partnership"},
    "NL": {"Limited Partnership"},
    "IP": {"Registered Society", "Industrial and Provident Society"},
    "RS": {"Registered Society"},
    "SP": {"Registered Society", "Industrial and Provident Society"},
    "NO": {"Industrial and Provident Society"},
    "NP": {"Registered Society"},
}

EXCLUDED_TYPES = (
    "overseas-entity", "overseas-establishment", "cio",
    "registered-society", "other-corporate-body",
)


def prefix_case_sql(col="company_number"):
    """SQL CASE mapping a company number to entityType, prefix rule first."""
    parts = []
    for pfx, (etype, _) in sorted(PREFIX_DECIDED.items()):
        parts.append(f"WHEN substr({col}, 1, 2) = '{pfx}' THEN '{etype}'")
    cic = CIC_CATEGORY.replace("'", "''")
    plc = ", ".join("'" + c.replace("'", "''") + "'" for c in PLC_CATEGORIES)
    parts.append(f"WHEN company_category = '{cic}' THEN 'cic'")
    parts.append(f"WHEN company_category IN ({plc}) THEN 'plc'")
    return "CASE\n      " + "\n      ".join(parts) + "\n      ELSE 'ltd'\n    END"


def companies_act_case_sql(col="entity_type"):
    excluded = ", ".join(f"'{t}'" for t in EXCLUDED_TYPES)
    return f"({col} NOT IN ({excluded}))"


def known_prefixes_sql(col="company_number"):
    """TRUE when the two-letter prefix is one we have classified."""
    known = ", ".join(f"'{p}'" for p in sorted(PREFIX_CLASSES))
    return (f"(regexp_matches(substr({col}, 1, 2), '^[A-Z]{{2}}$') = false "
            f"OR substr({col}, 1, 2) IN ({known}))")
