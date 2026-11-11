// I061. The rules the table recipe applies, shared by the rehype plugin
// (Markdown and raw HTML tables) and DataTable.astro.

const EMPTY = /^(|n\/a|na|none|nil|not published|not held|–|—|-|\.\.|:)$/i; // dash-ok: published tables use these marks for no value

// A cell is numeric when, footnote marks aside, it is one number with an
// optional sign, currency, unit or range end: 1,204  £3.2m  -4.1%  (12)  2025/26.
const NUMBER = /^\(?[-+−]?\s*[£$€]?\s*\d[\d,]*(\.\d+)?\s*(%|p|pp|m|bn|k|x|million|billion|trillion|per cent)?\)?$/i;
const YEAR_SPAN = /^\d{4}[/–-]\d{2,4}$/; // dash-ok: a financial year may be typed with either mark
const MARKS = /[*†‡§]+$|\[\d+\]$|\(\w\)$/;

/** True when a cell's text is a number a reader would compare down a column. */
export function isNumeric(text) {
  const t = String(text).replace(/ /g, " ").trim().replace(MARKS, "").trim();
  return NUMBER.test(t) || YEAR_SPAN.test(t);
}

/** True when a cell holds no value, which neither makes nor breaks a numeric column. */
export function isEmpty(text) {
  return EMPTY.test(String(text).replace(/ /g, " ").trim());
}

/**
 * Which columns are numeric: every cell with a value is a number, and at least one has a value.
 * The first column is never numeric when it is the row label (a year or rank still labels its row).
 * @param {string[][]} rows body rows as cell text
 * @returns {boolean[]}
 */
export function numericColumns(rows) {
  const width = Math.max(0, ...rows.map((r) => r.length));
  return Array.from({ length: width }, (_, i) => {
    const cells = rows.map((r) => r[i] ?? "").filter((c) => !isEmpty(c));
    return cells.length > 0 && cells.every(isNumeric);
  });
}

/** A table sticks its first column from this many columns up. */
export const STICKY_FROM = 4;

/** A paragraph that is the table's source line. */
export const SOURCE_LINE = /^\s*sources?\s*:/i;
