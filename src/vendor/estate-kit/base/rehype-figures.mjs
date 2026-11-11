// I057, the part CSS cannot do. A figure and its unit never part company at
// the end of a line: "£1.2" on one line and "billion" on the next reads as
// two things. This binds them with a no-break space at build time, in
// rendered Markdown only. It changes white space and nothing else: no digit,
// word or punctuation mark is touched.
//
// Bound: a number and the unit after it (12 million, 4.5 per cent, 30 km);
// a day and its month (12 November); a month and its year (November 2026);
// a reference and its number (Table 3, page 12, para. 38, Band D).

const NBSP = " ";
const UNITS = "million|billion|trillion|bn|per cent|percent|pence|km|miles|mph|kW|MW|GW|kWh|MWh|GWh|TWh|hectares|acres|tonnes|sq m|sq km";
const MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December";
const REFS = "Table|Figure|Chart|Appendix|Annex|Section|Chapter|Schedule|Part|Band|page|pages|p\\.|pp\\.|para\\.|paras\\.|s\\.|No\\.";

const RULES = [
  [new RegExp(`(\\d) (?=(?:${UNITS})\\b)`, "g"), `$1${NBSP}`],
  [new RegExp(`\\b(\\d{1,2}) (?=(?:${MONTHS})\\b)`, "g"), `$1${NBSP}`],
  [new RegExp(`\\b(${MONTHS}) (?=\\d{4}\\b)`, "g"), `$1${NBSP}`],
  [new RegExp(`(^|[\\s(])(${REFS}) (?=[\\dA-H]\\b|\\d)`, "g"), `$1$2${NBSP}`]
];

// Verbatim elements are left alone. So are headings: a site makes a heading's
// anchor from its text, and a bound space would change the anchor.
export const SKIP = new Set(["code", "pre", "kbd", "samp", "script", "style", "textarea", "svg", "math", "title", "h1", "h2", "h3", "h4", "h5", "h6"]);

/** Bind figures to their units in one string. */
export function bindFigures(value) {
  let out = value;
  for (const [pattern, replacement] of RULES) out = out.replace(pattern, replacement);
  return out;
}

export default function rehypeBindFigures() {
  return function transform(tree) {
    const visit = (node) => {
      if (node.type === "text") {
        if (/\d/.test(node.value) || /Band [A-H]\b/.test(node.value)) node.value = bindFigures(node.value);
        return;
      }
      if (node.type === "element" && SKIP.has(node.tagName)) return;
      for (const child of node.children ?? []) visit(child);
    };
    visit(tree);
  };
}
