// I061. One table recipe, applied at build time to every table in rendered
// Markdown. No dependencies: it walks the syntax tree itself, so a site can
// vendor this file alone.
//
// For each table it:
//   wraps it in a focusable region that scrolls sideways on its own;
//   marks the numeric columns (data-ek-num) so they align to the end;
//   gives every header cell a scope;
//   turns a "Source: ..." paragraph directly after the table into the
//   table's own caption, so the source travels with the table;
//   labels the region from the caption, or from the heading above it;
//   marks tables of four or more columns to keep their first column in view.
//
// A table already inside a wrapper, or carrying data-ek-skip, is left alone.
//
// This file is the rehype form (the unified processor: Astro 5 and 6, and
// Astro 7 with @astrojs/markdown-remark). satteri.mjs is the same recipe for
// Astro 7's default processor. Both take their decisions from planTable().
import { numericColumns, SOURCE_LINE, STICKY_FROM } from "./table-rules.mjs";

export const isElement = (n, tag) => Boolean(n) && n.type === "element" && (tag === undefined || n.tagName === tag);
export const textOf = (n) => (n.type === "text" ? n.value : (n.children ?? []).map(textOf).join(""));
export const classesOf = (n) => [].concat(n.properties?.className ?? n.properties?.class ?? []).join(" ").split(/\s+/).filter(Boolean);
const blank = (n) => n.type === "text" && !n.value.trim();
const childrenOf = (n, tag) => (n.children ?? []).filter((c) => isElement(c, tag));
const cellsOf = (tr) => (tr.children ?? []).filter((c) => isElement(c, "th") || isElement(c, "td"));
const spans = (cell) => Number(cell.properties?.colSpan ?? cell.properties?.colspan ?? 1) > 1 || Number(cell.properties?.rowSpan ?? cell.properties?.rowspan ?? 1) > 1;
const has = (n, camel, attr) => camel in (n.properties ?? {}) || attr in (n.properties ?? {});

function rowsOf(table) {
  const sections = (table.children ?? []).filter((c) => isElement(c) && ["thead", "tbody", "tfoot"].includes(c.tagName));
  const head = sections.filter((s) => s.tagName === "thead").flatMap((s) => childrenOf(s, "tr"));
  const body = [...sections.filter((s) => s.tagName !== "thead").flatMap((s) => childrenOf(s, "tr")), ...childrenOf(table, "tr")];
  // A table with no thead whose first row is all header cells: that row heads the columns.
  if (!head.length && body.length && cellsOf(body[0]).length && cellsOf(body[0]).every((c) => c.tagName === "th")) head.push(body.shift());
  return { head, body };
}

/**
 * Region names are how a screen reader user tells tables apart, so two tables
 * under one heading are numbered: "Table: Heading", then "Table 2: Heading".
 * @param {string} label from planTable()
 * @param {Map<string, number>} seen labels used so far in this document
 */
export function numberLabel(label, seen) {
  const n = (seen.get(label) ?? 0) + 1;
  seen.set(label, n);
  if (n === 1) return label;
  return label.startsWith("Table") ? label.replace(/^Table/, `Table ${n}`) : `${label} (${n})`;
}

/** True when the recipe should leave this table as it is. */
export function skipTable(table, skipClasses = ["ek-table"]) {
  return has(table, "dataEkSkip", "data-ek-skip") || classesOf(table).some((c) => skipClasses.includes(c));
}

/**
 * Everything the recipe decides about one table, without changing it.
 * @param {object} table the table element
 * @param {object[]} following the nodes after the table in its parent, in order
 * @param {string} heading the text of the nearest heading above the table, or ""
 * @param {number} [stickyFrom]
 */
export function planTable(table, following, heading, stickyFrom = STICKY_FROM) {
  const { head, body } = rowsOf(table);
  const all = [...head, ...body];
  const regular = all.length > 0 && !all.some((tr) => cellsOf(tr).some(spans));
  const width = Math.max(0, ...all.map((tr) => cellsOf(tr).length));

  const scope = head.flatMap((tr) => cellsOf(tr).filter((c) => c.tagName === "th" && !c.properties?.scope));
  const numeric = [];
  if (regular) {
    const columns = numericColumns(body.map((tr) => cellsOf(tr).map((c) => textOf(c))));
    // The first column labels its rows, whatever it holds.
    for (const tr of all) cellsOf(tr).forEach((c, i) => { if (columns[i] && i > 0) numeric.push(c); });
  }

  const caption = childrenOf(table, "caption")[0] ?? null;
  let source = null;
  let skipped = 0;
  if (!caption) {
    let j = 0;
    while (j < following.length && blank(following[j])) j += 1;
    if (isElement(following[j], "p") && SOURCE_LINE.test(textOf(following[j]))) { source = following[j]; skipped = j; }
  }
  const titled = Boolean(caption) && !SOURCE_LINE.test(textOf(caption));
  const title = titled ? textOf(caption).replace(/\s+/g, " ").trim() : "";
  return {
    scope,
    numeric,
    /** The paragraph to turn into the caption, or null. */
    source,
    /** White-space nodes between the table and that paragraph. */
    skipped,
    /** An existing caption that names the table. */
    titleCaption: titled ? caption : null,
    label: title || (heading ? `Table: ${heading}` : "Table"),
    sticky: width >= stickyFrom
  };
}

/**
 * @param {object} [options]
 * @param {number} [options.stickyFrom] columns from which the first column sticks
 * @param {string[]} [options.skipClasses] tables with one of these classes are left alone
 */
export default function rehypeEstateTables({ stickyFrom = STICKY_FROM, skipClasses = ["ek-table"] } = {}) {
  return function transform(tree) {
    let heading = "";
    const seen = new Map();
    const visit = (node, inWrap) => {
      if (!node.children) return;
      for (let i = 0; i < node.children.length; i += 1) {
        const child = node.children[i];
        if (!isElement(child)) continue;
        if (/^h[1-6]$/.test(child.tagName)) heading = textOf(child).replace(/\s+/g, " ").trim();
        const wrapped = inWrap || classesOf(child).includes("ek-table-wrap");
        if (child.tagName === "table" && !wrapped && !skipTable(child, skipClasses)) {
          const plan = planTable(child, node.children.slice(i + 1), heading, stickyFrom);
          for (const c of plan.scope) (c.properties ??= {}).scope = "col";
          for (const c of plan.numeric) (c.properties ??= {}).dataEkNum = "";
          if (plan.titleCaption) (plan.titleCaption.properties ??= {}).dataEkTitle = "";
          if (plan.source) {
            child.children.unshift({ type: "element", tagName: "caption", properties: {}, children: plan.source.children });
            node.children.splice(i + 1, plan.skipped + 1);
          }
          const properties = { className: ["ek-table-wrap"], role: "region", tabIndex: 0, ariaLabel: numberLabel(plan.label, seen) };
          if (plan.sticky) properties.dataEkSticky = "";
          node.children[i] = { type: "element", tagName: "div", properties, children: [child] };
          continue;
        }
        visit(child, wrapped);
      }
    };
    visit(tree, false);
  };
}
