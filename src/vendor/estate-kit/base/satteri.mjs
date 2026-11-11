// The build-time parts of the base layer for Sätteri, the Markdown processor
// Astro 7 uses by default. The same decisions as rehype-tables.mjs and
// rehype-figures.mjs, applied through Sätteri's visitor interface:
//
//   import { satteri } from "@astrojs/markdown-satteri";
//   import { estateTables, bindFiguresPlugin } from "./src/vendor/estate-kit/base/satteri.mjs";
//   export default defineConfig({
//     markdown: { processor: satteri({ hastPlugins: [estateTables(), bindFiguresPlugin()] }) },
//   });
//
// Raw HTML typed into Markdown stays a raw string in Sätteri's tree, so a
// hand-written <table> is not touched: only Markdown tables are.
import { bindFigures, SKIP } from "./rehype-figures.mjs";
import { classesOf, isElement, numberLabel, planTable, skipTable, textOf } from "./rehype-tables.mjs";
import { STICKY_FROM } from "./table-rules.mjs";

/** I061. One table recipe. */
export function estateTables({ stickyFrom = STICKY_FROM, skipClasses = ["ek-table"] } = {}) {
  return {
    name: "estate-kit-tables",
    element: {
      filter: ["table"],
      visit(table, ctx) {
        if (skipTable(table, skipClasses)) return;
        const parent = ctx.parent(table);
        if (parent && isElement(parent) && classesOf(parent).includes("ek-table-wrap")) return;
        const index = ctx.indexOf(table) ?? 0;
        const siblings = parent?.children ?? [];
        let heading = "";
        for (let i = index - 1; i >= 0; i -= 1) {
          if (isElement(siblings[i]) && /^h[1-6]$/.test(siblings[i].tagName)) { heading = textOf(siblings[i]).replace(/\s+/g, " ").trim(); break; }
        }
        const plan = planTable(table, siblings.slice(index + 1), heading, stickyFrom);
        for (const cell of plan.scope) ctx.setProperty(cell, "scope", "col");
        for (const cell of plan.numeric) ctx.setProperty(cell, "data-ek-num", "");
        if (plan.titleCaption) ctx.setProperty(plan.titleCaption, "data-ek-title", "");
        if (plan.source) {
          ctx.prependChild(table, { type: "element", tagName: "caption", properties: {}, children: structuredClone(plan.source.children) });
          ctx.removeNode(plan.source);
        }
        const seen = (ctx.data.ekTableLabels ??= new Map());
        const properties = { className: ["ek-table-wrap"], role: "region", tabIndex: 0, ariaLabel: numberLabel(plan.label, seen) };
        if (plan.sticky) properties["data-ek-sticky"] = "";
        ctx.wrapNode(table, { type: "element", tagName: "div", properties, children: [] });
      }
    }
  };
}

/** I057. A figure never parts from its unit at a line end. White space only. */
export function bindFiguresPlugin() {
  return {
    name: "estate-kit-bind-figures",
    text(node, ctx) {
      if (!/\d|Band [A-H]\b/.test(node.value)) return;
      const bound = bindFigures(node.value);
      if (bound === node.value) return;
      for (let up = ctx.parent(node); up; up = ctx.parent(up)) {
        if (isElement(up) && SKIP.has(up.tagName)) return;
      }
      ctx.setField(node, "value", bound);
    }
  };
}
