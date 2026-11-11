// I064. The sum behind the fluid type steps in type.css: one clamp() from a
// size at the narrow viewport to a size at the wide one, with a rem term so
// zoom and the reader's font size still scale it.

/** Round to four places and drop trailing zeros. */
const tidy = (n) => String(+n.toFixed(4));

/**
 * @param {number} minPx size at the narrow viewport, in px
 * @param {number} maxPx size at the wide viewport, in px
 * @param {number} [narrow] narrow viewport width in px
 * @param {number} [wide] wide viewport width in px
 * @returns {string} a CSS clamp()
 */
export function fluid(minPx, maxPx, narrow = 360, wide = 1240) {
  if (!(wide > narrow)) throw new Error("fluid: the wide viewport must be wider than the narrow one");
  if (minPx === maxPx) return `${tidy(minPx / 16)}rem`;
  const slope = (maxPx - minPx) / (wide - narrow);
  const intercept = minPx - slope * narrow;
  const lo = Math.min(minPx, maxPx);
  const hi = Math.max(minPx, maxPx);
  return `clamp(${tidy(lo / 16)}rem, ${tidy(intercept / 16)}rem + ${tidy(slope * 100)}vw, ${tidy(hi / 16)}rem)`;
}

/**
 * A modular scale between two base sizes and two ratios.
 * @returns {Record<string, string>} step name to clamp()
 */
export function scale({ baseMin = 17, baseMax = 18, ratioMin = 1.18, ratioMax = 1.26, from = 0, to = 5, narrow = 360, wide = 1240 } = {}) {
  const out = {};
  for (let n = from; n <= to; n += 1) {
    out[`--ek-step-${n}`] = fluid(+(baseMin * ratioMin ** n).toFixed(2), +(baseMax * ratioMax ** n).toFixed(2), narrow, wide);
  }
  return out;
}
