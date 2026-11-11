// I059. Names for shared elements across a navigation. The card on a list
// page and the heading image on the page it opens must carry the same
// view-transition-name, unique on each page, so both derive it from the
// address of the page being opened.

/**
 * A valid CSS identifier from a prefix and a page address or slug.
 * @param {string} prefix what is travelling, such as "media"
 * @param {string} key the address or slug of the page the element belongs to
 */
export function vtName(prefix, key) {
  const clean = (s) => String(s).normalize("NFKD").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  const head = clean(prefix);
  const tail = clean(key);
  if (!head) throw new Error("vtName: a prefix is required");
  if (!tail) throw new Error("vtName: the key has no letters or digits");
  // An identifier may not start with a digit; the prefix guarantees that only
  // if it starts with a letter.
  if (!/^[a-z]/.test(head)) throw new Error("vtName: the prefix must start with a letter");
  return `${head}-${tail}`;
}

/**
 * The inline style for an element that travels.
 * @param {string} name from vtName()
 * @param {string} [cls] the transition class in transitions.css: ek-morph or ek-hold
 */
export function vtStyle(name, cls = "ek-morph") {
  return `view-transition-name: ${name}; view-transition-class: ${cls};`;
}
