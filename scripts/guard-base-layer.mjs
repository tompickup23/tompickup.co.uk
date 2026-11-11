/* Base layer guard (postbuild). The shared base layer (src/vendor/estate-kit/base)
 * depends on two things the CSS minifier could break without any page looking
 * wrong in a build log:
 *
 *   1. Layer order. The reset must come before estate-base, or the reset's
 *      "padding: 0" beats the layer and every table loses its padding.
 *   2. Scroll timelines. A minifier that folds animation-timeline into the
 *      animation shorthand produces a value browsers reject, which drops the
 *      animation silently (found building the motion layer, PR 86).
 *
 * Reads the built CSS in dist/_astro and fails the build on either.
 */
import fs from 'node:fs';
import path from 'node:path';

const dir = path.join('dist', '_astro');
const files = fs.readdirSync(dir).filter((f) => f.endsWith('.css'));
const problems = [];
let layered = 0;

for (const file of files) {
  const css = fs.readFileSync(path.join(dir, file), 'utf8');
  const reset = css.search(/@layer\s+reset\b/);
  const base = css.search(/@layer\s+(?:[a-z-]+\s*,\s*)*estate-base\b/);
  if (base !== -1) {
    layered += 1;
    const stated = /@layer\s+reset\s*,\s*estate-base\s*;/.test(css);
    if (!stated && (reset === -1 || reset > base)) problems.push(`${file}: estate-base is declared before the reset layer`);
  }
  for (const [rule] of css.matchAll(/[^{}]*\{[^{}]*animation-timeline[^{}]*\}/g)) {
    if (/animation\s*:[^;}]*(?:view|scroll)\(/.test(rule)) problems.push(`${file}: a timeline was folded into the animation shorthand: ${rule.slice(0, 90)}`);
  }
}

if (!layered) problems.push('no built stylesheet carries the estate-base layer');
if (problems.length) {
  console.error(`Base layer guard failed:\n  ${problems.join('\n  ')}`);
  process.exit(1);
}
console.log(`Base layer guard: ok (${layered} stylesheet${layered === 1 ? '' : 's'} with the layer, reset first)`);
