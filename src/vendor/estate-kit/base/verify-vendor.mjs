#!/usr/bin/env node
// Checks a vendored copy of the base layer against the VERSION file written
// beside it by the kit's scripts/vendor-base.mjs: every recorded file present
// and unchanged, and no stray file. A site runs this in its build, so a hand
// edit to the vendored copy fails there and is made in the kit instead.
//
//   node src/vendor/estate-kit/base/verify-vendor.mjs
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");
const version = readFileSync(join(root, "VERSION"), "utf8");
const recorded = new Map([...version.matchAll(/^sha256 ([0-9a-f]{64}) (base\/.+)$/gm)].map((m) => [m[2], m[1]]));

const walk = (dir) => readdirSync(dir).flatMap((name) => {
  const path = join(dir, name);
  return statSync(path).isDirectory() ? walk(path) : [relative(root, path).split("\\").join("/")];
});

const problems = [];
if (!/^commit [0-9a-f]{40}$/m.test(version)) problems.push("VERSION names no commit");
if (!recorded.size) problems.push("VERSION records no base files");
const present = walk(here);
for (const file of present) if (!recorded.has(file)) problems.push(`${file}: not recorded in VERSION`);
for (const [file, hash] of recorded) {
  if (!present.includes(file)) { problems.push(`${file}: recorded but missing`); continue; }
  const actual = createHash("sha256").update(readFileSync(join(root, file))).digest("hex");
  if (actual !== hash) problems.push(`${file}: changed since it was vendored`);
}
if (problems.length) {
  console.error(`estate-kit base layer: ${problems.length} problem(s)\n${problems.join("\n")}\nChange the kit and vendor again; do not edit the copy.`);
  process.exit(1);
}
console.log(`estate-kit base layer: ${recorded.size} files match VERSION`);
