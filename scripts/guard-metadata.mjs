/* Metadata guard (register row R611). Reads every built page in dist/ and reports:
 *
 *   1. page titles over 60 characters (what a search result shows before it cuts);
 *   2. meta descriptions over 160 characters;
 *   3. sentences that invite the reader to act by a date that has already passed
 *      ("comments by 22 October 2026", "responses until ...", "closes on ...").
 *
 * Three audits in a month counted these by hand; this ends the count.
 *
 * Warn only: it prints a count and the offending paths and exits 0, so it never
 * blocks a deploy. Set FAIL_ON_METADATA=1 to make any finding fail the build, once
 * the first month's warnings are cleared. On GitHub Actions the counts also go to
 * the run's step summary and each finding becomes a warning annotation.
 *
 * Text is decoded from HTML entities before it is measured, so "&amp;" counts as
 * one character, as a reader sees it. Past tense ("closed on") is not an invitation
 * and is never flagged.
 */
import fs from 'node:fs';
import path from 'node:path';

const DIST = 'dist';
const TITLE_MAX = 60;
const DESC_MAX = 160;
const FAIL = process.env.FAIL_ON_METADATA === '1';
const BUILD_DATE = new Date(new Date().toISOString().slice(0, 10) + 'T00:00:00Z');

const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july',
  'august', 'september', 'october', 'november', 'december'];

const NAMED = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', pound: '£',
  middot: '·', rarr: '→', larr: '←', hellip: '…', rsquo: '’', lsquo: '‘', rdquo: '”',
  ldquo: '“', times: '×', euro: '€', copy: '©' };

function decode(s) {
  return s
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(Number(d)))
    .replace(/&([a-z]+);/gi, (m, n) => NAMED[n.toLowerCase()] ?? m);
}

function attr(tag, name) {
  return tag.match(new RegExp(`\\s${name}="([^"]*)"`, 'i'))?.[1];
}

function htmlFiles(dir) {
  const out = [];
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...htmlFiles(p));
    else if (e.name.endsWith('.html')) out.push(p);
  }
  return out;
}

/* Visible text only: scripts, styles, templates and tags removed. */
function visibleText(html) {
  const body = html.match(/<body[\s\S]*<\/body>/i)?.[0] ?? html;
  return decode(
    body
      .replace(/<(script|style|template|noscript)\b[\s\S]*?<\/\1>/gi, ' ')
      .replace(/<!--[\s\S]*?-->/g, ' ')
      .replace(/<[^>]+>/g, ' '),
  ).replace(/\s+/g, ' ');
}

/* "by 22 October 2026", "until Friday 6 November 2026", "closes on 1 Dec 2026",
   "deadline is 3 March 2027", "no later than ...", "before ...". */
const DATE_PHRASE = new RegExp(
  '\\b(by|until|till|before|no later than|closes on|closing on|closing date(?: is| of)?|deadline(?: is| of)?)\\s+' +
  '(?:(?:mon|tues|wednes|thurs|fri|satur|sun)day,?\\s+)?' +
  '(\\d{1,2})(?:st|nd|rd|th)?\\s+(' + MONTHS.map((m) => m.slice(0, 3) + '(?:' + m.slice(3) + ')?').join('|') + ')\\.?\\s+(\\d{4})',
  'gi',
);
/* The sentence must also invite action, so "published by 3 May 2026" is not flagged. */
const INVITES = /\b(comment|comments|commenting|respond|response|responses|submit|submission|object|objection|objections|have your say|tell us|tell the|let (?:me|us|them) know|apply|application|register|sign up|sign the|vote|send|email|write to|consultation|closes|closing date|deadline|book|enter)\b/i;
/* Past tense or past framing in the same sentence means a record, not an invitation. */
const PAST = /\b(closed|ended|was due|were due|had until|had to|passed|expired)\b/i;

function staleInvitations(text) {
  const hits = [];
  const sentences = text.split(/(?<=[.!?])\s+(?=[A-Z0-9"'‘“(])/);
  for (const s of sentences) {
    if (!INVITES.test(s) || PAST.test(s)) continue;
    for (const m of s.matchAll(DATE_PHRASE)) {
      // "Until 7 October 2026 a form let you ..." opens a sentence about the past.
      if (m.index === 0 && /^until\b/i.test(m[1])) continue;
      const month = MONTHS.findIndex((x) => x.startsWith(m[3].toLowerCase().slice(0, 3)));
      const when = new Date(Date.UTC(Number(m[4]), month, Number(m[2])));
      if (month >= 0 && when < BUILD_DATE) {
        hits.push(s.trim().slice(0, 160));
        break;
      }
    }
  }
  return hits;
}

if (!fs.existsSync(DIST)) {
  console.error('guard-metadata: dist/ not found; run the build first.');
  process.exit(FAIL ? 1 : 0);
}

const longTitles = [];
const longDescs = [];
const stale = [];

for (const file of htmlFiles(DIST).sort()) {
  const html = fs.readFileSync(file, 'utf8');
  const where = '/' + path.relative(DIST, file).split(path.sep).join('/').replace(/index\.html$/, '');

  const title = html.match(/<title>([\s\S]*?)<\/title>/i)?.[1];
  if (title !== undefined) {
    const t = decode(title).trim();
    if ([...t].length > TITLE_MAX) longTitles.push({ where, n: [...t].length, text: t });
  }

  const descTag = html.match(/<meta[^>]*name="description"[^>]*>/i)?.[0];
  const desc = descTag && attr(descTag, 'content');
  if (desc !== undefined) {
    const d = decode(desc).trim();
    if ([...d].length > DESC_MAX) longDescs.push({ where, n: [...d].length });
  }

  for (const s of staleInvitations(visibleText(html))) stale.push({ where, text: s });
}

const total = longTitles.length + longDescs.length + stale.length;
const lines = [];
lines.push(`Metadata guard (warn only${FAIL ? ', FAIL_ON_METADATA=1 set' : ''}); build date ${BUILD_DATE.toISOString().slice(0, 10)}`);
lines.push(`  titles over ${TITLE_MAX} characters: ${longTitles.length}`);
for (const x of longTitles) lines.push(`    ${x.where} (${x.n}): ${x.text}`);
lines.push(`  descriptions over ${DESC_MAX} characters: ${longDescs.length}`);
for (const x of longDescs) lines.push(`    ${x.where} (${x.n})`);
lines.push(`  invitations with a past date: ${stale.length}`);
for (const x of stale) lines.push(`    ${x.where}: ${x.text}`);
console.log(lines.join('\n'));

if (process.env.GITHUB_ACTIONS === 'true') {
  for (const x of longTitles) console.log(`::warning title=Title over ${TITLE_MAX}::${x.where} (${x.n} characters)`);
  for (const x of longDescs) console.log(`::warning title=Description over ${DESC_MAX}::${x.where} (${x.n} characters)`);
  for (const x of stale) console.log(`::warning title=Invitation with a past date::${x.where}`);
  if (process.env.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
      `### Metadata guard\n\n| Check | Count |\n|---|---|\n` +
      `| Titles over ${TITLE_MAX} characters | ${longTitles.length} |\n` +
      `| Descriptions over ${DESC_MAX} characters | ${longDescs.length} |\n` +
      `| Invitations with a past date | ${stale.length} |\n\n`);
  }
}

if (FAIL && total > 0) {
  console.error(`Metadata guard failed: ${total} finding(s) with FAIL_ON_METADATA=1.`);
  process.exit(1);
}
