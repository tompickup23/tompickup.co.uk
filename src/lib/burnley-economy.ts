/* The "Burnley's economy" collection reads every number from a data contract.
 *
 * The contracts are built in the Reports repo (Burnley_Growth_work/H_collection/
 * scripts/build_contracts.py), proved against their series files there by
 * check_contracts.py, and copied to public/data/burnley-economy/ by run_all.sh.
 * They are read here at build time, so the pages are static and carry no
 * client fetch. No page types a figure: when the ONS releases new data the
 * refresh changes these files, not the markup.
 */
import fs from 'node:fs';
import path from 'node:path';

export interface Row {
  rid: string;
  geography_code: string;
  geography_name: string;
  measure: string;
  period: string;
  period_type: string;
  value: number | null;
  unit: string;
  source_table: string;
  source_edition: string;
  note: string;
  [key: string]: unknown;
}

export interface Derived {
  id: string;
  label: string;
  value: number | string;
  decimals: number | null;
  unit: string;
  arithmetic: string;
  inputs: string[];
  note: string;
}

export interface Meta {
  panel: string;
  title: string;
  page: string;
  measures: { measure: string; definition: string; unit: string; decimals: number | null }[];
  publisher: string;
  table: string;
  edition: string;
  access_date: string;
  citation: string;
  sources: { file: string; sha256: string }[];
  vintage: string;
  caveat: string;
  next_release: string;
  generated: string;
  latest_period?: string;
  neighbour_period?: string;
}

export interface Contract {
  $meta: Meta;
  rows: Row[];
  derived: Derived[];
  [key: string]: unknown;
}

const DIR = path.join(process.cwd(), 'public', 'data', 'burnley-economy');
const cache = new Map<string, Contract>();

export function contract(panel: string): Contract {
  if (!cache.has(panel)) {
    const file = path.join(DIR, `${panel}.json`);
    cache.set(panel, JSON.parse(fs.readFileSync(file, 'utf-8')) as Contract);
  }
  return cache.get(panel)!;
}

export function panels(): string[] {
  return fs.readdirSync(DIR).filter((f) => f.endsWith('.json')).map((f) => f.replace(/\.json$/, '')).sort();
}

export const jsonUrl = (panel: string) => `/data/burnley-economy/${panel}.json`;
export const csvUrl = (panel: string) => `/data/burnley-economy/${panel}.csv`;

/* Finders. Each throws when the contract lacks the row, so a refresh that
   drops a series fails the build instead of printing a blank. */
export function rows(c: Contract, filter: Partial<Record<string, unknown>>): Row[] {
  return c.rows.filter((r) => Object.entries(filter).every(([k, v]) => r[k] === v));
}

export function row(c: Contract, filter: Partial<Record<string, unknown>>): Row {
  const hit = rows(c, filter);
  if (hit.length !== 1) throw new Error(`${c.$meta.panel}: ${JSON.stringify(filter)} selects ${hit.length} rows`);
  return hit[0];
}

export function derived(c: Contract, id: string): Derived {
  const d = c.derived.find((x) => x.id === id);
  if (!d) throw new Error(`${c.$meta.panel}: no derived entry ${id}`);
  return d;
}

export function latestRow(c: Contract, filter: Partial<Record<string, unknown>>): Row {
  const hit = rows(c, filter).filter((r) => r.value !== null);
  if (!hit.length) throw new Error(`${c.$meta.panel}: ${JSON.stringify(filter)} selects nothing`);
  return hit.reduce((a, b) => (b.period > a.period ? b : a));
}

/* Formatting. Numbers print at the precision the measure's codebook entry gives. */
export function decimalsFor(c: Contract, measure: string, fallback = 1): number {
  const m = c.$meta.measures.find((x) => x.measure === measure);
  return m?.decimals ?? fallback;
}

export function num(v: number | null | undefined, decimals = 0): string {
  if (v === null || v === undefined || Number.isNaN(v)) return '';
  return v.toLocaleString('en-GB', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function fmt(v: number | null | undefined, unit: string, decimals = 0): string {
  const n = num(v, decimals);
  if (!n) return '';
  if (unit === '%') return `${n}%`;
  if (unit.startsWith('GBP')) return `£${n}`;
  return n;
}

const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** A source period in words: "2025-07/2026-06" -> "July 2025 to June 2026"; "2026-09" -> "September 2026". */
export function periodText(p: string): string {
  let m = p.match(/^(\d{4})-(\d{2})\/(\d{4})-(\d{2})$/);
  if (m) return `${MONTHS[+m[2] - 1]} ${m[1]} to ${MONTHS[+m[4] - 1]} ${m[3]}`;
  m = p.match(/^(\d{4})-(\d{2})$/);
  if (m) return `${MONTHS[+m[2] - 1]} ${m[1]}`;
  m = p.match(/^(\d{4})-01-01\/P1Y$/);
  if (m) return m[1];
  m = p.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return `${+m[3]} ${MONTHS[+m[2] - 1]} ${m[1]}`;
  return p;
}

export function shortMonth(p: string): string {
  const m = p.match(/^(\d{4})-(\d{2})$/);
  return m ? `${SHORT[+m[2] - 1]} ${m[1]}` : p;
}

export function longDate(iso: string): string {
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${+m[3]} ${MONTHS[+m[2] - 1]} ${m[1]}` : iso;
}

/* CSV of a contract's rows, built from the same JSON the page reads. */
const CSV_COLS = ['geography_code', 'geography_name', 'geography_type', 'measure', 'period', 'period_type', 'value', 'unit',
  'source_publisher', 'source_title', 'source_table', 'source_edition', 'source_url', 'source_sha256', 'access_date', 'derived', 'derivation', 'note'];

function cell(v: unknown): string {
  let s = v === null || v === undefined ? '' : String(v);
  /* Spreadsheet formula injection: a cell never starts with = + - or @ unless it is a plain number. */
  if (/^[=+\-@]/.test(s) && !/^-?\d+(\.\d+)?$/.test(s)) s = `'${s}`;
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

export function toCsv(c: Contract): string {
  const extra = Array.from(new Set(c.rows.flatMap((r) => Object.keys(r)))).filter((k) => !CSV_COLS.includes(k) && k !== 'src' && k !== 'rid');
  const cols = [...CSV_COLS.filter((k) => c.rows.some((r) => k in r)), ...extra];
  const lines = [cols.join(',')];
  for (const r of c.rows) lines.push(cols.map((k) => cell(r[k])).join(','));
  return lines.join('\n') + '\n';
}

/* The six pages, in reading order, for the collection's sub-navigation. */
export const PAGES = [
  { href: '/burnley-economy/', label: 'Overview' },
  { href: '/burnley-economy/jobs-and-pay/', label: 'Jobs and pay' },
  { href: '/burnley-economy/business/', label: 'Business' },
  { href: '/burnley-economy/people-and-skills/', label: 'People and skills' },
  { href: '/burnley-economy/risks/', label: 'Risks' },
  { href: '/burnley-economy/method/', label: 'Method' },
];
