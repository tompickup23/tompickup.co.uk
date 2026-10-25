/* The CSV download behind each panel of the "Burnley's economy" collection,
   built at build time from the same contract the page reads. */
import type { APIRoute } from 'astro';
import { contract, panels, toCsv } from '../../../lib/burnley-economy';

export function getStaticPaths() {
  return panels().filter((p) => p !== 'method').map((panel) => ({ params: { panel } }));
}

export const GET: APIRoute = ({ params }) =>
  new Response(toCsv(contract(params.panel as string)), { headers: { 'Content-Type': 'text/csv; charset=utf-8' } });
