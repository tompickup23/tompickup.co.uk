/* Draws the 1200x630 share card for /straight-outta-padiham/.
 *
 * Run by hand when the card should change; the PNG is committed:
 *   node scripts/generate_padiham_game_card.mjs
 *
 * The card carries the game's name and a drawn street. No portrait and no figure,
 * in line with the share card rule.
 */
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Resvg } from '@resvg/resvg-js';

const OUT = fileURLToPath(new URL('../public/images/share/straight-outta-padiham.png', import.meta.url));
const W = 1200, H = 630;

const stone = ['#b9a27a', '#a8946f', '#c2ad86', '#9c8a6c', '#b59a70'];
let houses = '';
let x = -20;
for (let i = 0; x < W; i++) {
  const w = 120 + ((i * 37) % 50), h = 150 + ((i * 53) % 70), y = 520 - h;
  houses += `<rect x="${x}" y="${y}" width="${w}" height="${h}" fill="${stone[i % stone.length]}"/>`;
  houses += `<polygon points="${x - 8},${y} ${x + w / 2},${y - 46} ${x + w + 8},${y}" fill="#4b4f57"/>`;
  houses += `<rect x="${x + w * 0.62}" y="${y - 58}" width="18" height="34" fill="#8f8068"/>`;
  for (const wx of [0.18, 0.6]) houses += `<rect x="${x + w * wx}" y="${y + 24}" width="${w * 0.22}" height="34" fill="#2f3b4a" stroke="#f3efe4" stroke-width="3"/>`;
  houses += `<rect x="${x + w * 0.16}" y="${520 - 62}" width="${w * 0.2}" height="62" fill="${['#7a1f1f', '#1f3557', '#234d32'][i % 3]}"/>`;
  houses += `<rect x="${x + w * 0.52}" y="${520 - 62}" width="${w * 0.32}" height="40" fill="#2f3b4a" stroke="#f3efe4" stroke-width="3"/>`;
  x += w;
}
let flags = '';
for (let i = 0; i < 26; i++) {
  const fx = 20 + i * 46, fy = 296 + Math.sin((i / 25) * Math.PI) * 26;
  flags += `<polygon points="${fx},${fy} ${fx + 30},${fy} ${fx + 15},${fy + 26}" fill="${['#c8102e', '#f4f1e6', '#1f3a8a'][i % 3]}"/>`;
}
const pigeon = (px, py, s) => `<g transform="translate(${px} ${py}) scale(${s})"><ellipse cx="0" cy="0" rx="26" ry="15" fill="#7d8794"/><circle cx="24" cy="-14" r="10" fill="#5f6772"/><polygon points="33,-14 44,-11 33,-9" fill="#d9a05a"/><polygon points="-24,-4 -46,-10 -26,6" fill="#4a5058"/><rect x="-2" y="13" width="3" height="12" fill="#c96a5a"/><rect x="8" y="13" width="3" height="12" fill="#c96a5a"/></g>`;

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
  <defs><linearGradient id="sky" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#16324a"/><stop offset="1" stop-color="#3f6f8f"/></linearGradient></defs>
  <rect width="${W}" height="${H}" fill="url(#sky)"/>
  <text x="60" y="118" font-family="Arial Black, Arial, Helvetica, sans-serif" font-weight="900" font-size="78" fill="#f4f1e8">STRAIGHT OUTTA</text>
  <text x="60" y="232" font-family="Arial Black, Arial, Helvetica, sans-serif" font-weight="900" font-size="128" fill="#ffcf33">PADIHAM</text>
  <path d="M20 296 Q 600 356 1180 296" stroke="#e9e7df" stroke-width="2" fill="none"/>
  ${flags}
  ${houses}
  <rect x="0" y="520" width="${W}" height="110" fill="#3d3f44"/>
  <rect x="0" y="520" width="${W}" height="14" fill="#b9b4a8"/>
  ${[0, 1, 2, 3, 4, 5, 6].map((i) => `<rect x="${40 + i * 180}" y="574" width="90" height="8" fill="#e9e7df"/>`).join('')}
  ${pigeon(880, 548, 1)}${pigeon(960, 556, 0.8)}${pigeon(1040, 546, 0.9)}
  <text x="60" y="612" font-family="Arial, Helvetica, sans-serif" font-weight="700" font-size="30" fill="#f4f1e8">A free game on the real streets of Padiham</text>
  <text x="1140" y="612" text-anchor="end" font-family="Arial, Helvetica, sans-serif" font-weight="700" font-size="26" fill="#ffcf33">tompickup.co.uk</text>
</svg>`;

fs.writeFileSync(OUT, new Resvg(svg, { fitTo: { mode: 'width', value: W }, font: { loadSystemFonts: true } }).render().asPng());
console.log('wrote', OUT);
