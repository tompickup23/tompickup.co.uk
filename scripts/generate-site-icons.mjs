// Run with node scripts/generate-site-icons.mjs. Uses the header seal's font and colours.
import fs from 'node:fs';
import satori from 'satori';
import { Resvg } from '@resvg/resvg-js';

const font = fs.readFileSync('node_modules/@fontsource/source-serif-4/files/source-serif-4-latin-600-normal.woff');
const mark = await satori({ type: 'div', props: {
  style: { width: 64, height: 64, background: '#17645e', color: '#faf9f5', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'Source Serif', fontSize: 30, fontWeight: 600, borderRadius: 7 },
  children: 'TP',
} }, { width: 64, height: 64, fonts: [{ name: 'Source Serif', data: font, weight: 600, style: 'normal' }] });
const svg = mark.replace('</svg>', '<path d="M0 59H64Q63 64 57 64H7Q1 64 0 59Z" fill="#805c25"/></svg>');
fs.writeFileSync('public/favicon.svg', svg + '\n');
for (const [file, size] of [['apple-touch-icon.png', 180], ['icon-192.png', 192]]) {
  fs.writeFileSync(`public/${file}`, new Resvg(svg, { fitTo: { mode: 'width', value: size } }).render().asPng());
}
// Keep the monogram inside the maskable icon's central safe area.
const inner = svg.replace(/^<svg[^>]*>/, '').replace(/<\/svg>$/, '');
const mask = `<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512"><rect width="512" height="512" fill="#17645e"/><g transform="translate(96 96) scale(5)">${inner}</g></svg>`;
fs.writeFileSync('public/icon-maskable-512.png', new Resvg(mask).render().asPng());
// ICO permits PNG frames. Keep the directory offsets explicit for reproducibility.
const frames = [16, 32, 48].map(size => ({ size, png: new Resvg(svg, { fitTo: { mode: 'width', value: size } }).render().asPng() }));
const header = Buffer.alloc(6 + 16 * frames.length);
header.writeUInt16LE(1, 2);
header.writeUInt16LE(frames.length, 4);
let offset = header.length;
frames.forEach(({ size, png }, index) => {
  const entry = 6 + 16 * index;
  header[entry] = size;
  header[entry + 1] = size;
  header.writeUInt16LE(1, entry + 4);
  header.writeUInt16LE(32, entry + 6);
  header.writeUInt32LE(png.length, entry + 8);
  header.writeUInt32LE(offset, entry + 12);
  offset += png.length;
});
fs.writeFileSync('public/favicon.ico', Buffer.concat([header, ...frames.map(frame => frame.png)]));
