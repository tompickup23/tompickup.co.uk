// I063. The icon set, built from one SVG mark, so every site has the same
// six files and none is drawn by hand:
//
//   favicon.ico             32px, for browsers that ask for it by name
//   favicon.svg             the mark, with a dark-scheme variant inside it
//   apple-touch-icon.png    180px, opaque, with its own margin
//   icon-192.png            for the manifest
//   icon-maskable-512.png   the mark inside the 80 per cent safe zone
//   site.webmanifest
//
// Needs @resvg/resvg-js, which the kit and most sites already carry.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/** A one-image .ico file wrapping a PNG, which every current browser reads. */
export function icoFromPng(png, size) {
  const header = Buffer.alloc(22);
  header.writeUInt16LE(0, 0); // reserved
  header.writeUInt16LE(1, 2); // type: icon
  header.writeUInt16LE(1, 4); // one image
  header.writeUInt8(size >= 256 ? 0 : size, 6);
  header.writeUInt8(size >= 256 ? 0 : size, 7);
  header.writeUInt8(0, 8); // no palette
  header.writeUInt8(0, 9);
  header.writeUInt16LE(1, 10); // colour planes
  header.writeUInt16LE(32, 12); // bits per pixel
  header.writeUInt32LE(png.length, 14);
  header.writeUInt32LE(22, 18); // offset of the image data
  return Buffer.concat([header, png]);
}

/** Put a style block for the dark scheme inside an SVG, after its opening tag. */
export function withDarkScheme(svg, darkCss) {
  if (!darkCss) return svg;
  const open = svg.match(/<svg\b[^>]*>/);
  if (!open) throw new Error("withDarkScheme: no <svg> element");
  const at = open.index + open[0].length;
  return `${svg.slice(0, at)}<style>@media (prefers-color-scheme: dark){${darkCss}}</style>${svg.slice(at)}`;
}

/** The mark centred on a square ground, scaled to a share of the square. */
export function onGround(svg, { size, background, share }) {
  const inner = Math.round(size * share);
  const offset = Math.round((size - inner) / 2);
  const href = `data:image/svg+xml;base64,${Buffer.from(svg).toString("base64")}`;
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}"><rect width="${size}" height="${size}" fill="${background}"/><image href="${href}" x="${offset}" y="${offset}" width="${inner}" height="${inner}"/></svg>`;
}

export function manifest({ name, shortName = name, themeColor, background, startUrl = "/", base = "/" }) {
  const at = (file) => `${base.replace(/\/?$/, "/")}${file}`;
  return {
    name,
    short_name: shortName,
    start_url: startUrl,
    display: "browser",
    background_color: background,
    theme_color: themeColor,
    icons: [
      { src: at("icon-192.png"), sizes: "192x192", type: "image/png", purpose: "any" },
      { src: at("icon-maskable-512.png"), sizes: "512x512", type: "image/png", purpose: "maskable" }
    ]
  };
}

/**
 * Write the six files.
 * @param {object} o
 * @param {string} o.svg the mark, square, as SVG source
 * @param {string} o.outDir where the files go (a site's public folder)
 * @param {string} o.name site name for the manifest
 * @param {string} o.background the page ground, behind the touch and maskable icons
 * @param {string} o.themeColor
 * @param {string} [o.shortName]
 * @param {string} [o.darkCss] CSS applied to the SVG icon in the dark scheme
 * @param {number} [o.touchShare] share of the touch icon the mark fills
 */
export async function buildIcons({ svg, outDir, name, shortName, background, themeColor, darkCss = "", touchShare = 1, base = "/" }) {
  const { Resvg } = await import("@resvg/resvg-js");
  const png = (source, size) => new Resvg(source, { fitTo: { mode: "width", value: size }, font: { loadSystemFonts: false } }).render().asPng();
  mkdirSync(outDir, { recursive: true });
  const files = {
    "favicon.ico": icoFromPng(png(svg, 32), 32),
    "favicon.svg": Buffer.from(withDarkScheme(svg, darkCss)),
    "apple-touch-icon.png": png(onGround(svg, { size: 180, background, share: touchShare }), 180),
    "icon-192.png": png(svg, 192),
    "icon-maskable-512.png": png(onGround(svg, { size: 512, background, share: 0.8 }), 512),
    "site.webmanifest": Buffer.from(`${JSON.stringify(manifest({ name, shortName, themeColor, background, base }), null, 2)}\n`)
  };
  for (const [file, bytes] of Object.entries(files)) writeFileSync(join(outDir, file), bytes);
  return Object.keys(files);
}
