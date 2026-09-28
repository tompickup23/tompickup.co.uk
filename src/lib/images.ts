import type { ImageMetadata } from 'astro';
import { getImage } from 'astro:assets';

/* Article lead images live in src/assets/images so astro:assets can resize them
   and serve WebP at set widths. Frontmatter keeps the familiar public-style path
   (image: "/images/burnley-aerial.jpg"); this maps it to the processed asset.

   A path with no matching asset returns undefined, and callers fall back to a
   plain <img>, so an image that is still in public/ keeps working.

   Nothing here reads the asset's own properties (width, format, src): doing so
   in a page makes Astro ship the full-size original alongside the resized
   copies. Fixed widths are safe: Astro drops any wider than the source. */
const assets = import.meta.glob<{ default: ImageMetadata }>(
  '/src/assets/images/**/*.{jpg,jpeg,png,webp}',
  { eager: true },
);

const byPublicPath = new Map(
  Object.entries(assets).map(([file, mod]) => [file.replace(/^\/src\/assets/, ''), mod.default]),
);

export function articleImage(path?: string): ImageMetadata | undefined {
  return path ? byPublicPath.get(path) : undefined;
}

/* Charts and maps saved as PNG carry small text, so they get a higher quality
   than photographs when converted to WebP. */
export function imageQuality(path?: string): number {
  return path?.toLowerCase().endsWith('.png') ? 90 : 72;
}

/* A 1200px JPEG of the lead image, for articles without their own share card.
   Social platforms still handle JPEG more reliably than WebP. */
export async function shareImageUrl(path?: string): Promise<string | undefined> {
  const meta = articleImage(path);
  if (!meta) return path;
  const out = await getImage({ src: meta, width: 1200, format: 'jpg', quality: 80 });
  return out.src;
}
