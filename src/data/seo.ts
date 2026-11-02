/* The share card used by pages without their own image: 1200x630, the size
   link previews expect. */
export const DEFAULT_OG_IMAGE = '/og-image.jpg';
export const DEFAULT_OG_IMAGE_ALT = 'Tom Pickup, Lancashire County Councillor for Padiham and Burnley West';

/* The fallback share card for an article with no card of its own: 1200x630, no
   portrait. The portrait card above is for the homepage and about page only, so
   Tom's face never sits beside an article headline that leads with a figure.
   scripts/guard-share-cards.mjs fails the build if an article uses the portrait. */
export const DEFAULT_ARTICLE_OG_IMAGE = '/og-article.jpg';

/* Every share image this site emits for an article is this size. */
export const OG_IMAGE_WIDTH = 1200;
export const OG_IMAGE_HEIGHT = 630;
