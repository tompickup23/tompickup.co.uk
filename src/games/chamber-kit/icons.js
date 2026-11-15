// Chamber kit: line icons for every council chamber game (Budget Council, and later Young
// Lancashire Voice). Drawn here on a 24 unit grid in one stroke style, after the tray icons of the
// Virtual Cat Cafe (~/virtual-cat-cafe/site/src/game/offers.ts), not copied from it.
// icon(name) returns an inline SVG string; iconFor(label) guesses a fitting icon from a choice's words.

const P = {
  arrow: '<path d="M5 12h13M13 6l6 6-6 6"/>',
  mic: '<rect x="9" y="3" width="6" height="11" rx="3"/><path d="M6 11a6 6 0 0 0 12 0M12 17v4M9 21h6"/>',
  hand: '<path d="M8 13V6.5a1.5 1.5 0 0 1 3 0V12M11 11V4.5a1.5 1.5 0 0 1 3 0V12M14 11.5V6a1.5 1.5 0 0 1 3 0v8.5a6.5 6.5 0 0 1-6.5 6.5h-.6a6 6 0 0 1-4.6-2.2L3.6 15a1.5 1.5 0 0 1 2.3-1.9L8 15"/>',
  doc: '<path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5M9 13h6M9 17h4"/>',
  build: '<path d="M4 20h16M6 20V10M10 20V10M14 20V10M18 20V10M3 10l9-6 9 6"/>',
  gavel: '<path d="M13.5 5.5l5 5M11 8l5 5M9.5 9.5l-6 6a1.4 1.4 0 0 0 2 2l6-6M14 3l7 7-2.5 2.5-7-7z"/><path d="M13 21h8"/>',
  ask: '<circle cx="12" cy="12" r="9"/><path d="M9.6 9.3a2.5 2.5 0 0 1 4.8.9c0 1.7-2.4 2.1-2.4 3.6M12 17h.01"/>',
  offer: '<path d="M7 7h11l-3-3M17 17H6l3 3"/>',
  check: '<path d="M5 12.5l4.5 4.5L19 7.5"/>',
  cross: '<path d="M6 6l12 12M18 6L6 18"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  eye: '<path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12z"/><circle cx="12" cy="12" r="3"/>',
  people: '<circle cx="9" cy="8" r="3.2"/><path d="M3 20a6 6 0 0 1 12 0"/><path d="M16 4.8a3.2 3.2 0 0 1 0 6.4M17.5 14.2A6 6 0 0 1 21 20"/>',
  scale: '<path d="M12 4v16M7 20h10M5 7h14M5 7l-3 6a3 3 0 0 0 6 0zM19 7l-3 6a3 3 0 0 0 6 0z"/>',
  coin: '<ellipse cx="12" cy="7" rx="7" ry="3"/><path d="M5 7v5c0 1.7 3.1 3 7 3s7-1.3 7-3V7M5 12v5c0 1.7 3.1 3 7 3s7-1.3 7-3v-5"/>',
  pen: '<path d="M4 20l1-5L16 4l4 4L9 19z"/><path d="M14 6l4 4"/>',
  seat: '<path d="M7 11V5a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v6M5 11h14v4H5zM7 15v6M17 15v6"/>',
  menu: '<path d="M4 7h16M4 12h16M4 17h16"/>',
  close: '<path d="M6 6l12 12M18 6L6 18"/>',
  link: '<path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"/>',
  print: '<path d="M7 9V3h10v6M7 17H5a2 2 0 0 1-2-2v-4a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v4a2 2 0 0 1-2 2h-2M7 14h10v7H7z"/>',
  book: '<path d="M4 5a2 2 0 0 1 2-2h13v16H6a2 2 0 0 0-2 2zM4 21V5"/><path d="M8 7h7"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7.5h.01"/>',
  restart: '<path d="M4 12a8 8 0 1 0 2.4-5.7L4 8.7M4 4v4.7h4.7"/>',
  flag: '<path d="M5 21V4M5 4h11l-2 4 2 4H5"/>',
  layers: '<path d="M12 3l9 5-9 5-9-5z"/><path d="M3 13l9 5 9-5"/>',
  cube: '<path d="M12 2.8l8 4.6v9.2l-8 4.6-8-4.6V7.4z"/><path d="M12 12l8-4.6M12 12v9.2M12 12L4 7.4"/>',
  camera: '<path d="M4 8h3l2-3h6l2 3h3v11H4z"/><circle cx="12" cy="13" r="3.5"/>',
  gallery: '<path d="M3 20h18M5 20v-6l7-4 7 4v6M9 20v-4h6v4"/>',
  play: '<path d="M7 4.5v15L19 12z"/>',
  shield: '<path d="M12 3l8 3v6c0 4.6-3.4 8.2-8 9-4.6-.8-8-4.4-8-9V6z"/>',
};

export function icon(name, cls = '') {
  return `<svg class="ck-i${cls ? ' ' + cls : ''}" viewBox="0 0 24 24" aria-hidden="true" focusable="false">${P[name] || P.arrow}</svg>`;
}

// First match wins, so the more particular words come first.
const GUESS = [
  [/point of order|rule|ruling|standing order|procedure/i, 'gavel'],
  [/\bask\b|question|clarif/i, 'ask'],
  [/\bvote\b|for it|against it|abstain|hands/i, 'hand'],
  [/\bbuild|costed/i, 'build'],
  [/offer|share|send|take .* to|propose to|approach|talk to|meet the/i, 'offer'],
  [/accept|agree|support|back the|yes\b|consent/i, 'check'],
  [/refuse|decline|reject|oppose|withdraw/i, 'cross'],
  [/speak|say|reply|debate|statement|argue|answer|heckle/i, 'mic'],
  [/amend|motion|paper|report|read/i, 'doc'],
  [/wait|later|adjourn|pause|time/i, 'clock'],
  [/listen|watch|sit|stay quiet|hear/i, 'eye'],
  [/group|whip|colleague|councillors/i, 'people'],
  [/tax|money|reserve|saving|cost|£/i, 'coin'],
  [/officer|check|section 151|director/i, 'shield'],
  [/start|begin|continue|go on|next/i, 'arrow'],
];
export function iconFor(label) {
  for (const [re, name] of GUESS) if (re.test(label)) return name;
  return 'arrow';
}
