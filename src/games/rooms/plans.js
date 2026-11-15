// Seat plans for the chamber rooms, as plain numbers. No three.js here, so a page can draw its
// flat plan straight away and the 3D room (src/games/ylv/chamber.js) can stand the same plan up.
// Plan units are SVG pixels in a 600 x 372 box; the room turns them into metres with K.
//
// County Hall, Preston: five rows of 11 to 22 seats in a semicircle facing the Chairman (84 seats).
// Burnley Town Hall: four rows in a horseshoe whose arms reach towards the top table (45 seats).
// Both seatings are invented stand-ins, not the councils' real plans.

export const ROOMS = {
  'county-hall': {
    id: 'county-hall', K: 0.04, CX: 300, CY: 118, R0: 112, RGAP: 29, STEP: 0.2,
    rows: [11, 14, 17, 20, 22],
    // Seat angles in degrees, front of each half to its end. Benches run a little past them.
    left: [166, 96], right: [84, 14],
    benchArcs: [[11, 85.5], [94.5, 169]], tierArc: [6, 174],
    // Where the officers sit on the flat plan.
    office: { chair: [300, 40], ce: [252, 46], officer: [348, 46] },
  },
  burnley: {
    id: 'burnley', K: 0.04, CX: 300, CY: 150, R0: 78, RGAP: 28, STEP: 0.07,
    rows: [8, 11, 12, 13],
    left: [190, 99], right: [81, -10],
    benchArcs: [[-14, 85.5], [94.5, 194]], tierArc: [-14, 194],
    office: { chair: [300, 27], ce: [262, 29], officer: [338, 29] },
  },
};

export function makePlan(room) {
  const R = typeof room === 'string' ? ROOMS[room] : room;
  const seats = [];
  R.rows.forEach((n, r) => {
    const rad = R.R0 + r * R.RGAP, half = n / 2;
    for (let i = 0; i < n; i++) {
      const left = i < half, k = left ? i : i - Math.ceil(half), span = left ? Math.ceil(half) : Math.floor(half);
      const [a0, a1] = left ? R.left : R.right;
      const a = ((a0 + (a1 - a0) * (span === 1 ? 0.5 : k / (span - 1))) * Math.PI) / 180;
      seats.push({ x: R.CX + rad * Math.cos(a), y: R.CY + rad * Math.sin(a) });
    }
  });
  return seats;
}
