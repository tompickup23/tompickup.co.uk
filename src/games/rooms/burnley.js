// The council chamber at Burnley Town Hall, as a room you can sit in. Everything except the
// councillors' benches, which src/games/ylv/chamber.js builds from the seat plan in plans.js.
// Built from shapes and drawn textures only, read from photographs of the restored room: no
// photograph is used, and the coat of arms, the painted shields and the frieze are stand-ins,
// not copies. Proportions are estimates; nothing here was measured.
import { tex, rng } from '../ylv/chamber.js';

const W = 8.0, ZF = -6.3, ZB = 9.2, H = 8.2;
const DADO = 3.0, FRIEZE = 6.3, CORNICE = 6.8, COVE = 1.3;

// Pale stone panelling, coursed like ashlar, as on the lower walls.
const stoneTex = () => tex(256, 256, (g, w, h) => {
  const r = rng(11);
  g.fillStyle = '#d8ccb4'; g.fillRect(0, 0, w, h);
  for (let i = 0; i < 900; i++) { g.fillStyle = r() > 0.5 ? 'rgba(120,100,70,0.05)' : 'rgba(255,255,255,0.06)'; g.fillRect(r() * w, r() * h, 2 + r() * 6, 1 + r() * 3); }
  g.strokeStyle = 'rgba(110,92,66,0.45)'; g.lineWidth = 2;
  for (let y = 0; y <= h; y += 64) { g.beginPath(); g.moveTo(0, y); g.lineTo(w, y); g.stroke(); }
  for (let row = 0; row < 4; row++) for (let x = row % 2 ? 64 : 0; x < w; x += 128) { g.beginPath(); g.moveTo(x, row * 64); g.lineTo(x, row * 64 + 64); g.stroke(); }
}, [5, 1.2]);
// Deep red upper walls, faintly uneven so they read as paint, not plastic.
const redTex = () => tex(128, 128, (g, w, h) => {
  const r = rng(5);
  g.fillStyle = '#8e2f2c'; g.fillRect(0, 0, w, h);
  for (let i = 0; i < 400; i++) { g.fillStyle = `rgba(${r() > 0.5 ? '255,220,200' : '40,0,0'},0.035)`; g.fillRect(r() * w, r() * h, 3 + r() * 9, 3 + r() * 9); }
}, [6, 2]);
// The frieze: gold letters on a dark band. Stand-in wording: the year of the borough's charter.
const friezeTex = () => tex(1024, 64, (g) => {
  g.fillStyle = '#1f2a24'; g.fillRect(0, 0, 1024, 64);
  g.fillStyle = '#c9a54a'; g.fillRect(0, 2, 1024, 3); g.fillRect(0, 59, 1024, 3);
  g.font = '700 30px Georgia, serif'; g.textBaseline = 'middle'; g.fillStyle = '#e2c46c';
  g.fillText('1861 · CHARTER OF INCORPORATION · BOROUGH OF BURNLEY ·', 18, 34);
}, [2, 1]);
// Coffers: green squares with a pale rosette, framed in gold, round a cream field.
const cofferTex = () => tex(128, 128, (g) => {
  g.fillStyle = '#efe2b8'; g.fillRect(0, 0, 128, 128);
  g.fillStyle = '#c9a54a'; g.fillRect(14, 14, 100, 100);
  g.fillStyle = '#3f6b4f'; g.fillRect(22, 22, 84, 84);
  g.fillStyle = '#f4efe2';
  for (let k = 0; k < 12; k++) { const a = (k * Math.PI) / 6; g.beginPath(); g.ellipse(64 + 18 * Math.cos(a), 64 + 18 * Math.sin(a), 10, 5, a, 0, 7); g.fill(); }
  g.fillStyle = '#d4af4f'; g.beginPath(); g.arc(64, 64, 10, 0, 7); g.fill();
}, [9, 11]);
// The cove: painted lunettes, a dark panel under each arch with a shield in a ring.
const coveTex = () => tex(256, 128, (g) => {
  g.fillStyle = '#e7d7a6'; g.fillRect(0, 0, 256, 128);
  g.fillStyle = '#c9a54a'; g.fillRect(0, 118, 256, 10);
  g.fillStyle = '#2f4a45'; g.beginPath(); g.moveTo(26, 118); g.lineTo(26, 70); g.arc(128, 70, 102, Math.PI, 0); g.lineTo(230, 118); g.closePath(); g.fill();
  g.strokeStyle = '#d4af4f'; g.lineWidth = 6; g.stroke();
  g.fillStyle = '#e9e4d6'; g.beginPath(); g.arc(128, 78, 30, 0, 7); g.fill();
  g.strokeStyle = '#3a8aa0'; g.lineWidth = 6; g.beginPath(); g.arc(128, 78, 30, 0, 7); g.stroke();
  g.fillStyle = '#9a2a2a'; g.beginPath(); g.moveTo(112, 62); g.lineTo(144, 62); g.lineTo(144, 82); g.quadraticCurveTo(128, 104, 112, 82); g.closePath(); g.fill();
  g.fillStyle = '#7a1f2a'; for (const x of [52, 204]) { g.beginPath(); g.arc(x, 100, 7, 0, 7); g.fill(); }
}, [1, 1]);
// A coat of arms, as a stand-in: shield, helm, mantling and supporters drawn in plain shapes.
const armsTex = () => tex(256, 256, (g) => {
  g.clearRect(0, 0, 256, 256);
  g.fillStyle = '#c9a54a'; g.beginPath(); g.ellipse(128, 56, 26, 20, 0, 0, 7); g.fill();
  g.fillStyle = '#b5242c'; for (const s of [-1, 1]) { g.beginPath(); g.moveTo(128, 70); g.quadraticCurveTo(128 + s * 90, 60, 128 + s * 70, 150); g.quadraticCurveTo(128 + s * 50, 110, 128, 90); g.fill(); }
  g.fillStyle = '#f2ecdc'; g.beginPath(); g.moveTo(86, 84); g.lineTo(170, 84); g.lineTo(170, 150); g.quadraticCurveTo(128, 206, 86, 150); g.closePath(); g.fill();
  g.fillStyle = '#b5242c'; g.beginPath(); g.moveTo(86, 84); g.lineTo(170, 84); g.lineTo(170, 112); g.lineTo(86, 112); g.fill();
  g.fillStyle = '#2b4f8a'; g.beginPath(); g.moveTo(128, 120); g.lineTo(146, 150); g.lineTo(110, 150); g.closePath(); g.fill();
  g.strokeStyle = '#c9a54a'; g.lineWidth = 5; g.beginPath(); g.moveTo(86, 84); g.lineTo(170, 84); g.lineTo(170, 150); g.quadraticCurveTo(128, 206, 86, 150); g.closePath(); g.stroke();
  g.fillStyle = '#d9c9a0'; for (const s of [-1, 1]) { g.beginPath(); g.ellipse(128 + s * 66, 150, 16, 46, s * 0.2, 0, 7); g.fill(); g.beginPath(); g.arc(128 + s * 70, 100, 13, 0, 7); g.fill(); }
  g.fillStyle = '#efe6cf'; g.fillRect(64, 206, 128, 22); g.fillStyle = '#5a4630'; g.font = '700 13px Georgia, serif'; g.textAlign = 'center'; g.fillText('BOROUGH ARMS', 128, 222);
});
// Tall sash windows with round heads, white frames and small panes.
const windowTex = () => tex(128, 320, (g, w, h) => {
  const shape = (i) => { g.beginPath(); g.moveTo(i, h); g.lineTo(i, 64); g.arc(64, 64, 64 - i, Math.PI, 0); g.lineTo(w - i, h); g.closePath(); };
  shape(0); g.fillStyle = '#f2ede2'; g.fill();
  shape(9); const sky = g.createLinearGradient(0, 0, 0, h); sky.addColorStop(0, '#cfe0ea'); sky.addColorStop(1, '#eef2ee'); g.fillStyle = sky; g.fill();
  g.save(); shape(9); g.clip(); g.strokeStyle = '#f2ede2'; g.lineWidth = 4;
  for (const x of [43, 85]) { g.beginPath(); g.moveTo(x, 20); g.lineTo(x, h); g.stroke(); }
  for (let y = 100; y < h; y += 44) { g.beginPath(); g.moveTo(0, y); g.lineTo(w, y); g.stroke(); }
  g.lineWidth = 8; g.beginPath(); g.moveTo(0, 196); g.lineTo(w, 196); g.stroke();
  g.restore();
});
const sandTex = () => tex(96, 96, (g, w) => {
  const r = rng(3);
  g.fillStyle = '#b39a6e'; g.fillRect(0, 0, w, w);
  for (let i = 0; i < 260; i++) { g.fillStyle = r() > 0.5 ? 'rgba(80,60,30,0.08)' : 'rgba(255,240,210,0.08)'; g.fillRect(r() * w, r() * w, 2, 2); }
  g.strokeStyle = 'rgba(70,110,85,0.35)'; g.lineWidth = 2; g.strokeRect(0, 0, w, w);
}, [2.6, 2.6]);

export const BURNLEY = {
  bounds: { W, ZF, ZB, H },
  bg: 0x2a1c18,
  mats: (L) => ({
    carpet: L(0xffffff, sandTex()),
    leather: L(0x2c2b2e),
    wood: L(0xb06a38),
    dark: L(0x6a3a1e),
    light: L(0xc98a52),
    green: L(0x2f6b4a),
    stone: L(0xffffff, stoneTex()),
    red: L(0xffffff, redTex()),
    cream: L(0xefe2b8),
    gold2: L(0xc9a54a),
    plum: L(0x5d4a78),
  }),
  office: { chair: { x: 0, z: -5.1, fy: 0.3, ry: 0 }, ce: { x: -0.95, z: -5.1, fy: 0.3, ry: 0 }, officer: { x: 0.95, z: -5.1, fy: 0.3, ry: 0 }, pupil: { x: -2.4, z: -2.9, fy: -0.5, ry: Math.PI, guest: true } },
  cams: { start: [0, 4.5, 9.0], chair: { pos: [0, 2.35, -4.55], look: [0, 0.8, 4.2] }, over: { pos: [0, 3.7, 8.5], look: [0, 0.3, 0] }, vote: [0, 0.9, 1.2] },
  screen: { w: 3.0, h: 1.69, x: 0, y: 2.25, z: ZF + 0.08 },
  build({ THREE, scene, put, box, cyl, plane, mergeGeometries }) {
    const MB = (map, extra) => new THREE.MeshLambertMaterial({ map, ...extra });

    /* The top table: a low platform, a long timber table, green leather chairs, the Mayor's the tallest. */
    box('dark', 7.6, 0.28, 2.1, 0, 0, -5.2);
    box('carpet', 7.6, 0.01, 2.1, 0, 0.28, -5.2);
    box('wood', 6.8, 0.74, 0.06, 0, 0.28, -4.32);
    box('dark', 6.9, 0.05, 0.8, 0, 1.02, -4.6);
    for (let i = -3; i <= 3; i++) box('light', 0.82, 0.5, 0.02, i * 0.96, 0.4, -4.28);
    for (let i = -3; i <= 3; i++) {
      const x = i * 0.95, tall = i === 0 ? 1.45 : 1.05;
      box('dark', 0.58, 0.42, 0.52, x, 0.28, -5.3);
      box('green', 0.56, 0.08, 0.5, x, 0.7, -5.3);
      box('green', 0.6, tall, 0.1, x, 0.74, -5.6);
      box('gold2', 0.62, 0.04, 0.12, x, 0.74 + tall, -5.6);
    }
    for (const x of [-1.9, 0, 1.9]) box('black', 0.42, 0.28, 0.03, x + 0.3, 1.07, -4.7);
    // The clerks' table in the well, in front of the top table.
    box('wood', 2.4, 0.04, 0.9, 0, 0.72, -3.0); box('dark', 2.2, 0.68, 0.8, 0, 0.04, -3.0);

    /* Walls: stone to the dado, red above, a dark frieze with gold letters, a gold cornice. */
    const walls = [[2 * W, 0, ZF, 0], [2 * W, 0, ZB, Math.PI], [ZB - ZF, -W, (ZB + ZF) / 2, Math.PI / 2], [ZB - ZF, W, (ZB + ZF) / 2, -Math.PI / 2]];
    const friezeMat = MB(friezeTex()), fr = [];
    for (const [w, x, z, ry] of walls) {
      plane('stone', w, DADO, x, DADO / 2, z, ry);
      plane('red', w, FRIEZE - DADO, x, (DADO + FRIEZE) / 2, z, ry);
      plane('cream', w, H - CORNICE, x, (CORNICE + H) / 2, z, ry);
      const g = new THREE.PlaneGeometry(w, CORNICE - FRIEZE); g.rotateY(ry); g.translate(x + Math.sin(ry) * 0.03, (FRIEZE + CORNICE) / 2, z + Math.cos(ry) * 0.03); fr.push(g);
    }
    scene.add(new THREE.Mesh(mergeGeometries(fr), friezeMat));
    // Dado rail and the cornice mouldings.
    box('gold2', 2 * W, 0.08, 0.1, 0, DADO, ZF + 0.05); box('gold2', 2 * W, 0.08, 0.1, 0, DADO, ZB - 0.05);
    for (const s of [-1, 1]) box('gold2', 0.1, 0.08, ZB - ZF, s * (W - 0.05), DADO, (ZB + ZF) / 2);
    box('gold2', 2 * W, 0.16, 0.24, 0, CORNICE, ZF + 0.12); box('gold2', 2 * W, 0.16, 0.24, 0, CORNICE, ZB - 0.12);
    for (const s of [-1, 1]) box('gold2', 0.24, 0.16, ZB - ZF, s * (W - 0.12), CORNICE, (ZB + ZF) / 2);

    /* The coat of arms above the top table, and the frame of the wall screen below it. */
    { const m = new THREE.Mesh(new THREE.PlaneGeometry(1.9, 1.9), new THREE.MeshBasicMaterial({ map: armsTex(), transparent: true, alphaTest: 0.1 })); m.position.set(0, 4.75, ZF + 0.05); scene.add(m); }
    box('black', 3.16, 1.85, 0.06, 0, 2.25 - 0.925, ZF + 0.03);
    box('dark', 3.6, 0.9, 0.35, 0, 0, ZF + 0.2);

    /* Arched windows down one long side. */
    { const geo = [];
      for (const z of [-2.6, 1.4, 5.4]) { const g = new THREE.PlaneGeometry(1.9, 4.75); g.rotateY(-Math.PI / 2); g.translate(W - 0.03, 3.85, z); geo.push(g); box('stone', 0.16, 0.12, 2.2, W - 0.08, 1.45, z); }
      scene.add(new THREE.Mesh(mergeGeometries(geo), new THREE.MeshBasicMaterial({ map: windowTex(), transparent: true, alphaTest: 0.5 }))); }

    /* The far wall: the main door in its timber surround with a pediment, and the public seats. */
    { const x = 4.6, z = ZB - 0.06;
      box('dark', 2.1, 3.1, 0.12, x, 0, z); box('wood', 1.6, 2.7, 0.04, x, 0, z - 0.08);
      for (const s of [-1, 1]) for (const y of [0.3, 1.5]) box('light', 0.6, 1.0, 0.02, x + s * 0.4, y, z - 0.11);
      box('dark', 2.5, 0.16, 0.24, x, 3.1, z - 0.06);
      { const g = new THREE.CylinderGeometry(1, 1, 0.2, 3, 1); g.rotateX(Math.PI / 2); g.rotateZ(Math.PI); g.scale(1.3, 0.5, 1); g.translate(x, 3.5, z - 0.08); put('dark', g); } }
    for (let i = 0; i < 9; i++) { const x = -6.2 + i * 0.95, z = ZB - 0.75; box('plum', 0.5, 0.08, 0.48, x, 0.44, z); box('plum', 0.5, 0.5, 0.07, x, 0.48, z + 0.22); box('black', 0.46, 0.44, 0.04, x, 0, z); }
    // A notice board and a plain door on the other long wall.
    box('dark', 1.3, 2.4, 0.08, -W + 0.04, 0, 6.4, Math.PI / 2); box('cream', 1.4, 0.9, 0.04, -W + 0.03, 1.4, 2.0, Math.PI / 2);

    /* The ceiling: a coved edge painted with lunettes, and a flat coffered field with a gilded rose. */
    const coveMat = MB(coveTex()); coveMat.map.wrapS = THREE.RepeatWrapping;
    const quad = (a, b, c, d, rep) => {
      const g = new THREE.BufferGeometry();
      g.setAttribute('position', new THREE.Float32BufferAttribute([...a, ...b, ...c, ...a, ...c, ...d], 3));
      g.setAttribute('uv', new THREE.Float32BufferAttribute([0, 0, rep, 0, rep, 1, 0, 0, rep, 1, 0, 1], 2));
      g.computeVertexNormals(); return g;
    };
    const y0 = CORNICE + 0.16, i = COVE, xi = W - i, zf = ZF + i, zb = ZB - i;
    const cove = [
      quad([-W, y0, ZF], [W, y0, ZF], [xi, H, zf], [-xi, H, zf], 6),
      quad([W, y0, ZB], [-W, y0, ZB], [-xi, H, zb], [xi, H, zb], 6),
      quad([-W, y0, ZB], [-W, y0, ZF], [-xi, H, zf], [-xi, H, zb], 6),
      quad([W, y0, ZF], [W, y0, ZB], [xi, H, zb], [xi, H, zf], 6),
    ];
    scene.add(new THREE.Mesh(mergeGeometries(cove), Object.assign(coveMat, { side: THREE.DoubleSide })));
    { const g = new THREE.PlaneGeometry(2 * xi, zb - zf); g.rotateX(Math.PI / 2); g.translate(0, H, (zb + zf) / 2);
      const m = new THREE.Mesh(g, MB(cofferTex(), { side: THREE.DoubleSide })); scene.add(m); }
    { const zc = (zb + zf) / 2;
      box('cream', 5.4, 0.12, 7.2, 0, H - 0.12, zc);
      box('gold2', 5.0, 0.04, 6.8, 0, H - 0.16, zc);
      box('cream', 4.8, 0.05, 6.6, 0, H - 0.21, zc);
      cyl('gold2', 1.25, 1.25, 0.08, 0, H - 0.3, zc, 40); cyl('cream', 1.0, 1.0, 0.06, 0, H - 0.34, zc, 40);
      cyl('gold2', 0.55, 0.3, 0.3, 0, H - 0.64, zc, 20);
      for (const s of [-1, 1]) for (const t of [-1, 1]) { box('gold2', 0.2, 0.06, 1.8, s * 1.9, H - 0.24, zc + t * 2.1); box('gold2', 1.4, 0.06, 0.2, s * 1.6, H - 0.24, zc + t * 3.0); } }
    // Downlights in the coffers, which light the restored room.
    { const g = []; for (let x = -4.5; x <= 4.5; x += 3) for (let z = -3; z <= 7; z += 2.5) if (Math.abs(x) > 2 || Math.abs(z - 1.45) > 3.4) { const c = new THREE.CircleGeometry(0.09, 10); c.rotateX(Math.PI / 2); c.translate(x, H - 0.02, z); g.push(c); }
      scene.add(new THREE.Mesh(mergeGeometries(g), new THREE.MeshBasicMaterial({ color: 0xfff6dd, side: THREE.DoubleSide }))); }
  },
};
