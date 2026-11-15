// Budget Council: the page, shared by every council. A council page calls mount(pack) with:
//   data     { chamber, minutes, amendments, papers, sources, priors? }
//   content  its words module (getBeat, FIRST, whoLabel, ENDINGS, counterfactual, DEBRIEF_QUESTIONS, ROLE_CARDS)
//   ui       its page words (ui.js): start screen, the real or the next meeting, how it works, terms, theme
// The engine is engine.js; the room is src/games/ylv/chamber.js with the council's architecture; the
// look, the menu and the sheets are the chamber kit (src/games/chamber-kit/). The guide to adding a
// council is docs/COUNCIL-SIMULATOR-KIT.md. It collects, stores and sends nothing.
import { createEngine } from './engine.js';
import { makePlan, ROOMS } from '../rooms/plans.js';
import { icon, iconFor } from '../chamber-kit/icons.js';
import { createSheet, menuList } from '../chamber-kit/sheet.js';

export function mount({ data, content, ui }) {
  const { chamber, minutes, amendments, papers, sources } = data;
  const { whoLabel, ENDINGS, counterfactual, DEBRIEF_QUESTIONS, ROLE_CARDS } = content;
  const E = createEngine(data, content);
  const G = E.G;
  const N = chamber.seats;
  const $ = (s, r = document) => r.querySelector(s);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
  const SIDES = ['admin', ...(ui.sides || E.OPP)];
  const CHAIR = ui.chairTitle || 'The Chairman';
  const ROOT = $('.bc');
  const sheet = createSheet(ROOT);
  const wide = () => matchMedia('(min-width: 861px)').matches;

  /* ---- words a councillor may want explained. [name, pattern, meaning] ---- */
  const TERMS = ui.terms;
  const TERM_RE = new RegExp('\\b(?:' + TERMS.map((t) => `(${t[1]})`).join('|') + ')\\b', 'g');
  function words(text) {
    const seen = new Set();
    return esc(text).replace(TERM_RE, (hit, ...g) => {
      const i = g.findIndex((x, k) => k < TERMS.length && x != null);
      if (i < 0 || seen.has(i)) return hit;
      seen.add(i);
      return `<button type="button" class="bc-term" data-term="${i}">${hit}</button>`;
    });
  }

  /* ---- run state and the link that replays it ---- */
  const S = { tab: 0, side: ui.defaultSide || E.OPP[0], seed: newSeed(), fac: false, run: null, d3: true, cam: 'seat', hud: wide() };
  function newSeed() { return Math.floor(Math.random() * 90000) + 10000; }
  (function fromHash() {
    const h = new URLSearchParams(location.hash.slice(1));
    const seed = h.get('seed'), side = h.get('side');
    if (seed && /^\d{1,9}$/.test(seed)) S.seed = +seed;
    if (side && SIDES.includes(side)) S.side = side;
    if (h.get('fac') === '1') S.fac = true;
  })();
  const link = () => `${location.pathname}#seed=${S.seed}&side=${S.side}${S.fac ? '&fac=1' : ''}`;

  /* ---- the room: the 3D room behind the stage, or a flat plan coloured by group ---- */
  const ROOM = ROOMS[ui.room];
  const PLAN = makePlan(ROOM);
  const NB = PLAN.length; // benches; the officers' figures come after them
  const LEAD = E.ADMIN[0];
  const sideGroup = () => (S.side === 'admin' ? LEAD : S.side);
  const mySeat = (s) => E.SEATS.indexOf(s ? s.group : sideGroup());
  function seatOf(who, s) {
    const nth = (g, n) => { let c = -1; const i = E.SEATS.findIndex((x) => x === g && ++c === n); return i < 0 ? null : i; };
    if (!who) return null;
    if (who === 'me' || who === 'you' || (who === 'cm' && s.role === 'admin')) return 'me';
    if (who === 'chair') return NB;
    if (who === 'ce') return NB + 1;
    if (who === 'mo' || who === 's151' || who === 'officer') return NB + 2;
    if (who === 'leader') return ui.leaderGroup ? nth(ui.leaderGroup, 0) : nth(LEAD, s.role === 'admin' ? 1 : 0);
    if (who === 'cm') return ui.leaderGroup ? nth(LEAD, s.role === 'admin' ? 1 : 0) : nth(LEAD, 2);
    if (who === 'heckler' || who.startsWith('back:' + LEAD)) return nth(LEAD, Math.max(3, Math.floor(G[LEAD].seats * 0.8)));
    if (who === 'seconder') return nth(s.group, 1);
    if (who.startsWith('opp:')) return nth(who.slice(4), who.slice(4) === s.group ? 1 : 0);
    if (who.startsWith('back:')) return nth(who.slice(5), Math.min(2, G[who.slice(5)].seats - 1));
    return null;
  }
  const live = () => (S.tab === 1 && S.run && !S.run.ended ? S.run : null);
  function plan(s, tally) {
    const me = mySeat(s);
    const sp = s ? seatOf(E.view(s).who, s) : null;
    const off = ROOM.office;
    const at = (k) => ({ x: off[k][0], y: off[k][1] });
    const spAt = sp === 'me' ? PLAN[me] : typeof sp === 'number' && sp < NB ? PLAN[sp] : sp === NB ? at('chair') : sp === NB + 1 ? at('ce') : sp === NB + 2 ? at('officer') : null;
    const dots = PLAN.map((p, i) => {
      const g = E.SEATS[i];
      const t = tally ? tally[i] : null;
      const fill = t === 'for' ? '#2e6b30' : t === 'against' ? '#b3261e' : t === 'abstain' ? '#c9c4b6' : t === 'absent' ? 'none' : G[g].colour;
      return `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="6.4" fill="${fill}" stroke="${t === 'absent' ? '#aeb4ad' : '#3b2a1a'}" stroke-width="${i === me ? 3.5 : 1.2}" ${i === me ? 'class="me"' : ''}><title>${esc(G[g].short)}${t ? ': ' + t : ''}</title></circle>`;
    }).join('');
    const [cx, cy] = off.chair;
    return `<svg class="bc-plan" viewBox="0 0 600 372" role="img" aria-label="Plan of the council chamber, seats coloured by group${tally ? ', showing how each seat voted' : ''}. The seating is invented, not the council's real plan.">
      <rect x="6" y="6" width="588" height="360" rx="10" fill="#f3ecdb" stroke="#aeb4ad"/>
      ${ui.planFurniture}
      <text x="300" y="362" text-anchor="middle" class="lbl">${esc(ui.galleryLabel || 'Public gallery')}</text>
      ${dots}
      <circle cx="${cx}" cy="${cy}" r="8" fill="#d9bd83"><title>${esc(CHAIR)}</title></circle>
      <circle cx="${off.ce[0]}" cy="${off.ce[1]}" r="7" fill="#d9bd83"><title>The Chief Executive</title></circle>
      <circle cx="${off.officer[0]}" cy="${off.officer[1]}" r="7" fill="#d9bd83"><title>An officer</title></circle>
      ${spAt ? `<circle class="talk" cx="${spAt.x.toFixed(1)}" cy="${spAt.y.toFixed(1)}" r="13"/>` : ''}
    </svg>`;
  }
  let room = null, roomHost = null, roomLoading = null;
  function roomState() {
    const s = live();
    if (!s) return { seat: mySeat(S.run && S.tab !== 0 ? S.run : null), speaker: null, tally: null, mode: 'over', pick: false, item: ui.screenIdle || 'Budget Meeting', who: '' };
    const v = E.view(s);
    const vt = v.tally && s.votes[v.tally];
    return { seat: mySeat(s), speaker: seatOf(v.who, s), tally: vt ? vt.tally : null, mode: vt ? 'chair' : S.cam, pick: false, item: agendaName(v.item), who: whoLabel(v.who, s, E.api) };
  }
  function showRoom() {
    const slot = $('#bc-room');
    if (!S.d3) {
      const s = live();
      const v = s && E.view(s);
      slot.innerHTML = `<div class="ck-flat">${plan(s, v?.tally && s.votes[v.tally]?.tally)}</div>`;
      return;
    }
    if (room) {
      if (roomHost.parentNode !== slot) slot.replaceChildren(roomHost);
      room.set(roomState());
      return;
    }
    if (roomLoading) return;
    roomLoading = Promise.all([import('../ylv/chamber.js'), ui.architecture ? ui.architecture() : null]).then(([mod, arch]) => {
      roomHost = document.createElement('div');
      roomHost.className = 'ck-roomhost';
      roomHost.tabIndex = 0;
      roomHost.setAttribute('role', 'img');
      roomHost.setAttribute('aria-label', 'The council chamber as a room, figures dressed in their group\'s colour. Drag, or use the arrow keys, to look round.');
      const opts = { plan: PLAN, reduced: matchMedia('(prefers-reduced-motion: reduce)').matches, colours: E.SEATS.map((g) => G[g].colour), title: 'BUDGET MEETING', room: ui.room };
      if (arch) opts.architecture = arch;
      $('#bc-room').replaceChildren(roomHost);
      room = mod.createRoom(roomHost, opts);
      showRoom();
    }).catch((err) => { console.error(err); S.d3 = false; room = null; showRoom(); });
  }

  /* ---- agenda and the steps along the top ---- */
  function agenda() { return ui.agenda(E.OPP.map((g) => G[g].short)); }
  const agendaName = (i) => (i == null ? (ui.prepShort || 'The week before') : agenda()[i]);
  const PHASES = ['Choose a side', ui.prepShort || 'The week before', 'The meeting', 'The debrief'];
  function steps() {
    const s = S.run;
    const li = (label, i, state) => `<li class="${state}"${state === 'on' ? ' aria-current="step"' : ''}><i>${state === 'done' ? '✓' : i + 1}</i>${esc(label)}</li>`;
    if (s && !s.ended && S.tab !== 0) {
      const v = E.view(s);
      if (v.phase === 'meeting') return agenda().map((a, i) => li(a, i, i === v.item ? 'on' : v.item != null && i < v.item ? 'done' : '')).join('');
      return PHASES.map((p, i) => li(p, i, i === 1 ? 'on' : i < 1 ? 'done' : '')).join('');
    }
    const at = s && s.ended && S.tab !== 0 ? 3 : 0;
    return PHASES.map((p, i) => li(p, i, i === at ? 'on' : i < at ? 'done' : '')).join('');
  }

  /* ---- small pieces ---- */
  const src = (id, at) => `<span class="bc-src">${esc(sources[id].cite)}${at ? ', ' + esc(at) : ''}</span>`;
  function ruleCard(id) {
    const r = papers.rules[id];
    return r ? `<aside class="bc-rule"><b>The rule</b> ${words(r.text)} ${src(r.src, r.at)}</aside>` : '';
  }
  function figChips(ids) {
    return ids ? `<ul class="bc-figs">${ids.map((k) => { const f = papers.figures[k]; return `<li><b>${esc(f.value)}</b><span>${esc(f.what)}</span>${src(f.src, f.at)}</li>`; }).join('')}</ul>` : '';
  }
  function hud(s) {
    const bar = (label, v, max, note) => `<div class="ck-meter"><span>${label}</span><em>${note}</em><i style="--w:${Math.round((v / max) * 100)}%"></i></div>`;
    const moodWord = ['bitter', 'hostile', 'tense', 'businesslike', 'cordial', 'warm', 'generous'][s.v.mood + 3];
    const W151 = ui.s151Words || { approved: 'approved in writing by the Section 151 Officer', refused: 'not approved', none: 'not yet approved' };
    const s151 = s.role === 'admin' ? '' : `<p>Your amendment: ${s.amend.kind === 'none' ? 'none, a statement only' : s.amend.kind == null ? 'not chosen yet' : W151[s.amend.s151] || W151.none}${s.amend.notice === 'late' ? ', and late' : ''}.</p>`;
    return `<details class="ck-hud ck-glass" ${S.hud ? 'open' : ''}><summary>Where you stand</summary>
      ${bar('Your credibility', s.v.cred, 100, s.v.cred)}
      ${bar('Chamber mood', s.v.mood + 3, 6, moodWord)}
      ${bar(`${esc(CHAIR)}'s patience`, s.v.chair, 5, s.v.chair + ' of 5')}
      ${bar('Your group\'s discipline', s.v.disc[s.group], 100, s.v.disc[s.group])}
      ${s151}
      ${s.record.length ? `<p><b>On the record:</b> ${esc(s.record[s.record.length - 1])}</p>` : ''}
      <p>Meeting clock ${Math.floor(s.clock / 60)}h ${String(s.clock % 60).padStart(2, '0')}m. ${esc(ui.clockNote || '')}</p></details>`;
  }
  const H = { esc, src, words, figChips, E, G, data, amendCard: (h) => amendCard(h) };
  function amendCard(h) {
    const a = amendments.amendments?.[h];
    if (!a) return '';
    const rows = a.parts.map((id) => { const p = amendments.parts[id]; return `<tr><td>${esc(p.label)}</td>${p.cost.map((c) => `<td class="n">${c ? (c < 0 ? '(' + c.toFixed(3).slice(1) + ')' : c.toFixed(3)) : ''}</td>`).join('')}</tr>`; }).join('');
    return `<details class="bc-card"><summary>The ${esc(G[h].short)} amendment, line by line (£m)</summary>
      <div class="bc-tablewrap"><table><thead><tr><th>Line</th>${amendments.years.map((y) => `<th class="n">${y}</th>`).join('')}</tr></thead><tbody>${rows}</tbody></table></div>
      <p class="bc-small">Savings in brackets. ${ui.amendSource(a, H)}</p></details>`;
  }
  function builder(s) {
    const b = E.buildCheck(s);
    const sel = new Set(s.amend.parts);
    const fmt = (c) => (c ? (c < 0 ? '−' : '') + Math.abs(c).toFixed(3) : '0');
    const sets = ui.partGroups(E, G).map(({ legend, ids }) => `<fieldset class="bc-parts"><legend>${esc(legend)}</legend>${ids.map((id) => { const p = amendments.parts[id]; return `<label><input type="checkbox" data-part="${id}" ${sel.has(id) ? 'checked' : ''}><span>${esc(p.label)}</span><em>${p.cost.map(fmt).join(' · ')}</em></label>`; }).join('')}</fieldset>`).join('');
    const yr = (k, f = (x) => (x < 0 ? '(' + Math.abs(x).toFixed(3) + ')' : x.toFixed(3))) => b.years.map((y) => `<td class="n">${f(y[k])}</td>`).join('');
    return `<div class="bc-build">
      <aside class="bc-card bc-balance"><h3>Running balance (£m)</h3>
        <div class="bc-tablewrap"><table><thead><tr><th></th>${amendments.years.map((y) => `<th class="n">${y}</th>`).join('')}</tr></thead><tbody>
          ${ui.balanceRows.map(([k, label]) => `<tr><td>${esc(label)}</td>${yr(k)}</tr>`).join('')}
          <tr class="${b.balanced ? 'ok' : 'gap'}"><td>Still to find</td>${yr('gap')}</tr>
        </tbody></table></div>
        <p class="bc-small">${ui.balanceNote(b)}</p>
        <details><summary class="bc-small">Where the limits come from</summary><p class="bc-small">${Object.values(amendments.funds).filter((f) => f.label).map((f) => `${esc(f.label)}: ${esc(f.at)}.`).join(' ')}</p></details>
      </aside>
      <div class="bc-stack">${sets}<p class="bc-small">Costs are £m for ${amendments.years.join(', ')}. ${ui.partsSource(H)}</p></div>
      <p class="bc-gapbar ${b.balanced && !b.empty ? 'ok' : 'gap'}" aria-hidden="true">Still to find: ${b.years.map((y, i) => `${amendments.years[i]} ${y.gap.toFixed(3)}`).join(' · ')}</p></div>`;
  }
  function tallyBox(v) {
    if (!v) return '';
    const groups = chamber.groups.map((g) => { const x = v.byGroup[g.id] || {}; return `<tr><td><i class="bc-dot" style="background:${g.colour}"></i>${esc(g.short)}</td><td class="n">${x.for || 0}</td><td class="n">${x.against || 0}</td><td class="n">${x.abstain || 0}</td><td class="n">${x.out || 0}</td></tr>`; }).join('');
    const voting = v.for + v.against;
    const pc = (n) => (voting ? (n / voting) * 100 : 0).toFixed(1);
    return `<div class="bc-card"><h3>The recorded vote${v.room ? ', as counted in your room' : ''}</h3>
      <p class="bc-result ${v.carried ? 'yes' : 'no'}">${v.for} for, ${v.against} against <b>${v.carried ? 'carried' : 'lost'}</b></p>
      <div class="ck-votebar" role="img" aria-label="${v.for} for and ${v.against} against; the line marks half of those voting"><span class="f" style="width:${pc(v.for)}%"></span><span class="a" style="width:${pc(v.against)}%"></span></div>
      <p class="ck-votekey"><span><i style="background:var(--ck-yes)"></i>For ${v.for}</span><span><i style="background:var(--ck-no)"></i>Against ${v.against}</span><span>Abstaining ${v.abstain}</span><span>The line is half of those voting</span></p>
      <p class="bc-small">A simple majority of those present and voting decides it: ${voting} voted for or against, so ${Math.floor(voting / 2) + 1} were needed. Abstentions count towards neither side. ${ui.majoritySource(H)}</p>
      ${v.room ? '' : `<details><summary>How each group voted</summary><div class="bc-tablewrap"><table><thead><tr><th>Group</th><th class="n">For</th><th class="n">Against</th><th class="n">Abstain</th><th class="n">Absent or not voting</th></tr></thead><tbody>${groups}</tbody></table></div></details>`}
      ${S.fac ? `<details><summary>Use the hands counted in your room instead</summary><p class="bc-hands"><label>For <input id="bc-h-f" type="number" min="0" max="${N}" inputmode="numeric" value="${v.for}"></label><label>Against <input id="bc-h-a" type="number" min="0" max="${N}" inputmode="numeric" value="${v.against}"></label><label>Abstain <input id="bc-h-n" type="number" min="0" max="${N}" inputmode="numeric" value="${v.abstain}"></label><button class="ck-btn ghost" data-room="${v.id}">Use these</button></p></details>` : ''}
    </div>`;
  }
  const choiceTile = (c, alone) => `<button type="button" class="ck-choice${alone ? ' go' : ''}" data-choice="${esc(c.id)}"><span class="ico">${icon(alone ? 'arrow' : iconFor(c.label))}</span><span>${esc(c.label)}</span>${icon(alone ? 'play' : 'arrow')}</button>`;
  const camNames = () => ({ seat: 'Your seat', chair: ui.benchLabel || 'Chairman\'s bench', over: ui.galleryLabel || 'Public gallery' });
  const CAM_ICON = { seat: 'seat', chair: 'gavel', over: 'gallery' };

  /* ---- views: each returns the layer's mode and its contents ---- */
  const views = {
    0() {
      const side = (id) => {
        const g = id === 'admin' ? G[LEAD] : G[id];
        const name = id === 'admin' ? 'The Administration' : g.short;
        const seats = id === 'admin' ? E.ADMIN.reduce((n, k) => n + G[k].seats, 0) : g.seats;
        const blurb = id === 'admin' ? ui.adminBlurb(E, G) : ui.oppBlurb(g);
        return `<button type="button" class="ck-side" style="--g:${g.colour}" data-side="${id}" aria-pressed="${S.side === id}"><b>${esc(name)}</b><em>${seats} seat${seats === 1 ? '' : 's'}</em><span>${esc(blurb)}</span></button>`;
      };
      return { mode: 'start', html: `
        <div class="ck-glass ck-hero">${ui.startIntro(H)}</div>
        <div class="ck-panel">
          <fieldset class="ck-sides"><legend>Which side of the chamber?</legend>${SIDES.map(side).join('')}</fieldset>
          <div class="ck-field" role="group" aria-labelledby="bc-how-l"><span id="bc-how-l">How are you playing?</span>
            <div class="ck-seg"><button type="button" data-fac="0" aria-pressed="${!S.fac}">On my own</button><button type="button" data-fac="1" aria-pressed="${S.fac}">Training session, one screen</button></div></div>
          ${S.fac ? `<div class="bc-card" id="bc-roles"><h3>Role cards</h3><p class="bc-small">Hand these out. The screen shows the words; people in the room can read them or use their own. At each recorded vote you can type in the hands counted in the room.</p><dl class="bc-roles">${ROLE_CARDS.map((r) => `<div><dt>${r[0]}</dt><dd>${esc(r[1])}</dd></div>`).join('')}</dl><p><button class="ck-btn ghost" data-print="#bc-roles">${icon('print')}Print the role cards</button></p></div>` : ''}
          <details><summary>Meeting number ${S.seed}</summary><p class="bc-small"><label for="bc-seed">Meeting number</label> <input id="bc-seed" type="number" min="1" max="999999999" inputmode="numeric" value="${S.seed}"><br>The same number, side and choices always give the same meeting, so a group can replay one together.</p></details>
          <button class="ck-btn big" data-start>${icon('play')}${esc(ui.startLabel || 'Start the week before')}</button>
        </div>
        <div class="ck-glass ck-hero ck-span bc-credit">${ui.startFooter(H)}</div>` };
    },
    1() {
      const s = S.run;
      if (!s) return { mode: 'read', html: `<section class="ck-panel"><h2>The meeting</h2><p>Choose a side first.</p><p><button class="ck-btn" data-tab="0">${icon('people')}Choose a side</button></p></section>` };
      if (s.ended) return debrief(s);
      const v = E.view(s);
      const vt = v.tally && s.votes[v.tally];
      const who = whoLabel(v.who, s, E.api);
      const names = camNames();
      const cams = vt ? `<span class="ck-chip note">${icon('gavel')}The vote, from the ${esc(names.chair.replace(/^./, (c) => c.toLowerCase()))}</span>`
        : ['seat', 'chair', 'over'].map((c) => `<button type="button" class="ck-chip" data-cam="${c}" aria-pressed="${S.cam === c}" aria-label="${esc(names[c])}">${icon(CAM_ICON[c])}<span>${esc(names[c])}</span></button>`).join('');
      const say = v.say ? `<div class="ck-say ck-glass" id="bc-screen">${who ? `<span class="who">${esc(who)}<small> · words written for the simulator</small></span>` : ''}<q>${words(v.say)}</q></div>` : '<div></div>';
      const alone = v.choices.length === 1;
      return { mode: 'meet', wide: v.kind === 'build', html: `
        <div class="ck-pass">
          <div class="ck-passtop">${hud(s)}<div class="ck-chips">${S.d3 ? cams : ''}<button type="button" class="ck-chip" data-d3="${S.d3 ? 0 : 1}" aria-label="${S.d3 ? 'Flat plan' : 'Room in 3D'}">${icon(S.d3 ? 'layers' : 'cube')}<span>${S.d3 ? 'Flat plan' : 'Room in 3D'}</span></button></div></div>
          ${say}
        </div>
        <div class="ck-panel" id="bc-panel">
          <p class="ck-eyebrow">${s.role === 'admin' ? 'Administration' : esc(G[s.group].short)} · ${esc(agendaName(v.item))} · Meeting ${s.seed}</p>
          ${v.plain ? `<p class="bc-plain">${words(v.plain)}</p>` : ''}
          ${v.kind === 'build' ? builder(s) : ''}
          ${v.card && v.group ? amendCard(v.group) : ''}
          ${figChips(v.figs)}
          ${vt ? tallyBox(vt) : ''}
          ${v.rule ? ruleCard(v.rule) : ''}
          <div class="ck-choices" role="group" aria-label="Your choice">${alone ? '' : '<p class="ck-eyebrow">What do you do?</p>'}${v.choices.map((c) => choiceTile(c, alone)).join('')}</div>
        </div>` };
    },
    2() { return { mode: 'read', html: `<div class="ck-panel">${ui.tab2View(H)}${backRow()}</div>` }; },
    3() {
      return { mode: 'read', html: `<section class="ck-panel bc-how">
        <h2>How it works</h2>
        ${ui.howIntro(H)}
        <h3>How you are scored</h3>
        <p>Not on winning. ${esc(ui.scoreWhy)} You are scored out of 10 on three things: procedure (did you follow the rules), good faith (did you answer questions and argue with ideas, not people) and influence (did you change anything, or get anything on the record).</p>
        <h3>The rules used</h3>
        ${rulesList()}
        <h3>Words</h3>
        ${termsList()}
        <h3>Where the idea came from</h3>
        <p>The idea of a virtual council meeting came from Wayne Walsh, who lives in Burnley. The design, the wording and any mistakes are Tom Pickup's.</p>
        <h3>Your information</h3>
        <p>There are no accounts and no text boxes. Nothing is stored or sent. The link to a meeting holds its number, the side and whether it is a training session.</p>
        <h3>Sources</h3>
        <ul class="bc-sources">${Object.entries(sources).filter(([k]) => !k.startsWith('_')).map(([, x]) => `<li>${esc(x.text)}${x.url ? ` Available at: <a href="${esc(x.url)}">${esc(x.url.replace(/^https?:\/\//, '').split('/')[0])}</a>.` : ''}</li>`).join('')}</ul>
        ${backRow()}
      </section>` };
    },
  };
  const rulesList = () => `<ul class="bc-rules">${Object.values(papers.rules).map((r) => `<li>${esc(r.text)} ${src(r.src, r.at)}</li>`).join('')}</ul>`;
  const termsList = () => `<dl>${TERMS.slice().sort((a, b) => a[0].localeCompare(b[0])).map((t) => `<dt>${t[0]}</dt><dd>${esc(t[2])}</dd>`).join('')}</dl>`;
  const backRow = () => `<p class="ck-row">${S.run && !S.run.ended ? `<button class="ck-btn" data-tab="1">${icon('arrow')}Back to the meeting</button>` : `<button class="ck-btn" data-tab="0">${icon('people')}Choose a side</button>`}</p>`;

  function debrief(s) {
    const e = ENDINGS[s.ended];
    const sc = s.v.score;
    const moved = s.log.filter((l) => /^(lean|disc|cred|mood|chair|score)/.test(l.tag) || l.tag.startsWith('flag:')).slice(-10);
    const ring = (label, v) => `<div class="ck-ring" role="img" aria-label="${label}: ${v} out of 10"><b style="--v:${v}">${v}<small>/10</small></b>${label}</div>`;
    return { mode: 'end', html: `<section class="ck-panel bc-debrief">
      <p class="ck-eyebrow">Meeting ${s.seed} · ${s.role === 'admin' ? 'Administration' : esc(G[s.group].short)}</p>
      <h2>${esc(e.title)}</h2>
      <p class="bc-lede">${esc(e.text)}</p>
      <div class="bc-two">
        <div class="bc-card"><h3>Your scores</h3><div class="ck-rings">${ring('Procedure', sc.proc)}${ring('Good faith', sc.faith)}${ring('Influence', sc.infl)}</div>
          <p class="bc-small">${esc(ui.scoreNote || 'Scored on conduct, not on winning.')}</p></div>
        <div class="bc-card"><h3>What you could have done differently</h3><p>${esc(counterfactual(s))}</p>
          ${s.record.length ? `<h4>On the record</h4><ul>${s.record.map((r) => `<li>${esc(r)}</li>`).join('')}</ul>` : ''}</div>
      </div>
      <div class="bc-card"><h3>What moved things</h3><ol class="bc-moved">${moved.map((l) => `<li>${esc(l.text)}</li>`).join('') || '<li>Nothing much: the meeting went to form.</li>'}</ol></div>
      ${ui.debriefVotes(s, H)}
      <div class="bc-card"><h3>Talk about it</h3><ol class="bc-debriefq">${DEBRIEF_QUESTIONS.map((q) => `<li>${esc(q)}</li>`).join('')}</ol>
        <p class="bc-small">${ui.furtherReading(H)}</p></div>
      <p class="ck-row"><button class="ck-btn" data-again>${icon('restart')}Play this meeting again</button> <button class="ck-btn ghost" data-other>${icon('offer')}Try the other side</button> <button class="ck-btn ghost" data-copy>${icon('link')}Copy a link to this meeting</button></p>
    </section>` };
  }

  /* ---- the menu ---- */
  function openMenu() {
    const names = camNames();
    const meeting = [];
    if (S.run && !S.run.ended && S.tab !== 1) meeting.push({ icon: 'arrow', label: 'Back to the meeting', attrs: 'data-tab="1"' });
    if (S.run && S.run.ended && S.tab !== 1) meeting.push({ icon: 'flag', label: 'Back to the debrief', attrs: 'data-tab="1"' });
    meeting.push({ icon: 'people', label: S.run ? 'Choose a side and start again' : 'Choose a side', attrs: 'data-tab="0"' });
    if (S.run) meeting.push({ icon: 'link', label: 'Copy a link to this meeting', note: 'Same number, same side, same meeting', attrs: 'data-copy' });
    if (S.fac) meeting.push({ icon: 'print', label: 'Print the role cards', attrs: 'data-tab="0" data-then-print="#bc-roles"' });
    const read = [
      { icon: 'doc', label: ui.tab2, attrs: 'data-tab="2"' },
      { icon: 'info', label: 'How it works', note: 'What is real, how you are scored, sources', attrs: 'data-tab="3"' },
      { icon: 'book', label: 'Words explained', attrs: 'data-sheet="words"' },
      { icon: 'gavel', label: 'The rules used', attrs: 'data-sheet="rules"' },
    ];
    const roomItems = [
      ...(S.d3 ? ['seat', 'chair', 'over'].map((c) => ({ icon: CAM_ICON[c], label: names[c], attrs: `data-cam="${c}"`, pressed: S.cam === c })) : []),
      { icon: S.d3 ? 'layers' : 'cube', label: S.d3 ? 'Show the flat plan' : 'Show the room in 3D', attrs: `data-d3="${S.d3 ? 0 : 1}"` },
    ];
    sheet.open('Menu', menuList([{ title: 'The meeting', items: meeting }, { title: 'Read', items: read }, { title: 'The room', items: roomItems }]) + `<p class="bc-small" style="margin-top:14px">The room and the seating are modelled by eye, not measured. Figures dressed in a group's colour stand for no real person.</p>`);
  }

  /* ---- render and events ---- */
  function render(focus) {
    const view = views[S.tab]();
    const layer = $('#bc-app');
    ROOT.dataset.mode = view.mode;
    layer.dataset.mode = view.mode;
    layer.classList.toggle('wide', !!view.wide);
    layer.innerHTML = view.html;
    $('#bc-steps').innerHTML = steps();
    const on = $('#bc-steps .on');
    if (on) $('#bc-steps').scrollLeft = on.offsetLeft - 40;
    if (focus) { const el = $(focus); if (el) el.focus({ preventScroll: true }); }
    const q = $('#bc-screen');
    if (q) $('#bc-sr').textContent = q.textContent;
    showRoom();
  }
  const toStage = () => { const b = $('#bc-steps'); const top = b.getBoundingClientRect().top + scrollY - (parseFloat(getComputedStyle(ROOT).getPropertyValue('--ck-top')) || 64); if (scrollY > top + 4 || !wide()) window.scrollTo({ top: Math.max(0, top), behavior: 'auto' }); };
  function start() {
    const seedIn = $('#bc-seed');
    if (seedIn && /^\d{1,9}$/.test(seedIn.value)) S.seed = +seedIn.value;
    S.run = E.start({ seed: S.seed, role: S.side === 'admin' ? 'admin' : 'opp', group: S.side === 'admin' ? undefined : S.side, facilitator: S.fac });
    history.replaceState(null, '', link());
    S.tab = 1; render('[data-choice]');
    toStage();
  }
  function printOnly(sel) {
    const el = $(sel); if (!el) return;
    el.classList.add('bc-printme'); document.documentElement.classList.add('bc-print');
    addEventListener('afterprint', () => { el.classList.remove('bc-printme'); document.documentElement.classList.remove('bc-print'); }, { once: true });
    window.print();
  }
  $('#bc-menu').addEventListener('click', openMenu);
  ROOT.addEventListener('click', (e) => {
    const sum = e.target.closest('.ck-hud > summary');
    if (sum) { S.hud = !sum.parentElement.open; return; }
    const b = e.target.closest('button');
    if (!b || b.id === 'bc-menu') return;
    const d = b.dataset;
    const inSheet = !!b.closest('dialog');
    if (inSheet && (d.tab != null || d.cam || d.d3 != null || 'copy' in d || d.sheet)) sheet.close();
    if (d.tab != null) {
      S.tab = +d.tab; render(); toStage();
      if (d.thenPrint) printOnly(d.thenPrint);
    }
    else if (d.side) { S.side = d.side; render(`[data-side="${d.side}"]`); }
    else if (d.fac != null) { S.fac = d.fac === '1'; render(`[data-fac="${d.fac}"]`); }
    else if ('start' in d) start();
    else if (d.choice) {
      S.run = E.act(S.run, d.choice);
      if (S.run.ended) history.replaceState(null, '', link());
      render('[data-choice], [data-again]');
      toStage();
    }
    else if (d.term != null) { const t = TERMS[+d.term]; sheet.open(t[0], `<p>${esc(t[2])}</p><p class="bc-small">All the words are in the menu, under Words explained.</p>`); }
    else if (d.sheet === 'words') sheet.open('Words explained', termsList(), { wide: true });
    else if (d.sheet === 'rules') sheet.open('The rules used', rulesList(), { wide: true });
    else if (d.d3 != null) { S.d3 = d.d3 === '1'; render(); }
    else if (d.cam) { S.cam = d.cam; render(inSheet ? null : `[data-cam="${d.cam}"]`); }
    else if (d.room) {
      const n = (id) => Math.max(0, Math.min(N, Math.floor(+$('#bc-h-' + id).value || 0)));
      S.run = E.roomVote(S.run, d.room, { for: n('f'), against: n('a'), abstain: n('n') });
      render('[data-choice]');
    }
    else if (d.print) printOnly(d.print);
    else if ('again' in d) start();
    else if ('other' in d) { S.side = S.side === 'admin' ? E.OPP[0] : 'admin'; S.seed = newSeed(); S.tab = 0; render(`[data-side="${S.side}"]`); toStage(); }
    else if ('copy' in d) {
      const say = (t) => { if (!inSheet) b.lastChild.textContent = t; $('#bc-sr').textContent = t; };
      navigator.clipboard?.writeText(location.origin + link()).then(() => say('Link copied'), () => say('Copy failed'));
    }
  });
  ROOT.addEventListener('change', (e) => {
    const t = e.target;
    if (t.dataset.part && S.run) {
      const parts = new Set(S.run.amend.parts);
      t.checked ? parts.add(t.dataset.part) : parts.delete(t.dataset.part);
      const y = scrollY;
      S.run = E.act(S.run, 'parts', [...parts]);
      render(`[data-part="${t.dataset.part}"]`);
      window.scrollTo({ top: y, behavior: 'instant' });
    }
  });
  render();
}
