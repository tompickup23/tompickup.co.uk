// Budget Council: the page, shared by every council. A council page calls mount(pack) with:
//   data     { chamber, minutes, amendments, papers, sources, priors? }
//   content  its words module (getBeat, FIRST, whoLabel, ENDINGS, counterfactual, DEBRIEF_QUESTIONS, ROLE_CARDS)
//   ui       its page words (ui.js): start screen, the real or the next meeting, how it works, terms
// The engine is engine.js; the room is src/games/ylv/chamber.js with the council's architecture.
// It collects, stores and sends nothing.
import { createEngine } from './engine.js';
import { makePlan, ROOMS } from '../rooms/plans.js';

export function mount({ data, content, ui }) {
  const { chamber, minutes, amendments, papers, sources } = data;
  const { whoLabel, ENDINGS, counterfactual, DEBRIEF_QUESTIONS, ROLE_CARDS } = content;
  const E = createEngine(data, content);
  const G = E.G;
  const N = chamber.seats;
  const $ = (s, r = document) => r.querySelector(s);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
  const TABS = ['Start', 'The meeting', ui.tab2, 'How it works'];
  const SIDES = ['admin', ...(ui.sides || E.OPP)];
  const CHAIR = ui.chairTitle || 'The Chairman';

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
  const S = { tab: 0, side: ui.defaultSide || E.OPP[0], seed: newSeed(), fac: false, run: null, d3: true, cam: 'seat' };
  function newSeed() { return Math.floor(Math.random() * 90000) + 10000; }
  (function fromHash() {
    const h = new URLSearchParams(location.hash.slice(1));
    const seed = h.get('seed'), side = h.get('side');
    if (seed && /^\d{1,9}$/.test(seed)) S.seed = +seed;
    if (side && SIDES.includes(side)) S.side = side;
    if (h.get('fac') === '1') S.fac = true;
  })();
  const link = () => `${location.pathname}#seed=${S.seed}&side=${S.side}${S.fac ? '&fac=1' : ''}`;

  /* ---- the room: a flat plan coloured by group, and the 3D room when it loads ---- */
  const ROOM = ROOMS[ui.room];
  const PLAN = makePlan(ROOM);
  const NB = PLAN.length; // benches; the officers' figures come after them
  const mySeat = (s) => E.SEATS.indexOf(s.group);
  const LEAD = E.ADMIN[0];
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
  function plan(s, tally) {
    const me = s ? mySeat(s) : -1;
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
    const s = S.run, v = E.view(s);
    const vt = v.tally && s.votes[v.tally];
    const sp = seatOf(v.who, s);
    return { seat: mySeat(s), speaker: sp, tally: vt ? vt.tally : null, mode: vt ? 'chair' : S.cam, pick: false, item: agendaName(v.item), who: whoLabel(v.who, s, E.api) };
  }
  function mountRoom() {
    const slot = $('#bc-room');
    if (!slot) return;
    if (room) { slot.replaceChildren(roomHost); room.set(roomState()); room.resize(); return; }
    if (roomLoading) return;
    roomLoading = Promise.all([import('../ylv/chamber.js'), ui.architecture ? ui.architecture() : null]).then(([mod, arch]) => {
      roomHost = document.createElement('div');
      roomHost.className = 'bc-roomhost';
      roomHost.tabIndex = 0;
      roomHost.setAttribute('role', 'img');
      roomHost.setAttribute('aria-label', 'The council chamber as a room, figures dressed in their group\'s colour. Drag, or use the arrow keys, to look round.');
      const opts = { plan: PLAN, reduced: matchMedia('(prefers-reduced-motion: reduce)').matches, colours: E.SEATS.map((g) => G[g].colour), title: 'BUDGET MEETING', room: ui.room };
      if (arch) opts.architecture = arch;
      room = mod.createRoom(roomHost, opts);
      mountRoom();
    }).catch((err) => { console.error(err); S.d3 = false; room = null; if (S.tab === 1) render(); });
  }

  /* ---- agenda ---- */
  function agenda() { return ui.agenda(E.OPP.map((g) => G[g].short)); }
  const agendaName = (i) => (i == null ? (ui.prepShort || 'The week before') : agenda()[i]);

  /* ---- small pieces ---- */
  const src = (id, at) => `<span class="bc-src">${esc(sources[id].cite)}${at ? ', ' + esc(at) : ''}</span>`;
  function ruleCard(id) {
    const r = papers.rules[id];
    return r ? `<aside class="bc-rule"><b>The rule</b> ${words(r.text)} ${src(r.src, r.at)}</aside>` : '';
  }
  function figChips(ids) {
    return ids ? `<ul class="bc-figs">${ids.map((k) => { const f = papers.figures[k]; return `<li><b>${esc(f.value)}</b><span>${esc(f.what)}</span>${src(f.src, f.at)}</li>`; }).join('')}</ul>` : '';
  }
  function meters(s) {
    const bar = (label, v, max, note) => `<div class="bc-meter"><span>${label}</span><i style="--w:${Math.round((v / max) * 100)}%"></i><em>${note}</em></div>`;
    const moodWord = ['bitter', 'hostile', 'tense', 'businesslike', 'cordial', 'warm', 'generous'][s.v.mood + 3];
    const W151 = ui.s151Words || { approved: 'approved in writing by the Section 151 Officer', refused: 'not approved', none: 'not yet approved' };
    const s151 = s.role === 'admin' ? '' : `<p class="bc-small">Your amendment: ${s.amend.kind === 'none' ? 'none, a statement only' : s.amend.kind == null ? 'not chosen yet' : W151[s.amend.s151] || W151.none}${s.amend.notice === 'late' ? ', and late' : ''}.</p>`;
    return `<div class="bc-card bc-meters"><h3>Where you stand</h3>
      ${bar('Your credibility', s.v.cred, 100, s.v.cred)}
      ${bar('Chamber mood', s.v.mood + 3, 6, moodWord)}
      ${bar(`${esc(CHAIR)}'s patience`, s.v.chair, 5, s.v.chair + ' of 5')}
      ${bar('Your group\'s discipline', s.v.disc[s.group], 100, s.v.disc[s.group])}
      ${s151}
      ${s.record.length ? `<p class="bc-small"><b>On the record:</b> ${esc(s.record[s.record.length - 1])}</p>` : ''}
      <p class="bc-small">Meeting clock: ${Math.floor(s.clock / 60)}h ${String(s.clock % 60).padStart(2, '0')}m. ${esc(ui.clockNote || '')}</p></div>`;
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
    const partsList = `<div class="bc-stack">${sets}<p class="bc-small">Costs are £m for ${amendments.years.join(', ')}. ${ui.partsSource(H)}</p></div>`;
    return `<div class="bc-build">
      <aside class="bc-card bc-balance"><h3>Running balance (£m)</h3>
        <div class="bc-tablewrap"><table><thead><tr><th></th>${amendments.years.map((y) => `<th class="n">${y}</th>`).join('')}</tr></thead><tbody>
          ${ui.balanceRows.map(([k, label]) => `<tr><td>${esc(label)}</td>${yr(k)}</tr>`).join('')}
          <tr class="${b.balanced ? 'ok' : 'gap'}"><td>Still to find</td>${yr('gap')}</tr>
        </tbody></table></div>
        <p class="bc-small">${ui.balanceNote(b)}</p>
        <details><summary class="bc-small">Where the limits come from</summary><p class="bc-small">${Object.values(amendments.funds).filter((f) => f.label).map((f) => `${esc(f.label)}: ${esc(f.at)}.`).join(' ')}</p></details>
      </aside>
      ${partsList}
      <p class="bc-gapbar ${b.balanced && !b.empty ? 'ok' : 'gap'}" aria-hidden="true">Still to find: ${b.years.map((y, i) => `${amendments.years[i]} ${y.gap.toFixed(3)}`).join(' · ')}</p></div>`;
  }
  function tallyBox(v) {
    if (!v) return '';
    const groups = chamber.groups.map((g) => { const x = v.byGroup[g.id] || {}; return `<tr><td><i class="bc-dot" style="background:${g.colour}"></i>${esc(g.short)}</td><td class="n">${x.for || 0}</td><td class="n">${x.against || 0}</td><td class="n">${x.abstain || 0}</td><td class="n">${x.out || 0}</td></tr>`; }).join('');
    const voting = v.for + v.against;
    return `<div class="bc-card"><h3>The recorded vote${v.room ? ', as counted in your room' : ''}</h3>
      <p class="bc-result ${v.carried ? 'yes' : 'no'}">${v.for} for · ${v.against} against · ${v.abstain} abstaining: <b>${v.carried ? 'carried' : 'lost'}</b></p>
      <p class="bc-small">A simple majority of those present and voting decides it: ${voting} voted for or against, so ${Math.floor(voting / 2) + 1} were needed. Abstentions count towards neither side. ${ui.majoritySource(H)}</p>
      ${v.room ? '' : `<div class="bc-tablewrap"><table><thead><tr><th>Group</th><th class="n">For</th><th class="n">Against</th><th class="n">Abstain</th><th class="n">Absent or not voting</th></tr></thead><tbody>${groups}</tbody></table></div>`}
      ${S.fac ? `<details><summary>Use the hands counted in your room instead</summary><p class="bc-hands"><label>For <input id="bc-h-f" type="number" min="0" max="${N}" inputmode="numeric" value="${v.for}"></label><label>Against <input id="bc-h-a" type="number" min="0" max="${N}" inputmode="numeric" value="${v.against}"></label><label>Abstain <input id="bc-h-n" type="number" min="0" max="${N}" inputmode="numeric" value="${v.abstain}"></label><button class="bc-btn ghost" data-room="${v.id}">Use these</button></p></details>` : ''}
    </div>`;
  }

  /* ---- views ---- */
  const views = {
    0() {
      const side = (id) => id === 'admin' ? `<button type="button" data-side="admin" aria-pressed="${S.side === 'admin'}"><b>The Administration</b><span>${esc(ui.adminBlurb(E, G))}</span></button>`
        : `<button type="button" data-side="${id}" aria-pressed="${S.side === id}"><b>${esc(G[id].short)}</b><span>${G[id].seats} seat${G[id].seats === 1 ? '' : 's'}. ${esc(ui.oppBlurb(G[id]))}</span></button>`;
      return `<section class="bc-two">
        <div class="bc-stack">
          ${ui.startIntro(H)}
        </div>
        <div class="bc-stack">
          <fieldset class="bc-sides"><legend>Which side of the chamber?</legend>${SIDES.map(side).join('')}</fieldset>
          <fieldset class="bc-pick"><legend>How are you playing?</legend>
            <button type="button" data-fac="0" aria-pressed="${!S.fac}">On my own</button>
            <button type="button" data-fac="1" aria-pressed="${S.fac}">As a training session, on one screen</button>
          </fieldset>
          ${S.fac ? `<div class="bc-card" id="bc-roles"><h3>Role cards</h3><p class="bc-small">Hand these out. The screen shows the words; people in the room can read them or use their own. At each recorded vote you can type in the hands counted in the room.</p><dl class="bc-roles">${ROLE_CARDS.map((r) => `<div><dt>${r[0]}</dt><dd>${esc(r[1])}</dd></div>`).join('')}</dl><p><button class="bc-btn ghost" data-print="#bc-roles">Print the role cards</button></p></div>` : ''}
          <p class="bc-seed"><label for="bc-seed">Meeting number</label> <input id="bc-seed" type="number" min="1" max="999999999" inputmode="numeric" value="${S.seed}"> <span class="bc-small">The same number, side and choices always give the same meeting, so a group can replay one together.</span></p>
          <p><button class="bc-btn" data-start>${esc(ui.startLabel || 'Start the week before')}</button></p>
        </div>
      </section>
      ${ui.startFooter(H)}`;
    },
    1() {
      const s = S.run;
      if (!s) return `<section class="bc-stack"><h2>The meeting</h2><p>Choose a side on the Start tab first.</p><p><button class="bc-btn" data-tab="0">Choose a side</button></p></section>`;
      if (s.ended) return debrief(s);
      const v = E.view(s);
      const vt = v.tally && s.votes[v.tally];
      const meetingNow = v.phase === 'meeting';
      const ag = agenda().map((a, i) => `<li class="${i === v.item ? 'on' : v.item != null && i < v.item ? 'done' : ''}"><b>${esc(a)}</b></li>`).join('');
      const who = whoLabel(v.who, s, E.api);
      const camNames = { seat: 'Your seat', chair: ui.benchLabel || 'Chairman\'s bench', over: ui.galleryLabel || 'Public gallery' };
      const roomTools = vt ? `<span class="bc-chip is-note">The vote, seen from the ${esc(camNames.chair.replace(/^./, (c) => c.toLowerCase()))}</span>` : ['seat', 'chair', 'over'].map((c) => `<button type="button" class="bc-chip" data-cam="${c}" aria-pressed="${S.cam === c}">${esc(camNames[c])}</button>`).join('');
      const roomBox = S.d3 ? `<div class="bc-room" id="bc-room">${plan(s, vt?.tally)}</div><p class="bc-tools">${roomTools}<button type="button" class="bc-chip" data-d3="0">Flat plan</button></p>` : `${plan(s, vt?.tally)}<p class="bc-tools"><button type="button" class="bc-chip" data-d3="1">Show the room in 3D</button></p>`;
      const choices = v.choices.map((c) => `<button type="button" class="bc-btn ${v.choices.length > 1 ? 'ghost' : ''}" data-choice="${esc(c.id)}">${esc(c.label)}</button>`).join('');
      return `<section class="bc-two bc-meet">
        <div class="bc-m-room">${roomBox}<ol class="bc-agenda" aria-label="Agenda">${meetingNow ? ag : `<li class="on"><b>${esc(ui.prepLabel || 'The week before the meeting')}</b></li>`}</ol>${meters(s)}</div>
        <div class="bc-stack bc-m-words">
          <p class="bc-eyebrow">${s.role === 'admin' ? 'Administration' : esc(G[s.group].short)} · ${esc(agendaName(v.item))} · Meeting ${s.seed}</p>
          ${v.say ? `<div class="bc-screen" id="bc-screen">${who ? `<span class="who">${esc(who)}<small> · words written for the simulator</small></span>` : ''}<q>${words(v.say)}</q></div>` : ''}
          ${v.plain ? `<p class="bc-plain">${words(v.plain)}</p>` : ''}
          <p class="bc-def" id="bc-def" role="status"></p>
          ${v.kind === 'build' ? builder(s) : ''}
          ${v.card && v.group ? amendCard(v.group) : ''}
          ${figChips(v.figs)}
          ${vt ? tallyBox(vt) : ''}
          ${v.rule ? ruleCard(v.rule) : ''}
          <div class="bc-choices" role="group" aria-label="Your choice">${choices}</div>
        </div>
      </section>`;
    },
    2() { return ui.tab2View(H); },
    3() {
      return `<section class="bc-stack bc-how">
        <h2>How it works</h2>
        ${ui.howIntro(H)}
        <h3>How you are scored</h3>
        <p>Not on winning. ${esc(ui.scoreWhy)} You are scored out of 10 on three things: procedure (did you follow the rules), good faith (did you answer questions and argue with ideas, not people) and influence (did you change anything, or get anything on the record).</p>
        <h3>The rules used</h3>
        <ul class="bc-rules">${Object.values(papers.rules).map((r) => `<li>${esc(r.text)} ${src(r.src, r.at)}</li>`).join('')}</ul>
        <h3>Words</h3>
        <dl>${TERMS.slice().sort((a, b) => a[0].localeCompare(b[0])).map((t) => `<dt>${t[0]}</dt><dd>${esc(t[2])}</dd>`).join('')}</dl>
        <h3>Where the idea came from</h3>
        <p>The idea of a virtual council meeting came from Wayne Walsh, who lives in Burnley. The design, the wording and any mistakes are Tom Pickup's.</p>
        <h3>Your information</h3>
        <p>There are no accounts and no text boxes. Nothing is stored or sent. The link to a meeting holds its number, the side and whether it is a training session.</p>
        <h3>Sources</h3>
        <ul class="bc-sources">${Object.entries(sources).filter(([k]) => !k.startsWith('_')).map(([, x]) => `<li>${esc(x.text)}${x.url ? ` Available at: <a href="${esc(x.url)}">${esc(x.url.replace(/^https?:\/\//, '').split('/')[0])}</a>.` : ''}</li>`).join('')}</ul>
      </section>`;
    },
  };

  function debrief(s) {
    const e = ENDINGS[s.ended];
    const sc = s.v.score;
    const moved = s.log.filter((l) => /^(lean|disc|cred|mood|chair|score)/.test(l.tag) || l.tag.startsWith('flag:')).slice(-10);
    const bar = (label, v) => `<div class="bc-meter"><span>${label}</span><i style="--w:${v * 10}%"></i><em>${v} of 10</em></div>`;
    return `<section class="bc-stack bc-debrief">
      <p class="bc-eyebrow">Meeting ${s.seed} · ${s.role === 'admin' ? 'Administration' : esc(G[s.group].short)}</p>
      <h2>${esc(e.title)}</h2>
      <p class="bc-lede">${esc(e.text)}</p>
      <div class="bc-two">
        <div class="bc-card"><h3>Your scores</h3>${bar('Procedure', sc.proc)}${bar('Good faith', sc.faith)}${bar('Influence', sc.infl)}
          <p class="bc-small">${esc(ui.scoreNote || 'Scored on conduct, not on winning.')}</p></div>
        <div class="bc-card"><h3>What you could have done differently</h3><p>${esc(counterfactual(s))}</p>
          ${s.record.length ? `<h4>On the record</h4><ul>${s.record.map((r) => `<li>${esc(r)}</li>`).join('')}</ul>` : ''}</div>
      </div>
      <div class="bc-card"><h3>What moved things</h3><ol class="bc-moved">${moved.map((l) => `<li>${esc(l.text)}</li>`).join('') || '<li>Nothing much: the meeting went to form.</li>'}</ol></div>
      ${ui.debriefVotes(s, H)}
      <div class="bc-card"><h3>Talk about it</h3><ol class="bc-debriefq">${DEBRIEF_QUESTIONS.map((q) => `<li>${esc(q)}</li>`).join('')}</ol>
        <p class="bc-small">${ui.furtherReading(H)}</p></div>
      <p><button class="bc-btn" data-again>Play this meeting again</button> <button class="bc-btn ghost" data-other>Try the other side</button> <button class="bc-btn ghost" data-copy>Copy a link to this meeting</button></p>
    </section>`;
  }

  /* ---- render and events ---- */
  function render(focus) {
    $('#bc-tabs').innerHTML = TABS.map((t, i) => `<button type="button" data-tab="${i}" ${i === S.tab ? 'aria-current="page"' : ''}>${t}</button>`).join('');
    $('#bc-app').innerHTML = views[S.tab]();
    if (focus) { const el = $(focus); if (el) el.focus({ preventScroll: true }); }
    const q = $('#bc-screen');
    if (q) $('#bc-sr').textContent = q.textContent;
    if (S.tab === 1 && S.run && !S.run.ended && S.d3) mountRoom();
  }
  function start() {
    const seedIn = $('#bc-seed');
    if (seedIn && /^\d{1,9}$/.test(seedIn.value)) S.seed = +seedIn.value;
    S.run = E.start({ seed: S.seed, role: S.side === 'admin' ? 'admin' : 'opp', group: S.side === 'admin' ? undefined : S.side, facilitator: S.fac });
    history.replaceState(null, '', link());
    S.tab = 1; render('[data-choice]');
    window.scrollTo({ top: $('.bc').offsetTop, behavior: 'auto' });
  }
  function printOnly(sel) {
    const el = $(sel); if (!el) return;
    el.classList.add('bc-printme'); document.documentElement.classList.add('bc-print');
    addEventListener('afterprint', () => { el.classList.remove('bc-printme'); document.documentElement.classList.remove('bc-print'); }, { once: true });
    window.print();
  }
  $('.bc').addEventListener('click', (e) => {
    const b = e.target.closest('button');
    if (!b) return;
    const d = b.dataset;
    if (d.tab != null) { S.tab = +d.tab; render(); window.scrollTo({ top: $('.bc').offsetTop, behavior: 'auto' }); }
    else if (d.side) { S.side = d.side; render(`[data-side="${d.side}"]`); }
    else if (d.fac != null) { S.fac = d.fac === '1'; render(`[data-fac="${d.fac}"]`); }
    else if ('start' in d) start();
    else if (d.choice) {
      const before = S.run.beat;
      S.run = E.act(S.run, d.choice);
      if (S.run.ended && before) history.replaceState(null, '', link());
      render('[data-choice], [data-again]');
      const w = $('.bc-m-words') || $('.bc-debrief');
      if (w && w.getBoundingClientRect().top < 0) w.scrollIntoView({ block: 'start', behavior: 'instant' });
    }
    else if (d.term != null) { const t = TERMS[+d.term]; $('#bc-def').innerHTML = `<b>${t[0]}.</b> ${esc(t[2])}`; }
    else if (d.d3 != null) { S.d3 = d.d3 === '1'; render('[data-d3]'); }
    else if (d.cam) { S.cam = d.cam; render(`[data-cam="${d.cam}"]`); }
    else if (d.room) {
      const n = (id) => Math.max(0, Math.min(N, Math.floor(+$('#bc-h-' + id).value || 0)));
      S.run = E.roomVote(S.run, d.room, { for: n('f'), against: n('a'), abstain: n('n') });
      render('[data-choice]');
    }
    else if (d.print) printOnly(d.print);
    else if ('again' in d) start();
    else if ('other' in d) { S.side = S.side === 'admin' ? E.OPP[0] : 'admin'; S.seed = newSeed(); S.tab = 0; render(`[data-side="${S.side}"]`); }
    else if ('copy' in d) { navigator.clipboard?.writeText(location.origin + link()).then(() => { b.textContent = 'Link copied'; }, () => { b.textContent = 'Copy failed'; }); }
  });
  $('.bc').addEventListener('change', (e) => {
    const t = e.target;
    if (t.dataset.part && S.run) {
      const parts = new Set(S.run.amend.parts);
      t.checked ? parts.add(t.dataset.part) : parts.delete(t.dataset.part);
      S.run = E.act(S.run, 'parts', [...parts]);
      render(`[data-part="${t.dataset.part}"]`);
    }
  });
  render();
}
