// Burnley Budget Council: everything that is said and every choice. Pure data and small functions;
// the shared engine runs it. A rehearsal of Burnley Borough Council's 2027/28 Budget Meeting, due in
// March 2027, under the council's own Council Procedure Rules and Budget and Policy Framework Rules.
// Speeches are written for the game: nobody's real words are quoted and no councillor is named.
// Figures come from papers.json and amendments.json, with their sources.
//
// What makes Burnley different from Lancashire: the coalition running the council is a minority
// (19 of 45), there is no notice deadline and no rule that the Section 151 Officer approves an
// amendment, a mover can take another group's ideas into the Budget by altering the motion (rule
// 14.7), and an amended Budget is only an in-principle decision the Leader can object to.

export const FIRST = 'prep:intro';

const THEME = {
  tax: 'council tax', fees: 'fees and charges', growth: 'economic development', waste: 'bin collections',
  running: 'how the council runs itself', culture: 'Towneley Hall', poverty: 'help with the cost of living', leisure: 'swimming for children',
};

/* Who is speaking. Offices and groups only. */
export function whoLabel(key, s, api) {
  const g = (id) => api.G[id]?.short || id;
  const cmTitle = 'Executive Member for Resources and Performance Management';
  const map = {
    chair: 'The Mayor', ce: 'The Chief Executive', mo: 'The Monitoring Officer (Head of Legal and Democratic Services)', s151: 'The Director of Resources (Section 151 Officer)',
    leader: 'The Leader of the Council', cm: s.role === 'admin' ? `You, the Deputy Leader and ${cmTitle}` : `The Deputy Leader and ${cmTitle}`,
    me: s.role === 'admin' ? `You, the Deputy Leader and ${cmTitle}` : `You, Leader of the ${g(s.group)} Group`,
    heckler: s.role === 'admin' ? 'A councillor on the opposition benches' : 'A councillor across the chamber', seconder: `Your seconder, ${g(s.group)} Group`, you: 'You', narrator: '',
    group: s.role === 'admin' ? 'The coalition group leaders' : 'Your group meeting', officer: 'An officer',
  };
  if (map[key] != null) return map[key];
  if (key.startsWith('opp:')) { const id = key.slice(4); return api.G[id]?.seats === 1 ? `The ${g(id)} councillor` : `The Leader of the ${g(id)} Group`; }
  if (key.startsWith('back:')) return `A ${g(key.slice(5))} councillor`;
  return key;
}

/* ---------------- helpers ---------------- */
const myQ = (s) => 'am_' + s.group;
const movers = (api) => api.OPP.filter((g) => api.G[g].mover);
const others = (s, api) => api.OPP.filter((g) => g !== s.group);
const pct = (x) => Math.round(x * 100);
const folded = (s, h) => s.flags.includes('folded_' + h);
const nextGroupBeat = (s, api, h) => {
  const order = movers(api);
  const i = order.indexOf(h);
  return i < order.length - 1 ? `am:${order[i + 1]}:intro` : 'sub:debate';
};

// The amendment builder for 2027/28: continuing costs need continuing savings or income;
// one-off spending may come from the Revenue Support Reserve (council policy, MTFS para. 63).
export function buildCheck(s, api) {
  const A = api.amendments;
  const r3 = (v) => Math.round(v * 100000) / 100000;
  const parts = (s.amend.parts || []).map((id) => ({ id, ...A.parts[id] })).filter((p) => p.label);
  const recur = parts.filter((p) => p.recurring).reduce((n, p) => n + p.cost[0], 0);
  const once = parts.filter((p) => !p.recurring).reduce((n, p) => n + p.cost[0], 0);
  const spare = Math.max(0, -recur);
  const reserve = Math.max(0, once - spare);
  const gap = Math.max(0, recur);
  const clash = parts.filter((p) => p.theme === 'tax').length > 1 || parts.filter((p) => p.theme === 'fees').length > 1;
  const risky = parts.filter((p) => p.risk);
  const size = parts.reduce((n, p) => n + Math.abs(p.cost[0]), 0);
  return {
    parts, clash, risky, size,
    years: [{ spend: r3(recur + once), recur: r3(recur), reserve: r3(reserve), gap: r3(gap) }],
    balanced: gap <= 0 && !clash && parts.length > 0,
    usesSc: reserve > 0, usesReserve: reserve > 0,
    groups: [...new Set(parts.map((p) => p.group))],
    y1: size, empty: parts.length === 0,
    cutsTax: parts.some((p) => p.id === 'ct099'), raisesTax: parts.some((p) => p.id === 'ct299'),
  };
}
const build = (s, api) => buildCheck(s, api);

function leadTheme(s, api) {
  const b = build(s, api);
  const top = b.parts.slice().sort((a, c) => Math.abs(c.cost[0]) - Math.abs(a.cost[0]))[0];
  return top ? THEME[top.theme] : 'these services';
}

/* The coalition's stance on an opposition amendment. It needs votes from somewhere: it can take a
   group's ideas into the Budget beforehand (rule 14.7), back a small sound amendment, or oppose. */
function adminStance(s, api, h) {
  if (s.role === 'admin') return s.adminLine?.[h] || 'against';
  if (h !== s.group) return 'against';
  const b = build(s, api);
  const ready = s.amend.s151 === 'confirmed' && s.amend.circulate === 'early' && api.has(s, 'briefed_cm');
  if (b.size <= 0.1 && b.balanced && ready && s.v.cred >= 70 && s.v.mood >= 1) return 'for';
  return 'against';
}

/* ---------------- opponents' moves ----------------
   Storylets drawn on the seeded generator, weighted by the state, so meetings differ for a reason. */
const MINE = [
  {
    id: 'whopays', who: 'heckler', weight: (s) => 3 + (s.v.mood < 0 ? 3 : 0),
    say: 'A councillor across the chamber calls out, without being called: "And which service pays for this?"',
    plain: 'A heckle is not a question you have to answer. Members address the Mayor (rule 20.1). If you answer, do it through the Mayor, and with a figure.',
    choices: [
      { id: 'figure', label: 'Answer through the Mayor, with the figure from your running balance', fx: (s, a) => { a.add(s, 'cred', 6, 'You answered the heckle through the Mayor with your own figures.'); a.score(s, 'faith', 1, 'You answered the question that was asked.'); } },
      { id: 'ignore', label: 'Ignore it and carry on', fx: (s, a) => a.score(s, 'proc', 1, 'You did not rise to a heckle.') },
      { id: 'back', label: 'Answer it straight back from your seat', fx: (s, a) => { a.add(s, 'mood', -1, 'You traded shouts across the chamber.'); a.add(s, 'chair', -1, 'The Mayor had to call for order after your exchange.'); a.score(s, 'faith', -1, 'You answered a heckle with a heckle.'); } },
    ],
  },
  {
    id: 'reserves', who: 'cm', when: (s, a) => build(s, a).usesReserve || build(s, a).parts.some((p) => p.recurring && p.cost[0] > 0), weight: () => 5,
    say: (s, a) => build(s, a).parts.some((p) => p.recurring && p.cost[0] > 0)
      ? 'Mayor, part of this is spending that carries on every year. Where does the money come from in the year after next?'
      : 'Mayor, this spends from the Revenue Support Reserve. That reserve is there for one-off pressures. What is left for the next one?',
    plain: 'Council policy is that earmarked reserves do not pay for continuing spending (MTFS 2027-31, para. 63). A reserve can pay for something once.',
    choices: [
      { id: 'oneoff', label: 'Say through the Mayor that it is one-off spending, as last year\'s additions were', when: (s, a) => !build(s, a).parts.some((p) => p.recurring && p.cost[0] > 0), fx: (s, a) => { a.add(s, 'cred', 5, 'You showed your reserve spending was one-off, as policy allows.'); a.score(s, 'faith', 1, 'You answered the question.'); } },
      { id: 'review', label: 'Accept it is a fair point and say your group would review it during the year', fx: (s, a) => { a.score(s, 'faith', 1, 'You conceded a fair point instead of denying it.'); a.add(s, 'mood', 1, 'The chamber warmed to an honest answer.'); } },
      { id: 'dismiss', label: 'Turn it round: ask what the Executive plans for the year after next', fx: (s, a) => { a.add(s, 'cred', -6, 'You dismissed a money question instead of answering it.'); a.score(s, 'faith', -1, 'You avoided the question.'); } },
    ],
  },
  {
    id: 'risk', who: 's151', when: (s, a) => build(s, a).risky.length > 0, weight: () => 7,
    say: (s, a) => `Mayor, if I may advise members. ${build(s, a).risky[0].risk} Members should weigh that before relying on it in the Budget.`,
    plain: 'The Section 151 Officer speaks only to advise. A saving the council cannot deliver itself weakens the Budget\'s robustness, which the officer must report on (section 25).',
    choices: [
      { id: 'accept', label: 'Thank the officer, and say you would replace that line during the year if it fell through', fx: (s, a) => { a.score(s, 'faith', 1, 'You took the officer\'s advice seriously.'); a.add(s, 'cred', 2); } },
      { id: 'argue', label: 'Say the officer is being too cautious', fx: (s, a) => { a.add(s, 'cred', -8, 'You argued with the Section 151 Officer\'s advice.'); a.OPP.forEach((g) => g !== s.group && a.lean(s, myQ(s), g, -4)); } },
    ],
  },
  {
    id: 'timing', who: 'cm', when: (s) => s.amend.circulate === 'night', weight: () => 8,
    say: 'Mayor, members are seeing this for the first time tonight. Nobody has had a chance to check a single figure in it.',
    plain: 'An amendment may be moved without notice (rule 13(g)); the Mayor may ask for it in writing first (rule 14.2). It is allowed. It costs goodwill, and votes from groups who had no time to read it.',
    choices: [
      { id: 'rule', label: 'Say through the Mayor that the rules allow it, and read out the lines', fx: (s, a) => a.score(s, 'proc', 1, 'You knew rule 13(g).') },
      { id: 'sorry', label: 'Accept it is late for other groups, and offer to take questions on it', fx: (s, a) => { a.add(s, 'mood', 1, 'Your offer to take questions eased the room.'); a.score(s, 'faith', 1, 'You owned the timing.'); } },
      { id: 'counter', label: 'Say the Executive\'s own papers came late too', fx: (s, a) => a.add(s, 'mood', -1, 'Tit for tat on timing soured the debate.') },
    ],
  },
  {
    id: 'pointoforder', who: 'heckler', weight: () => 2,
    say: 'A councillor across the chamber says, in passing, that your amendment would close a leisure centre. You are sure that is wrong.',
    plain: 'A point of order must name the rule or law broken (rule 14.12). Disagreeing with a fact is not one. A personal explanation can only correct your own earlier speech (rule 14.13). Your reply is the place to set the record straight.',
    choices: [
      { id: 'po', label: 'Rise on a point of order', fx: (s, a) => { a.flag(s, 'po_refused'); a.add(s, 'chair', -1, 'The Mayor refused a point of order that named no rule.'); a.score(s, 'proc', -2, 'A disagreement is not a point of order.'); } },
      { id: 'pe', label: 'Ask to make a personal explanation', fx: (s, a) => { a.flag(s, 'pe_refused'); a.add(s, 'chair', -1, 'The Mayor ruled it out: a personal explanation is only about your own earlier speech.'); a.score(s, 'proc', -1, 'Rule 14.13 covers only your own words.'); } },
      { id: 'reply', label: 'Let it go now and correct it in your reply', fx: (s, a) => { a.flag(s, 'correct_in_reply'); a.score(s, 'proc', 1, 'You kept your correction for your right of reply.'); } },
    ],
  },
  {
    id: 'ally', who: (s) => 'opp:' + s.allyFor, when: (s) => !!s.allyFor, weight: () => 7,
    say: (s, a) => `Mayor, ${a.G[s.allyFor].seats === 1 ? 'I have read this carefully, and I will support it' : 'our group has read this carefully, and we will support it'}. Between the opposition benches there are 26 of the 45 seats in this room.`,
    plain: 'The coalition has 19 seats. If the opposition groups vote together, they can carry an amendment. Whether they do is the whole of this meeting.',
    choices: [
      { id: 'thank', label: 'Thank them, through the Mayor, when you reply', fx: (s, a) => { a.add(s, 'mood', 1, 'Cross-group support lifted the debate.'); a.lean(s, myQ(s), s.allyFor, 8, 'Their leader\'s speech firmed up their own group.'); } },
      { id: 'claim', label: 'Say it shows the coalition has lost the chamber', fx: (s, a) => { others(s, a).forEach((g) => g !== s.allyFor && a.lean(s, myQ(s), g, -5, `${a.G[g].short} members bristled at being counted in.`)); a.add(s, 'mood', -1); } },
    ],
  },
  {
    id: 'exposed', who: 'heckler', when: (s) => s.flags.includes('undeclared') && !s.flags.includes('declared_late'), weight: () => 20,
    say: 'Mayor, on a point of order: should the councillor not have declared an interest in the leisure items?',
    plain: 'A member declares an interest when the item comes up (rule 20.A and the Code of Conduct). Declaring late is better than not at all.',
    choices: [
      { id: 'declare', label: 'Apologise, and declare the interest now', fx: (s, a) => { a.flag(s, 'declared_late', 'You declared your interest late, when challenged.'); a.score(s, 'proc', -1, 'You declared late.'); a.add(s, 'cred', -4, 'The late declaration cost you some standing.'); } },
      { id: 'deny', label: 'Say it is not an interest the Code requires you to declare', fx: (s, a) => { a.flag(s, 'named', 'You refused to declare an interest when challenged.'); a.add(s, 'chair', -3, 'The Mayor referred your conduct to the Monitoring Officer.'); a.score(s, 'proc', -3, 'You refused to declare an interest.'); } },
    ],
  },
];

const THEIRS = [
  {
    id: 'support', who: 'you', when: (s) => s.role === 'opp', weight: () => 5,
    say: (s, a, h) => `The Mayor asks if any other member wishes to speak on the ${a.G[h].short} amendment.`,
    plain: 'You may speak once on another member\'s amendment (rule 14.5). With 26 opposition seats against 19, your group\'s votes may decide it.',
    choices: [
      { id: 'speak', label: 'Speak in support of it', fx: (s, a, h) => { a.lean(s, myQ(s), h, 8, `You spoke for the ${a.G[h].short} amendment, and they remembered.`); a.lean(s, 'am_' + h, s.group, 8); a.add(s, 'mood', 1, 'A generous speech helped the mood.'); } },
      { id: 'question', label: 'Ask the mover whether it carries on into later years', fx: (s, a) => { a.add(s, 'cred', 3, 'You asked a fair question about another group\'s money.'); a.score(s, 'faith', 1, 'You tested an amendment on its merits.'); } },
      { id: 'sit', label: 'Stay seated', fx: (s) => { s.clock -= 4; } },
      { id: 'attack', label: 'Speak against it, to set your own group apart', fx: (s, a, h) => { a.lean(s, myQ(s), h, -10, `You attacked the ${a.G[h].short} amendment, and they remembered.`); a.add(s, 'mood', -1, 'Speaking against another opposition group cost you goodwill with it.'); } },
    ],
  },
  {
    id: 'asks_cm', who: (s, a, h) => 'opp:' + h, when: (s) => s.role === 'admin', weight: () => 9,
    say: (s, a, h) => `Mayor, a simple question for the Executive Member: if this amendment falls tonight, will the Executive look again at our proposals during the year?`,
    plain: 'You need votes from outside the coalition to be sure of your Budget. A commitment goes in the minutes; so does a refusal.',
    choices: [
      { id: 'commit', label: 'Commit to bring their proposals to the Executive during the year', fx: (s, a, h) => { a.note(s, `The Executive Member committed to bring the ${a.G[h].short} group's proposals to the Executive during the year.`); a.score(s, 'infl', 1, 'You turned an opposition idea into a commitment on the record.'); a.lean(s, 'budget', h, 10, `The ${a.G[h].short} group warmed to the Budget after your commitment.`); } },
      { id: 'figures', label: 'Explain, with figures, why the Budget cannot carry it this year', fx: (s, a) => { a.add(s, 'cred', 4, 'You answered with figures from the papers.'); a.score(s, 'faith', 1, 'You answered the question.'); } },
      { id: 'dodge', label: 'Say they should have come to the Executive with it sooner', fx: (s, a, h) => { a.score(s, 'faith', -1, 'You did not answer the question asked.'); a.lean(s, 'budget', h, -8, 'The non-answer hardened them against the Budget.'); } },
    ],
  },
  {
    id: 'heckle_admin', who: 'heckler', when: (s) => s.role === 'admin', weight: (s) => 3 + (s.v.mood < 0 ? 3 : 0),
    say: 'A councillor on the opposition benches calls out, without being called: "You have 19 seats. Who gave you this Budget?"',
    plain: 'A heckle is not a question you have to answer. If you answer, do it through the Mayor.',
    choices: [
      { id: 'figure', label: 'Answer through the Mayor: the Budget closes the gap the officers set out', fx: (s, a) => { a.add(s, 'cred', 5, 'You answered a heckle through the Mayor with the papers\' figures.'); a.score(s, 'faith', 1, 'You answered the question that was asked.'); } },
      { id: 'ignore', label: 'Carry on with your point', fx: (s, a) => a.score(s, 'proc', 1, 'You did not rise to a heckle.') },
      { id: 'back', label: 'Answer it from your seat: "More than voted for yours"', fx: (s, a) => { a.add(s, 'mood', -1, 'You traded remarks across the chamber.'); a.add(s, 'chair', -1, 'The Mayor had to call for order.'); a.score(s, 'faith', -1, 'You answered a heckle with a heckle.'); a.OPP.forEach((g) => a.lean(s, 'budget', g, -3)); } },
    ],
  },
  {
    id: 'closure', who: (s, a) => 'back:' + a.ADMIN[0], when: (s) => s.clock > 120, weight: (s) => 2 + Math.floor(s.clock / 60),
    say: 'Mayor, I move that the question be now put.',
    plain: 'A closure motion is moved at the end of another member\'s speech. The Mayor decides whether the matter has been debated enough, and that ruling is final (rule 14.11).',
    choices: [
      { id: 'accept', label: 'Do not object', fx: (s, a) => { s.clock -= 10; a.score(s, 'proc', 1, 'You knew when a debate had run its course.'); } },
      { id: 'object', label: 'Rise to say some groups have not been heard on this item', fx: (s, a) => { a.add(s, 'chair', -1, 'The Mayor had to restore order over the closure motion.'); a.add(s, 'mood', -1, 'Tempers rose over the closure motion.'); } },
    ],
  },
  {
    id: 'relevance', who: 'chair', weight: () => 2,
    say: 'A councillor has spent two minutes on national politics. The Mayor reminds members that speeches must be on the question, and of the five-minute limit.',
    plain: 'Speeches must be on the question, and no longer than five minutes without the Mayor\'s consent (rule 14.4). The Mayor must stay apolitical (Article 5.01).',
    choices: [
      { id: 'nod', label: 'Nod, and keep your own remarks on the Budget', fx: (s, a) => a.score(s, 'proc', 1, 'You noted the Mayor\'s ruling.') },
      { id: 'heckle', label: 'Call out that the member is entitled to make the point', fx: (s, a) => a.add(s, 'chair', -1, 'You heckled the Mayor\'s ruling.') },
    ],
  },
];

function drawMoves(s, api, list, n, h) {
  const live = list.filter((m) => (!m.when || m.when(s, api, h)) && !s.flags.includes('used:' + m.id + ':' + h));
  const out = [];
  for (let k = 0; k < n && live.length; k++) {
    const w = live.map((m) => Math.max(0, typeof m.weight === 'function' ? m.weight(s, api, h) : m.weight || 1));
    const tot = w.reduce((a, b) => a + b, 0);
    if (!tot) break;
    let r = api.rand(s) * tot, i = 0;
    while (r > w[i]) { r -= w[i]; i++; }
    out.push(live[i].id);
    s.flags.push('used:' + live[i].id + ':' + h);
    live.splice(i, 1);
  }
  return out;
}
const moveById = (id) => MINE.find((m) => m.id === id) || THEIRS.find((m) => m.id === id);
const val = (x, s, api, h) => (typeof x === 'function' ? x(s, api, h) : x);

/* ---------------- the beats ---------------- */
export function getBeat(id, s, api) {
  const G = api.G;
  const P = api.data.papers.figures;
  const [fam, a1, a2] = id.split(':');
  const admin = s.role === 'admin';
  const coalition = api.ADMIN.reduce((n, g) => n + G[g].seats, 0);
  const opposition = 45 - coalition;
  const M = movers(api);

  /* ===== The weeks before ===== */
  if (id === 'prep:intro') {
    if (admin) {
      return {
        phase: 'prep', who: 'narrator', mins: 0, figs: ['gap', 'ctAssumed', 'gap3'],
        say: `You are the Deputy Leader and Executive Member for Resources. In March Full Council meets to set the 2027/28 Budget, and you will move it, as the Deputy Leader did last year. The coalition holds ${coalition} of the 45 seats.`,
        plain: `The officers' latest figures show a gap of ${P.gap.value} for 2027/28 at a ${P.ctAssumed.value} council tax rise. The opposition groups hold ${opposition} seats between them. If they vote together, they can amend your Budget or defeat it.`,
        choices: [{ id: 'go', label: 'Meet the coalition\'s group leaders', fx: (s, a) => { s.partner = a.pick(s, a.ADMIN.slice(1)); }, go: 'prep:a_group' }],
      };
    }
    const g = G[s.group];
    return {
      phase: 'prep', who: 'narrator', mins: 0, figs: ['gap', 'ctAssumed', 'approved'],
      say: `You lead the ${g.name} group, ${g.seats} of the 45 councillors. In March Full Council sets the 2027/28 Budget. The coalition running the council has ${coalition} seats; the opposition groups have ${opposition}. What will your group do?`,
      plain: `The officers' latest figures show a gap of ${P.gap.value} for 2027/28. You can move an amendment to the Executive's Budget, or try to get your ideas taken into the Budget before the vote, as Labour did last year, or speak in the debate without an amendment.`,
      rule: 'alteration',
      choices: [
        { id: 'own', label: 'Build an amendment from costed options', fx: (s) => { s.amend.kind = 'own'; s.amend.parts = []; }, go: 'prep:build' },
        { id: 'deal', label: 'Build costed proposals and offer them to the Executive first', fx: (s, a) => { s.amend.kind = 'own'; s.amend.parts = []; a.flag(s, 'seek_deal', 'You set out to get your proposals into the Budget itself.'); }, go: 'prep:build' },
        { id: 'none', label: 'Speak in the debate, with no amendment', fx: (s, a) => { s.amend.kind = 'none'; a.score(s, 'infl', -1, 'Without an amendment there is nothing to vote on.'); }, go: 'prep:allies1' },
      ],
    };
  }
  if (id === 'prep:build') {
    return {
      phase: 'prep', kind: 'build', who: 'narrator', mins: 0,
      say: 'Choose what your proposals would do in 2027/28. Every option is costed from a council paper, or from last year\'s amendments.',
      plain: 'Continuing spending, or a council tax or fees cut, needs a continuing saving or income to pay for it. One-off spending can come from the Revenue Support Reserve, as last year\'s additions did. The running balance must reach zero.',
      rule: 'reserves',
      choices: [
        { id: 'done', label: 'Take it to the Director of Resources', when: (s, a) => !build(s, a).empty, fx: (s, a) => { const b = build(s, a); a.flag(s, b.balanced ? 'built_balanced' : 'built_gap', b.balanced ? 'Your proposals balanced for 2027/28.' : 'Your proposals did not balance.'); }, go: 'prep:cfo' },
        { id: 'drop', label: 'Drop it, and speak in the debate instead', when: (s, a) => build(s, a).empty, fx: (s, a) => { s.amend.kind = 'none'; a.score(s, 'infl', -1, 'Without an amendment there is nothing to vote on.'); }, go: 'prep:allies1' },
      ],
    };
  }
  if (id === 'prep:cfo') {
    return {
      phase: 'prep', who: 's151', mins: 0, rule: 'confirm',
      say: 'The rules do not need my sign-off on an amendment. But if you want, I can look at your lines and tell you whether they are possible and realistic.',
      plain: 'Last year\'s Conservative amendment quoted the Director of Resources\' confirmation. A line the officer cannot confirm will be raised in the chamber.',
      choices: [
        { id: 'consult', label: 'Yes: ask the Director of Resources to check it now', fx: (s, a) => {
          const b = build(s, a);
          if (!b.balanced) { s.amend.s151 = 'refused'; a.score(s, 'proc', 1, 'You asked early, so you found the gap in time.'); return; }
          s.amend.s151 = 'confirmed'; a.add(s, 'cred', b.risky.length ? 3 : 8, b.risky.length ? 'The officer confirmed most lines, with a caution on one.' : 'The Director of Resources confirmed your lines were possible and realistic.'); a.score(s, 'proc', 2, 'You had the officer check your figures.');
          if (b.risky.length) a.flag(s, 'risk_noted', `The officer noted: ${b.risky[0].risk}`);
        }, go: (s) => (s.amend.s151 === 'refused' ? 'prep:cfo_fix' : 'prep:notice') },
        { id: 'skip', label: 'No: the rules do not require it, and you are confident it adds up', fx: (s, a) => { s.amend.s151 = 'none'; a.flag(s, 'no_cfo', 'You did not have the Director of Resources check your lines.'); }, go: 'prep:notice' },
      ],
    };
  }
  if (id === 'prep:cfo_fix') {
    const b = build(s, api);
    return {
      phase: 'prep', who: 's151', mins: 0,
      say: b.clash ? 'You have two different council tax or fees changes in here. Choose one.' : `I could not tell Council this balances. As it stands it is ${api.money(b.years[0].gap)} short in 2027/28.`,
      plain: 'Nothing stops you moving it anyway. But the Executive Member will say it does not add up, and the officer will be asked.',
      choices: [
        { id: 'fix', label: 'Go back and change it', go: 'prep:build', fx: (s) => { s.amend.s151 = 'none'; } },
        { id: 'anyway', label: 'Move it as it is', go: 'prep:notice', fx: (s, a) => a.flag(s, 'unbalanced', 'You took an amendment that did not balance into the meeting.') },
      ],
    };
  }
  if (id === 'prep:notice') {
    return {
      phase: 'prep', who: 'narrator', mins: 0, rule: 'noNotice',
      say: 'There is no deadline for a budget amendment. When should other councillors see yours?',
      plain: 'Sharing early gives other groups time to back you, and gives the Executive time to answer, or to take your ideas in. Moving it on the night is allowed, and surprises everyone, including the groups whose votes you need.',
      choices: [
        { id: 'early', label: 'Send it to every group leader and the Executive Member a week before', fx: (s, a) => { s.amend.circulate = 'early'; others(s, a).forEach((g) => a.lean(s, myQ(s), g, 5)); a.flag(s, 'early', 'You shared your proposals a week early.'); a.score(s, 'faith', 1, 'Early sharing lets everyone weigh it properly.'); } },
        { id: 'supp', label: 'Have it published in a supplementary agenda on the day, as last year', fx: (s) => { s.amend.circulate = 'supp'; } },
        { id: 'night', label: 'Move it without notice on the night', fx: (s, a) => { s.amend.circulate = 'night'; a.flag(s, 'night', 'You moved your amendment without notice.'); others(s, a).forEach((g) => a.lean(s, myQ(s), g, -6)); } },
      ],
      skip: (s) => s.amend.kind === 'none',
      next: (s) => (s.flags.includes('seek_deal') ? 'prep:deal' : 'prep:allies1'),
    };
  }
  if (id === 'prep:deal') {
    const b = build(s, api);
    const chance = (b.balanced ? 0.35 : 0) + (s.amend.s151 === 'confirmed' ? 0.2 : 0) + (s.amend.circulate === 'early' ? 0.15 : 0) + (b.cutsTax ? -0.25 : 0) + (b.size <= 0.15 ? 0.1 : -0.1);
    return {
      phase: 'prep', who: 'cm', mins: 0, rule: 'alteration',
      enter: (s, a) => { s.dealOffer = a.rand(s) < chance; },
      say: s.dealOffer
        ? `I have read your proposals. If your group will support the Budget, I will ask the meeting's consent to alter the motion and take them in, as we did with Labour's last year.`
        : 'I have read your proposals, and I cannot take them into the Budget as they stand. You are free to move them as an amendment.',
      plain: s.dealOffer ? 'If you agree, your proposals become part of the Executive\'s Budget and are not voted on separately. In return, your group is expected to vote for the Budget.' : `The Executive Member was more likely to agree to something balanced, small, checked by the officer and shared early.${b.cutsTax ? ' A council tax cut is the hardest thing to take in.' : ''}`,
      choices: s.dealOffer ? [
        { id: 'accept', label: 'Agree: your group will vote for the Budget with your proposals in it', fx: (s, a) => { a.flag(s, 'folded_' + s.group, 'The Executive Member agreed to take your proposals into the Budget.'); s.myVote.budget = 'for'; a.score(s, 'infl', 3, 'You got your proposals into the Budget before the vote.'); a.add(s, `disc.${s.group}`, -4, 'Some of your group were uneasy about backing the coalition\'s Budget.'); } },
        { id: 'refuse', label: 'Decline: you would rather move it yourselves and keep your hands free', fx: (s, a) => a.score(s, 'faith', 0) },
      ] : [{ id: 'ok', label: 'Move it as an amendment, then', fx: (s, a) => a.add(s, 'cred', 0) }],
      next: 'prep:allies1',
    };
  }
  if (fam === 'prep' && (a1 === 'allies1' || a1 === 'allies2')) {
    const round = a1 === 'allies1' ? 1 : 2;
    const asked = s.asked || [];
    return {
      phase: 'prep', who: 'narrator', mins: 0,
      say: round === 1 ? 'You have time for two conversations before the meeting. Who do you talk to first?' : 'And your second conversation?',
      plain: `The opposition groups hold ${opposition} seats to the coalition's ${coalition}. An amendment only carries if most of them vote for it. Groups with different priorities have to agree on that.`,
      choices: [
        ...others(s, api).filter((g) => !asked.includes(g)).map((g) => ({
          id: 'g_' + g, label: G[g].seats === 1 ? `The ${G[g].short} councillor` : `The ${G[g].short} group leader`,
          fx: (s, a) => {
            (s.asked ||= []).push(g);
            const theirs = s.amend.kind !== 'none' && build(s, a).groups.includes(g) ? 8 : 0;
            a.lean(s, myQ(s), g, 10 + theirs, theirs ? `The ${G[g].short} group saw its own ideas in your amendment.` : `You talked to the ${G[g].short} group before the meeting.`);
            if (!s.allyFor && a.rand(s) < 0.5 + theirs / 20) s.allyFor = g;
          },
        })),
        { id: 'cm', label: 'The Executive Member for Resources', when: (s) => !asked.includes('cm') && s.amend.kind !== 'none' && !s.flags.includes('seek_deal'), fx: (s, a) => { (s.asked ||= []).push('cm'); a.flag(s, 'briefed_cm', 'You briefed the Executive Member before the meeting.'); a.add(s, 'cred', 5, 'Briefing the Executive privately gave them time to consider your ideas.'); } },
        { id: 'none', label: 'Nobody: spend the time on your speech', fx: (s, a) => { (s.asked ||= []).push('none' + round); a.add(s, 'cred', 3, 'You spent the time preparing your figures.'); } },
      ],
      next: round === 1 ? 'prep:allies2' : 'prep:group',
    };
  }
  if (id === 'prep:group') {
    return {
      phase: 'prep', who: 'group', mins: 0,
      say: `Your ${G[s.group].seats} councillors meet the night before. How will you vote on the other groups' amendments?`,
      plain: 'Each group decides how it votes. A group line keeps you together; a free vote is honest about differences, but splits show in a recorded vote.',
      choices: [
        { id: 'line', label: 'Agree a group line on every vote, decided in the room', fx: (s, a) => a.add(s, `disc.${s.group}`, 12, 'Your group agreed a line on every vote.') },
        { id: 'free', label: 'Allow a free vote on the other groups\' amendments', fx: (s, a) => { a.add(s, `disc.${s.group}`, -20, 'You allowed your group a free vote.'); a.score(s, 'faith', 1, 'You let members vote as they judged.'); } },
        { id: 'joint', label: 'Agree to back any opposition amendment that balances', fx: (s, a) => { a.flag(s, 'joint', 'Your group agreed to back any opposition amendment that balances.'); others(s, a).forEach((g) => a.lean(s, myQ(s), g, 6)); } },
      ],
      next: 'm:open',
    };
  }

  /* Administration preparation */
  if (id === 'prep:a_group') {
    return {
      phase: 'prep', who: 'group', mins: 0,
      say: 'The coalition\'s group leaders meet. One partner group is uneasy about raising fees and charges by 2% again, and asks for a freeze on some charges.',
      plain: `The figures assume fees rise 2% a year. Each one per cent is worth ${P.feesSens.value}. A coalition of four groups holds together by listening; it falls apart one partner at a time.`,
      figs: ['feesSens'],
      choices: [
        { id: 'listen', label: 'Listen, and ask officers to cost a freeze on the charges they name', fx: (s, a) => { a.ADMIN.forEach((g) => a.add(s, `disc.${g}`, 3)); a.add(s, `disc.${s.partner}`, 4, 'Your partners felt heard.'); a.score(s, 'faith', 1, 'You dealt with doubts on the merits.'); } },
        { id: 'whip', label: 'Say the coalition agreed the assumptions and must stick to them', fx: (s, a) => { a.add(s, `disc.${s.partner}`, -10, 'A partner felt overruled, not answered.'); a.lean(s, 'budget', s.partner, -15, 'That partner\'s members drifted towards abstaining.'); a.flag(s, 'partner_unhappy'); } },
        { id: 'promise', label: 'Promise a freeze, paid for from the Revenue Support Reserve every year', fx: (s, a) => { a.flag(s, 'promise', 'You promised to pay for a continuing cost from a reserve.'); a.add(s, `disc.${s.partner}`, 4); } },
      ],
      next: (s) => (s.flags.includes('promise') ? 'prep:a_s151' : 'prep:a_reach'),
    };
  }
  if (id === 'prep:a_s151') {
    return {
      phase: 'prep', who: 's151', mins: 0, rule: 'reserves',
      say: 'I have to tell you that paying for a continuing cost from an earmarked reserve is against the council\'s reserves policy. In my report I would have to say the Budget relies on it.',
      plain: 'The Section 25 report must say whether the estimates are robust and reserves adequate. Council must have regard to it.',
      choices: [
        { id: 'withdraw', label: 'Go back to your partner and offer a one-year freeze instead', fx: (s, a) => { a.add(s, `disc.${s.partner}`, -2, 'Your partner accepted a smaller promise.'); a.score(s, 'proc', 1, 'You kept the Budget within policy.'); a.flag(s, 'promise_withdrawn'); } },
        { id: 'keep', label: 'Keep the promise: you will find the money later', fx: (s, a) => { a.flag(s, 'policy_risk', 'You took a promise against reserves policy into the meeting.'); a.score(s, 'proc', -2, 'You set aside the Section 151 Officer\'s advice.'); } },
      ],
      next: 'prep:a_reach',
    };
  }
  if (id === 'prep:a_reach') {
    return {
      phase: 'prep', who: 'narrator', mins: 0, rule: 'alteration',
      say: `You have ${coalition} seats. To be sure of the Budget you need votes from outside the coalition. Last year the Executive took Labour's proposals into its own motion. Do you approach an opposition group?`,
      plain: 'Taking a group\'s ideas in costs money you have to find, and wins that group\'s votes for the Budget. Their amendment is then not moved.',
      choices: [
        ...M.map((g) => ({ id: 'g_' + g, label: `Offer the ${G[g].short} group room in the Budget for one costed proposal`, fx: (s, a) => {
          // The same odds for every group: they turn on how you have run the coalition so far, not on party.
          const p = 0.5 + (s.v.cred - 50) / 200 + s.v.mood * 0.05 + (s.flags.includes('partner_unhappy') ? -0.15 : 0);
          if (a.rand(s) < p) { a.flag(s, 'folded_' + g, `The ${G[g].short} group agreed to back the Budget with its idea taken in.`); a.score(s, 'infl', 2, 'You widened support for the Budget beyond the coalition.'); a.ADMIN.forEach((x) => a.add(s, `disc.${x}`, -2)); }
          else { a.flag(s, 'refused_' + g, `The ${G[g].short} group turned down your offer, and will move its own amendment.`); a.lean(s, 'budget', g, 5, 'They noted that you asked.'); }
        } })),
        { id: 'none', label: 'No: the Budget stands as the Executive agreed it', fx: (s, a) => a.ADMIN.forEach((x) => a.add(s, `disc.${x}`, 2, 'Your partners liked a firm line.')) },
      ],
      next: 'prep:a_attend',
    };
  }
  if (id === 'prep:a_attend') {
    const L = api.ADMIN[0];
    return {
      phase: 'prep', who: 'narrator', mins: 0, rule: 'majority',
      say: `${Object.values(s.att).reduce((n, g) => n + g.seats - g.present, 0)} councillors have sent apologies so far, some of them from the coalition. Do you chase attendance?`,
      plain: 'Votes are decided by a simple majority of those present and voting. With 19 seats, every absence on your side matters, and so does every opposition councillor who abstains.',
      choices: [
        { id: 'chase', label: 'Chase every coalition member who can attend', fx: (s, a) => { a.ADMIN.forEach((g) => { s.att[g].present = s.att[g].seats; }); a.score(s, 'proc', 1, 'You made sure the coalition would be there.'); } },
        { id: 'leave', label: 'Leave it: people have their reasons', fx: (s, a) => a.add(s, `disc.${L}`, 0) },
      ],
      next: 'm:open',
    };
  }

  /* ===== The meeting ===== */
  if (id === 'm:open') {
    return {
      phase: 'meeting', item: 0, who: 'chair', mins: 5, rule: 'recorded',
      say: 'Good evening, and welcome to this meeting of Full Council to set the Budget. Members will stand to speak and address the chair. Every vote on the Budget tonight, and on every amendment, will be a recorded vote.',
      plain: 'The names of everyone voting for, against or abstaining go in the minutes (rule 16.7). The Mayor chairs, votes as a councillor, and has a casting vote on a tie.',
      choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: 'm:apol',
    };
  }
  if (id === 'm:apol') {
    const away = Object.values(s.att).reduce((n, g) => n + g.seats - g.present, 0);
    return {
      phase: 'meeting', item: 1, who: 'ce', mins: 2, rule: 'majority',
      say: `Thank you, Mayor. We have apologies from ${away} councillor${away === 1 ? '' : 's'}.`,
      plain: 'The rules make no provision for voting from home: members vote in the room. Every absence changes the arithmetic.',
      choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: 'm:decl',
    };
  }
  if (id === 'm:decl') {
    const base = {
      phase: 'meeting', item: 2, who: 'mo', mins: 3, rule: 'arrears',
      say: 'Declarations of interest. And a reminder: any member who has had council tax unpaid for two months or more must say so now, and may not vote on the Budget. That is the law.',
      next: 'm:move',
    };
    if (s.personal === 'arrears') return { ...base, plain: 'Your direct debit failed in December and you missed the letter. You are now more than two months behind. What do you do?', choices: [
      { id: 'declare', label: 'Rise and say so: you will speak but not vote', fx: (s, a) => { a.flag(s, 'arrears_declared', 'You declared council tax arrears and gave up your vote, as the law requires.'); a.score(s, 'proc', 2, 'You complied with section 106.'); } },
      { id: 'quiet', label: 'Say nothing: the payment is already going through', fx: (s, a) => { a.flag(s, 'arrears_hidden', 'You voted while in council tax arrears.'); a.score(s, 'proc', -5, 'Voting in arrears breaks section 106 of the Local Government Finance Act 1992.'); } },
    ] };
    if (s.personal === 'interest') return { ...base, rule: 'interests', plain: 'You are a trustee of a local swimming club that would use any free swimming offer. What do you do?', choices: [
      { id: 'declare', label: 'Declare an other interest (non-prejudicial), and stay to take part', fx: (s, a) => { a.flag(s, 'declared', 'You declared your interest at the start.'); a.score(s, 'proc', 2, 'You declared the interest at the right time.'); } },
      { id: 'quiet', label: 'Say nothing: it is a voluntary role', fx: (s, a) => { a.flag(s, 'undeclared', 'You did not declare an interest.'); a.score(s, 'proc', -1); } },
      { id: 'leave', label: 'Leave the chamber for the whole Budget item', fx: (s, a) => { a.flag(s, 'declared'); a.score(s, 'infl', -2, 'You were not in the room to argue your case.'); } },
    ] };
    return { ...base, plain: 'Nothing on tonight\'s agenda affects you or anyone close to you.', choices: [{ id: 'none', label: 'Nothing to declare', fx: (s, a) => a.score(s, 'proc', 0) }] };
  }
  if (id === 'm:move') {
    const deals = M.filter((g) => folded(s, g));
    return {
      phase: 'meeting', item: 3, who: admin ? 'me' : 'cm', mins: 12, figs: ['gap', 'ctAssumed', 'generalReserve'], rule: 'speech',
      say: `Mayor, I move the Executive's Budget for 2027/28. It closes the gap the officers set out, with a council tax rise of ${P.ctAssumed.value}, and keeps the General Reserve at ${P.generalReserve.value}.${deals.length ? ` With the consent of the meeting, I alter the motion to take in the proposals of the ${deals.map((g) => G[g].short).join(' and ')} group${deals.length > 1 ? 's' : ''}.` : ''}`,
      plain: `This is a rehearsal: the real Budget papers come out in February 2027. The Leader seconds. Speeches are limited to five minutes.${deals.length ? ' Altering a motion needs the consent of the meeting, given without discussion (rule 14.7).' : ''}`,
      choices: admin ? [
        { id: 's25', label: 'Open with the officers\' gap and how the Budget closes it', fx: (s, a) => { a.add(s, 'cred', 6, 'You opened with the figures.'); a.OPP.forEach((g) => a.lean(s, 'budget', g, 3)); } },
        { id: 'people', label: 'Open with what the Budget protects for residents', fx: (s, a) => { a.add(s, 'mood', 1, 'Opening with residents warmed the room.'); a.OPP.forEach((g) => a.lean(s, 'budget', g, 2)); } },
        { id: 'attack', label: 'Open by saying the opposition has no plan of its own', fx: (s, a) => { a.add(s, 'mood', -1, 'An attacking opening set a hard tone.'); a.OPP.forEach((g) => a.lean(s, 'budget', g, -6, 'The opposition hardened against the Budget.')); } },
      ] : [
        { id: 'listen', label: 'Listen, and note the figures you will use', fx: (s, a) => a.add(s, 'cred', 2, 'You noted the Executive\'s own figures.') },
        { id: 'heckle', label: 'Heckle about the coalition having only 19 seats', fx: (s, a) => { a.add(s, 'chair', -1, 'The Mayor warned you for heckling.'); a.add(s, 'mood', -1, 'Heckling set a hard tone.'); } },
      ],
      next: (s, a) => { const live = movers(a).filter((g) => !folded(s, g)); return live.length ? `am:${live[0]}:intro` : 'sub:debate'; },
    };
  }

  /* ===== Amendments, one at a time (rule 14.6) ===== */
  if (fam === 'am') {
    const h = a1, step = a2;
    const g = G[h];
    const mine = !admin && h === s.group;
    const q = { id: 'am_' + h, prior: 'other', label: `${g.short} Group amendment`, chairCasts: 'against' };
    const base = { phase: 'meeting', item: 4 + M.indexOf(h), group: h };
    const after = () => { const live = M.filter((x) => !folded(s, x)); const i = live.indexOf(h); return i >= 0 && i < live.length - 1 ? `am:${live[i + 1]}:intro` : 'sub:debate'; };

    if (folded(s, h)) return { ...base, skip: () => true, next: after() };
    if (step === 'intro') {
      if (mine && s.amend.kind === 'none') {
        return { ...base, who: 'chair', mins: 4, say: `I call the Leader of the ${g.short} Group.`, plain: 'Your group brought no amendment, so you speak in the debate and there is no vote on your proposals.',
          choices: [
            { id: 'costed', label: 'Set out where the Budget falls short, with figures from the papers', fx: (s, a) => { a.add(s, 'cred', 4, 'Your speech used the papers\' own figures.'); a.score(s, 'faith', 1, 'A fair, figured speech.'); } },
            { id: 'attack', label: 'Attack the coalition\'s record', fx: (s, a) => a.add(s, 'mood', -1, 'An attacking speech set a hard tone.') },
          ], next: after() };
      }
      if (mine) {
        const b = build(s, api);
        return { ...base, who: 'chair', mins: 4, rule: s.amend.circulate === 'night' ? 'noNotice' : 'oneAtATime',
          say: `I call the Leader of the ${g.short} Group to move the group's amendment.${s.amend.circulate === 'night' ? ' Members have not seen it, so I ask that it be handed to me in writing first.' : ''}`,
          plain: `Your amendment changes 2027/28 by ${api.money(b.years[0].spend)} before funding. ${s.amend.s151 === 'confirmed' ? 'The Director of Resources has checked it.' : ''} You have five minutes. How do you open?`,
          choices: [
            { id: 'people', label: 'Open with who it helps', fx: (s, a) => { others(s, a).forEach((x) => a.lean(s, myQ(s), x, 4)); a.add(s, 'mood', 1, 'Opening with residents warmed the room.'); } },
            { id: 'money', label: 'Open with the money: how it balances, and the officer\'s check', fx: (s, a) => { a.add(s, 'cred', s.amend.s151 === 'confirmed' ? 8 : 2, 'You opened with how the amendment is paid for.'); others(s, a).forEach((x) => a.lean(s, myQ(s), x, 3)); a.ADMIN.forEach((x) => a.lean(s, myQ(s), x, 3)); } },
            { id: 'attack', label: 'Open with an attack on the coalition', fx: (s, a) => { a.add(s, `disc.${s.group}`, 5, 'Your group enjoyed the attack.'); a.add(s, 'mood', -1, 'The attack hardened the coalition benches.'); a.score(s, 'faith', -1, 'The opening was about them, not the amendment.'); } },
          ], next: `am:${h}:second` };
      }
      return { ...base, who: 'opp:' + h, mins: 15,
        say: `Mayor, I move the ${g.short} Group's amendment to the Budget. It changes the Budget's priorities for 2027/28 and sets out how each change is paid for.`,
        plain: 'No group\'s amendment for 2027/28 has been published, so the simulator does not put words or policies in any real group\'s mouth: this one stands for whatever they bring. A colleague seconds and reserves the right to speak (rule 14.3).',
        choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
        enter: (s, a) => { s.ev ||= {}; s.ev[h] = drawMoves(s, a, THEIRS, 1, h); },
        next: `am:${h}:ev0` };
    }
    if (step === 'second') {
      return { ...base, who: 'seconder', mins: 3,
        say: 'Mayor, I second the amendment.',
        plain: 'Your seconder can speak now, or reserve their speech for later in the debate (rule 14.3).',
        choices: [
          { id: 'reserve', label: 'Ask them to reserve their speech', fx: (s, a) => { a.flag(s, 'sec_reserved'); a.score(s, 'proc', 1, 'Your seconder kept a speech for later.'); } },
          { id: 'now', label: 'Ask them to speak now, while the room is listening', fx: (s, a) => a.add(s, 'cred', 2, 'Your seconder backed you up straight away.') },
        ],
        enter: (s, a) => { s.ev ||= {}; s.ev[h] = drawMoves(s, a, MINE, 2, h); },
        next: `am:${h}:ev0` };
    }
    if (step === 'ev0' || step === 'ev1') {
      const k = step === 'ev0' ? 0 : 1;
      const mv = moveById((s.ev?.[h] || [])[k]);
      const nx = mine ? (k === 0 ? `am:${h}:ev1` : `am:${h}:reply`) : `am:${h}:line`;
      if (!mv) return { ...base, skip: () => true, next: nx };
      return { ...base, who: val(mv.who, s, api, h), mins: 10,
        say: val(mv.say, s, api, h), plain: mv.plain,
        choices: mv.choices.map((c) => ({ ...c, fx: (s, a, p) => c.fx(s, a, h, p) })),
        next: nx };
    }
    if (step === 'reply') {
      return { ...base, who: 'me', mins: 5, rule: 'reply',
        say: 'The Mayor calls you to reply to the debate on your amendment.',
        plain: 'The mover of the amendment replies first, then the mover of the Budget, and then the vote. No new matter in a reply.',
        choices: [
          { id: 'money', label: 'Answer the hardest question raised, about the money', fx: (s, a) => { a.add(s, 'cred', 6, 'Your reply answered the money question.'); others(s, a).forEach((x) => a.lean(s, myQ(s), x, 4)); a.score(s, 'faith', 1, 'You answered the debate, not just repeated your speech.'); } },
          { id: 'correct', label: 'Correct the record on what your amendment does', when: (s) => s.flags.includes('correct_in_reply'), fx: (s, a) => { a.add(s, 'cred', 4, 'You corrected the record calmly in your reply.'); a.score(s, 'proc', 1); } },
          { id: 'unite', label: 'Appeal to every opposition group: together you can carry this', fx: (s, a) => { others(s, a).forEach((x) => a.lean(s, myQ(s), x, a.has(s, 'early') ? 6 : 2)); a.add(s, 'mood', -1, 'The coalition benches bristled.'); } },
          { id: 'new', label: 'Add a new line to answer a point made in the debate', fx: (s, a) => { a.add(s, 'chair', -1, 'The Mayor stopped you: a reply may not bring in new matter.'); a.score(s, 'proc', -1, 'Rule 14.9(f): no new matter in a reply.'); } },
        ],
        next: `am:${h}:leader` };
    }
    if (step === 'leader') {
      const stance = adminStance(s, api, h);
      const concede = stance !== 'for' && (api.has(s, 'briefed_cm') ? 0.35 : 0.05) + Math.max(0, s.v.cred - 60) / 150 + s.v.mood * 0.08;
      const theme = leadTheme(s, api);
      return { ...base, who: 'cm', mins: 6,
        enter: (s, a) => { if (stance !== 'for' && a.rand(s) < concede) { s.concession = theme; a.note(s, `The Executive Member committed, in reply, to look at ${theme} during the year, after the ${G[h].short} amendment.`); a.score(s, 'infl', 3, 'You won a commitment on the record.'); } },
        say: stance === 'for'
          ? 'Mayor, this is small, it balances, and the officers have checked it. The Executive will support it.'
          : s.concession
            ? `Mayor, we cannot support this tonight. But the point about ${s.concession} is a fair one, and I will bring it back to the Executive during the year.`
            : s.amend.circulate === 'night'
              ? 'Mayor, members were given no time to read this, and a budget is not built in an evening. We will vote against it.'
              : 'Mayor, the Budget before you balances, and the Director of Resources has reported on it. We will vote against this amendment.',
        plain: 'The mover of the Budget replies last, and then the amendment is put to the vote without further debate (rule 14.9).',
        choices: [{ id: 'vote', label: 'To the vote', fx: (s) => { s.myVote['am_' + h] = 'for'; } }],
        next: `am:${h}:vote` };
    }
    if (step === 'line') {
      return { ...base, who: 'chair', mins: 4,
        say: `The ${g.short} Group's mover replies, and the Executive Member replies. We now move to the recorded vote on the ${g.short} Group amendment.`,
        plain: admin
          ? `You advise the coalition how to vote. You have ${coalition} seats; if the opposition groups back this, it carries and the Budget becomes an in-principle decision.`
          : `How does your group vote? ${opposition} opposition seats against ${coalition}: if the opposition votes together, this carries.`,
        choices: admin ? [
          { id: 'against', label: 'Against', fx: (s) => { (s.adminLine ||= {})[h] = 'against'; s.myVote['am_' + h] = 'against'; } },
          { id: 'abstain', label: 'Abstain', fx: (s, a) => { (s.adminLine ||= {})[h] = 'abstain'; s.myVote['am_' + h] = 'abstain'; a.score(s, 'faith', 1, 'You did not oppose a sound amendment for the sake of it.'); } },
          { id: 'for', label: 'For', fx: (s, a) => { (s.adminLine ||= {})[h] = 'for'; s.myVote['am_' + h] = 'for'; a.lean(s, 'budget', h, 12, `Backing their amendment brought the ${G[h].short} group closer to the Budget.`); a.score(s, 'infl', 1); } },
        ] : [
          { id: 'for', label: 'For', fx: (s, a) => { s.myVote['am_' + h] = 'for'; a.lean(s, myQ(s), h, 6, `You voted for the ${G[h].short} amendment, and they remembered.`); } },
          { id: 'abstain', label: 'Abstain', fx: (s) => { s.myVote['am_' + h] = 'abstain'; } },
          { id: 'against', label: 'Against', fx: (s, a) => { s.myVote['am_' + h] = 'against'; a.lean(s, myQ(s), h, -8, `You voted against the ${G[h].short} amendment, and they remembered.`); } },
        ],
        next: `am:${h}:vote` };
    }
    if (step === 'vote') {
      return { ...base, who: 'ce', mins: 5, tally: 'am_' + h, rule: 'recorded',
        enter: (s, a) => {
          const whip = {};
          for (const x of a.data.chamber.groups.map((gg) => gg.id)) {
            if (a.isAdmin(x)) whip[x] = adminStance(s, a, h);
            else if (x === s.group) whip[x] = s.myVote['am_' + h] || (x === h ? 'for' : null);
            else if (x === h) whip[x] = 'for';
          }
          if (s.flags.includes('joint') && h !== s.group && !admin && a.build(s).balanced !== false) whip[s.group] = s.myVote['am_' + h] || 'for';
          const v = a.vote(s, { ...q, prior: 'other', whip });
          if (v.carried) a.flag(s, 'carried_' + h, `The ${G[h].short} amendment was carried, and the Budget became an in-principle decision.`);
        },
        say: (() => { const v = s.votes['am_' + h]; return v ? `Mayor, on the ${g.short} Group amendment: ${v.for} for, ${v.against} against, ${v.abstain} abstaining. The amendment is ${v.carried ? 'carried' : 'lost'}.` : ''; })(),
        plain: (() => { const v = s.votes['am_' + h]; if (!v) return ''; return `${v.cast ? 'The votes were equal, so the Mayor used the casting vote. ' : ''}${v.absent} councillor${v.absent === 1 ? ' was' : 's were'} absent. ${v.carried ? 'The amended motion is now the substantive motion (rule 14.6).' : `Last year the Conservative amendment was lost ${P.lastAmend.value}.`}`; })(),
        figs: ['lastAmend'],
        choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
        next: after() };
    }
  }

  /* ===== The substantive motion ===== */
  const item = 4 + M.length;
  if (id === 'sub:debate') {
    const amended = M.filter((h) => s.flags.includes('carried_' + h));
    return {
      phase: 'meeting', item, who: 'chair', mins: 30,
      say: `The amendments have been dealt with. We return to the Budget${amended.length ? ', as amended' : ''}.`,
      plain: amended.length ? 'Because the Budget has been amended, if it is agreed it will be an in-principle decision: it takes effect in five working days unless the Leader objects (Budget and Policy Framework Rules 2(e) and 2(g)).' : 'Members who have spoken on an amendment may now speak on the main question (rule 14.5).',
      rule: amended.length ? 'inPrinciple' : 'speech',
      choices: admin ? [
        { id: 'speak', label: 'Speak on the Section 25 report and the gap still ahead', fx: (s, a) => { a.add(s, 'cred', 5, 'You used the debate to explain the officer\'s report.'); a.OPP.forEach((g) => a.lean(s, 'budget', g, 3)); } },
        { id: 'promise', label: 'Announce the charges freeze you promised your partner', when: (s) => s.flags.includes('policy_risk'), fx: (s, a) => a.flag(s, 'promise_public', 'You announced a freeze paid from reserves on the floor of the chamber.') },
        { id: 'listen', label: 'Listen, and note points for your reply', fx: (s, a) => a.add(s, 'cred', 1) },
      ] : [
        { id: 'against', label: 'Speak against the Budget, and say your group will vote against', when: (s) => !folded(s, s.group), fx: (s, a) => { s.myVote.budget = 'against'; a.add(s, `disc.${s.group}`, 3); } },
        { id: 'abstain', label: 'Explain why your group will abstain', when: (s) => !folded(s, s.group), fx: (s, a) => { s.myVote.budget = 'abstain'; a.score(s, 'faith', 1, 'You explained a nuanced position.'); } },
        { id: 'support', label: 'Say your group will support the Budget, with its reservations on record', fx: (s, a) => { s.myVote.budget = 'for'; a.score(s, 'faith', 1, 'You put reservations on record without opposing for its own sake.'); } },
      ],
      next: (s) => (s.clock > 180 ? 'sub:continue' : 'sub:reply'),
    };
  }
  if (id === 'sub:continue') {
    return { phase: 'meeting', item, who: 'chair', mins: 2, rule: 'threeHours',
      say: 'Members, we have been sitting for three hours. Under the rules the meeting adjourns unless the Council votes to continue. All those in favour of continuing?',
      plain: 'A Budget left unfinished tonight means another meeting. The vote to continue is carried by a show of hands.',
      choices: [{ id: 'yes', label: 'Vote to continue', fx: (s, a) => a.score(s, 'proc', 1, 'You knew the three-hour rule.') }, { id: 'no', label: 'Vote against continuing', fx: (s, a) => a.add(s, 'mood', -1, 'Voting to stop mid-Budget irritated the chamber.') }],
      next: 'sub:reply' };
  }
  if (id === 'sub:reply') {
    if (admin && s.flags.includes('promise_public')) {
      return { phase: 'meeting', item, who: 's151', mins: 6, rule: 'reserves',
        say: 'Mayor, I must advise Council. Paying for a continuing freeze from an earmarked reserve is against the council\'s reserves policy, and I could not call that part of the Budget robust.',
        plain: 'Council must have regard to the Section 151 Officer\'s report when it sets the Budget.',
        choices: [
          { id: 'withdraw', label: 'Withdraw it, and offer a one-year freeze instead', fx: (s, a) => { a.add(s, 'cred', -10, 'You had to withdraw a promise in public.'); a.add(s, `disc.${s.partner}`, -4); a.score(s, 'proc', -1); } },
          { id: 'insist', label: 'Insist the money will be found', fx: (s, a) => { a.flag(s, 'unsound', 'You insisted on a Budget the Section 151 Officer could not call robust.'); a.score(s, 'proc', -4); a.OPP.forEach((g) => a.lean(s, 'budget', g, -15)); a.ADMIN.forEach((g) => a.lean(s, 'budget', g, -10)); } },
        ],
        next: 'sub:vote' };
    }
    return {
      phase: 'meeting', item, who: admin ? 'me' : 'cm', mins: 5, rule: 'reply',
      say: s.concession
        ? `Mayor, I thank members for the debate. I have said we will look again at ${s.concession}, and we will. I commend the Budget to the Council.`
        : 'Mayor, I thank members for the debate. This Budget balances, the Director of Resources has reported on it, and I commend it to the Council.',
      plain: 'The mover of the motion has the last word, with no new matter, and then the recorded vote (rule 14.9).',
      choices: [{ id: 'vote', label: 'To the vote', fx: (s) => { if (s.role === 'admin') s.myVote.budget = 'for'; } }],
      next: 'sub:vote',
    };
  }
  if (id === 'sub:vote') {
    return { phase: 'meeting', item: item + 1, who: 'ce', mins: 8, tally: 'budget', rule: 'recorded', figs: ['lastBudget'],
      enter: (s, a) => {
        const whip = {};
        a.ADMIN.forEach((g) => { whip[g] = 'for'; });
        movers(a).forEach((g) => { if (folded(s, g)) whip[g] = 'for'; });
        if (s.role === 'opp') whip[s.group] = s.myVote.budget || (folded(s, s.group) ? 'for' : 'abstain');
        a.vote(s, { id: 'budget', prior: 'budget', label: 'The Budget', whip, chairCasts: 'for' });
      },
      say: (() => { const v = s.votes.budget; return v ? `Mayor, on the Budget: ${v.for} for, ${v.against} against, ${v.abstain} abstaining. The Budget is ${v.carried ? 'carried' : 'lost'}.` : ''; })(),
      plain: (() => { const v = s.votes.budget; if (!v) return ''; return `${v.cast ? 'The votes were equal, so the Mayor used the casting vote. ' : ''}Last year the Budget was carried ${P.lastBudget.value}, with a different council.`; })(),
      choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: (s, a) => (!s.votes.budget?.carried ? 'end' : M.some((h) => s.flags.includes('carried_' + h)) ? 'res:objection' : 'res') };
  }
  if (id === 'res:objection') {
    const amendedBy = M.filter((h) => s.flags.includes('carried_' + h)).map((h) => G[h].short).join(' and ');
    // The Leader objects to a chamber in a bad mood, and to an amendment that is risky or does not balance.
    const leaderObjects = (s, a) => s.v.mood < 0 || (s.flags.includes('carried_' + s.group)
      ? s.v.cred < 55 || build(s, a).risky.length > 0 || s.flags.includes('unbalanced')
      : a.rand(s) < 0.5);
    return { phase: 'meeting', item: item + 2, who: 'mo', mins: 2, rule: 'objection',
      say: `Mayor, as the Budget has been amended by the ${amendedBy} amendment, tonight's decision is in principle. It takes effect in five working days unless the Leader objects in writing, with reasons.`,
      plain: admin ? 'You are the Deputy Leader. What do you advise the Leader?' : 'The Leader of the Council now decides whether to object. If the Leader objects, Council meets again within ten working days and decides by simple majority.',
      choices: admin ? [
        { id: 'accept', label: 'Advise the Leader to accept the amended Budget', fx: (s, a) => { a.score(s, 'faith', 1, 'You respected the chamber\'s decision.'); } },
        { id: 'object', label: 'Advise the Leader to object, and take it back to Council', fx: (s, a) => { a.flag(s, 'objected', 'The Leader objected to the in-principle decision.'); a.add(s, 'mood', -1); } },
      ] : [{ id: 'wait', label: 'Wait for the Leader\'s decision', fx: (s, a) => { if (leaderObjects(s, a)) a.flag(s, 'objected', 'The Leader objected to the in-principle decision.'); } }],
      next: 'end' };
  }
  if (id === 'res') {
    return { phase: 'meeting', item: item + 2, who: 'chair', mins: 2, figs: ['bandD'],
      say: 'The Budget is agreed, and with it the council tax for the borough for 2027/28. That concludes our business.',
      plain: `Because the Executive's Budget was agreed without amendment, the decision has immediate effect. For comparison, the borough's Band D for 2026/27 was ${P.bandD.value}; parish and county shares are added to the bills.`,
      choices: [{ id: 'end', label: 'See how you did', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: 'end' };
  }

  if (id === 'end') return { kind: 'end', phase: 'end', ending: ending(s, api), who: 'narrator', say: '', plain: '' };
  throw new Error('Unknown beat ' + id);
}

/* ---------------- endings ---------------- */
export const ENDINGS = {
  misstep: { title: 'A procedural misstep', text: 'Whatever happened to the Budget, your own conduct is now the story. Declarations of interest and the council tax rule are personal duties, and they come before anything you say.' },
  nobudget: { title: 'No Budget tonight', text: 'The Budget was voted down. The council still has to set a Budget and a council tax, so Full Council will have to meet again, and the Executive must find a Budget the chamber will pass.' },
  objected: { title: 'Amended, then objected to', text: 'The chamber amended the Budget, and the Leader objected. Council must meet again within ten working days and decide by simple majority between the Executive\'s Budget and the amended one.' },
  amended: { title: 'Budget agreed, as amended', text: 'The Budget was agreed with an opposition amendment in it. The decision took effect after five working days without an objection from the Leader. With 19 of 45 seats, the coalition could not stop it.' },
  folded: { title: 'Your proposals are in the Budget', text: 'Your group\'s proposals went into the Executive\'s Budget before the vote, as Labour\'s did last year, and your group voted for it. That is influence without winning a vote.' },
  carried: { title: 'Your amendment carried', text: 'Your amendment carried, because the opposition groups voted together. The Budget became an in-principle decision, and the Leader did not object.' },
  record: { title: 'Amendment lost, commitment won', text: 'Your amendment was lost. But the Executive Member committed, on the record, to look at part of it during the year. That is a result you can hold them to.' },
  defeated: { title: 'Amendment lost', text: 'Your amendment was lost. It is in the minutes, with every councillor\'s vote on it, and that record is what you can point to when the issue returns.' },
  statement: { title: 'A speech, not an amendment', text: 'Your group spoke, but there was nothing to vote on. A costed amendment gives the chamber something concrete to accept or reject, and puts every vote on the record.' },
  widened: { title: 'Budget agreed, with wider support', text: 'The Budget was agreed as moved, with an opposition group\'s ideas taken in beforehand. A minority coalition passed its Budget by sharing it.' },
  concession: { title: 'Budget agreed, with a commitment on the record', text: 'The Budget was agreed as moved, and the minutes record a commitment to look again at an opposition idea during the year.' },
  clean: { title: 'Budget agreed as moved', text: 'The Budget was agreed as moved, with immediate effect. With 19 seats out of 45, that was not certain until the votes were counted.' },
};
export function ending(s, api) {
  const M = movers(api);
  if (s.flags.includes('arrears_hidden') || s.flags.includes('named') || (s.flags.includes('undeclared') && !s.flags.includes('declared_late'))) return 'misstep';
  if (s.votes.budget && !s.votes.budget.carried) return 'nobudget';
  if (s.flags.includes('objected')) return 'objected';
  if (s.role === 'admin') {
    if (M.some((h) => s.flags.includes('carried_' + h))) return 'amended';
    if (M.some((h) => folded(s, h))) return 'widened';
    if (s.record.length) return 'concession';
    return 'clean';
  }
  if (folded(s, s.group)) return 'folded';
  if (s.amend.kind === 'none') return 'statement';
  if (s.flags.includes('carried_' + s.group)) return 'carried';
  if (M.some((h) => s.flags.includes('carried_' + h))) return 'amended';
  if (s.record.length) return 'record';
  return 'defeated';
}

export function counterfactual(s) {
  const f = (x) => s.flags.includes(x);
  if (f('arrears_hidden')) return 'Had you declared your arrears, you would have lost your vote tonight but nothing else. Voting while in arrears is a criminal offence under section 106.';
  if (f('undeclared')) return 'Had you declared your interest at the start, nobody could have used it against you in the debate.';
  if (f('unsound')) return 'Had you withdrawn the freeze when the Section 151 Officer first warned you, the Budget would not have rested on a promise against policy.';
  if (s.role === 'admin' && !['lab', 'con', 'ref'].some((g) => f('folded_' + g)) && s.votes.budget && !s.votes.budget.carried) return 'Had you taken one opposition group\'s ideas into the Budget beforehand, as the Executive did with Labour last year, you would have had the votes.';
  if (s.role === 'admin' && f('partner_unhappy')) return 'Had you listened to your uneasy partner, the coalition would have voted as one.';
  if (f('unbalanced')) return 'Had you gone back and balanced your amendment, the Executive Member would have lost the easy answer that it did not add up.';
  if (f('night')) return 'Had you shared your amendment a week early, other opposition groups would have had time to back it, and you need most of them to carry anything.';
  if (f('no_cfo')) return 'Had you asked the Director of Resources to check your lines, you could have quoted that in the chamber, as last year\'s Conservative amendment did.';
  if (s.role === 'opp' && !f('seek_deal') && s.amend.kind !== 'none') return 'Had you offered your proposals to the Executive first, they might have been taken into the Budget, as Labour\'s were last year.';
  return 'You made few mistakes. Try the other side of the chamber, or another group, and see how the same meeting looks from there.';
}

export const DEBRIEF_QUESTIONS = [
  'With 19 of 45 seats, what does the coalition have to give to pass its Budget, and what should it never give?',
  'With 26 seats between them, what would the opposition groups have to agree on to carry an amendment?',
  'When is taking an opposition group\'s ideas into the Budget better for that group than moving its own amendment?',
  'What does the in-principle rule protect, and who does it protect?',
  'What would you do differently in the weeks before the meeting?',
];

export const ROLE_CARDS = [
  ['The Mayor', 'Chairs the meeting, calls each speaker, rules on points of order, relevance and closure, and must stay apolitical. Votes as a councillor, and has a casting vote on a tie.'],
  ['The Chief Executive', 'Reads the apologies and calls the roll for every recorded vote.'],
  ['The Monitoring Officer', 'Gives the warning on council tax arrears, advises on declarations, and explains the in-principle rule.'],
  ['The Director of Resources (Section 151 Officer)', 'Reports on the robustness of the Budget and the adequacy of reserves. Speaks only to advise.'],
  ['The Deputy Leader (Executive Member for Resources)', 'Moves the Budget, may alter it with the consent of the meeting, and replies last on each amendment and on the Budget.'],
  ['The Leader of the Council', 'Seconds the Budget, and decides whether to object to an amended Budget.'],
  ['Each opposition group leader', 'Moves the group\'s amendment or speaks in the debate, and replies first on their own amendment.'],
  ['Councillors', 'Stand to speak, for no more than five minutes, speak once on each amendment if called, and vote in every recorded vote.'],
];
