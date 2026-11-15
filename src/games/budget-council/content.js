// Budget Council: everything that is said and every choice. Pure data and small functions;
// the engine runs it. Speeches are written for the game. They follow the order and the rules
// of the real Budget Meeting of 26 February 2026, but nobody's real words are quoted, and no
// councillor is named. Figures come from papers.json and amendments.json, with their sources.

export const FIRST = 'prep:intro';

const THEME = {
  care: 'care homes and day services', roads: 'roads, gullies and flooding', local: 'grants for parish and community groups',
  running: 'how the council runs itself', transport: 'buses, fares and parking', growth: 'jobs and the local economy',
  health: 'public health', schools: 'school places',
};

/* Who is speaking. Offices and groups only. */
export function whoLabel(key, s, api) {
  const g = (id) => api.G[id]?.short || id;
  const map = {
    chair: 'The Chairman', ce: 'The Chief Executive', mo: 'The Monitoring Officer', s151: 'The Director of Finance (Section 151 Officer)',
    leader: 'The Leader of the Council', cm: s.role === 'admin' ? 'You, the Cabinet Member for Resources' : 'The Cabinet Member for Resources',
    me: s.role === 'admin' ? 'You, the Cabinet Member for Resources' : `You, Leader of the ${g(s.group)} Group`,
    heckler: 'A councillor on the Administration benches', seconder: `Your seconder, ${g(s.group)} Group`, you: 'You', narrator: '',
    group: 'Your group meeting', officer: 'An officer',
  };
  if (map[key] != null) return map[key];
  if (key.startsWith('opp:')) return `The Leader of the ${g(key.slice(4))} Group`;
  if (key.startsWith('back:')) return `A ${g(key.slice(5))} councillor`;
  return key;
}

/* ---------------- helpers ---------------- */
const myQ = (s) => 'am_' + s.group;
const parts = (s, api) => api.build(s);
const realParts = (api, g) => api.amendments.amendments[g].parts;
const ruledOut = (s) => s.role === 'opp' && s.amend.kind !== 'none' && (s.amend.notice === 'late' || s.amend.s151 !== 'approved');
const nextGroupBeat = (s, api, h) => {
  const order = api.OPP;
  const i = order.indexOf(h);
  return i < order.length - 1 ? `am:${order[i + 1]}:intro` : 'sub:debate';
};
const pct = (x) => Math.round(x * 100);

// Which theme a home-made or real amendment leads on, for the Leader's reply.
function leadTheme(s, api) {
  const b = parts(s, api);
  const t = {};
  b.parts.filter((p) => !p.saving && !p.realOnly).forEach((p) => (t[p.theme] = (t[p.theme] || 0) + p.cost[0]));
  const top = Object.entries(t).sort((a, b) => b[1] - a[1])[0];
  return top ? THEME[top[0]] : 'these services';
}

/* The Administration's stance on an opposition amendment. Almost always against; a small,
   balanced, well-prepared one can win a commitment, and very rarely support. */
function leaderStance(s, api, h) {
  if (s.role === 'admin') {
    if (s.adminLine?.[h]) return s.adminLine[h];
    return 'against';
  }
  if (h !== s.group) return 'against';
  const b = parts(s, api);
  const small = s.amend.kind === 'own' ? b.y1 <= 0.6 : false;
  const ready = s.amend.s151 === 'approved' && s.amend.circulate === 'early' && api.has(s, 'briefed_cm');
  if (small && ready && s.v.cred >= 70 && s.v.mood >= 1) return 'for';
  return 'against';
}

/* ---------------- opponents' moves ----------------
   Each move is a storylet: who makes it, when it can happen, how likely it is given the state,
   and the choices it gives you. The engine draws from these with the seeded generator, so the
   meeting differs from run to run, but always for a reason you can see in the debrief. */
const MINE = [
  {
    id: 'whopays', who: 'heckler', weight: (s) => 3 + (s.v.mood < 0 ? 3 : 0),
    say: 'Councillor on the Administration benches, from a sedentary position: "And who pays for all this?"',
    plain: 'A heckle is not a question you have to answer. If you answer, do it through the Chair, and with a figure.',
    choices: [
      { id: 'figure', label: 'Answer through the Chair, with the figure from your funding table', fx: (s, a) => { a.add(s, 'cred', 6, 'You answered the heckle through the Chair with your own funding figures.'); a.score(s, 'faith', 1, 'You answered the question that was asked.'); a.lean(s, myQ(s), 'con', 3, 'Conservative members noted a costed answer.'); } },
      { id: 'ignore', label: 'Ignore it and carry on', fx: (s, a) => { a.add(s, 'chair', 0); a.score(s, 'proc', 1, 'You did not rise to a heckle.'); } },
      { id: 'back', label: 'Shout back across the chamber', fx: (s, a) => { a.add(s, 'mood', -1, 'You traded shouts across the chamber.'); a.add(s, 'chair', -1, 'The Chairman had to call for order after your exchange.'); a.score(s, 'faith', -1, 'You answered a heckle with a heckle.'); } },
    ],
  },
  {
    id: 'recurring', who: 'cm', when: (s, a) => s.amend.kind === 'own' ? a.build(s).usesSc : true, weight: () => 3,
    say: (s, a) => s.amend.kind === 'own' && a.build(s).usesSc
      ? 'Chairman, this amendment dips into the savings delivery contingency. That money is there in case savings fall short. Spend it now and what covers the gap if they do?'
      : 'Chairman, some of this is spending every year, paid for with grant we only have for two. What happens in the third year?',
    plain: 'The hardest question in most budget debates is about the years after this one. The Section 151 Officer\'s statement on your amendment is your best answer.',
    choices: [
      { id: 's25', label: 'Point to the Section 151 Officer\'s written statement on your amendment', when: (s) => s.amend.s151 === 'approved', fx: (s, a) => { a.add(s, 'cred', 6, 'You cited the Section 151 Officer\'s statement on your amendment.'); a.lean(s, myQ(s), 'con', 4, 'The written statement reassured Conservative members.'); a.lean(s, myQ(s), 'owl', 4, 'The written statement reassured Our West Lancashire members.'); } },
      { id: 'review', label: 'Accept the point is fair and say your group would review it in year', fx: (s, a) => { a.score(s, 'faith', 1, 'You conceded a fair point instead of denying it.'); a.add(s, 'mood', 1, 'The chamber warmed to an honest answer.'); } },
      { id: 'dismiss', label: 'Say the Administration has no lessons to give anyone', fx: (s, a) => { a.add(s, 'cred', -6, 'You dismissed a money question instead of answering it.'); a.score(s, 'faith', -1, 'You avoided the question.'); } },
    ],
  },
  {
    id: 'timing', who: 'leader', when: (s) => s.amend.circulate === 'late_start', weight: () => 8,
    say: 'Chairman, members saw this amendment for the first time as they sat down. That is no way to treat a budget of this size.',
    plain: 'Standing Order 4.5 lets a group leader choose when the amendment is shared, as long as it was sent in on time. It is allowed; it costs goodwill.',
    choices: [
      { id: 'rule', label: 'Say, through the Chair, that the rules allow it and the officers had it on time', fx: (s, a) => { a.score(s, 'proc', 1, 'You knew Standing Order 4.5.'); } },
      { id: 'sorry', label: 'Accept it was late for other groups, and offer to take questions on it', fx: (s, a) => { a.add(s, 'mood', 1, 'Your offer to take questions eased the room.'); a.score(s, 'faith', 1, 'You owned the timing.'); a.lean(s, myQ(s), 'owl', 3, 'Our West Lancashire members appreciated the offer.'); } },
      { id: 'counter', label: 'Say the Administration\'s own papers came late too', fx: (s, a) => { a.add(s, 'mood', -1, 'Tit for tat on timing soured the debate.'); } },
    ],
  },
  {
    id: 'pointoforder', who: 'heckler', weight: () => 2,
    say: 'A councillor on the Administration benches says your group voted against road repairs last year. You are sure that is wrong.',
    plain: 'A point of order is about the rules, and must name the Standing Order broken (Standing Order 19). Disagreeing with a fact is not one. If you have been misrepresented, you can ask for a point of personal explanation (Standing Order 18).',
    choices: [
      { id: 'po', label: 'Rise on a point of order, without naming a Standing Order', fx: (s, a) => { a.add(s, 'chair', -1, 'The Chairman refused a point of order that named no Standing Order.'); a.score(s, 'proc', -2, 'A disagreement is not a point of order.'); } },
      { id: 'pe', label: 'Ask for a point of personal explanation, as you were misrepresented', fx: (s, a) => { a.score(s, 'proc', 1, 'You used the right procedure for a misrepresentation.'); a.add(s, 'cred', 3, 'You corrected the record briefly.'); } },
      { id: 'speech', label: 'Let it go now and correct it in your reply', fx: (s, a) => { a.flag(s, 'correct_in_reply'); a.score(s, 'proc', 1, 'You kept your correction for your right of reply.'); } },
    ],
  },
  {
    id: 'ally', who: (s, a) => 'opp:' + (s.allyFor || a.OPP.find((g) => g !== s.group)), when: (s) => !!s.allyFor, weight: () => 6,
    say: (s, a) => `Chairman, our group has read this amendment carefully, and we will support it. ${a.G[s.allyFor].short} members have raised these same concerns all year.`,
    plain: 'Support from another group rarely changes the result, but it puts a wider view on the record and shows the amendment is not one group alone.',
    choices: [
      { id: 'thank', label: 'Thank them, through the Chair, when you reply', fx: (s, a) => { a.add(s, 'mood', 1, 'Cross-group support lifted the debate.'); a.lean(s, myQ(s), s.allyFor, 6, 'Their leader\'s speech firmed up their own group.'); } },
      { id: 'claim', label: 'Say it proves the whole opposition is behind you', fx: (s, a) => { a.lean(s, myQ(s), 'con', -4, 'Conservative members bristled at being counted in.'); a.lean(s, myQ(s), 'owl', -4, 'Our West Lancashire members bristled at being counted in.'); } },
    ],
  },
  {
    id: 'exposed', who: 'heckler', when: (s) => s.flags.includes('undeclared') && !s.flags.includes('declared_late'), weight: () => 20,
    say: 'Chairman, on a point of order under Standing Order 24: should the councillor not have declared an interest in the care home savings?',
    plain: 'A member who has an interest must declare it when the item comes up. Declaring late is better than not at all.',
    choices: [
      { id: 'declare', label: 'Apologise, and declare the interest now', fx: (s, a) => { a.flag(s, 'declared_late', 'You declared your interest late, when challenged.'); a.score(s, 'proc', -1, 'You declared late.'); a.add(s, 'cred', -4, 'The late declaration cost you some standing.'); } },
      { id: 'deny', label: 'Say it is nobody\'s business', fx: (s, a) => { a.flag(s, 'named', 'You refused to declare an interest when challenged.'); a.add(s, 'chair', -3, 'The Chairman warned you about your conduct.'); a.score(s, 'proc', -3, 'You refused to declare an interest.'); } },
    ],
  },
];

const THEIRS = [
  {
    id: 'support', who: 'you', when: (s) => s.role === 'opp', weight: () => 4,
    say: (s, a, h) => `The Chairman asks if any other member wishes to speak on the ${a.G[h].short} amendment.`,
    plain: 'You can speak on another group\'s amendment, but you only get one speech on it. Speaking for it can build goodwill for your own; saying nothing saves time.',
    choices: [
      { id: 'speak', label: 'Speak in support of it', fx: (s, a, h) => { a.lean(s, myQ(s), h, 6, `You spoke for the ${a.G[h].short} amendment, and they remembered.`); a.lean(s, 'am_' + h, s.group, 6); a.add(s, 'mood', 1, 'A generous speech helped the mood.'); } },
      { id: 'question', label: 'Ask the mover how it is paid for in the third year', fx: (s, a, h) => { a.add(s, 'cred', 3, 'You asked a fair question about another group\'s money.'); a.score(s, 'faith', 1, 'You tested an amendment on its merits.'); } },
      { id: 'sit', label: 'Stay seated', fx: (s, a) => { a.add(s, 'chair', 0); s.clock -= 4; } },
      { id: 'attack', label: 'Speak against it, to set your own amendment apart', fx: (s, a, h) => { a.lean(s, myQ(s), h, -8, `You attacked the ${a.G[h].short} amendment, and they remembered.`); a.add(s, 'mood', -1, 'Opposition groups fighting each other pleased only the Administration benches.'); } },
    ],
  },
  {
    id: 'asks_cm', who: (s, a, h) => 'opp:' + h, when: (s) => s.role === 'admin', weight: () => 5,
    say: (s, a, h) => `Chairman, I ask the Cabinet Member a simple question: if this amendment falls tonight, will the Administration look again at ${THEME[a.amendments.parts[a.amendments.amendments[h].parts[0]].theme]} during the year?`,
    plain: 'A direct question in debate is a test. A commitment goes in the minutes and is remembered; a refusal is remembered too.',
    choices: [
      { id: 'commit', label: 'Commit to take it to Cabinet during the year', fx: (s, a, h) => { a.note(s, `The Cabinet Member committed to take the ${a.G[h].short} group's ${THEME[a.amendments.parts[a.amendments.amendments[h].parts[0]].theme]} proposal to Cabinet during the year.`); a.score(s, 'infl', 1, 'You turned an opposition idea into a commitment on the record.'); a.score(s, 'faith', 1, 'You answered the question.'); a.add(s, 'disc.reform', -2, 'Some of your group would have preferred no commitment.'); } },
      { id: 'figures', label: 'Explain, with figures from the papers, why the budget already covers it', fx: (s, a) => { a.add(s, 'cred', 4, 'You answered with figures from the papers.'); a.score(s, 'faith', 1, 'You answered the question.'); } },
      { id: 'dodge', label: 'Say the opposition should have gone to Budget Scrutiny', fx: (s, a) => { a.score(s, 'faith', -1, 'You did not answer the question asked.'); a.add(s, 'mood', -1, 'The non-answer drew groans.'); } },
    ],
  },
  {
    id: 'closure', who: 'back:reform', when: (s) => s.clock > 170, weight: (s) => 2 + Math.floor(s.clock / 60),
    say: 'Chairman, I move that the question be now put.',
    plain: 'A closure motion (Standing Order 45) can only be moved by someone who has not spoken. The Chairman decides whether the item has been sufficiently discussed before putting it to the vote.',
    choices: [
      { id: 'accept', label: 'Accept it: the arguments have been made', fx: (s, a) => { s.clock -= 10; a.score(s, 'proc', 1, 'You knew when a debate had run its course.'); } },
      { id: 'object', label: 'Object loudly that the opposition is being gagged', fx: (s, a) => { a.add(s, 'chair', -1, 'The Chairman had to restore order over the closure motion.'); a.add(s, 'mood', -1, 'Tempers rose over the closure motion.'); } },
    ],
  },
  {
    id: 'relevance', who: 'chair', weight: () => 2,
    say: (s, a, h) => `A ${a.G[h].short} councillor has spent two minutes on local government reorganisation. The Chairman asks members to keep to the amendment.`,
    plain: 'The Chairman decides what is relevant, and their ruling is final (Standing Order 12). Members who keep to the point keep the Chairman\'s patience.',
    choices: [
      { id: 'nod', label: 'Nod, and make a note to keep your own remarks on the budget', fx: (s, a) => { a.score(s, 'proc', 1, 'You noted the Chairman\'s ruling on relevance.'); } },
      { id: 'heckle', label: 'Call out "let them speak!"', fx: (s, a) => { a.add(s, 'chair', -1, 'You heckled the Chairman\'s ruling.'); } },
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
  const A = api.amendments, G = api.G;
  const P = api.data.papers.figures, R = api.data.papers.rules;
  const [fam, a1, a2] = id.split(':');
  const admin = s.role === 'admin';

  /* ===== The week before ===== */
  if (id === 'prep:intro') {
    if (admin) {
      return {
        phase: 'prep', who: 'narrator', mins: 0,
        say: 'You are the Cabinet Member for Resources. In two weeks Full Council meets to set the 2026/27 Budget. The Leader will move it and you will second it.',
        plain: `The Budget asks for ${P.netBudget.value} of net spending and a council tax rise of ${P.ctRise.value}. You hold a majority, so the vote is not the hard part. Keeping your group together, answering the opposition honestly and keeping the Budget lawful are.`,
        choices: [{ id: 'go', label: 'Go to the group meeting', fx: (s, a) => a.score(s, 'proc', 0), go: 'prep:a_group' }],
      };
    }
    const g = G[s.group];
    return {
      phase: 'prep', who: 'narrator', mins: 0,
      say: `You lead the ${g.name}, ${g.seats} of the 84 councillors. In two weeks Full Council meets to set the 2026/27 Budget. What will your group bring?`,
      plain: 'Each opposition group may make a statement, or propose a list of amendments to the Administration\'s Budget. The list is debated and voted on as one package (Standing Order 4.4).',
      rule: 'package',
      choices: [
        { id: 'real', label: `Bring your group's real amendment from 26 February`, fx: (s, a) => { s.amend.kind = 'real'; s.amend.parts = realParts(a, s.group).slice(); a.flag(s, 'real', 'You brought your group\'s real February amendment.'); }, go: 'prep:cfo' },
        { id: 'own', label: 'Build your own from costed parts', fx: (s) => { s.amend.kind = 'own'; s.amend.parts = []; }, go: 'prep:build' },
        { id: 'none', label: 'Make a statement only, with no amendment', fx: (s, a) => { s.amend.kind = 'none'; a.score(s, 'infl', -1, 'Without an amendment there is nothing to vote on.'); }, go: 'prep:allies1' },
      ],
    };
  }
  if (id === 'prep:build') {
    return {
      phase: 'prep', kind: 'build', who: 'narrator', mins: 0,
      say: 'Choose what your amendment would do. Every part is costed from an amendment a group really moved on 26 February.',
      plain: 'The running balance must be zero or better in all three years, or the Chief Financial Officer cannot approve it (Standing Order 4.3). Spending that carries on into 2028/29 is the hard part: the extra grant runs out.',
      choices: [
        { id: 'done', label: 'Take it to the Chief Financial Officer', when: (s, a) => !a.build(s).empty, fx: (s, a) => { const b = a.build(s); a.flag(s, b.balanced ? 'built_balanced' : 'built_gap', b.balanced ? 'Your amendment balanced in all three years.' : 'Your amendment did not balance in every year.'); }, go: 'prep:cfo' },
        { id: 'real', label: 'Use your group\'s real amendment instead', fx: (s, a) => { s.amend.kind = 'real'; s.amend.parts = realParts(a, s.group).slice(); a.flag(s, 'real'); }, go: 'prep:cfo' },
      ],
    };
  }
  if (id === 'prep:cfo') {
    const own = s.amend.kind === 'own';
    return {
      phase: 'prep', who: 's151', mins: 0, rule: 'notice',
      say: 'Any alternative budget needs my written approval before it goes in. Do you want me to look at yours now?',
      plain: 'Asking early is the safest course. The officer checks it balances and is lawful, and writes a short statement you can quote in the chamber.',
      choices: [
        { id: 'consult', label: 'Yes: take it to the Section 151 Officer now', fx: (s, a) => {
          if (!own) { s.amend.s151 = 'approved'; a.add(s, 'cred', 8, 'The Section 151 Officer approved your amendment in writing.'); a.score(s, 'proc', 2, 'You got written approval before the deadline.'); return; }
          const b = a.build(s);
          if (!b.balanced) { s.amend.s151 = 'refused'; a.score(s, 'proc', 1, 'You asked early, so you found the gap in time.'); return; }
          s.amend.s151 = 'approved'; a.add(s, 'cred', 8, 'The Section 151 Officer approved your amendment in writing.'); a.score(s, 'proc', 2, 'You got written approval before the deadline.');
          if (b.usesSc) a.flag(s, 'sc_risk', 'The officer approved it but noted it draws on the savings delivery contingency.');
        }, go: (s) => s.amend.s151 === 'refused' ? 'prep:cfo_fix' : 'prep:notice' },
        { id: 'skip', label: 'No: you are confident it adds up', fx: (s, a) => { s.amend.s151 = 'none'; a.flag(s, 'no_cfo', 'You did not get the Chief Financial Officer\'s written approval.'); }, go: 'prep:notice' },
      ],
    };
  }
  if (id === 'prep:cfo_fix') {
    const b = api.build(s);
    const gaps = b.years.map((y, i) => (y.gap > 0 ? `${A.years[i]}: ${api.money(y.gap)} short` : '')).filter(Boolean).join('; ');
    return {
      phase: 'prep', who: 's151', mins: 0,
      say: b.busgates ? 'You have two different bus gate proposals in here. Choose one.' : `I cannot approve this as it stands. It does not balance. ${gaps}.`,
      plain: 'Without written approval the amendment will not be allowed at the meeting (Standing Order 4.2 and 4.3). You still have time to change it.',
      choices: [
        { id: 'fix', label: 'Go back and change it', go: 'prep:build', fx: (s, a) => { s.amend.s151 = 'none'; a.add(s, 'cred', 0); } },
        { id: 'anyway', label: 'Send it in without approval', go: 'prep:notice', fx: (s, a) => { a.flag(s, 'no_cfo', 'You sent in an amendment the officer had not approved.'); } },
      ],
    };
  }
  if (id === 'prep:notice') {
    return {
      phase: 'prep', who: 'narrator', mins: 0, rule: 'circulate',
      say: 'The deadline is five clear working days before the meeting. When do you send it, and when should other councillors see it?',
      plain: 'Sent on time, the Monitoring Officer holds it until you say it can be shared, as long as that is no later than the start of the meeting. Early sharing gives other groups time to back you, and gives the Administration time to prepare its answer.',
      choices: [
        { id: 'early', label: 'Send it now, and share it with all councillors straight away', fx: (s, a) => { s.amend.notice = 'on_time'; s.amend.circulate = 'early'; a.OPP.forEach((g) => g !== s.group && a.lean(s, myQ(s), g, 4)); a.flag(s, 'early', 'You shared your amendment early, so other groups could consider it.'); a.score(s, 'faith', 1, 'Early sharing lets everyone weigh it properly.'); } },
        { id: 'late_start', label: 'Send it now, but share it only at the start of the meeting', fx: (s, a) => { s.amend.notice = 'on_time'; s.amend.circulate = 'late_start'; a.flag(s, 'late_start', 'You held your amendment back until the meeting began.'); } },
        { id: 'late', label: 'Hold it back to polish it, and send it the day before', fx: (s, a) => { s.amend.notice = 'late'; s.amend.circulate = 'late_start'; a.flag(s, 'missed_deadline', 'You missed the five-day deadline.'); } },
      ],
      skip: (s) => s.amend.kind === 'none',
      next: 'prep:allies1',
    };
  }
  if (fam === 'prep' && (a1 === 'allies1' || a1 === 'allies2')) {
    const round = a1 === 'allies1' ? 1 : 2;
    const asked = s.asked || [];
    return {
      phase: 'prep', who: 'narrator', mins: 0,
      say: round === 1 ? 'You have time for two conversations before the meeting. Who do you talk to first?' : 'And your second conversation?',
      plain: 'In February the opposition groups backed each other\'s amendments in a clear pattern. The Conservatives and Our West Lancashire mostly abstained on the others. Talking first can move that, a little.',
      choices: [
        ...api.OPP.filter((g) => g !== s.group && !asked.includes(g)).map((g) => ({
          id: 'g_' + g, label: `The ${G[g].short} group leader`,
          fx: (s, a) => {
            (s.asked ||= []).push(g);
            const b = a.build(s);
            const theirs = s.amend.kind !== 'none' && b.groups.includes(g) ? 8 : 0;
            a.lean(s, myQ(s), g, 8 + theirs, theirs ? `The ${G[g].short} group saw its own ideas in your amendment.` : `You talked to the ${G[g].short} group before the meeting.`);
            if (!s.allyFor && (a.data.minutes.votes.find((v) => v.id === s.group)?.byGroup.for[g] || 0) + theirs > 0) s.allyFor = g;
          },
        })),
        { id: 'cm', label: 'The Cabinet Member for Resources', when: (s) => !asked.includes('cm') && s.amend.kind !== 'none', fx: (s, a) => { (s.asked ||= []).push('cm'); a.flag(s, 'briefed_cm', 'You briefed the Cabinet Member for Resources before the meeting.'); a.add(s, 'cred', 5, 'Briefing the Administration privately showed you wanted the ideas, not just the headline.'); } },
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
        { id: 'joint', label: 'Agree to back every opposition amendment that has the officer\'s approval', fx: (s, a) => { a.flag(s, 'joint', 'Your group agreed to back every approved opposition amendment.'); a.OPP.forEach((g) => g !== s.group && a.lean(s, myQ(s), g, 5)); } },
      ],
      next: 'm:open',
    };
  }

  /* Administration preparation */
  if (id === 'prep:a_group') {
    return {
      phase: 'prep', who: 'group', mins: 0,
      say: 'At the group meeting two of your councillors say they are uneasy about the care home savings and the consultation on them. They may not back the Budget.',
      plain: `The savings in the Budget total ${P.savings.value}. Each one is part of what balances it. A saving dropped without another found in its place leaves a gap the Budget cannot carry.`,
      choices: [
        { id: 'listen', label: 'Listen, and arrange a briefing with the officers on the consultation', fx: (s, a) => { a.add(s, 'disc.reform', 4, 'You listened to two uneasy members and got them a briefing.'); a.score(s, 'faith', 1, 'You dealt with doubts on the merits.'); } },
        { id: 'whip', label: 'Remind them the whip is the whip', fx: (s, a) => { a.add(s, 'disc.reform', -6, 'Two members felt overruled, not answered.'); a.lean(s, 'budget', 'reform', -10, 'Two uneasy members drifted towards abstaining.'); a.flag(s, 'two_unhappy'); } },
        { id: 'promise', label: 'Promise them the saving will be dropped', fx: (s, a) => { a.flag(s, 'promise', 'You promised to drop a saving that helps balance the Budget.'); a.add(s, 'disc.reform', 4); } },
      ],
      next: (s) => (s.flags.includes('promise') ? 'prep:a_s151' : 'prep:a_amends'),
    };
  }
  if (id === 'prep:a_s151') {
    return {
      phase: 'prep', who: 's151', mins: 0, rule: 'lawfulForm',
      say: 'If that saving goes, the Budget no longer balances. I would have to tell Council it could not set a lawful budget as it stands.',
      plain: 'This is what the Section 25 report means by an amendment that results in a loss: Council would have to think again (Budget Report, para. 48).',
      choices: [
        { id: 'withdraw', label: 'Go back to the two members and withdraw the promise', fx: (s, a) => { a.add(s, 'disc.reform', -5, 'Withdrawing a promise cost you trust in the group.'); a.score(s, 'proc', 1, 'You put the lawful Budget first.'); a.flag(s, 'promise_withdrawn'); } },
        { id: 'keep', label: 'Keep the promise: you will find the money on the night', fx: (s, a) => { a.flag(s, 'unlawful_risk', 'You kept an unfunded promise into the meeting.'); a.score(s, 'proc', -2, 'You ignored the Section 151 Officer\'s advice.'); } },
      ],
      next: 'prep:a_amends',
    };
  }
  if (id === 'prep:a_amends') {
    return {
      phase: 'prep', who: 'narrator', mins: 0,
      say: 'Five opposition amendments are in. Two will not be shared with you until the meeting begins, as the rules allow. How do you prepare?',
      plain: 'Each one has a written statement from the Section 151 Officer. Every one in February said much the same: it moves money around, and makes no appreciable difference to the council\'s financial resilience.',
      choices: [
        { id: 'officers', label: 'Ask officers to brief your group on each amendment as it arrives', fx: (s, a) => { a.add(s, 'cred', 6, 'Your group knew what each amendment did.'); a.flag(s, 'prepared'); } },
        { id: 'lines', label: 'Prepare attack lines on the late ones', fx: (s, a) => { a.flag(s, 'attack_lines'); a.add(s, 'disc.reform', 3, 'Attack lines rallied your group.'); } },
      ],
      next: 'prep:a_attend',
    };
  }
  if (id === 'prep:a_attend') {
    return {
      phase: 'prep', who: 'narrator', mins: 0, rule: 'majority',
      say: `Your group has ${s.att.reform.seats} councillors, and ${s.att.reform.seats - s.att.reform.present} have sent apologies so far. Do you chase attendance?`,
      plain: 'Votes are decided by a simple majority of those present and voting. A majority group with members missing, and a few abstaining, can find its margin thinner than it looks.',
      choices: [
        { id: 'chase', label: 'Chase every member who can attend', fx: (s, a) => { s.att.reform.present = Math.min(s.att.reform.seats, s.att.reform.present + 2); a.score(s, 'proc', 1, 'You made sure your group would be there.'); } },
        { id: 'leave', label: 'Leave it: the majority is safe', fx: (s, a) => a.add(s, 'disc.reform', 0) },
      ],
      next: 'prep:a_concede',
    };
  }
  if (id === 'prep:a_concede') {
    return {
      phase: 'prep', who: 'narrator', mins: 0,
      say: 'Is there anything in the opposition amendments your Administration could accept?',
      plain: 'An Administration can vote for an opposition amendment, or commit to look at an idea during the year. Either goes in the minutes. Neither is weakness if the money is sound.',
      choices: [
        { id: 'none', label: 'No: the Budget stands as it is', fx: (s, a) => a.add(s, 'disc.reform', 2, 'Your group liked a firm line.') },
        { id: 'look', label: 'Be ready to commit to take one good idea to Cabinet', fx: (s, a) => { a.flag(s, 'offer_look'); } },
        { id: 'owl', label: 'Recommend your group backs the smallest amendment, from Our West Lancashire', fx: (s, a) => { (s.adminLine ||= {}).owl = 'for'; a.add(s, 'disc.reform', -4, 'Some of your group were surprised to back an opposition amendment.'); a.flag(s, 'back_owl', 'You recommended backing the Our West Lancashire amendment.'); } },
      ],
      next: 'm:open',
    };
  }

  /* ===== The meeting ===== */
  if (id === 'm:open') {
    return {
      phase: 'meeting', item: 0, who: 'chair', mins: 8,
      say: 'Good afternoon and welcome to this Budget Meeting of the Full Council. A reminder that we debate the issue and not the person. Every vote on the Budget tonight is a recorded vote: your name and how you vote will be written in the minutes.',
      plain: 'The law requires a recorded vote on every Budget decision, and the council\'s rules cannot set that aside (Standing Order 46.7).',
      rule: 'recorded',
      choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: 'm:apol',
    };
  }
  if (id === 'm:apol') {
    const away = Object.values(s.att).reduce((n, g) => n + g.seats - g.present, 0);
    return {
      phase: 'meeting', item: 1, who: 'ce', mins: 3,
      say: `Thank you, Chairman. We have apologies from ${away} councillors. One ${G[s.remote.group].short} councillor has joined us online.`,
      plain: 'A councillor watching online can be allowed to speak, but cannot vote (Standing Order 9). Every absence changes the arithmetic.',
      rule: 'remote',
      choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: 'm:decl',
    };
  }
  if (id === 'm:decl') {
    const base = {
      phase: 'meeting', item: 2, who: 'mo', mins: 4, rule: 'arrears',
      say: 'Before we start: any member more than two months behind with their council tax must say so now, and may not vote on the Budget. That is a legal requirement, and failing to comply is a criminal offence. Are there any other interests to declare?',
      next: 'm:move',
    };
    if (s.personal === 'arrears') return { ...base, plain: 'Your direct debit failed in November and you missed the email. You are now more than two months behind. What do you do?', choices: [
      { id: 'declare', label: 'Rise and say so: you will speak but not vote', fx: (s, a) => { a.flag(s, 'arrears_declared', 'You declared council tax arrears and gave up your vote, as the law requires.'); a.score(s, 'proc', 2, 'You complied with section 106.'); } },
      { id: 'quiet', label: 'Say nothing and settle it next week', fx: (s, a) => { a.flag(s, 'arrears_hidden', 'You voted while in council tax arrears.'); a.score(s, 'proc', -5, 'Voting in arrears breaks section 106 of the Local Government Finance Act 1992.'); } },
    ] };
    if (s.personal === 'interest') return { ...base, plain: 'A close relative of yours lives in one of the care homes named in the savings consultation. What do you do?', choices: [
      { id: 'declare', label: 'Declare a non-pecuniary interest, and stay to take part', fx: (s, a) => { a.flag(s, 'declared', 'You declared your interest at the start.'); a.score(s, 'proc', 2, 'You declared the interest at the right time.'); } },
      { id: 'quiet', label: 'Say nothing: it is a private matter', fx: (s, a) => { a.flag(s, 'undeclared', 'You did not declare an interest.'); a.score(s, 'proc', -1); } },
      { id: 'leave', label: 'Leave the chamber for the whole Budget item', fx: (s, a) => { a.flag(s, 'declared'); a.score(s, 'proc', 0, 'Leaving was more than the code asked; a non-pecuniary interest lets you stay.'); a.score(s, 'infl', -2, 'You were not in the room to argue your case.'); } },
    ] };
    return { ...base, plain: 'Nothing on tonight\'s agenda affects you or anyone close to you.', choices: [{ id: 'none', label: 'Nothing to declare', fx: (s, a) => a.score(s, 'proc', 0) }] };
  }
  if (id === 'm:move') {
    return {
      phase: 'meeting', item: 3, who: 'leader', mins: 15, figs: ['netBudget', 'ctRise', 'bandD', 'savings', 'noReserves'],
      say: `Chairman, I move the Budget in the papers: net spending of ${P.netBudget.value}, a council tax rise of ${P.ctRise.value}, ${P.savings.value} of savings, and no use of reserves to balance it.`,
      plain: 'The Leader moves the Budget and may speak without a time limit. The seconder then has up to five minutes, or may second formally and keep their speech for later (Standing Order 4.9(b) and (g)).',
      rule: 'order',
      choices: admin ? [
        { id: 'reserve', label: 'Second it formally, and reserve your right to speak', fx: (s, a) => { a.flag(s, 'reserved'); a.score(s, 'proc', 1, 'You kept your speech for the substantive debate.'); } },
        { id: 's25', label: 'Second it with a short speech on the Section 151 Officer\'s report', fx: (s, a) => { a.add(s, 'cred', 6, 'You seconded on the strength of the Section 25 report.'); a.lean(s, 'budget', 'con', 6, 'The Section 25 case reassured Conservative members.'); } },
        { id: 'attack', label: 'Second it with a speech attacking the opposition\'s record', fx: (s, a) => { a.add(s, 'mood', -1, 'An attacking seconding speech set a hard tone.'); a.add(s, 'disc.reform', 3, 'Your group enjoyed it.'); a.lean(s, 'budget', 'con', -6, 'Conservative members disliked the tone.'); } },
      ] : [
        { id: 'listen', label: 'Listen, and note the figures you will use', fx: (s, a) => a.add(s, 'cred', 2, 'You noted the Administration\'s own figures.') },
        { id: 'heckle', label: 'Heckle the Leader about the council tax rise', fx: (s, a) => { a.add(s, 'chair', -1, 'The Chairman warned you for heckling.'); a.add(s, 'mood', -1, 'Heckling the Leader set a hard tone.'); } },
      ],
      next: (s, a) => `am:${a.OPP[0]}:intro`,
    };
  }

  /* ===== Amendments, one group at a time ===== */
  if (fam === 'am') {
    const h = a1, step = a2;
    const g = G[h];
    const mine = !admin && h === s.group;
    const real = A.amendments[h];
    const q = { id: 'am_' + h, prior: mine && s.amend.kind === 'own' ? h : h, label: `${g.short} Group amendment`, chairCasts: 'against' };
    const base = { phase: 'meeting', item: 4 + api.OPP.indexOf(h), group: h };

    if (step === 'intro') {
      if (mine && s.amend.kind === 'none') {
        return { ...base, who: 'chair', mins: 5, say: `I call the Leader of the ${g.short} Group to make a statement.`, plain: 'Your group brought no amendment, so you make a statement and there is no vote.',
          choices: [
            { id: 'costed', label: 'Set out where you think the Budget falls short, with figures from the papers', fx: (s, a) => { a.add(s, 'cred', 4, 'Your statement used the papers\' own figures.'); a.score(s, 'faith', 1, 'A fair, figured statement.'); } },
            { id: 'attack', label: 'Attack the Administration\'s record', fx: (s, a) => a.add(s, 'mood', -1, 'An attacking statement set a hard tone.') },
          ], next: (s, a) => nextGroupBeat(s, a, h) };
      }
      if (mine && ruledOut(s)) {
        const why = s.amend.notice === 'late' ? 'it was not received five clear working days before the meeting' : 'it does not have the Chief Financial Officer\'s written approval';
        return { ...base, who: 'chair', mins: 4, rule: s.amend.notice === 'late' ? 'notice' : 'lawfulForm',
          say: `I am advised that the ${g.short} Group amendment cannot be moved, because ${why}. The Leader of the group may make a statement instead.`,
          plain: 'The rule is strict: late or unapproved alternative budgets are not allowed (Standing Order 4.2 and 4.3). The Chairman has no discretion to let it through.',
          choices: [
            { id: 'accept', label: 'Accept the ruling, and make a statement setting out your ideas', fx: (s, a) => { a.flag(s, 'ruled_out'); a.score(s, 'proc', 1, 'You accepted the Chairman\'s ruling.'); a.add(s, 'cred', 2); } },
            { id: 'argue', label: 'Argue with the Chairman', fx: (s, a) => { a.flag(s, 'ruled_out'); a.add(s, 'chair', -2, 'You argued with the Chairman\'s ruling, which is final.'); a.score(s, 'proc', -2, 'The Chairman\'s ruling is final (Standing Order 12).'); a.add(s, 'mood', -1); } },
          ], next: (s, a) => nextGroupBeat(s, a, h) };
      }
      if (mine) {
        const b = api.build(s);
        return { ...base, who: 'chair', mins: 4, card: true,
          say: `I call the Leader of the ${g.short} Group to move the group's amendment.`,
          plain: `Your amendment costs ${api.money(b.years[0].spend)} in 2026/27. ${s.amend.s151 === 'approved' ? 'The Section 151 Officer has approved it in writing.' : ''} Group budget speeches have no time limit (Standing Order 39.1(c)), but the room has a limited appetite. How do you open?`,
          choices: [
            { id: 'people', label: 'Open with who it helps: the residents and services behind each line', fx: (s, a) => { a.OPP.forEach((x) => x !== s.group && a.lean(s, myQ(s), x, 4)); a.add(s, 'mood', 1, 'Opening with residents warmed the room.'); } },
            { id: 'money', label: 'Open with the money: how it balances, and the officer\'s statement', fx: (s, a) => { a.add(s, 'cred', s.amend.s151 === 'approved' ? 8 : 2, 'You opened with how the amendment is paid for.'); a.lean(s, myQ(s), 'con', 6, 'Conservative members respond to a costed case.'); a.lean(s, myQ(s), 'owl', 6, 'Our West Lancashire members respond to a costed case.'); } },
            { id: 'attack', label: 'Open with an attack on the Administration\'s first year', fx: (s, a) => { a.add(s, `disc.${s.group}`, 5, 'Your group enjoyed the attack.'); a.add(s, 'mood', -1, 'The attack hardened the Administration benches.'); a.lean(s, myQ(s), 'con', -6, 'Conservative members disliked the tone.'); a.score(s, 'faith', -1, 'The opening was about them, not the amendment.'); } },
          ], next: `am:${h}:second` };
      }
      // Another group's amendment, or you are the Administration.
      return { ...base, who: 'opp:' + h, mins: 20, card: true,
        say: `Chairman, I move the ${g.short} Group's amendment. ${real.summary}`,
        plain: `The Section 151 Officer's written statement on it: it ${real.s151}. A colleague seconds and reserves the right to speak.`,
        choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
        enter: (s, a) => { s.ev ||= {}; s.ev[h] = drawMoves(s, a, THEIRS, 1, h); },
        next: `am:${h}:ev0` };
    }
    if (step === 'second') {
      return { ...base, who: 'seconder', mins: 3,
        say: 'Chairman, I second the amendment.',
        plain: 'Your seconder can speak now for up to five minutes, or reserve the right to speak and sum up at the end of the debate, as most did in February.',
        choices: [
          { id: 'reserve', label: 'Ask them to reserve their speech and sum up at the end', fx: (s, a) => { a.flag(s, 'sec_reserved'); a.score(s, 'proc', 1, 'Your seconder kept a speech for the end of the debate.'); } },
          { id: 'now', label: 'Ask them to speak now, while the room is listening', fx: (s, a) => a.add(s, 'cred', 2, 'Your seconder backed you up straight away.') },
        ],
        enter: (s, a) => { s.ev ||= {}; s.ev[h] = drawMoves(s, a, MINE, 2, h); },
        next: `am:${h}:ev0` };
    }
    if (step === 'ev0' || step === 'ev1') {
      const k = step === 'ev0' ? 0 : 1;
      const mv = moveById((s.ev?.[h] || [])[k]);
      const after = mine ? (k === 0 ? `am:${h}:ev1` : `am:${h}:reply`) : `am:${h}:line`;
      if (!mv) return { ...base, skip: () => true, next: after };
      return { ...base, who: val(mv.who, s, api, h), mins: 12,
        say: val(mv.say, s, api, h), plain: mv.plain,
        choices: mv.choices.map((c) => ({ ...c, fx: (s, a, p) => c.fx(s, a, h, p) })),
        next: after };
    }
    if (step === 'reply') {
      return { ...base, who: 'me', mins: 6, rule: 'reply',
        say: 'The Chairman calls you to reply to the debate on your amendment.',
        plain: 'At a Budget meeting the proposer of an amendment has a right of reply (Standing Order 4.9(f)). The Leader of the Council then replies last, before the vote.',
        choices: [
          { id: 'money', label: 'Answer the hardest question raised, about the money', fx: (s, a) => { a.add(s, 'cred', 6, 'Your reply answered the money question.'); a.lean(s, myQ(s), 'con', 4); a.lean(s, myQ(s), 'owl', 4); a.score(s, 'faith', 1, 'You answered the debate, not just repeated your speech.'); } },
          { id: 'correct', label: 'Correct the record on what your group voted for last year', when: (s) => s.flags.includes('correct_in_reply'), fx: (s, a) => { a.add(s, 'cred', 4, 'You corrected the record calmly in your reply.'); a.score(s, 'proc', 1); } },
          { id: 'thank', label: 'Thank members and restate the amendment', fx: (s, a) => a.add(s, 'mood', 1, 'A gracious reply eased the room.') },
          { id: 'attack', label: 'Use it to attack the Leader', fx: (s, a) => { a.add(s, 'mood', -1, 'Your reply turned personal.'); a.add(s, 'chair', -1, 'The Chairman asked you to address the amendment.'); a.score(s, 'faith', -1, 'Argue with the idea, not the person.'); } },
        ],
        next: `am:${h}:leader` };
    }
    if (step === 'leader') {
      const stance = leaderStance(s, api, h);
      // No commitment was given on the real night; one is earned by preparation and conduct.
      const concede = stance !== 'for' && (api.has(s, 'briefed_cm') ? 0.35 : 0.05) + Math.max(0, s.v.cred - 60) / 150 + s.v.mood * 0.08;
      const theme = leadTheme(s, api);
      return { ...base, who: 'leader', mins: 8,
        enter: (s, a) => { if (stance !== 'for' && a.rand(s) < concede) { s.concession = theme; a.note(s, `The Leader committed, in reply, to look at ${theme} during the year, after the ${G[h].short} amendment.`); a.score(s, 'infl', 3, 'You won a commitment on the record.'); } },
        say: stance === 'for'
          ? 'Chairman, this is a small, costed amendment that the officers have approved and that we have had time to read. The Administration will support it.'
          : s.concession
            ? `Chairman, we will not support this amendment tonight. But the point about ${s.concession} is a fair one, and I will ask the Cabinet Member to look at it during the year.`
            : s.amend.circulate === 'late_start'
              ? 'Chairman, members were given no time to read this, and a budget is not built in an afternoon. We will vote against it.'
              : 'Chairman, the Budget before you is balanced, and the Section 151 Officer has reported on it. We will vote against this amendment.',
        plain: stance === 'for' ? 'It is unusual, but an Administration can back an opposition amendment. When it does, the amendment becomes part of the Budget.' : 'The Leader of the Council has the last word before every vote on an amendment.',
        choices: [{ id: 'vote', label: 'To the vote', fx: (s, a) => { s.myVote['am_' + h] = 'for'; } }],
        next: `am:${h}:vote` };
    }
    if (step === 'line') {
      // Your vote on another group's amendment (or your group's line, as the Administration).
      return { ...base, who: 'chair', mins: 5,
        say: `The ${g.short} Group's seconder sums up, and the Leader of the Council replies. We now move to the recorded vote on the ${g.short} Group amendment.`,
        plain: admin ? 'You advise your group how to vote. In February the Administration voted against every amendment. Remember the arithmetic: if your group abstains, the opposition\'s votes for are likely to carry it, and it becomes part of the Budget.' :`How does your group vote? Each package is all or nothing: you cannot back some lines and not others.`,
        choices: admin ? [
          { id: 'against', label: 'Against', fx: (s, a) => { (s.adminLine ||= {})[h] = 'against'; s.myVote['am_' + h] = 'against'; } },
          { id: 'abstain', label: 'Abstain: it has merit, but this is not the night', fx: (s, a) => { (s.adminLine ||= {})[h] = 'abstain'; s.myVote['am_' + h] = 'abstain'; a.score(s, 'faith', 1, 'You did not oppose an approved amendment for the sake of it.'); a.add(s, 'disc.reform', -3, 'Abstaining on an opposition amendment unsettled some of your group.'); } },
          { id: 'for', label: 'For', fx: (s, a) => { (s.adminLine ||= {})[h] = 'for'; s.myVote['am_' + h] = 'for'; a.add(s, 'disc.reform', -5, 'Backing an opposition amendment surprised your group.'); a.score(s, 'infl', 1); } },
        ] : [
          { id: 'for', label: 'For', fx: (s, a) => { s.myVote['am_' + h] = 'for'; a.lean(s, myQ(s), h, 0); } },
          { id: 'abstain', label: 'Abstain', fx: (s, a) => { s.myVote['am_' + h] = 'abstain'; } },
          { id: 'against', label: 'Against', fx: (s, a) => { s.myVote['am_' + h] = 'against'; a.lean(s, myQ(s), h, -6, `You voted against the ${G[h].short} amendment, and they remembered.`); } },
        ],
        next: `am:${h}:vote` };
    }
    if (step === 'vote') {
      return { ...base, who: 'ce', mins: 6, tally: 'am_' + h,
        enter: (s, a) => {
          const whip = {};
          for (const x of a.data.chamber.groups.map((gg) => gg.id)) {
            if (x === a.data.chamber.administration) whip[x] = leaderStance(s, a, h);
            else if (x === s.group) whip[x] = s.myVote['am_' + h] || (x === h ? 'for' : null);
            else if (x === h) whip[x] = 'for';
          }
          if (s.flags.includes('joint') && h !== s.group && !admin) whip[s.group] = s.myVote['am_' + h] || 'for';
          a.vote(s, { ...q, whip });
        },
        say: (() => { const v = s.votes['am_' + h]; return v ? `Members, the votes on the ${g.short} Group amendment were ${v.for} for, ${v.against} against, and ${v.abstain} abstaining. The amendment is ${v.carried ? 'carried' : 'lost'}.` : ''; })(),
        plain: (() => { const v = s.votes['am_' + h]; if (!v) return ''; const real = api.data.minutes.votes.find((x) => x.id === h); return `${v.cast ? 'The votes were equal, so the Chairman used the casting vote. ' : ''}${v.absent + v.remote} councillors were absent or online and could not vote. On 26 February the real result was ${real.totals.for} for, ${real.totals.against} against and ${real.totals.abstain} abstaining.`; })(),
        choices: [{ id: 'next', label: 'Next', fx: (s, a) => { const v = s.votes['am_' + h]; if (v.carried) a.flag(s, 'carried_' + h, `The ${G[h].short} amendment was carried and became part of the Budget.`); } }],
        next: (s, a) => nextGroupBeat(s, a, h) };
    }
  }

  /* ===== The substantive motion ===== */
  if (id === 'sub:debate') {
    const amended = api.OPP.filter((h) => s.flags.includes('carried_' + h));
    return {
      phase: 'meeting', item: 4 + api.OPP.length, who: 'chair', mins: 40,
      say: `The amendments have been dealt with. We return to the substantive motion: the Budget${amended.length ? ', as amended' : ''}. No further amendments may be moved.`,
      plain: 'Every member may now speak once, except the mover and seconder of the Budget, unless the seconder reserved their speech (Standing Order 4.9(g) and (h)).',
      rule: 'noNew',
      choices: admin ? [
        { id: 'speak', label: 'Make the speech you reserved: the Section 25 report and the savings', when: (s) => s.flags.includes('reserved'), fx: (s, a) => { a.add(s, 'cred', 5, 'You used your reserved speech on the Section 25 report.'); a.lean(s, 'budget', 'con', 6, 'Conservative members were reassured.'); } },
        { id: 'promise', label: 'Tell the chamber the care home saving will be dropped', when: (s) => s.flags.includes('unlawful_risk'), fx: (s, a) => { a.flag(s, 'promise_public', 'You announced an unfunded change on the floor of the chamber.'); } },
        { id: 'listen', label: 'Listen, and note points for the Leader\'s reply', fx: (s, a) => a.add(s, 'cred', 1) },
      ] : [
        { id: 'against', label: 'Speak against the Budget, and say why your group will vote against', fx: (s, a) => { s.myVote.budget = 'against'; a.add(s, `disc.${s.group}`, 3); } },
        { id: 'abstain', label: 'Explain why your group will abstain', fx: (s, a) => { s.myVote.budget = 'abstain'; a.score(s, 'faith', 1, 'You explained a nuanced position.'); } },
        { id: 'support', label: 'Say your group will support the Budget, with its reservations on record', fx: (s, a) => { s.myVote.budget = 'for'; a.score(s, 'faith', 1, 'You put reservations on record without opposing for its own sake.'); } },
      ],
      next: (s) => (s.clock > 230 ? 'sub:closure' : 'sub:reply'),
    };
  }
  if (id === 'sub:closure') {
    const mv = THEIRS.find((m) => m.id === 'closure');
    return { phase: 'meeting', item: 4 + api.OPP.length, who: 'back:reform', mins: 2, say: val(mv.say, s, api), plain: mv.plain, rule: 'closure',
      choices: mv.choices.map((c) => ({ ...c, fx: (s, a, p) => c.fx(s, a, null, p) })), next: 'sub:reply' };
  }
  if (id === 'sub:reply') {
    if (admin && s.flags.includes('promise_public')) {
      return { phase: 'meeting', item: 4 + api.OPP.length, who: 's151', mins: 10, rule: 'lawfulForm',
        say: 'Chairman, I must advise Council. If that saving is dropped with nothing in its place, the Budget does not balance, and Council cannot set a lawful budget tonight on that basis.',
        plain: 'This is the Section 25 warning in the papers (Budget Report, para. 48). Members have a duty to facilitate, rather than obstruct, a lawful budget (para. 78).',
        choices: [
          { id: 'withdraw', label: 'Withdraw what you said, and apologise to the chamber', fx: (s, a) => { a.add(s, 'cred', -12, 'You had to withdraw an unfunded promise in public.'); a.score(s, 'proc', -1); a.add(s, 'disc.reform', -6, 'The climbdown embarrassed your group.'); } },
          { id: 'insist', label: 'Insist: Council can sort out the detail later', fx: (s, a) => { a.flag(s, 'reconvene', 'You insisted on an unlawful Budget against the Section 151 Officer\'s advice.'); a.score(s, 'proc', -4); } },
        ],
        next: (s) => (s.flags.includes('reconvene') ? 'end' : 'sub:vote') };
    }
    return {
      phase: 'meeting', item: 4 + api.OPP.length, who: 'leader', mins: 6, rule: 'noNew',
      say: s.concession
        ? `Chairman, I thank members for the debate. I have said we will look again at ${s.concession}, and we will. I commend the Budget to the Council.`
        : 'Chairman, I thank members for the debate. This Budget is balanced, the Section 151 Officer has reported on it, and I commend it to the Council.',
      plain: 'The mover of the Budget has the last word, for up to five minutes. Then the recorded vote (Standing Order 4.9(i)).',
      choices: [{ id: 'vote', label: 'To the vote', fx: (s, a) => { if (s.role === 'admin') s.myVote.budget = 'for'; } }],
      next: 'sub:vote',
    };
  }
  if (id === 'sub:vote') {
    return { phase: 'meeting', item: 5 + api.OPP.length, who: 'ce', mins: 10, tally: 'budget', rule: 'recorded',
      enter: (s, a) => {
        const whip = { reform: 'for' };
        if (s.role === 'opp') whip[s.group] = s.myVote.budget || 'abstain';
        a.vote(s, { id: 'budget', prior: 'budget', label: 'The Budget', whip, chairCasts: 'for' });
      },
      say: (() => { const v = s.votes.budget; return v ? `Members, the votes on the Budget were ${v.for} for, ${v.against} against, and ${v.abstain} abstaining. The Budget is ${v.carried ? 'carried' : 'lost'}.` : ''; })(),
      plain: (() => { const v = s.votes.budget; if (!v) return ''; const real = api.data.minutes.votes.find((x) => x.id === 'budget'); return `On 26 February the real result was ${real.totals.for} for, ${real.totals.against} against and ${real.totals.abstain} abstaining.`; })(),
      choices: [{ id: 'next', label: 'Next', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: (s) => (s.votes.budget?.carried ? 'res' : 'end') };
  }
  if (id === 'res') {
    return { phase: 'meeting', item: 6 + api.OPP.length, who: 'chair', mins: 2, figs: ['bandD', 'bandA', 'ctRequirement'],
      say: `The Budget is agreed, and with it the council tax for 2026/27: ${P.bandD.value} at Band D, and ${P.bandA.value} at Band A, the band 35% of homes in the county are in. That concludes our business.`,
      plain: 'The county council does not send the bills. The district councils add the county\'s share to their own and send them out.',
      choices: [{ id: 'end', label: 'See how you did', fx: (s, a) => a.add(s, 'chair', 0) }],
      next: 'end' };
  }

  if (id === 'end') {
    return { kind: 'end', phase: 'end', ending: ending(s, api), who: 'narrator', say: '', plain: '' };
  }
  throw new Error('Unknown beat ' + id);
}

/* ---------------- endings ---------------- */
export const ENDINGS = {
  misstep: { title: 'A procedural misstep', text: 'The Budget may have gone through, but your own conduct is now the story. Declarations of interest and the council tax rule are personal duties, and they come before anything you say about the Budget.' },
  reconvene: { title: 'No lawful Budget tonight', text: 'Council could not set a lawful Budget, and must meet again. The county has a duty to set its budget before 1 March, and the district councils need the county\'s figure to send out bills before 11 March. Every day of delay costs money and confidence.' },
  amended: { title: 'Budget agreed, as amended', text: 'The Budget was agreed with an opposition amendment in it. That is rare, and it happened because the amendment was small, costed, approved by the Section 151 Officer and shared in good time.' },
  concession: { title: 'Budget agreed, with a commitment on the record', text: 'The Budget was agreed as moved, and the minutes record a commitment to look again at an opposition idea during the year. Influence often looks like this.' },
  clean: { title: 'Budget agreed as moved', text: 'The Budget was agreed as moved. A majority wins the vote; how it wins is what the chamber, the officers and the public remember.' },
  ruled_out: { title: 'Amendment ruled out', text: 'Your amendment was never debated. The rules on notice and the Chief Financial Officer\'s approval are strict, and the Chairman cannot waive them. Your ideas are in the minutes only as a statement.' },
  carried: { title: 'Your amendment carried', text: 'Your amendment is now part of the Budget. It happened because it was small, costed, approved in writing, shared early, and you briefed the Administration first.' },
  record: { title: 'Amendment lost, commitment won', text: 'Your amendment was lost, as opposition amendments usually are. But the Leader committed, on the record, to look at part of it during the year. That is a result you can hold them to.' },
  defeated: { title: 'Amendment lost', text: 'Your amendment was lost. It is in the minutes, with every councillor\'s vote on it, and that record is what you can campaign on.' },
  statement: { title: 'A statement, not an amendment', text: 'Your group made a statement, so there was nothing to vote on. A costed amendment gives the chamber something concrete to accept or reject, and puts every vote on the record.' },
};
export function ending(s, api) {
  if (s.flags.includes('arrears_hidden') || s.flags.includes('named') || (s.flags.includes('undeclared') && !s.flags.includes('declared_late'))) return 'misstep';
  if (s.flags.includes('reconvene') || (s.votes.budget && !s.votes.budget.carried)) return 'reconvene';
  if (s.role === 'admin') {
    if (api.OPP.some((h) => s.flags.includes('carried_' + h))) return 'amended';
    if (s.record.length) return 'concession';
    return 'clean';
  }
  if (s.amend.kind === 'none') return 'statement';
  if (s.flags.includes('ruled_out')) return 'ruled_out';
  if (s.flags.includes('carried_' + s.group)) return 'carried';
  if (s.record.length) return 'record';
  return 'defeated';
}

/* What the debrief says you could have done differently. */
export function counterfactual(s) {
  const f = (x) => s.flags.includes(x);
  if (f('arrears_hidden')) return 'Had you declared your arrears, you would have lost your vote tonight but nothing else. Voting while in arrears is a criminal offence under section 106.';
  if (f('undeclared')) return 'Had you declared your interest at item 2, nobody could have used it against you in the debate.';
  if (f('reconvene')) return 'Had you withdrawn the unfunded promise when the Section 151 Officer first warned you, the Budget would have been agreed tonight.';
  if (f('missed_deadline')) return 'Had you sent the amendment five clear working days before the meeting, it would have been debated and voted on.';
  if (f('no_cfo')) return 'Had you asked the Chief Financial Officer to approve it, the amendment would have been allowed, or you would have found the gap in time to fix it.';
  if (s.role === 'opp' && !f('briefed_cm') && !s.record.length) return 'Had you briefed the Cabinet Member for Resources before the meeting, the Leader would have been more likely to commit to part of your amendment.';
  if (s.role === 'opp' && f('late_start')) return 'Had you shared the amendment early, other groups would have had time to back it, and the Leader would have lost the easy line about timing.';
  if (s.role === 'admin' && f('two_unhappy')) return 'Had you listened to the two uneasy members and got them a briefing, your margin would have been wider and your group happier.';
  if (s.role === 'admin' && !s.record.length) return 'Had you committed to take one opposition idea to Cabinet, you would have given away nothing in money, and gained a record of listening.';
  return 'You made few mistakes. Try the other side of the chamber, or another group, and see how the same meeting looks from there.';
}

export const DEBRIEF_QUESTIONS = [
  'What did the Section 151 Officer\'s statement on each amendment actually tell you, and what did it not?',
  'If you were in opposition, what result short of winning the vote would you count as success?',
  'If you were in the Administration, which opposition idea would you have been willing to take forward, and how would you have paid for it?',
  'Which of the procedural rules tonight protects the public, and which protects the council?',
  'What would you do differently in the two weeks before the meeting?',
];

export const ROLE_CARDS = [
  ['The Chairman', 'Runs the meeting, calls each speaker in order, rules on relevance and points of order. Their ruling is final.'],
  ['The Chief Executive', 'Reads the apologies and calls the roll for every recorded vote.'],
  ['The Monitoring Officer', 'Gives the warning on council tax arrears and advises on declarations of interest.'],
  ['The Section 151 Officer', 'Wrote the Section 25 report on the Budget, and a statement on each amendment. Speaks only to give advice.'],
  ['The Leader of the Council', 'Moves the Budget, and replies last on each amendment and on the Budget.'],
  ['The Cabinet Member for Resources', 'Seconds the Budget, and answers questions on the money.'],
  ['Each opposition group leader', 'Moves the group\'s amendment or makes a statement, and has a right of reply on the amendment.'],
  ['Group seconders', 'Second their group\'s amendment, usually reserving the right to sum up at the end.'],
  ['Councillors', 'Speak once on each item if called, and vote in every recorded vote: for, against or abstain.'],
];
