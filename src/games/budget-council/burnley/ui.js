// Burnley Budget Council: the page words that are not in content.js.

export const ui = {
  room: 'burnley',
  // Colours for the chamber kit, taken from the Town Hall chamber as modelled: deep red walls, a gold cornice.
  theme: { stage: '#1c0d0f', 'stage-2': '#371418', accent: '#e0bd6a', 'accent-ink': '#1d1305', primary: '#7a2430' },
  architecture: () => import('../../rooms/burnley.js').then((m) => m.BURNLEY),
  sides: ['ref', 'lab', 'con'],
  defaultSide: 'lab',
  leaderGroup: 'ind',
  chairTitle: 'The Mayor',
  benchLabel: 'The top table',
  galleryLabel: 'Public seats',
  tab2: 'Last year and next',
  prepLabel: 'The weeks before the meeting',
  prepShort: 'The weeks before',
  startLabel: 'Start the weeks before',
  clockNote: 'A meeting adjourns after three hours unless Council votes to carry on (rule 8.1).',
  scoreWhy: 'Winning depends on arithmetic you do not control: the coalition has 19 seats, the opposition 26, across three groups and one Restore Britain councillor.',
  scoreNote: 'Scored on conduct, not on winning.',
  s151Words: { confirmed: 'checked by the Director of Resources', refused: 'not confirmed by the officer', none: 'not checked by the officer' },
  planFurniture: `<rect x="190" y="12" width="220" height="30" rx="6" fill="#6a3a1e"/>
      <rect x="262" y="66" width="76" height="22" rx="4" fill="#9a5a2e"/>
      <text x="300" y="56" text-anchor="middle" class="lbl">TOP TABLE</text>`,

  terms: [
    ['Section 151 Officer', 'Section 151 Officer|Director of Resources|Chief Finance Officer', 'The officer legally responsible for the council\'s finances. At Burnley this is the Director of Resources. They report on the Budget, and can be asked to check an amendment, though the rules do not require it.'],
    ['Section 25 report', 'Section 25 report|Section 25|section 25', 'The Section 151 Officer\'s report, required by law, on whether the Budget\'s estimates are robust and its reserves adequate. Council must have regard to it.'],
    ['Monitoring Officer', 'Monitoring Officer', 'The officer responsible for the council acting lawfully and for the Code of Conduct. At Burnley, the Head of Legal and Democratic Services.'],
    ['Executive', 'Executive', 'The Leader and the Executive Members, who propose the Budget to Full Council. At Burnley a coalition of Independent, The Independent Group, Liberal Democrat and Green councillors.'],
    ['Revenue Support Reserve', 'Revenue Support Reserve', 'A reserve the council used last year to pay for one-off additions. Council policy is that reserves do not pay for continuing spending.'],
    ['General Reserve', 'General Reserve', 'The council\'s fund of last resort, held at a prudent minimum.'],
    ['In-principle decision', 'in-principle decision|in principle', 'Any Budget decision other than accepting the Executive\'s Budget unamended. It takes effect after five working days unless the Leader objects.'],
    ['Altered motion', 'alter the motion|altered motion|altering a motion', 'A motion changed by its mover with the consent of the meeting (rule 14.7). Last year Labour\'s proposals went into the Budget this way.'],
    ['Recorded vote', 'recorded votes?', 'A vote where each councillor\'s name and vote is written in the minutes. Required for every Budget decision and every amendment.'],
    ['Substantive motion', 'substantive motion', 'The motion being decided once amendments are dealt with: the Budget, amended or not.'],
    ['Right of reply', 'right of reply', 'A last speech in answer to the debate, with no new matter, before the vote.'],
    ['Band D', 'Band D', 'The middle council tax band, used to compare council tax between councils.'],
    ['Point of order', 'points? of order', 'A claim that the rules or the law are being broken. It must name the rule. The Mayor\'s ruling is final.'],
    ['Personal explanation', 'personal explanation', 'A short correction of a misunderstood part of your own earlier speech.'],
    ['Closure motion', 'closure motion|question be now put', 'A motion to end the debate and vote now. The Mayor decides whether the matter has been debated enough.'],
    ['Abstain', 'abstain(?:ing)?|abstentions?', 'To be present and vote neither for nor against. Abstentions count towards neither side.'],
    ['Casting vote', 'casting vote', 'The Mayor\'s second vote when the votes are equal, which can be used either way.'],
  ],

  agenda: (opp) => ['Mayor\'s welcome', 'Apologies', 'Declarations of interest', 'The Budget moved and seconded', ...opp.filter((g) => ['Reform UK', 'Labour', 'Conservative'].includes(g)).map((g) => `${g} amendment`), 'Debate on the Budget', 'Recorded vote on the Budget', 'Council tax and the decision'],

  adminBlurb: () => 'The coalition: Independent, The Independent Group, Liberal Democrat and Green, 19 of 45 seats. You are the Deputy Leader and Executive Member for Resources: you move the Budget.',
  oppBlurb: () => 'You lead the group: you move its amendment, offer it to the Executive, or speak.',

  startIntro: ({ data }) => `<p class="bc-lede">A coalition with 19 of 45 seats has to pass a Budget. Three opposition groups and a Restore Britain councillor hold the other 26, and could carry any amendment they agree on. This simulator rehearses Burnley Borough Council's 2027/28 Budget Meeting, due in March 2027, under the council's own rules, with today's seats and the latest published figures.</p>
          <ol class="bc-steps">
            <li><b>The weeks before.</b> Build costed proposals, ask the Director of Resources to check them, decide when to share them, and whether to offer them to the Executive first.</li>
            <li><b>The meeting.</b> Declarations, the Budget moved, each group's amendment debated and put to a recorded vote, then the Budget itself.</li>
            <li><b>The debrief.</b> What moved votes, what you could have done differently, and how last year's meeting went.</li>
          </ol>
          <p class="bc-small">The real Budget papers will be published in February 2027. Until then the figures are the council's September 2026 position. Opponents react to what you do, with some chance in it. A run takes 15 to 25 minutes.</p>`,
  startFooter: () => `<section class="bc-card"><h2>Whose idea was this?</h2><p>The idea of a virtual council meeting came from Wayne Walsh, who lives in Burnley. There is also a version for <a href="/budget-council/">Lancashire County Council's Budget Meeting</a>, and one for young people, <a href="/young-lancashire-voice/">Young Lancashire Voice</a>. To see where Burnley's money goes and try closing the gap yourself, play the <a href="/burnley-budget-game/">Burnley Budget Challenge</a>.</p></section>`,

  amendSource: () => '',
  partsSource: ({ src }) => `${src('mtfs', 'para. 10 and Tables 4 and 5')}; ${src('savings', 'Appendix 2, p. 2')}; ${src('conamend', 'Table A')}; ${src('labamend')}`,
  partGroups: (E) => {
    const P = E.api.amendments.parts;
    const ids = (set) => Object.keys(P).filter((id) => P[id].set === set);
    return [
      { legend: 'Council tax and fees', ids: ids('tax') },
      { legend: 'Savings and income', ids: ids('save') },
      { legend: 'Spending', ids: ids('spend') },
    ];
  },
  balanceRows: [['recur', 'Continuing costs less continuing savings (a saving in brackets)'], ['reserve', 'One-off spending from the Revenue Support Reserve']],
  balanceNote: (b) => `${b.clash ? 'Two different council tax or fees changes: choose one. ' : ''}${b.empty ? 'Choose at least one change.' : b.balanced ? 'It balances for 2027/28.' : 'It does not balance yet. Continuing costs need a continuing saving or income; a reserve can only pay for something once.'}${b.risky.length ? ' One line depends on something the council cannot deliver on its own.' : ''}`,
  majoritySource: ({ src }) => `${src('cpr', 'rules 16.1 and 16.2')}. The Mayor votes as a councillor too.`,

  tab2View({ esc, src, figChips, data }) {
    const { minutes } = data;
    return `<section class="bc-stack bc-real">
        <h2>Last year's meeting, and the one ahead</h2>
        <p>Burnley set its 2026/27 Budget at Full Council on Tuesday 3 March 2026. The Deputy Leader moved the Executive's Budget and the Leader seconded it. With the consent of the meeting, the Deputy Leader altered the motion to take in the Labour Group's proposals, and, in the minutes' words, made "an additional increase to the Empty Homes budget above that proposed by the Labour Group". The Conservative Group moved an amendment to cut council tax by 1%, paid for by savings. It was lost. ${src('minutes', 'minute 95, pp. 4 to 8')}</p>
        <div class="bc-tablewrap"><table><thead><tr><th>Vote, 3 March 2026</th><th class="n">For</th><th class="n">Against</th><th class="n">Abstain</th><th>Result</th></tr></thead><tbody>
          ${minutes.lastYear.map((v) => `<tr><td>${esc(v.label)}</td><td class="n">${v.totals.for}</td><td class="n">${v.totals.against}</td><td class="n">${v.totals.abstain}</td><td>${v.result === 'carried' ? 'Carried' : 'Lost'}</td></tr>`).join('')}
        </tbody></table></div>
        <p class="bc-small">${esc(minutes.present)} The minutes record names, not groups, so no group totals are given. Source: ${src('minutes', 'minute 95, pp. 6 to 8')}.</p>
        ${figChips(['nbr', 'bandD'])}
        <h3>What has changed since</h3>
        <p>The May 2026 elections changed the council. The coalition that runs it now holds 19 of the 45 seats, a minority. Reform UK has 12, Labour 10, the Conservatives 3 and Restore Britain 1. ${src('party')}</p>
        <h3>The 2027/28 Budget</h3>
        <p>The council's latest published position, in September 2026, shows the gap below. Proposals to close it go to Full Council "in March 2027". No date or papers for that meeting have been published yet. ${src('mtfs', 'para. 8, p. 7')}</p>
        ${figChips(['gap', 'gap3', 'ctAssumed', 'gap299', 'ctSens', 'feesSens', 'paySens', 'generalReserve'])}
      </section>`;
  },

  howIntro: () => `<p>Burnley Budget Council is a prototype training simulator. It is not a council service and has not been made with or approved by Burnley Borough Council.</p>
        <h3>What is real and what is made up</h3>
        <div class="bc-tablewrap"><table><thead><tr><th>Real, from the public record</th><th>Made up for the simulator</th></tr></thead><tbody>
          <tr><td>The rules of the meeting, from the council's Constitution</td><td>Every speech, question and answer, and every group's amendment</td></tr>
          <tr><td>The seats each party holds today, from the council's website</td><td>How each group's councillors tend to vote, and every count in the game</td></tr>
          <tr><td>The September 2026 budget gap, its assumptions and the costed options</td><td>The Executive's 2027/28 Budget, which has not been published</td></tr>
          <tr><td>Last year's two recorded votes, shown for comparison</td><td>The seating plan, the faces, your credibility, the mood and the scores</td></tr>
        </tbody></table></div>
        <p>No councillor is named, and nobody's words are quoted. Offices and political groups appear because the rules give them their roles. No group's position on the 2027/28 Budget is assumed.</p>
        <p>Any councillor may move an amendment. The simulator gives amendments to the three largest opposition groups; the Restore Britain councillor votes but does not move one. Last year's Labour and Conservative proposals are among the costed options because they are on the record. No group has published proposals for 2027/28.</p>
        <h3>How the votes work</h3>
        <p>Each councillor votes on their own. There is no record of how each group voted in the past (the minutes list names only), so each group's starting tendency is made up: the coalition backs its own Budget, the opposition starts against it or abstaining, and groups start cool on each other's amendments. What you do moves that. The Mayor votes as a councillor and has a casting vote on a tie. Absent members cannot vote, and there is no voting from home. A seeded generator decides the close calls, so a meeting replays exactly from its number.</p>`,

  debriefVotes(s, { esc, src, data }) {
    const { minutes } = data;
    const yours = Object.values(s.votes);
    return `<div class="bc-card"><h3>Your meeting, and last year's</h3><div class="bc-tablewrap"><table><thead><tr><th>Vote in your meeting</th><th class="n">Result</th></tr></thead><tbody>
        ${yours.map((v) => `<tr><td>${esc(v.label)}</td><td class="n">${v.for} to ${v.against}, ${v.abstain} abst. ${v.carried ? 'Carried' : 'Lost'}</td></tr>`).join('')}
      </tbody></table></div><p class="bc-small">Last year, with a different council: ${minutes.lastYear.map((v) => `${esc(v.label)}, ${v.totals.for} to ${v.totals.against}, ${v.result}`).join('; ')}. ${src('minutes', 'minute 95, pp. 6 to 8')}.</p></div>`;
  },
  furtherReading: ({ src, data }) => `Further reading: ${src('lgaworkbook')}; the council's <a href="${data.sources.mtfs.url}">September 2026 budget update</a>, and its <a href="${data.sources.bpfr.url}">Budget and Policy Framework Procedure Rules</a>.`,
};
