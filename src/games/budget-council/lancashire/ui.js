// Budget Council, Lancashire: the page words that are not in content.js. Moved out of
// src/pages/budget-council/index.astro on 15 November 2026 unchanged, so the page reads as before.

export const ui = {
  room: 'county-hall',
  // Colours for the chamber kit, taken from County Hall as modelled: green leather benches, gold.
  theme: { stage: '#0f1d19', 'stage-2': '#1b3029', accent: '#e2be5a', 'accent-ink': '#1c1504', primary: '#2f5247' },
  prepShort: 'The week before',
  defaultSide: 'pl',
  tab2: 'The real meeting',
  clockNote: 'The real meeting ran about five and a half hours.',
  scoreWhy: 'The Administration has a majority, so a score for winning would only teach that.',
  scoreNote: 'Scored on conduct, not on winning: a majority wins the vote either way.',
  planFurniture: `<rect x="196" y="18" width="208" height="46" rx="6" fill="#7a4f2a"/>
      <ellipse cx="300" cy="150" rx="46" ry="22" fill="#7a4f2a"/>`,

  terms: [
    ['Section 151 Officer', 'Section 151 Officer|Chief Financial Officer|Director of Finance', 'The officer legally responsible for the council\'s finances. They must report on the budget, and approve any alternative budget before it can be moved. The council\'s Standing Orders call this post the Chief Financial Officer.'],
    ['Section 25 report', 'Section 25 report|Section 25', 'The Section 151 Officer\'s report, required by law, on whether the budget\'s estimates are robust and its reserves adequate.'],
    ['Monitoring Officer', 'Monitoring Officer', 'The officer responsible for the council acting lawfully and for the code of conduct.'],
    ['Revenue Support Grant', 'Revenue Support Grant', 'General government grant to the council. The final settlement for 2026/27 gave more than expected, and that extra became the money most amendments spent.'],
    ['Strategic Contingency', 'Strategic Contingency', 'Where the extra grant from the final settlement was held in the Budget, not yet allocated.'],
    ['Savings delivery contingency', 'savings delivery contingency|savings contingency', 'Money held in case planned savings are not delivered in full.'],
    ['Recorded vote', 'recorded votes?', 'A vote where each councillor\'s name and vote is written in the minutes. The law requires one for every Budget decision.'],
    ['Substantive motion', 'substantive motion', 'The motion being decided after any amendments have been dealt with: here, the Budget, amended or not.'],
    ['Right of reply', 'right of reply', 'A last speech in answer to the debate, before the vote.'],
    ['Administration', 'Administration', 'The group running the council. Here, Reform UK, with 51 of the 84 seats.'],
    ['Band D', 'Band D', 'The middle council tax band, used to compare council tax between councils.'],
    ['Point of order', 'points? of order', 'A claim that the rules are being broken. It must name the Standing Order concerned.'],
    ['Closure motion', 'closure motion|question be now put', 'A motion to end the debate and vote now. The Chairman decides whether the matter has been discussed enough.'],
    ['Non-pecuniary interest', 'non-pecuniary interest', 'An interest that is not financial, such as a relative affected by a decision. You declare it, and can usually still take part.'],
    ['Abstain', 'abstain(?:ing)?|abstentions?', 'To be present and vote neither for nor against. Abstentions do not count towards either side.'],
    ['Standing Order', 'Standing Orders?', 'The council\'s own rules of procedure for meetings.'],
  ],

  agenda: (opp) => ['Welcome', 'Apologies', 'Declarations of interest', 'The Budget moved and seconded', ...opp.map((g) => `${g} amendment`), 'Debate on the Budget', 'Recorded vote on the Budget', 'Council tax resolution'],

  adminBlurb: () => 'Reform UK, 51 seats. You are the Cabinet Member for Resources: you second the Budget and answer for the money.',
  oppBlurb: () => 'You lead the group: you move its amendment, or make a statement.',

  startIntro: () => `<p class="bc-lede">Miss the deadline by a day and your amendment is never debated. Every February, Lancashire's 84 county councillors set the budget and council tax under rules of their own. This simulator puts you in that meeting, with the real papers and the five real opposition amendments from 26 February 2026, and today's group sizes.</p>
          <ol class="bc-steps">
            <li><b>The week before.</b> Build or choose your amendment, get the Section 151 Officer's approval, decide when to share it and who to talk to.</li>
            <li><b>The meeting.</b> Declarations, the Budget moved, each group's amendment debated and put to a recorded vote, then the Budget itself.</li>
            <li><b>The debrief.</b> What moved votes, what you could have done differently, and what really happened on the night.</li>
          </ol>
          <p class="bc-small">Opponents react to what you do, with some chance in it, so no two meetings are the same. A run takes 20 to 30 minutes.</p>`,
  startFooter: () => `<section class="bc-card"><h2>Whose idea was this?</h2><p>The idea of a virtual council meeting came from Wayne Walsh, who lives in Burnley. This adult version follows the one for young people, <a href="/young-lancashire-voice/">Young Lancashire Voice</a>. There is also a version for <a href="/budget-council/burnley/">Burnley Borough Council's Budget Meeting</a>.</p></section>`,

  amendSource: (a, { src }) => src('annexes', 'Annex ' + a.annex),
  partsSource: ({ src }) => src('annexes'),
  partGroups: (E, G) => E.OPP.map((g) => ({ legend: `From the ${G[g].short} amendment`, ids: Object.keys(E.api.amendments.parts).filter((id) => E.api.amendments.parts[id].group === g && !E.api.amendments.parts[id].realOnly) })),
  balanceRows: [['spend', 'Net spending (a saving in brackets)'], ['ph', 'From the Public Health Grant Reserve'], ['rsg', 'From the extra Revenue Support Grant'], ['sc', 'From the savings delivery contingency']],
  balanceNote: (b) => `${b.busgates ? 'Two different bus gate proposals: choose one. ' : ''}${b.balanced ? (b.empty ? 'Choose at least one thing to spend on.' : 'It balances in every year.') : 'It does not balance yet. Spending that runs into 2028/29 needs a saving or the contingency to cover it.'}${b.usesSc ? ' It uses the savings delivery contingency, which the Administration will say is there for a reason.' : ''}`,
  majoritySource: ({ src }) => src('so', 'Standing Order 46.2'),

  tab2View({ esc, src, figChips, E, G, data, amendCard }) {
    const { minutes, amendments } = data;
    const b = minutes.votes.find((v) => v.id === 'budget');
    return `<section class="bc-stack bc-real">
        <h2>The real meeting: 26 February 2026</h2>
        <p>The Budget Meeting began at 1pm in the council chamber at County Hall, Preston. The Leader moved the Budget and the Cabinet Member for Resources seconded it. Five opposition groups moved amendments, each debated and put to a recorded vote. All five were lost. The Budget was carried by ${b.totals.for} votes to ${b.totals.against}, with ${b.totals.abstain} abstaining. ${src('minutes', 'item 3')}</p>
        ${figChips(['netBudget', 'ctRise', 'bandD', 'savings', 'newSavings', 'reserve'])}
        <div class="bc-tablewrap"><table><thead><tr><th>Vote</th><th class="n">For</th><th class="n">Against</th><th class="n">Abstain</th><th>Result</th></tr></thead><tbody>
          ${minutes.votes.map((v) => `<tr><td>${esc(v.label)}</td><td class="n">${v.totals.for}</td><td class="n">${v.totals.against}</td><td class="n">${v.totals.abstain}</td><td>${v.result === 'carried' ? 'Carried' : 'Lost'}</td></tr>`).join('')}
        </tbody></table></div>
        <p class="bc-small">${esc(minutes.absent)} ${esc(minutes.february)} Source: ${src('minutes', 'item 3')}.</p>
        <h3>The five amendments</h3>
        ${E.OPP.map((g) => { const a = amendments.amendments[g]; return `<div class="bc-card"><h4>${esc(G[g].name)}: ${esc(a.title)}</h4><p>${esc(a.summary)}</p><p class="bc-small">The Section 151 Officer's statement: it ${esc(a.s151)}. ${src('annexes', 'Annex ' + a.annex)}</p>${amendCard(g)}</div>`; }).join('')}
        <p class="bc-small">In the simulator the groups are called in order of today's group size, so the order differs from February's, when Progressive Lancashire, the Conservatives, the Liberal Democrats, Labour and Our West Lancashire were taken in that order.</p>
      </section>`;
  },

  howIntro: () => `<p>Budget Council is a prototype training simulator. It is not a council service and has not been made with or approved by Lancashire County Council.</p>
        <h3>What is real and what is made up</h3>
        <div class="bc-tablewrap"><table><thead><tr><th>Real, from the public record</th><th>Made up for the simulator</th></tr></thead><tbody>
          <tr><td>The Budget's figures, the five amendments and their costs, and the Section 151 Officer's statements</td><td>Every speech, question and answer</td></tr>
          <tr><td>The rules of the meeting, from the council's Standing Orders</td><td>How opponents react, and every vote count in the game</td></tr>
          <tr><td>Today's group sizes: Reform UK 51 of 84</td><td>The seating plan, and the faces in the room</td></tr>
          <tr><td>The real result of each vote on 26 February, shown after each simulated one</td><td>Your credibility, the chamber's mood and the scores</td></tr>
        </tbody></table></div>
        <p>No councillor is named, and nobody's words are quoted. Offices and political groups appear because the rules give them their roles.</p>
        <h3>How the votes work</h3>
        <p>Each of the 84 seats votes on its own. Its starting tendency comes from how its group voted on the same question on 26 February. What you do moves that: who you talk to, how you open, how you answer, how the room feels. Groups with a strong whip vote together. Absent members and the member online cannot vote. A seeded generator decides the close calls, so a meeting replays exactly from its number.</p>`,

  debriefVotes(s, { esc, src, data }) {
    const { minutes } = data;
    const realB = minutes.votes.find((v) => v.id === 'budget');
    const yours = Object.values(s.votes);
    return `<div class="bc-card"><h3>Your meeting and the real one</h3><div class="bc-tablewrap"><table><thead><tr><th>Vote</th><th class="n">Yours</th><th class="n">26 February</th></tr></thead><tbody>
        ${yours.map((v) => { const real = minutes.votes.find((x) => 'am_' + x.id === v.id || x.id === v.id); return `<tr><td>${esc(v.label)}</td><td class="n">${v.for} to ${v.against}, ${v.abstain} abst. ${v.carried ? 'Carried' : 'Lost'}</td><td class="n">${real ? `${real.totals.for} to ${real.totals.against}, ${real.totals.abstain} abst. ${real.result === 'carried' ? 'Carried' : 'Lost'}` : ''}</td></tr>`; }).join('')}
      </tbody></table></div><p class="bc-small">Real results: ${src('minutes', 'item 3')}. The real Budget was carried ${realB.totals.for} to ${realB.totals.against}. Today's groups are not the same size as February's.</p></div>`;
  },
  furtherReading: ({ src, data }) => `Further reading: ${src('lgaworkbook')}; the <a href="${data.sources.report.url}">Budget Report</a>, paras 28 to 49 (the Section 25 report) and 78 to 81 (members' duties).`,
};
