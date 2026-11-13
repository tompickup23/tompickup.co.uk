---
title: "Lancashire Reorganisation Paper, Version 3.2: One Council and the County Tested Beside the Four Proposals"
ogImage: "/images/share/lgr-working-paper-v3-2-single-councils.png"
date: 2026-11-13T18:00:00
description: "Version 3.2 of the working paper on the finances of council reorganisation in Lancashire adds two single-council forms that nobody proposed, and prints every alternative result beside the central one. What it finds, what it cannot show, and how to check it."
seoTitle: "Lancashire reorganisation paper, version 3.2"
seoDescription: "Version 3.2 of the Lancashire reorganisation working paper adds two single-council forms to the four proposals, with data and code on Zenodo."
category: "Lancashire"
subcategory: "Local government reorganisation"
tags: ["lancashire", "lgr", "local-government-reorganisation", "releases", "data"]
featured: false
draft: false
sources:
  - text: "Pickup, T. (2026) The Financial Case for Local Government Reorganisation in Lancashire: A Transparent Component Model of Four Proposals and Two Single-Council Forms, with a Postscript on the 2026 Decisions. Working paper, version 3.2. Zenodo. Tables 6c, 8, 8a, 8h, 12b and 13d; Sections 5.7, 6.5, 8.6 and 12; Declaration of interests."
    url: "https://doi.org/10.5281/zenodo.24208546"
  - text: "Ministry of Housing, Communities and Local Government (2026) Local government reorganisation: withdrawn letter to 9 areas. Letter from the Minister for Local Government, Devolution and Regional Growth to council leaders and chief executives, 29 October 2026, p. 2, paras 7 and 8, and p. 3, para. 13."
    url: "https://assets.publishing.service.gov.uk/media/6ae36c6ed22fc5fecb3eb7f9/Local_government_reorganisation_-_withdrawn_letter_to_9_areas.pdf"
data:
  links:
    - label: "Zenodo: working paper, data and code, version 3.2 (DOI 10.5281/zenodo.24208546)"
      url: "https://doi.org/10.5281/zenodo.24208546"
    - label: "Version 3.1, unchanged and still citable (DOI 10.5281/zenodo.23016761)"
      url: "https://doi.org/10.5281/zenodo.23016761"
  method: "Run uv run python reproduce.py in the unzipped package. It uses the Python 3.12 standard library only, with a fixed Monte Carlo seed, so every run gives the same tables. tools/check_published_four.py confirms that every version 3.1 figure for the four proposals is unchanged."
---

*By Tom Pickup, Lancashire county councillor and Cabinet Member for Adult Social Care. The county council proposed two unitaries and would carry on under one of the two forms this version adds. That interest is declared in full in the paper and again below.*

Version 3.2 of my working paper on the finances of council reorganisation in Lancashire is on Zenodo, with its data and code: [10.5281/zenodo.24208546](https://doi.org/10.5281/zenodo.24208546).<sup class="cite"><a href="#source-1">1</a></sup>

It asks one new question. Lancashire's councils put forward plans for two, three, four and five unitaries. Nobody proposed a single council. On 29 October the Government withdrew its choice of four and said all options and proposals remain under consideration.<sup class="cite"><a href="#source-2">2</a></sup> So the paper now runs the same model on two forms that were never on the table: one new council for the whole area, and the county council continuing with its twelve districts.

## What the model gives

The figures below are the steady-state net annual position: the yearly saving from structure alone once the change has settled, less the yearly cost of running more or fewer councils. They are model outputs in 2024/25 prices, not forecasts of any council's budget. Transformation savings, which any structure could pursue, are left out.

Read this table with the section below it. The model favours fewer councils by construction, and past reorganisations have not shown savings on this scale.

| Form | Net annual position | Ten-year value |
|---|---:|---:|
| One new council | £133.0M | £943.4M |
| County council with its twelve districts | £88.4M | £619.1M |
| Two unitaries | £75.9M | £491.6M |
| Three unitaries | £31.7M | £135.4M |
| Four unitaries | −£2.6M | −£140.5M |
| Five unitaries | −£33.3M | −£389.1M |

Source: Pickup (2026), working paper version 3.2, Tables 8, 8a and 6c, central case, from `reproduce.py`. Ten-year value is the net present value over ten years at 3.5 per cent.

The four proposals' figures are the same as in version 3.1. A script in the package checks that none has moved.

## Why that table is not the answer

Three things in the paper cut against reading the table as a ranking to act on.

**The model favours fewer councils by construction.** It counts overheads that fall as councils merge and costs that rise as services are split. It has no term for what is lost when a council gets further from the people it serves. One new council would be larger than any single-tier council in England (Section 5.7).

**Past reorganisations have not shown savings on this scale.** In the areas reorganised between 2019 and 2023, three of which became single county unitaries, total spending per resident shows no detectable change against areas left alone (Section 8.6). When the model's savings are scaled to what Buckinghamshire and the two Northamptonshire councils reported, only two unitaries of the four proposals stay positive each year (Table 8a). On that scaling, one new council falls to between £31.6M and £61.3M, and the county with its districts to between £29.3M and £43.8M.

**The older evidence is mixed and comes with a label.** A new test on the five single county unitaries created in 2009 finds spending per resident fell relative to two-tier counties over the following ten years. It comes with three weaknesses (Table 13d). It rests on two years of data before the change, where the test as first specified required three. The check on prior trends failed. One of the nine alternative runs finds no detectable difference. The paper prints all of that beside the result.

## What else is new

- **Every alternative, side by side.** Tables 6c and 8a print the result under each treatment the paper did not choose as central, so a reader can see how much rests on each choice.
- **The alternatives in combination.** Taking the paper's alternative treatments together gives 120 distinct combinations for each form. Table 12b counts in how many each form's net annual position is positive: all 120 for each single council, 90 for two unitaries, 39 for three, 11 for four and 3 for five. The combinations are not equally likely. A count is not a probability.
- **The 2029 timetable.** For the areas whose July decisions were withdrawn, the Government's letter states elections in May 2028 and new councils from 1 April 2029.<sup class="cite"><a href="#source-2">2</a></sup> No outcome has been announced for Lancashire, so the paper keeps 1 April 2028 as its central date and reruns the model on the later one in Table 8h. The yearly position does not move, and the order of the six forms is the same.
- **An area annex.** Appendix C sets out what one council's areas would look like, from published data.
- **Rules written first.** Each new test was run under rules committed before its results were computed. There are four exceptions and the Declaration of interests counts them. Appendix D gives the date of each rule file and what I had seen when it was written.

## What is in the deposit

- **The paper**, version 3.2, as a Markdown file.
- **Source data and derived tables**, each described in the package README with its source.
- **Code**: `reproduce.py` rebuilds every model table; `tools/check_paper.py` checks the figures in the paper against that output; `tools/check_published_four.py` checks version 3.1's figures have not changed.
- **Review records** in `review/`: the rules for each test and the reviews of this version. Those reviews were carried out by AI agents working for me. No human peer review has taken place.

Version 3.1 stays on Zenodo under its own DOI and is not altered.

## Licence

The paper, derived files, review records and code are released under Creative Commons Attribution 4.0 (CC BY 4.0). Fields derived from government statistics remain Crown copyright under the Open Government Licence v3.0, as the package licence sets out.

## Interest

I am the county councillor for Padiham and Burnley West and Lancashire County Council's Cabinet Member for Adult Social Care. The county council proposed two unitaries, and it would be the continuing authority under the second single-council form. The analysis and any errors are mine and do not represent the position of the county council or its Cabinet.

**Cite as:** Pickup, T. (2026) *The Financial Case for Local Government Reorganisation in Lancashire: A Transparent Component Model of Four Proposals and Two Single-Council Forms, with a Postscript on the 2026 Decisions*. Working paper, version 3.2. Zenodo. doi:10.5281/zenodo.24208546.
