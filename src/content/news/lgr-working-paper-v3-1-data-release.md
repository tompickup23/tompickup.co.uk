---
title: "Data Release: The Lancashire Reorganisation Working Paper, Version 3.1"
date: 2026-09-29T12:00:00
description: "Version 3.1 of the working paper on the finances of council reorganisation in Lancashire is deposited on Zenodo with its data and code, under CC BY 4.0. What is in the deposit and how to reproduce it."
category: "Lancashire"
subcategory: "Council"
tags: ["lancashire", "lgr", "releases", "data"]
featured: false
draft: false
data:
  links:
    - label: "Zenodo record 23016761: working paper, data and code, version 3.1 (DOI 10.5281/zenodo.23016761)"
      url: "https://doi.org/10.5281/zenodo.23016761"
    - label: "The results on this site: Lancashire's paused reorganisation plan"
      url: "https://tompickup.co.uk/lgr/"
  method: "Run uv run python reproduce.py in the unzipped package. It uses the Python 3.12 standard library only, with a fixed Monte Carlo seed, so every run gives the same tables."
---

Version 3.1 of my working paper on the finances of council reorganisation in Lancashire went on Zenodo on 28 September 2026. The paper, its data and its code are in one package, under one DOI: [10.5281/zenodo.23016761](https://doi.org/10.5281/zenodo.23016761).

The results, in 2024/25 prices, are set out on the [reorganisation page](/lgr/).

## What is in the deposit

- **The paper**, version 3.1, as a Markdown file.
- **Source data** in `data/`: council-level expenditure for the 15 Lancashire councils from 2021/22 to 2024/25, from the Ministry of Housing, Communities and Local Government's revenue outturn returns, with population and context fields and a codebook.
- **Derived tables** in `derived/`: each file described in the package README with its source, including 2026/27 budgets, reserves and borrowing, council tax, adult social care unit costs, population estimates and the paper's model results.
- **Code**: `reproduce.py`, which rebuilds every model table, and `tools/check_paper.py`, which checks the figures in the paper against that output.
- **Review records** in `review/`: the fact-check of version 3, the rules for the later tests, committed before those tests were run, and the reviews of the revision.

## What changed from version 3

Version 3.1 changes the method in five places, after a fact-check of version 3:

- how administrative costs scale with council size, and how corporate overheads are measured;
- the people-services term;
- the procurement base;
- how council tax is brought into line;
- the transition cost, and how quickly savings arrive.

The paper's change log lists every change.

## Licence

The paper, derived files, review records and code are released under Creative Commons Attribution 4.0 (CC BY 4.0). Fields derived from government statistics remain Crown copyright under the Open Government Licence v3.0, as the package licence sets out.

## Interest

I am a Cabinet member of Lancashire County Council, which proposed two councils. The paper declares this interest in full.

**Cite as:** Pickup, T. (2026) *The Financial Case for Local Government Reorganisation in Lancashire: A Transparent Component Model of Five Structural Proposals, with a Postscript on the 2026 Decisions*. Working paper, version 3.1. Zenodo. doi:10.5281/zenodo.23016761.
