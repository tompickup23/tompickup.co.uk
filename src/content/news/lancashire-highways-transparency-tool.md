---
title: "Why I Built Lancashire's First Live Roadworks Map"
date: 2026-03-09
description: "Lancashire County Council's IT systems are broken. Oracle Fusion has failed. Residents can't find basic roadworks information. So I built a tool that does what the council should have done years ago. Here's what it does and why it matters for LGR."
image: "/images/lcc-highways.jpg"
tags: ["reform", "lancashire", "highways", "technology", "transparency"]
featured: true
---

Lancashire County Council manages 7,142 kilometres of road. That is more than the distance from London to Mumbai. It has a maintenance backlog north of &pound;650 million. There are 1,596 active and planned roadworks happening right now across 12 districts.

And until I built this tool, there was no single place where any resident could see them all on a map.

## The Problem

LCC's inherited IT systems are, to put it simply, not fit for purpose. The Oracle Fusion rollout has been marked by data breaches, payment errors, and missed deadlines. The highways database, MARIO, is a closed ArcGIS system that was never designed for public access. Residents who want to know whether their road is being dug up have to search through one.network or phone the council.

This is a county that spends &pound;72 million a year on highways. The public should be able to see where that money is going without submitting a Freedom of Information request.

## What the Tool Does

The [Live Roadworks Map](/roadworks) pulls data directly from LCC's official highways database twice daily. Every current and planned work across all 12 Lancashire districts, on one map, updated automatically.

<div class="viz-panel viz-panel-gradient">
<div class="viz-label" style="text-align: center;">Live Roadworks Map: What You Can See</div>
<div class="viz-grid viz-grid-3">
<div class="viz-stat teal">
<span class="value text-teal">1,596</span>
<span class="label">Live roadworks</span>
<span class="sublabel">Active + planned, all 12 districts</span>
</div>
<div class="viz-stat orange">
<span class="value text-orange">12</span>
<span class="label">Districts covered</span>
<span class="sublabel">Every LCC-managed road</span>
</div>
<div class="viz-stat red">
<span class="value text-red">2x</span>
<span class="label">Daily data refresh</span>
<span class="sublabel">Direct from LCC MARIO database</span>
</div>
</div>
</div>

It is not just dots on a map. Every work shows:

- **Road closure type**: full closure, lane restriction, or minor works, colour-coded with pulsing markers for active closures
- **Capacity impact**: a visual bar showing how much road capacity is lost
- **Duration and working hours**: when the work started, when it ends, and what hours disruption is expected
- **Operator**: whether it is LCC, a utility company, or a private contractor
- **County division and ward boundaries**: so you can see exactly what is happening in your area

## Features No Other Council Offers

I have searched for comparable tools. Some councils publish a list of planned works on their website. A few link to one.network. None, as far as I am aware, offer anything close to this.

<div class="viz-panel">
<div class="viz-label" style="text-align: center;">How Lancashire Compares</div>
<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 12px;">
<div style="padding: 16px; background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px;">
<div style="font-weight: 700; color: #12b6cf; margin-bottom: 8px; font-size: 0.85rem;">Lancashire (this tool)</div>
<ul style="font-size: 0.78rem; color: rgba(235,235,245,0.6); line-height: 1.8; padding-left: 16px; margin: 0;">
<li>Live interactive map with 1,596 works</li>
<li>District filtering across all 12 boroughs</li>
<li>Timeline playback with hourly disruption</li>
<li>Traffic intelligence overlay with JCI model</li>
<li>Congestion corridor modelling</li>
<li>Postcode and road name search</li>
<li>90-day forecast chart</li>
<li>County division and ward boundaries</li>
</ul>
</div>
<div style="padding: 16px; background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px;">
<div style="font-weight: 700; color: #8e8e93; margin-bottom: 8px; font-size: 0.85rem;">Typical council highways page</div>
<ul style="font-size: 0.78rem; color: rgba(235,235,245,0.4); line-height: 1.8; padding-left: 16px; margin: 0;">
<li>Link to one.network or roadworks.org</li>
<li>Static list of planned works (if anything)</li>
<li>No filtering or search</li>
<li>No traffic intelligence</li>
<li>No congestion modelling</li>
<li>No postcode search</li>
<li>No forecasting</li>
<li>No boundary overlay</li>
</ul>
</div>
</div>
</div>

The timeline feature alone sets it apart. You can play through the roadworks day by day, see which works overlap, spot periods of maximum disruption, and identify days when school-run corridors are worst affected. The hourly disruption chart breaks this down to the hour, highlighting AM and PM rush periods.

The traffic intelligence layer adds another dimension. Toggle it on and you see traffic signals, DfT count points, and modelled congestion junctions with a Junction Congestion Index that combines traffic volume, roadworks proximity, signal density, and school proximity into a single severity score.

<div class="viz-panel viz-panel-gradient">
<div class="viz-label" style="text-align: center;">Map Features at a Glance</div>
<div class="viz-grid viz-grid-4">
<div class="viz-stat teal">
<span class="value text-teal" style="font-size: 1.2rem;">&#x1F534;</span>
<span class="label">Road Closures</span>
<span class="sublabel">Pulsing red markers with X icon</span>
</div>
<div class="viz-stat orange">
<span class="value text-orange" style="font-size: 1.2rem;">&#x26A0;</span>
<span class="label">Lane Restrictions</span>
<span class="sublabel">Amber warning triangle markers</span>
</div>
<div class="viz-stat green">
<span class="value text-green" style="font-size: 1.2rem;">&#x25CF;</span>
<span class="label">Minor Works</span>
<span class="sublabel">Small grey circle markers</span>
</div>
<div class="viz-stat purple">
<span class="value text-purple" style="font-size: 1.2rem;">&#x1F4CA;</span>
<span class="label">Traffic Intel</span>
<span class="sublabel">JCI junctions, signals, count points</span>
</div>
</div>
</div>

## Why This Matters for LGR

Local Government Reorganisation will abolish Lancashire County Council and the 12 district councils, replacing them with two or three new unitary authorities. That means migrating every IT system both tiers currently use.

The critical path analysis is sobering.

<div class="viz-panel viz-panel-alert">
<div class="viz-label" style="text-align: center;">LGR IT Integration: The Critical Path</div>
<div class="viz-grid viz-grid-3">
<div class="viz-stat red">
<span class="value text-red">15</span>
<span class="label">Separate IT systems</span>
<span class="sublabel">HR, finance, planning, social care, highways</span>
</div>
<div class="viz-stat orange">
<span class="value text-orange">18 months</span>
<span class="label">IT integration workstream</span>
<span class="sublabel">One of four critical path items</span>
</div>
<div class="viz-stat red">
<span class="value text-red">&pound;8-15M</span>
<span class="label">IT migration cost risk</span>
<span class="sublabel">If rushed to 18-month timeline</span>
</div>
</div>
</div>

Fifteen separate systems need integrating. HR and payroll. Revenues and benefits. Planning. Housing. Social care. Finance. And highways. The MARIO system that feeds this roadworks tool, the Oracle Fusion system that has already failed, the Northgate system that handles council tax billing. All of them have to be harmonised, migrated, tested, and made to talk to each other.

IT integration is one of four items on the critical path for LGR. It requires a minimum of 18 months of work after the legal framework is in place. The government's proposed 18-month timeline from decision to vesting is, in the words of our analysis, "physically impossible without cutting corners."

Previous reorganisations tell us exactly what happens when IT is rushed. Buckinghamshire reorganised in 2020. Three years later, it was still running parallel finance systems at double the cost. The estimated excess cost: &pound;8 to &pound;12 million.

LCC is currently in the process of awarding three major IT contracts that will bind successor authorities for years after vesting:

- **ICT Managed Services**: &pound;68 million, 5 years (Jun 2026 to Jun 2031)
- **ERP/Finance System**: &pound;45 million, 7 years, replacing SAP (Sep 2026 to Sep 2033)
- **Revenues and Benefits**: &pound;40 million, 5 years, Northgate system (Apr 2027 to Apr 2032)

That is &pound;153 million in IT contracts being signed by a council that is about to be abolished. The successor authorities will inherit whatever is chosen. This is why the vesting date matters. Rush it, and you get Buckinghamshire. Take the time to do it properly, and you save &pound;14 to &pound;34 million in avoided transition costs.

## Transparency Is Not Optional

I built this tool because residents deserve to know what is happening on their roads. Not buried in a closed database. Not behind a council login. On a map, in real time, with the data that matters.

The fact that a single councillor can build a more useful highways information tool than the entire LCC IT department tells you something about the state of council technology. Oracle Fusion failed. MARIO was never designed for the public. The digital front door is a &pound;4.3 million contract that has not yet delivered.

<div class="viz-callout">
<span style="display: block; font-size: 1.15rem; font-weight: 900; color: #12b6cf; line-height: 1.4;">If a councillor with a laptop can build a better highways tool than the council's entire IT department, that should worry you about what happens when 15 IT systems need merging under LGR.</span>
</div>

This is not a technology problem. It is a priorities problem. And it is exactly the kind of problem that Reform was elected to fix.

**[View the Live Roadworks Map](/roadworks)**

<div class="viz-sources">
<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#30d158" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink: 0; margin-top: 1px;"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path><polyline points="9 12 11.5 14.5 15 9.5" stroke-width="2.5"></polyline></svg>
<span style="font-size: 0.75rem; color: #8e8e93; line-height: 1.5;">Sources: LCC MARIO ArcGIS highways database. DfT Road Traffic Statistics (AADF). LCC Digital Strategy 2025-2029. LCC Procurement Pipeline (ICT, ERP, Revenues contracts). LGR Critical Path Analysis (procurement_pipeline.json). Buckinghamshire Council post-vesting IT audit. LCC Highways Maintenance Transparency Report (March 2026).</span>
</div>
