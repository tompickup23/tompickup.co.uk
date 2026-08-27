---
title: "What the Councils Own in Burnley: Every Public Title, Mapped"
date: 2026-06-21T10:00:00
description: "Burnley Borough Council and Lancashire County Council hold more than a thousand property titles in the town. Every one I could place, on a map."
image: "/images/burnley-aerial.jpg"
ogImage: "/images/share/what-the-councils-own.png"
imageCredit: "Photo: Childzy / Wikimedia Commons (CC BY 3.0)"
category: "Burnley"
subcategory: "Transparency"
tags: ["burnley", "transparency", "property", "data", "map"]
featured: false
draft: false
---

I have written a lot about who owns the *private* half of Burnley: the landlords, the companies, the faraway funds. This article is about the other half, the part you already own. Between them, Burnley Borough Council and Lancashire County Council hold **2,043 property titles** in the town, and almost nobody has ever seen them set out in one place. So I took the public ownership records and built a map.

<div class="viz-info">
This is the public counterpart to my series on <a href="/news/who-owns-burnley/">who owns Burnley</a>. The same Land Registry that names the private owners also records the public ones. Every title below is held by a council, which means it is held on your behalf, and paid for with your money.
</div>

<div class="viz-panel-reform">
<div class="viz-grid viz-grid-2">
<div class="viz-stat teal">
<span class="value xl" style="color: #12b6cf;">2,043</span>
<span class="label">Council-owned titles in Burnley</span>
<span class="sublabel">HM Land Registry, June 2026</span>
</div>
<div class="viz-stat orange">
<span class="value xl" style="color: #ff9f0a;">1,095</span>
<span class="label">Held by Burnley Borough Council</span>
<span class="sublabel">Civic buildings, parks and town land</span>
</div>
<div class="viz-stat teal">
<span class="value xl" style="color: #12b6cf;">948</span>
<span class="label">Held by Lancashire County Council</span>
<span class="sublabel">Schools, highways and county land</span>
</div>
<div class="viz-stat purple">
<span class="value xl" style="color: #bf5af2;">50 / 50</span>
<span class="label">Split freehold and leasehold</span>
<span class="sublabel">1,019 freehold, 1,024 leasehold</span>
</div>
</div>
</div>

## The map

Every marker is one property held by a council. **Teal is Lancashire County Council, amber is Burnley Borough Council.** A **ringed** marker is a named asset taken from the council's own register, a school, the shopping centre, a depot, placed precisely. A plain solid marker is placed from its postcode. A faded, dashed marker is land recorded only by its street, so it is placed approximately. Click any marker for its name, type, title number and tenure. Use the boxes to switch each council on or off.

<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" crossorigin="" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" crossorigin="" />

<div class="bpp-wrap">
<div class="bpp-controls">
<label class="bpp-toggle bpp-toggle-lcc"><input type="checkbox" id="bpp-lcc" checked /> <span>Lancashire County Council</span></label>
<label class="bpp-toggle bpp-toggle-bbc"><input type="checkbox" id="bpp-bbc" checked /> <span>Burnley Borough Council</span></label>
<span class="bpp-legend-note">Ringed = named asset (register) &nbsp;·&nbsp; Solid = by postcode &nbsp;·&nbsp; Faded = approximate (street)</span>
</div>
<div id="bpp-map" class="bpp-map">Loading the map...</div>
<p id="bpp-stat" class="bpp-stat">Loading property data...</p>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js" crossorigin=""></script>
<script>
(function () {
  var COL = { LCC: '#12b6cf', BBC: '#ff9f0a' };
  var tries = 0;
  function esc(s){ return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];}); }
  function boot() {
    if (!(window.L && window.L.markerClusterGroup)) { if (tries++ < 120) return setTimeout(boot, 100); return; }
    var el = document.getElementById('bpp-map');
    if (!el || el._init) return; el._init = true; el.textContent = '';
    var map = L.map('bpp-map', { scrollWheelZoom: false }).setView([53.789, -2.245], 13);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd', maxZoom: 19
    }).addTo(map);
    map.on('click', function(){ map.scrollWheelZoom.enable(); });
    map.whenReady(function(){ setTimeout(function(){ map.invalidateSize(); }, 250); });
    if (window.ResizeObserver) { new ResizeObserver(function(){ map.invalidateSize(); }).observe(el); }
    function clusterIcon(color){
      return function(cluster){
        var n = cluster.getChildCount();
        var size = n < 10 ? 32 : n < 100 ? 40 : 48;
        return L.divIcon({
          html: '<div class="bpp-cluster" style="background:' + color + ';width:' + size + 'px;height:' + size + 'px;line-height:' + (size - 4) + 'px;">' + n + '</div>',
          className: 'bpp-cluster-wrap', iconSize: L.point(size, size)
        });
      };
    }
    var groups = {
      LCC: L.markerClusterGroup({ chunkedLoading: true, maxClusterRadius: 45, spiderfyOnMaxZoom: true, iconCreateFunction: clusterIcon(COL.LCC) }),
      BBC: L.markerClusterGroup({ chunkedLoading: true, maxClusterRadius: 45, spiderfyOnMaxZoom: true, iconCreateFunction: clusterIcon(COL.BBC) })
    };
    fetch('/data/burnley-public-property.json').then(function(r){ return r.json(); }).then(function(d){
      d.features.filter(function(f){ return typeof f.lat === 'number'; }).forEach(function(f){
        var tier = f.p, isReg = tier === 'register', isApprox = tier === 'st';
        var m = L.circleMarker([f.lat, f.lng], {
          radius: isReg ? 7 : (tier === 'pc' ? 6 : 5),
          color: isReg ? '#ffffff' : COL[f.o], weight: isReg ? 2 : (isApprox ? 1 : 1.4),
          fillColor: COL[f.o], fillOpacity: isApprox ? 0.28 : (isReg ? 0.95 : 0.85),
          opacity: isApprox ? 0.6 : 1, dashArray: isApprox ? '2,3' : null
        });
        var owner = f.o === 'LCC' ? 'Lancashire County Council' : 'Burnley Borough Council';
        var loc = isReg ? 'council asset register' : (tier === 'pc' ? 'by postcode' : 'approximate (street)');
        var header = f.nm ? esc(f.nm) : esc(f.a || '(no address recorded)');
        var titleRow = String(f.t || '').indexOf('LCC-') === 0 ? '' : '<tr><td>Title</td><td>' + esc(f.t) + '</td></tr>';
        m.bindPopup(
          '<div class="bpp-pop"><span class="bpp-pop-owner" style="color:' + COL[f.o] + '">' + esc(owner) + '</span>' +
          '<div class="bpp-pop-addr">' + header + '</div>' +
          (f.nm && f.a ? '<div class="bpp-pop-sub">' + esc(f.a) + '</div>' : '') +
          '<table class="bpp-pop-tbl">' +
          (f.cat ? '<tr><td>Type</td><td>' + esc(f.cat) + '</td></tr>' : '') +
          titleRow +
          '<tr><td>Tenure</td><td>' + esc(f.te || 'n/a') + '</td></tr>' +
          (f.pc ? '<tr><td>Postcode</td><td>' + esc(f.pc) + '</td></tr>' : '') +
          '<tr><td>Location</td><td>' + loc + '</td></tr></table>' +
          '<div class="bpp-pop-src">' + (isReg ? 'Council asset register + HM Land Registry' : 'HM Land Registry CCOD, June 2026') + '</div></div>'
        );
        groups[f.o].addLayer(m);
      });
      map.addLayer(groups.LCC); map.addLayer(groups.BBC);
      try { map.fitBounds(L.featureGroup([groups.LCC, groups.BBC]).getBounds().pad(0.05)); } catch (e) {}
      function wire(id, key){ var c = document.getElementById(id); if (!c) return; c.addEventListener('change', function(){ c.checked ? map.addLayer(groups[key]) : map.removeLayer(groups[key]); }); }
      wire('bpp-lcc', 'LCC'); wire('bpp-bbc', 'BBC');
      var s = document.getElementById('bpp-stat');
      if (s) s.textContent = 'Of ' + d.total_titles + ' titles plus ' + d.lcc_register_sites + ' named LCC sites: ' + d.mapped_register + ' placed precisely from the councils’ asset registers, ' + d.mapped_pc + ' by postcode, ' + d.mapped_st + ' approximately by street. ' + d.unmapped + ' have no mappable location and appear in the list only.';
      buildList(d.features);
    }).catch(function(){ var s = document.getElementById('bpp-stat'); if (s) s.textContent = 'The map data could not be loaded.'; });

    function buildList(features){
      var list = document.getElementById('bpp-list'); var inp = document.getElementById('bpp-search'); var cnt = document.getElementById('bpp-list-count');
      if (!list) return;
      function render(q){
        q = (q || '').toLowerCase().trim(); list.textContent = ''; var n = 0, shown = 0;
        for (var i = 0; i < features.length; i++){
          var f = features[i];
          var hay = ((f.nm || '') + ' ' + (f.a || '') + ' ' + (f.t || '') + ' ' + (f.pc || '')).toLowerCase();
          if (q && hay.indexOf(q) < 0) continue;
          n++; if (shown >= 500) continue; shown++;
          var row = document.createElement('div'); row.className = 'bpp-row';
          var dot = document.createElement('span'); dot.className = 'bpp-dot'; dot.style.background = COL[f.o];
          var main = document.createElement('div'); main.className = 'bpp-row-main';
          var addr = document.createElement('div'); addr.className = 'bpp-row-addr'; addr.textContent = f.nm || f.a || '(no address recorded)';
          var meta = document.createElement('div'); meta.className = 'bpp-row-meta';
          meta.textContent = (f.o === 'LCC' ? 'LCC' : 'Burnley BC') + ' · ' + (f.cat ? f.cat + ' · ' : '') + (f.te || 'n/a') + ' · ' + f.t + (f.p === 'none' ? ' · not on map' : '');
          main.appendChild(addr); main.appendChild(meta); row.appendChild(dot); row.appendChild(main); list.appendChild(row);
        }
        if (cnt) cnt.textContent = q ? (n + ' matching title' + (n === 1 ? '' : 's') + (shown < n ? ' (showing first 500)' : '')) : (features.length + ' titles in total (search to narrow)');
      }
      if (inp) inp.addEventListener('input', function(){ render(inp.value); });
      render('');
    }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
</script>

## What the map shows

Two things stand out the moment you look.

First, **the two councils own the town between them almost equally**: the borough holds 1,095 titles and the county 948. People assume the county, with its schools and roads, must dominate. It does not. Burnley Borough Council holds more than Lancashire County Council, because a borough accumulates a vast number of small urban parcels over a century and more: back streets and ginnels, scraps of land beside houses, former housing sites, parks, allotments, car parks and civic buildings. The county's holdings are fewer but larger, the school sites and the ribbons of roadside land it owns as the highways authority. Most of the faded markers, the land recorded only by a street name, are these parcels from both councils.

Second, **the ownership splits almost exactly in half between freehold and leasehold**: 1,019 titles held outright, 1,024 held on a long lease. That is not a detail. A leasehold interest is a different kind of asset from a freehold, with different costs and different value, and a council that cannot say at a glance which of its two thousand holdings are which cannot manage either well.

## Putting names to the parcels

A bare title number tells you nothing. So I went one step further and brought in the councils' own **asset registers**, the lists each authority publishes under the Local Government Transparency Code of what it manages.

Matching Burnley Borough Council's register to the live title record puts a real name to **573** of the borough's parcels, and an exact map pin on **572** of them: Charter Walk, the markets, the town's depots, yards and former works. Lancashire County Council's register adds **75** named sites across the town, each tagged by what it is: **35 schools**, plus libraries, depots, children's centres and civic offices. On the map these named assets are the **ringed** markers. Click one for its name, type and tenure.

One honest caveat: the borough's published register dates from 2015, so I show only the assets whose titles the council still holds today. Neither council publishes a reliable per-asset valuation, so this map names and locates the estate; it does not price it.

## Why this matters

None of this is hidden, exactly. It is just never put in front of you. The Land Registry holds all of it, but it takes a councillor with a spreadsheet and a map library to turn it into something a resident can actually see. That is the wrong way round. A public body should be able to show the public what it owns on its behalf, on a map, as a matter of course.

It matters for more than principle. Every one of these titles costs money to hold: insurance, maintenance, security, lost opportunity. Some of it is essential, the schools and the working civic buildings. Some of it is land doing nothing, that could be built on, brought back into use, or sold to fund services instead of sitting idle. You cannot make any of those decisions, or hold anyone to account for them, until you can see the whole estate at once. That is what asset management is, and it starts with a list nobody has ever published.

I am the Cabinet Member for Adult Social Care at Lancashire County Council, so I have a direct interest in the county getting full value from what it owns, because every pound a well-run estate frees up is a pound that can go to the services people actually need.

## Search every title

The map can only place a title if the records give it a location. Below is the full list of all **2,043** titles plus the **75** named county sites, including the land parcels that have no postcode or street to map. Search by name, street, postcode or title number.

<div class="bpp-wrap">
<input type="search" id="bpp-search" class="bpp-search" placeholder="Search by street, postcode or title number..." aria-label="Search council property titles" />
<p id="bpp-list-count" class="bpp-list-count">Loading...</p>
<div id="bpp-list" class="bpp-list"></div>
</div>

## What I want done

- **Publish an open asset register.** Both councils should publish, and keep up to date, a plain list and map of everything they own. The data already exists. Putting it in public view costs almost nothing and is the foundation of everything else.
- **Review the land that does nothing.** Idle council land in a town short of homes is a wasted asset. Every parcel should have an answer to one question: is this earning its keep, serving a purpose, or should it be brought back into use or sold?
- **Know freehold from leasehold.** An estate split half and half between the two has to be managed as two different things. A single register that records tenure on every title is the minimum.
- **Treat transparency as the default.** Residents own this. They should not have to rely on one councillor with a spreadsheet to find out what.

The records are public. As with my articles on the private owners, I am simply putting them where the people who actually own this property, the public, can see them.

## Where these numbers come from

You do not need this part to follow the story. It is here so the working can be checked.

- **Ownership and titles** come from **[HM Land Registry](https://www.gov.uk/government/collections/price-paid-data)'s Commercial and Corporate Ownership Data (CCOD)**, the official public record of property in England and Wales owned by companies and corporate bodies, downloaded for **June 2026** and filtered to the **Burnley district** (the borough council area). I counted every title whose registered proprietor is Burnley Borough Council or Lancashire County Council: 2,043 in total, 1,095 borough and 948 county, split 1,019 freehold and 1,024 leasehold. The borough's titles are mostly registered under its formal legal name, "The Council of the Borough of Burnley", which I have merged with the shorter "Burnley Borough Council" and a handful of spelling variants; the county's appear as "Lancashire County Council" and "The Lancashire County Council". Parish and town councils, neighbouring boroughs and the local voluntary-service body are excluded.
- **A title is not the same as a building.** One title can cover a single house, a large school site, or a strip of roadside land. The count is of titles, the unit the Land Registry uses, not of separate buildings.
- **Locations are added, not official.** CCOD records an address but no map coordinates. Titles with a postcode are placed precisely using the [Office for National Statistics](https://www.ons.gov.uk/) postcode directory (via postcodes.io). Titles recorded only by a street ("land at Manchester Road, Burnley") are placed approximately on that street using OpenStreetMap's Nominatim geocoder, and are shown faded and dashed to make clear they are indicative, not exact. Land with no usable street is listed but not mapped. **These markers show roughly where a title is, not its legal boundary.** Exact boundaries are in HM Land Registry's National Polygon dataset, which sits behind a paid licence and is not used here.
- **Named assets come from the councils' own registers.** Burnley Borough Council's Local Government Transparency Code asset register (published 2015) carries a name, title number and Ordnance Survey grid reference for each holding; I matched it to the current CCOD title list, so only assets still owned appear, and converted the grid references to map points, naming 573 borough parcels and placing 572 precisely. Lancashire County Council's 75 named Burnley sites, with their use-categories, come from the AI DOGE property dataset built from the county's asset data. Neither register publishes a reliable per-asset valuation, so none is shown.
- **The map** uses the open-source Leaflet library with OpenStreetMap and CARTO basemap tiles.

One honest note. CCOD covers property held by UK companies and corporate bodies, which includes councils. A small number of holdings can be registered under a slightly different legal name or sit just outside the district boundary, so treat 2,043 as a close and honest count of the councils' Burnley estate rather than a number to the last title.
