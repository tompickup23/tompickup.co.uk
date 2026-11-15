// Chamber kit: sheets. One <dialog> per page that rises from the bottom edge on a phone and sits
// in the middle on a wider screen, after the sheets of the Virtual Cat Cafe. openSheet() fills it
// and opens it; Escape, the close button or a tap on the shade closes it, and focus goes back to
// whatever opened it. menuList() draws the grouped lists the menu and other sheets use.
import { icon } from './icons.js';

const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

export function createSheet(host) {
  const dlg = document.createElement('dialog');
  dlg.className = 'ck-sheet';
  dlg.setAttribute('aria-labelledby', 'ck-sheet-title');
  host.append(dlg);
  let opener = null;
  dlg.addEventListener('click', (e) => {
    if (e.target === dlg) dlg.close(); // a tap on the shade outside the sheet
    else if (e.target.closest('[data-sheet-close]')) dlg.close();
  });
  dlg.addEventListener('close', () => { dlg.classList.remove('wide'); if (opener?.isConnected) opener.focus({ preventScroll: true }); opener = null; });
  return {
    el: dlg,
    open(title, body, { wide = false } = {}) {
      opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      dlg.innerHTML = `<div class="ck-sheet-head"><h2 id="ck-sheet-title">${esc(title)}</h2><button type="button" class="ck-iconbtn" data-sheet-close aria-label="Close">${icon('close')}</button></div><div class="ck-sheet-body">${body}</div>`;
      dlg.classList.toggle('wide', wide);
      if (!dlg.open) dlg.showModal();
      dlg.scrollTop = 0;
    },
    close() { if (dlg.open) dlg.close(); },
    get isOpen() { return dlg.open; },
  };
}

// groups: [{ title, items: [{ icon, label, attrs, pressed, note }] }]. attrs is a string of data
// attributes for the page's own click handler; a disabled item is shown as a plain line.
export function menuList(groups) {
  return groups.filter((g) => g.items.length).map((g) => `${g.title ? `<h3>${esc(g.title)}</h3>` : ''}<ul class="ck-list">${g.items.map((it) => {
    const inner = `${icon(it.icon)}<span>${esc(it.label)}${it.note ? `<small>${esc(it.note)}</small>` : ''}</span>${it.pressed ? icon('check', 'ck-tick') : ''}`;
    return `<li>${it.href ? `<a href="${esc(it.href)}">${inner}</a>` : `<button type="button" ${it.attrs || ''}${it.pressed != null ? ` aria-pressed="${!!it.pressed}"` : ''}>${inner}</button>`}</li>`;
  }).join('')}</ul>`).join('');
}
