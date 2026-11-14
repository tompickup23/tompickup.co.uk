"""Typeset a working paper's Markdown as an A4 PDF.

Offline pre-processing, like the other scripts here: the PDF it writes is
committed under public/documents/. The text is the deposited Markdown,
unchanged; only the presentation is added.

    uv run --with markdown-it-py --with "pypdf>=7" python \
        scripts/paper_pdf/build_paper_pdf.py PAPER.md OUT.pdf

It renders twice with headless Chrome. The first pass finds the page each
section starts on; the second prints those numbers in the contents.
"""

from __future__ import annotations

import html
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from markdown_it import MarkdownIt
from pypdf import PdfReader, PdfWriter

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
FONTS = REPO / "node_modules" / "@fontsource"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

FONT_CSS = [
    "source-serif-4/400.css",
    "source-serif-4/400-italic.css",
    "source-serif-4/600.css",
    "source-serif-4/600-italic.css",
    "source-serif-4/700.css",
    "source-sans-3/400.css",
    "source-sans-3/400-italic.css",
    "source-sans-3/600.css",
    "source-sans-3/700.css",
    "source-code-pro/400.css",
]


def slug(text: str) -> str:
    s = re.sub(r"<[^>]+>", "", text).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:80] or "section"


def plain(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", fragment)).strip()


def split_front(md: str) -> tuple[str, str]:
    """Front matter is everything before the first '## ' heading."""
    m = re.search(r"^## ", md, flags=re.M)
    return md[: m.start()], md[m.start():]


def front_page(front: str, mdi: MarkdownIt) -> tuple[str, str]:
    lines = front.strip().splitlines()
    title = lines[0].lstrip("# ").strip()
    rest = "\n".join(lines[1:])
    rest = re.sub(r"^---\s*$", "", rest, flags=re.M)
    # Hard line breaks in the author block, as the Markdown intends.
    blocks = [b.strip() for b in rest.split("\n\n") if b.strip()]
    author_block = blocks[0]
    others = blocks[1:]

    if ":" in title:
        main, sub = title.split(":", 1)
        title_html = (
            f'<h1 class="title">{html.escape(main.strip())}</h1>'
            f'<p class="subtitle">{html.escape(sub.strip())}</p>'
        )
    else:
        title_html = f'<h1 class="title">{html.escape(title)}</h1>'

    a_lines = author_block.splitlines()
    name = a_lines[0].strip("* ")
    affil = "<br>".join(mdi.renderInline(l.strip()) for l in a_lines[1:])

    version_html = ""
    meta_html = []
    for b in others:
        rendered = mdi.render(b)
        if b.startswith("**Working paper"):
            version_html = rendered
        else:
            meta_html.append(rendered)

    cover = f"""
<section class="cover">
  <div class="cover-top">
    <p class="eyebrow">Working paper &middot; Version 3.2</p>
    {title_html}
    <p class="author">{html.escape(name)}</p>
    <p class="affil">{affil}</p>
  </div>
  <div class="cover-version">{version_html}</div>
  <div class="cover-meta">{''.join(meta_html)}
    <p class="typeset">This PDF is typeset from the Markdown file deposited at Zenodo. The text is unchanged; the deposited package, with its data and code, is the record.</p>
  </div>
</section>
"""
    return title, cover


def post_process(body: str) -> tuple[str, list[tuple[int, str, str]]]:
    heads: list[tuple[int, str, str]] = []
    seen: dict[str, int] = {}

    def head(m: re.Match) -> str:
        level, inner = int(m.group(1)), m.group(2)
        text = plain(inner)
        sid = slug(text)
        if sid in seen:
            seen[sid] += 1
            sid = f"{sid}-{seen[sid]}"
        else:
            seen[sid] = 1
        cls = []
        if level == 2 and re.match(r"(Appendix A|References|Change log)", text):
            cls.append("new-page")
        if level == 2 and text == "Abstract":
            cls.append("abstract-head")
        num = re.match(r"^((?:\d+\.)+\d*|\d+\.)\s+(.*)$", text)
        if num and level in (2, 3):
            n, rest = num.group(1).rstrip("."), num.group(2)
            inner = f'<span class="num">{html.escape(n)}</span>{html.escape(rest)}'
        if level in (2, 3):
            heads.append((level, sid, text))
        c = f' class="{" ".join(cls)}"' if cls else ""
        return f'<h{level} id="{sid}"{c}>{inner}</h{level}>'

    body = re.sub(r"<h([2-4])>(.*?)</h\1>", head, body, flags=re.S)

    # "**Table 5. ...**" paragraph directly before a table becomes its caption.
    def caption(m: re.Match) -> str:
        cap, table = m.group(1), m.group(2)
        cap_txt = plain(cap)
        label = re.match(r"^(Table [A-Z]?\d+[a-z]?(?: \(continued\))?)\.?\s*(.*)$", cap_txt)
        if label:
            cap_html = (
                f'<span class="tlabel">{html.escape(label.group(1))}</span>'
                f"{html.escape(label.group(2))}"
            )
        else:
            cap_html = html.escape(cap_txt)
        return f'<figure class="tbl"><figcaption>{cap_html}</figcaption>{table}'

    body = re.sub(
        r"<p><strong>(Table [^<]*?)</strong></p>\s*(<table>)",
        caption,
        body,
    )
    # Close figures after their table, and pull a following note into them.
    body = re.sub(
        r"(<figure class=\"tbl\">.*?</table>)(\s*<p>((?:Sources?|Notes?)[ :].*?)</p>)?",
        lambda m: m.group(1)
        + (f'<p class="tnote">{m.group(3)}</p>' if m.group(3) else "")
        + "</figure>",
        body,
        flags=re.S,
    )
    # Uncaptioned tables still get notes styled.
    body = re.sub(
        r"(</table>)\s*<p>((?:Sources?|Notes?):.*?)</p>",
        r'\1<p class="tnote">\2</p>',
        body,
        flags=re.S,
    )

    # Mark wide tables and rows that are totals.
    def table(m: re.Match) -> str:
        t = m.group(0)
        cols = len(re.findall(r"<th", t.split("</tr>", 1)[0]))
        cls = "xl" if cols >= 12 else "wide" if cols >= 8 else ("mid" if cols >= 6 else "")
        t = re.sub(
            r"<tr>(\s*<td[^>]*><strong>)",
            r'<tr class="total">\1',
            t,
        )
        return t.replace("<table>", f'<table class="{cls}">', 1)

    body = re.sub(r"<table>.*?</table>", table, body, flags=re.S)

    # Very wide tables go on landscape pages, caption and note with them.
    body = re.sub(
        r'<figure class="tbl">(<figcaption>(?:(?!</figcaption>).)*</figcaption>)<table class="xl">',
        r'<figure class="tbl landscape">\1<table class="xl">',
        body,
        flags=re.S,
    )

    # Keep a minus sign with its figure: Chrome will otherwise break after it.
    body = re.sub(r"(?<![\w])(−[£€]?\d[\d.,]*(?:[MKk%]|bn)?)", r'<span class="nb">\1</span>', body)

    # Section dividers in the Markdown are redundant beside the headings.
    body = body.replace("<hr />", "").replace("<hr>", "")
    return body, heads


def contents(heads, pages: dict[str, int] | None) -> str:
    rows = []
    in_log = False
    for level, sid, text in heads:
        if text == "Abstract":
            continue
        if level == 2:
            in_log = text.startswith("Change log")
        elif in_log:
            continue
        p = str(pages.get(sid, "")) if pages else "000"
        num = re.match(r"^((?:\d+\.)+\d*|\d+\.)\s+(.*)$", text)
        if num:
            label = (
                f'<span class="cnum">{html.escape(num.group(1).rstrip("."))}</span>'
                f"{html.escape(num.group(2))}"
            )
        else:
            label = f'<span class="cnum"></span>{html.escape(text)}'
        rows.append(
            f'<li class="l{level}"><a href="#{sid}">{label}</a>'
            f'<span class="dots"></span><span class="pg">{p}</span></li>'
        )
    return (
        '<section class="toc"><h2 class="toc-head">Contents</h2><ol>'
        + "".join(rows)
        + "</ol></section>"
    )


def page_html(title, cover, toc, body) -> str:
    font_links = "\n".join(
        f'<link rel="stylesheet" href="{(FONTS / f).as_uri()}">' for f in FONT_CSS
    )
    css = (HERE / "paper.css").read_text()
    return f"""<!doctype html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<title>{html.escape(title)}</title>
{font_links}
<style>{css}</style>
</head>
<body>
{cover}
{toc}
<main>{body}</main>
</body>
</html>"""


def render(html_text: str, out_pdf: Path, work: Path) -> None:
    src = work / "paper.html"
    src.write_text(html_text)
    if os.environ.get("KEEP_HTML"):
        Path(os.environ["KEEP_HTML"]).write_text(html_text)
    subprocess.run(
        [
            CHROME,
            "--headless=new",
            "--disable-gpu",
            "--no-pdf-header-footer",
            "--generate-pdf-document-outline",
            "--allow-file-access-from-files",
            "--virtual-time-budget=20000",
            f"--print-to-pdf={out_pdf}",
            src.as_uri(),
        ],
        check=True,
        capture_output=True,
    )


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def find_pages(pdf: Path, heads, toc_last_page: int) -> dict[str, int]:
    reader = PdfReader(str(pdf))
    texts = [norm(p.extract_text() or "") for p in reader.pages]
    pages: dict[str, int] = {}
    cursor = toc_last_page
    for _level, sid, text in heads:
        key = norm(text)[:60]
        for i in range(cursor, len(texts)):
            if key in texts[i]:
                pages[sid] = i + 1
                cursor = i
                break
    return pages


def toc_end(pdf: Path, body: str) -> int:
    """Index of the page where the body begins: the first paragraph's text."""
    first_para = plain(re.search(r"<p>(.*?)</p>", body, flags=re.S).group(1))
    key = norm(first_para)[:50]
    reader = PdfReader(str(pdf))
    for i, p in enumerate(reader.pages):
        if key in norm(p.extract_text() or ""):
            return i
    return 0


def main() -> None:
    src, out = Path(sys.argv[1]), Path(sys.argv[2])
    md = src.read_text()
    mdi = MarkdownIt("commonmark", {"html": False, "typographer": False}).enable("table")
    front, rest = split_front(md)
    title, cover = front_page(front, mdi)
    body, heads = post_process(mdi.render(rest))

    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        first = work / "pass1.pdf"
        render(page_html(title, cover, contents(heads, None), body), first, work)
        last_toc = toc_end(first, body)
        pages = find_pages(first, heads, last_toc)
        missing = [t for _l, s, t in heads if s not in pages]
        if missing:
            print("No page found for:", *missing, sep="\n  ", file=sys.stderr)
        second = work / "pass2.pdf"
        render(page_html(title, cover, contents(heads, pages), body), second, work)

        writer = PdfWriter(clone_from=str(second))
        writer.add_metadata(
            {
                "/Title": title,
                "/Author": "Tom Pickup",
                "/Subject": "Working paper, version 3.2, 13 November 2026. "
                "Zenodo, doi:10.5281/zenodo.24208546",
                "/Keywords": "local government reorganisation; Lancashire; "
                "unitary authorities; public expenditure",
            }
        )
        writer.compress_identical_objects(remove_duplicates=True, remove_unreferenced=True)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("wb") as f:
            writer.write(f)
    n = len(PdfReader(str(out)).pages)
    print(f"{out}: {n} pages, {out.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
