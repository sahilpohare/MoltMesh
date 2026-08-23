#!/usr/bin/env bash
# Build docs/DISSERTATION.docx from docs/DISSERTATION.md.
#
# Pandoc does not render mermaid, so the two diagrams are pre-rendered to PNG
# (checked in under docs/assets/diagrams/) and swapped in for their fenced
# mermaid blocks at build time. Regenerate those PNGs with:
#
#   ./scripts/render-diagrams.sh
#
# which extracts the blocks from this same document, so the diagrams in the
# markdown and the diagrams in the docx cannot drift apart.
#
# Screenshots live in Chapter 13 alongside the SDK examples, so they appear in
# the markdown and the docx alike rather than only in this build's output.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SRC="docs/DISSERTATION.md"
OUT="docs/DISSERTATION.docx"
BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT

command -v pandoc >/dev/null || { echo "pandoc is required: brew install pandoc" >&2; exit 1; }
[[ -f "$SRC" ]] || { echo "missing $SRC" >&2; exit 1; }

# Replace each fenced mermaid block with its pre-rendered figure, in order.
python3 - "$SRC" "$BUILD/body.md" <<'PY'
import re, sys
src, dst = sys.argv[1], sys.argv[2]
figures = [
    "assets/diagrams/fig-2-1-elixir-stack.png",
    "assets/diagrams/fig-3-1-go-libp2p-stack.png",
    "assets/diagrams/fig-5-1-capability-discovery.png",
    "assets/diagrams/fig-6-1-thread-append-commit.png",
    "assets/diagrams/fig-7-1-task-delegation.png",
]
text = open(src).read()
blocks = re.findall(r"```mermaid\n.*?```", text, re.S)
if len(blocks) != len(figures):
    sys.exit(f"expected {len(figures)} mermaid blocks, found {len(blocks)}; "
             "update the figures list in build-dissertation-docx.sh")
for block, png in zip(blocks, figures):
    text = text.replace(block, f"![]({png})", 1)

# The markdown opens with its own title, subtitle, blurb and a "Project
# links" section. All of that belongs on the cover page here, so drop it
# rather than printing it twice. The markdown keeps it so the file still
# reads as a complete document on its own.
text = text[text.index("## Abstract"):]
open(dst, "w").write(text)
PY

cat > "$BUILD/meta.yaml" <<'YAML'
---
lang: en-GB
---
YAML

# The departmental title page. Generated rather than hand-written so the
# word count on it cannot drift from the document it counts.
python3 scripts/make-cover.py "$SRC" "$BUILD/cover.md"

# Build the table of contents as real content rather than using pandoc's
# --toc. For docx, --toc emits a Word TOC *field* with no cached entries, so
# it renders blank until the reader presses F9 in Word, and stays blank in
# Google Docs, Preview, and most PDF converters. A static list of internal
# links always displays and stays clickable. It has no page numbers; a reader
# who wants those can insert a native TOC in Word over the top.
#
# Heading IDs come from pandoc itself (via an HTML pass) rather than from a
# reimplementation of its slug rules, so the links cannot drift from the
# anchors pandoc actually emits.
# --wrap=none matters: by default pandoc hard-wraps its HTML output, and it
# will break even inside an opening tag ("<h3\nid=..."), which silently hides
# a heading from any regex expecting "<h3 id=" and leaves stray newlines in
# the heading text. The parser below also tolerates that wrapping anyway.
pandoc "$BUILD/body.md" --from=gfm --to=html --wrap=none --output="$BUILD/probe.html"

python3 - "$BUILD/probe.html" "$BUILD/toc.md" <<'PY'
import html, re, sys
probe, dst = sys.argv[1], sys.argv[2]
doc = open(probe).read()
pattern = re.compile(r'<h([1-3])\s[^>]*?id="([^"]+)"[^>]*>(.*?)</h\1>', re.S)
out = ["## Contents", "",
       "Source: <https://github.com/sahilpohare/MoltMesh>", "",
       "Site: <https://openmolt.network>", ""]
count = 0
seen_top = False
for level, ident, raw in pattern.findall(doc):
    level = int(level)
    if level == 1:               # document title, already on the title page
        continue
    if level > 2 and not seen_top:
        # The subtitle is an h3 sitting above the first real section. It is
        # already on the title page, and emitting it here would open the list
        # at a nested indent, which Markdown reads as an indented code block
        # and renders as literal "- [text](#anchor)" instead of a link.
        continue
    if level == 2:
        seen_top = True
    text = html.unescape(re.sub(r"<[^>]+>", "", raw))
    text = re.sub(r"\s+", " ", text).strip()
    # Two spaces per level, not four: four is the indented-code threshold.
    indent = "  " * (level - 2)
    # Escape brackets so a heading like "ADR-0002 [x]" cannot break the link.
    label = text.replace("[", r"\[").replace("]", r"\]")
    out.append(f"{indent}- [{label}](#{ident})")
    count += 1
out.append("")
out.append("```{=openxml}")
out.append('<w:p><w:r><w:br w:type="page"/></w:r></w:p>')
out.append("```")
out.append("")
open(dst, "w").write("\n".join(out))
print(f"  table of contents: {count} entries", file=sys.stderr)
PY

pandoc "$BUILD/meta.yaml" "$BUILD/cover.md" "$BUILD/toc.md" "$BUILD/body.md" \
  --from=gfm+raw_attribute \
  --to=docx \
  --resource-path="$ROOT/docs" \
  --output="$OUT"

python3 - "$OUT" <<'PY'
import re, shutil, sys, zipfile, os

out = sys.argv[1]
tmp = out + ".tmp"
FONT = '<w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:cs="Times New Roman"/>'
SIZE = '<w:sz w:val="24"/><w:szCs w:val="24"/>'

def restyle(xml):
    # Document-wide defaults: Times New Roman 12pt, single spacing, no
    # inter-paragraph padding beyond what each style asks for.
    xml = re.sub(r'<w:rPrDefault>.*?</w:rPrDefault>',
                 f'<w:rPrDefault><w:rPr>{FONT}{SIZE}</w:rPr></w:rPrDefault>', xml, flags=re.S)
    xml = re.sub(r'<w:spacing w:line="[^"]*" w:lineRule="[^"]*"/>',
                 '<w:spacing w:line="240" w:lineRule="auto"/>', xml)
    return xml

with zipfile.ZipFile(out) as zin:
    names = zin.namelist()
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
        for n in names:
            data = zin.read(n)
            if n == "word/styles.xml":
                data = restyle(data.decode("utf8")).encode("utf8")
            elif n == "docProps/core.xml":
                # meta.yaml no longer carries these, so set them here.
                t = data.decode("utf8")
                t = re.sub(r"<dc:title>.*?</dc:title>", "", t, flags=re.S)
                t = t.replace("</cp:coreProperties>",
                    "<dc:title>The Architecture of OpenMolt Network</dc:title>"
                    "<dc:creator>Sahil Pohare</dc:creator></cp:coreProperties>")
                t = re.sub(r"<dc:creator>.*?</dc:creator>(?=.*<dc:creator>)", "", t, flags=re.S)
                data = t.encode("utf8")
            zout.writestr(n, data)
shutil.move(tmp, out)
print("  typography: Times New Roman 12pt, single spacing", file=sys.stderr)
PY

echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
