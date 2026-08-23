#!/usr/bin/env bash
# Render the mermaid figures in docs/DISSERTATION.md to PNG.
#
# Pandoc does not render mermaid, so docx and PDF builds swap each fenced
# mermaid block for a pre-rendered PNG. The blocks are extracted from the
# dissertation itself rather than kept as separate .mmd files, so the diagram
# in the document and the diagram in the docx cannot drift apart.
#
# Two settings are not defaults and matter:
#
#   wrappingWidth  mermaid wraps node labels at ~200px by default, which turns
#                  this stack diagram into a single narrow column of two-word
#                  lines. mmdc's -w flag sets the viewport, not the wrap point,
#                  so it does not help. 520 keeps each label on one or two lines.
#   <br/>          mermaid 11 dropped "\n" as a line break inside labels and
#                  renders it as the literal two characters. Labels in the
#                  source use <br/>; scripts/check-diagrams.sh guards that.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SRC="docs/DISSERTATION.md"
OUTDIR="docs/assets/diagrams"
CONFIG="docs/mermaid-config.json"

# Figure order must match the fenced-block order in the document; the extractor
# below fails loudly if the counts disagree.
FIGURES=(
  fig-2-1-elixir-stack.png
  fig-3-1-go-libp2p-stack.png
  fig-5-1-capability-discovery.png
  fig-6-1-thread-append-commit.png
  fig-7-1-task-delegation.png
)

command -v npx >/dev/null || { echo "npx is required (Node.js)" >&2; exit 1; }
if [[ -z "${PUPPETEER_EXECUTABLE_PATH:-}" ]]; then
  for candidate in \
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    "$(command -v google-chrome || true)" \
    "$(command -v chromium || true)"; do
    [[ -x "$candidate" ]] && { export PUPPETEER_EXECUTABLE_PATH="$candidate"; break; }
  done
fi
[[ -n "${PUPPETEER_EXECUTABLE_PATH:-}" ]] || {
  echo "no Chrome found; set PUPPETEER_EXECUTABLE_PATH" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
echo '{ "args": ["--no-sandbox", "--disable-setuid-sandbox"] }' > "$WORK/puppeteer.json"

mkdir -p "$OUTDIR"
python3 - "$SRC" "$WORK" "${FIGURES[@]}" <<'PY'
import re, sys
src, work, *figures = sys.argv[1:]
blocks = re.findall(r"```mermaid\n(.*?)```", open(src).read(), re.S)
if len(blocks) != len(figures):
    sys.exit(f"{src} has {len(blocks)} mermaid blocks but {len(figures)} figures "
             "are configured; update FIGURES in scripts/render-diagrams.sh")
for i, block in enumerate(blocks):
    if "\\n" in block:
        sys.exit(f"figure {i+1} contains a literal \\n; mermaid 11 renders that "
                 "as two characters, use <br/> instead")
    open(f"{work}/{i}.mmd", "w").write(block)
PY

for i in "${!FIGURES[@]}"; do
  npx -y @mermaid-js/mermaid-cli -i "$WORK/$i.mmd" -o "$OUTDIR/${FIGURES[$i]}" \
    -b white -s 2 -c "$CONFIG" -p "$WORK/puppeteer.json" >/dev/null
  echo "  wrote $OUTDIR/${FIGURES[$i]}"
done
