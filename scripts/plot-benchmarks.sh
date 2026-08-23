#!/usr/bin/env bash
# Render every chart used by docs/DISSERTATION.md into docs/assets/charts/.
#
# Two scripts: the benchmark charts, which read bench/results/*.csv, and the
# dissertation charts, which read the document and the daemon source directly.
#
# matplotlib is installed into a local venv rather than the system Python,
# which on recent macOS/Homebrew is externally managed (PEP 668) and should
# not be written to by a build script.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
VENV="$ROOT/.venv-charts"
[[ -x "$VENV/bin/python" ]] || { echo "creating $VENV"; python3 -m venv "$VENV"; }
"$VENV/bin/python" -c "import matplotlib" 2>/dev/null || "$VENV/bin/pip" install -q matplotlib
"$VENV/bin/python" scripts/plot-benchmarks.py
exec "$VENV/bin/python" scripts/plot-dissertation-charts.py
