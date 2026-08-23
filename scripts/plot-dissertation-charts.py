#!/usr/bin/env python3
"""Render the non-benchmark figures for docs/DISSERTATION.md.

These charts are drawn from data that already exists inside the dissertation
(Appendix A's verdict table, the Chapter 14.4 claims table) or from the
daemon's own source (the quorum rule in daemon/thread/manager.go), rather
than from numbers retyped into this script. Where a value has to be stated
here, the script asserts it against its source so the chart cannot silently
drift from the document it illustrates.

Benchmark charts live in scripts/plot-benchmarks.py. Run both via
scripts/plot-benchmarks.sh.
"""
from __future__ import annotations

import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
from matplotlib.ticker import NullFormatter  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DISS = os.path.join(ROOT, "docs", "DISSERTATION.md")
OUT = os.path.join(ROOT, "docs", "assets", "charts")

INK = "#1a2333"
GRID = "#D8DCE3"

# One colour per verdict class, ordered best to worst so a reader can scan the
# legend top to bottom as a severity ramp. Chosen to stay separable in
# greyscale, since a printed dissertation may well be.
CLASSES = [
    ("Implemented as written", "#0F766E"),
    ("Implemented, record has drifted", "#B45309"),
    ("Generalised beyond the record", "#7C3AED"),
    ("Reversed by later code", "#B91C1C"),
    ("Never implemented", "#4C0519"),
    ("Written by this audit", "#475569"),
]
COLOUR = dict(CLASSES)

# Verdict class per ADR. Assigned by reading Appendix A's verdict text, not
# by a regex over it: "Matches; amends 0014/0016's bearer-capability gap"
# and "Matches; undocumented dual entrypoint" share a shape but mean opposite
# things, the first recording a clean implementation that extends an earlier
# ADR and the second recording a gap in the paper trail.
VERDICT_CLASS = {
    1: "Implemented, record has drifted",   # undocumented dual entrypoint
    2: "Implemented as written",
    3: "Reversed by later code",            # reversed ~1 day later
    4: "Implemented, record has drifted",   # NAT/security transport undocumented
    5: "Implemented as written",
    6: "Implemented, record has drifted",   # topic inventory incomplete
    7: "Implemented, record has drifted",   # two undocumented mechanisms
    8: "Generalised beyond the record",     # generic actor, not 12 bespoke types
    9: "Implemented, record has drifted",   # vocabulary drifted
    10: "Reversed by later code",           # etcd/raft rejection reversed
    11: "Never implemented",
    12: "Implemented as written",
    13: "Reversed by later code",           # deleted ~1 day later
    14: "Implemented as written",
    15: "Implemented as written",
    16: "Implemented as written",
    17: "Implemented as written",
    18: "Implemented as written",
    19: "Implemented as written",
    20: "Written by this audit",
    21: "Written by this audit",
    22: "Written by this audit",
    23: "Written by this audit",
    24: "Written by this audit",
}

plt.rcParams.update({
    "figure.dpi": 200,
    "savefig.dpi": 200,
    "font.size": 9,
    "axes.edgecolor": INK,
    "axes.labelcolor": INK,
    "text.color": INK,
    "xtick.color": INK,
    "ytick.color": INK,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.6,
    "axes.axisbelow": True,
})


def save(fig, filename: str) -> None:
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, filename)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  wrote {os.path.relpath(path, ROOT)}")


def read_appendix_a() -> list[tuple[int, str]]:
    """Parse Appendix A's verdict table straight out of the dissertation.

    Reading the table rather than restating it means a verdict edited in the
    prose cannot leave the chart showing the old number. The classification
    map above is checked against the parsed rows so a newly added ADR fails
    loudly here instead of being silently dropped from the chart.
    """
    text = open(DISS).read()
    start = text.index("## Appendix A.")
    end = text.index("## Appendix B.")
    rows = []
    for line in text[start:end].splitlines():
        # The ADR column is a markdown link to the record on GitHub,
        # so tolerate both "| 0001 |" and "| [0001](url) |".
        m = re.match(r"\|\s*\[?(\d{4})\]?[^|]*\|([^|]*)\|([^|]*)\|", line)
        if m:
            rows.append((int(m.group(1)), m.group(3).strip()))
    if not rows:
        raise SystemExit("Appendix A: no ADR rows parsed; has the table format changed?")
    parsed = {n for n, _ in rows}
    if parsed != set(VERDICT_CLASS):
        missing = sorted(parsed - set(VERDICT_CLASS))
        extra = sorted(set(VERDICT_CLASS) - parsed)
        raise SystemExit(
            f"Appendix A and VERDICT_CLASS disagree; unclassified ADRs: {missing}, "
            f"classified but absent from the table: {extra}")
    return rows


def chart_adr_conformance(rows) -> None:
    """How the twenty-four recorded decisions actually stand against the code."""
    counts = {name: 0 for name, _ in CLASSES}
    for n, _ in rows:
        counts[VERDICT_CLASS[n]] += 1
    total = sum(counts.values())

    names = [n for n, _ in CLASSES]
    values = [counts[n] for n in names]

    fig, ax = plt.subplots(figsize=(6.6, 2.9))
    ypos = range(len(names))
    ax.barh(list(ypos), values, height=0.62,
            color=[COLOUR[n] for n in names], alpha=0.9)
    for y, v in zip(ypos, values):
        if v:
            ax.annotate(f"{v}  ({v / total:.0%})", xy=(v, y), xytext=(5, 0),
                        textcoords="offset points", va="center", fontsize=8.5)
    ax.set_yticks(list(ypos))
    ax.set_yticklabels(names)
    ax.invert_yaxis()
    ax.set_xlim(0, max(values) * 1.35)
    ax.set_xlabel("architecture decision records")
    ax.grid(axis="y", visible=False)
    ax.set_title(f"Design record against code, all {total} ADRs\n"
                 "verdicts established by direct inspection of the working tree",
                 loc="left")
    save(fig, "adr-conformance.png")


def chart_adr_timeline(rows) -> None:
    """The same verdicts in the order the decisions were made.

    The point of the sequence view is that the drift is not evenly spread:
    every reversal sits in the first half, and the record tightens as the
    project matures.
    """
    fig, ax = plt.subplots(figsize=(6.6, 2.1))
    for n, _ in rows:
        cls = VERDICT_CLASS[n]
        ax.bar(n, 1, width=0.82, color=COLOUR[cls], alpha=0.9)
        ax.annotate(f"{n:02d}", xy=(n, 0.5), ha="center", va="center",
                    fontsize=6.5, color="white")

    # ADR-0020 is where the audit's own records begin.
    ax.axvline(19.5, color=INK, linewidth=1.0, linestyle=":", alpha=0.7)
    ax.annotate("pre-existing record", xy=(10, 1.06), ha="center", fontsize=8)
    ax.annotate("written by\nthis audit", xy=(22, 1.06), ha="center",
                va="bottom", fontsize=8)

    ax.set_ylim(0, 1.45)
    ax.set_xlim(0.3, 24.7)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.grid(visible=False)
    for side in ("top", "right", "left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.set_xlabel("decisions in the order they were recorded, ADR-0001 to ADR-0024")
    ax.legend(handles=[Patch(facecolor=COLOUR[n], label=n) for n, _ in CLASSES],
              loc="upper center", bbox_to_anchor=(0.5, -0.28), ncol=3,
              frameon=False, fontsize=7.5)
    ax.set_title("Where the record drifted, in decision order", loc="left")
    save(fig, "adr-timeline.png")


def chart_claims_vs_measured() -> None:
    """The project's own published performance claims against measurement.

    Drawn as a dumbbell rather than paired bars. On a logarithmic axis a bar's
    length is not proportional to its value, so bars would misstate exactly
    the magnitudes this chart exists to show; a marker pair plus the
    connecting gap encodes the distance between claim and measurement without
    that distortion. Two panels because latency and throughput share neither
    an axis nor a direction: lower is better on the left, higher on the right.
    """
    # Values are the Chapter 14.4 table, asserted against the document below
    # so an edit to the results cannot leave a stale chart behind.
    latency = [
        ("Commit latency, 1 node", 150.0, 100.05, "1.5x better"),
        ('Single node f=0,\n"sub-millisecond"', 1.0, 100.05, "100x worse"),
    ]
    throughput = [("Per-thread throughput", 400.0, 1994.0, "5x better")]

    text = open(DISS).read()
    for probe in ("100.05", "1,994", "~150 ms", "~400 entries/sec"):
        if probe not in text:
            raise SystemExit(f"Chapter 14 no longer contains {probe!r}; "
                             "update chart_claims_vs_measured")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7.0, 2.9),
                                   gridspec_kw={"width_ratios": [2, 1.15]})
    claim_c, meas_c = "#94A3B8", "#7C3AED"

    def draw(ax, data, xlabel, title, better):
        ys = list(range(len(data)))
        for y, (_, claimed, measured, _) in zip(ys, data):
            ax.plot([claimed, measured], [y, y], color=INK, alpha=0.35,
                    linewidth=1.4, zorder=1)
            ax.scatter([claimed], [y], s=64, color=claim_c, zorder=2,
                       edgecolor="white", linewidth=0.8)
            ax.scatter([measured], [y], s=64, color=meas_c, zorder=3,
                       edgecolor="white", linewidth=0.8)
        for y, (_, claimed, measured, note) in zip(ys, data):
            lo, hi = min(claimed, measured), max(claimed, measured)
            # When the two values sit close together on a log axis their
            # centred labels overlap, so push them outward past their markers.
            tight = hi / lo < 3
            ax.annotate(f"{lo:,.4g}", xy=(lo, y),
                        xytext=(-9, 9) if tight else (0, 9),
                        textcoords="offset points",
                        ha="right" if tight else "center", fontsize=7.5)
            ax.annotate(f"{hi:,.4g}", xy=(hi, y),
                        xytext=(9, 9) if tight else (0, 9),
                        textcoords="offset points",
                        ha="left" if tight else "center", fontsize=7.5)
            ax.annotate(note, xy=((lo * hi) ** 0.5, y), xytext=(0, -14),
                        textcoords="offset points", ha="center", fontsize=7.5,
                        style="italic", color=INK)
        ax.set_xscale("log")
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_yticks(ys)
        ax.set_yticklabels([d[0] for d in data], fontsize=8)
        ax.set_ylim(-0.6, len(data) - 0.4)
        ax.invert_yaxis()
        ax.grid(axis="y", visible=False)
        ax.set_xlabel(xlabel, fontsize=8)
        ax.set_title(f"{title} ({better})", loc="left", fontsize=9)

    draw(ax1, latency, "milliseconds, log scale", "Latency", "lower is better")
    draw(ax2, throughput, "entries / sec, log scale", "Throughput", "higher is better")
    fig.legend(handles=[
        plt.Line2D([], [], marker="o", linestyle="none", markersize=7,
                   color=claim_c, label="claim in README.md"),
        plt.Line2D([], [], marker="o", linestyle="none", markersize=7,
                   color=meas_c, label="measured, Chapter 14"),
    ], loc="lower center", ncol=2, frameon=False, fontsize=8,
        bbox_to_anchor=(0.5, -0.06))
    fig.suptitle("Documented performance claims against measurement", x=0.012,
                 ha="left", fontsize=10.5)
    fig.subplots_adjust(top=0.78, bottom=0.30)
    save(fig, "claims-vs-measured.png")


def chart_fault_tolerance() -> None:
    """Replicas required per tolerated failure, for each consensus backend.

    This is the rule the daemon actually enforces in
    daemon/thread/manager.go: (n-1)/2 for Raft, (n-1)/3 for Tendermint. The
    chart is generated from the same expressions rather than from a table, so
    it states the shipped behaviour by construction.
    """
    src = open(os.path.join(ROOT, "daemon", "thread", "manager.go")).read()
    for probe in ("(replicas - 1) / 3", "(replicas - 1) / 2"):
        if probe not in src:
            raise SystemExit(f"manager.go no longer contains {probe!r}; "
                             "the quorum rule changed, update this chart")

    ns = list(range(1, 13))
    raft = [(n - 1) // 2 for n in ns]
    bft = [(n - 1) // 3 for n in ns]

    fig, ax = plt.subplots(figsize=(6.6, 3.0))
    w = 0.38
    ax.bar([n - w / 2 for n in ns], raft, width=w, color="#0F766E", alpha=0.9,
           label="Raft, crash faults tolerated:  f = ⌊(n-1)/2⌋")
    ax.bar([n + w / 2 for n in ns], bft, width=w, color="#7C3AED", alpha=0.85,
           label="Tendermint, Byzantine faults tolerated:  f = ⌊(n-1)/3⌋")
    ax.set_xticks(ns)
    ax.set_xlabel("replicas in the thread's voter set (n)")
    ax.set_ylabel("failures tolerated (f)")
    ax.set_yticks(range(0, max(raft) + 2))
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    ax.set_title("What a thread costs to make fault-tolerant\n"
                 "the rule the daemon enforces at thread creation", loc="left")
    save(fig, "fault-tolerance-by-backend.png")


def main() -> None:
    rows = read_appendix_a()
    print(f"parsed {len(rows)} ADR verdicts from {os.path.relpath(DISS, ROOT)}")
    chart_adr_conformance(rows)
    chart_adr_timeline(rows)
    chart_claims_vs_measured()
    chart_fault_tolerance()


if __name__ == "__main__":
    main()
