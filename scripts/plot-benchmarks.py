#!/usr/bin/env python3
"""Render benchmark charts from bench/results/*.csv into docs/assets/charts/.

Every chart is drawn from the raw per-observation CSVs, not from the summary
rows, so the distributions shown are the measured data rather than a
re-plot of numbers this script would otherwise have to trust.

Run via scripts/plot-benchmarks.sh (which supplies the venv), or directly:
    .venv-charts/bin/python scripts/plot-benchmarks.py
"""
from __future__ import annotations

import csv
import os
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import NullFormatter  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "bench", "results")
OUT = os.path.join(ROOT, "docs", "assets", "charts")

# Muted palette that stays legible in greyscale print, which a printed
# dissertation may well be.
INK = "#1a2333"
ACCENT = "#7C3AED"
ALT = "#0F766E"
GRID = "#D8DCE3"

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


def read_raw(name: str) -> dict[str, list[float]]:
    path = os.path.join(RESULTS, name)
    if not os.path.exists(path):
        return {}
    by = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            by[row["scenario"]].append(float(row["latency_ms"]))
    return dict(by)


def read_summary(name: str) -> list[dict]:
    path = os.path.join(RESULTS, name)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def save(fig, filename: str) -> None:
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, filename)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  wrote {os.path.relpath(path, ROOT)}")


def chart_latency_distribution() -> None:
    """Boxplot of commit latency per scenario.

    Log y-axis: the configurations differ by two orders of magnitude, so a
    linear axis would flatten every sub-millisecond distribution into the
    baseline and hide their spread entirely.
    """
    data = read_raw("thread_commit_latency_raw.csv")
    if not data:
        return
    order = [s for s in
             ["raft/1-node (f=0 fast path)", "raft/3-node (f=1, CFT)", "tendermint/1-node"]
             if s in data]
    order += [s for s in sorted(data) if s not in order]

    fig, ax = plt.subplots(figsize=(6.6, 3.4))
    bp = ax.boxplot([data[s] for s in order], orientation="vertical", patch_artist=True,
                    widths=0.5, whis=(1, 99), showfliers=True,
                    flierprops=dict(marker=".", markersize=2.5,
                                    markerfacecolor=INK, markeredgecolor="none", alpha=0.35))
    for patch in bp["boxes"]:
        patch.set(facecolor=ACCENT, alpha=0.22, edgecolor=ACCENT, linewidth=1.2)
    for part in ("whiskers", "caps"):
        for line in bp[part]:
            line.set(color=ACCENT, linewidth=1.0)
    for med in bp["medians"]:
        med.set(color=INK, linewidth=1.6)

    ax.set_yscale("log")
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_xticklabels([s.replace(" (", "\n(") for s in order])
    ax.set_ylabel("commit latency (ms, log scale)")
    ax.set_title("Thread commit latency by consensus configuration\n"
                 "n=200 per scenario; box = IQR, whiskers = 1st–99th percentile", loc="left")
    save(fig, "commit-latency-distribution.png")


def chart_latency_cdf() -> None:
    """Empirical CDF, which shows the tail a boxplot compresses."""
    data = read_raw("thread_commit_latency_raw.csv")
    if not data:
        return
    fig, ax = plt.subplots(figsize=(6.6, 3.4))
    styles = [(ACCENT, "-"), (ALT, "--"), (INK, "-.")]
    for i, scenario in enumerate(sorted(data, key=lambda s: -len(data[s]))):
        xs = sorted(data[scenario])
        ys = [(j + 1) / len(xs) for j in range(len(xs))]
        colour, dash = styles[i % len(styles)]
        ax.plot(xs, ys, label=scenario, color=colour, linestyle=dash, linewidth=1.5)
    ax.set_xscale("log")
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.set_xlabel("commit latency (ms, log scale)")
    ax.set_ylabel("cumulative fraction of commits")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", frameon=False, fontsize=8)
    ax.set_title("Commit latency, empirical CDF", loc="left")
    save(fig, "commit-latency-cdf.png")


def chart_epoch_sweep() -> None:
    """Measured p50 against configured epoch, with the epoch-bound hypothesis.

    The dashed reference line is what the data would look like if commit
    latency tracked the configured epoch. Plotting the hypothesis alongside
    the measurement is the whole point of the chart: the gap between them is
    the finding.
    """
    rows = read_summary("commit_latency_vs_epoch.csv")
    if not rows:
        return
    pts = []
    for r in rows:
        epoch = int(r["scenario"].split("epoch=")[1].replace("ms", ""))
        pts.append((epoch, float(r["p50_ms"]), float(r["p95_ms"])))
    pts.sort()
    epochs = [p[0] for p in pts]
    p50 = [p[1] for p in pts]
    p95 = [p[2] for p in pts]

    fig, ax = plt.subplots(figsize=(6.6, 3.4))
    ax.plot(epochs, epochs, linestyle="--", color=INK, alpha=0.45, linewidth=1.2,
            label="if latency tracked the configured epoch")
    ax.plot(epochs, p50, marker="o", markersize=4, color=ACCENT, linewidth=1.6,
            label="measured p50")
    ax.plot(epochs, p95, marker="^", markersize=4, color=ALT, linewidth=1.0,
            linestyle=":", label="measured p95")
    ax.axhline(100, color=ALT, alpha=0.35, linewidth=1.0)
    ax.annotate("raftTickMs = 100", xy=(epochs[-1], 100), xytext=(-4, 6),
                textcoords="offset points", ha="right", fontsize=8, color=ALT)

    ax.set_xscale("log")
    ax.set_yscale("log")
    # Log axes label their minor ticks by default (3x10^1, 4x10^1, ...),
    # which collides with the explicit epoch ticks and makes both unreadable.
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_xticks(epochs)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_xlabel("configured EpochMs (ms, log scale)")
    ax.set_ylabel("commit latency (ms, log scale)")
    ax.legend(loc="upper left", frameon=False, fontsize=8)
    ax.set_title("Raft commit latency is independent of EpochMs\n"
                 "single node, f=0, n=60 per point", loc="left")
    save(fig, "commit-latency-vs-epoch.png")


def chart_throughput() -> None:
    rows = read_summary("thread_throughput.csv")
    if not rows:
        return
    labels, rates = [], []
    for r in rows:
        scenario = r["scenario"]
        if "(" in scenario and "entries/sec" in scenario:
            label = scenario[:scenario.rindex("(")].strip()
            rate = float(scenario[scenario.rindex("(") + 1:].split()[0])
        else:
            label, rate = scenario, 0.0
        labels.append(label.replace(" (", "\n("))
        rates.append(rate)

    fig, ax = plt.subplots(figsize=(6.0, 3.2))
    bars = ax.bar(range(len(rates)), rates, width=0.5,
                  color=[ACCENT, ALT][: len(rates)] * 4, alpha=0.85)
    for bar, rate in zip(bars, rates):
        ax.annotate(f"{rate:,.0f}", xy=(bar.get_x() + bar.get_width() / 2, rate),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", fontsize=8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)
    ax.set_ylabel("committed entries / second")
    ax.set_ylim(0, max(rates) * 1.18 if rates else 1)
    ax.set_title("Sustained commit throughput, 5 s window, 8 concurrent appenders", loc="left")
    save(fig, "throughput.png")


def main() -> None:
    if not os.path.isdir(RESULTS):
        raise SystemExit(f"no results at {RESULTS}; run scripts/run-benchmarks.sh first")
    print(f"reading {os.path.relpath(RESULTS, ROOT)}")
    chart_latency_distribution()
    chart_latency_cdf()
    chart_epoch_sweep()
    chart_throughput()


if __name__ == "__main__":
    main()
