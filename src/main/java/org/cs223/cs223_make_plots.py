#!/usr/bin/env python3
"""
Compare CS 223 experiment results collected on two different machines.

This script expects two aggregate summary CSV files and two ZIP bundles
containing per-transaction response-time logs.

Default project layout (relative to this script):
    ../../../../../results/
        summary_mac.csv
        summary_windows.csv
        results_mac.zip
        results_windows.zip

It generates:
  - OCC vs Conservative 2PL plots for each workload
  - machine-vs-machine overlays on the same graphs
  - log-scale plots for throughput / latency / retries where appropriate
  - response-time distribution CDFs, boxplots, violin plots from both ZIP bundles
  - Pareto-style throughput-vs-latency comparison plots
  - speedup / efficiency plots
  - CSV and LaTeX tables
  - a markdown summary report

Visual encoding:
  - Color = machine
  - Marker shape + line style = protocol
  - Filled marker = Mac, hollow marker = Windows
  - Bubble size = retry rate on Pareto plots
"""
from __future__ import annotations

import argparse
import io
import math
import re
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

PROTOCOL_LABELS = {
    "OCC": "OCC",
    "TWO_PL": "Conservative 2PL",
    "2PL": "Conservative 2PL",
    "CONSERVATIVE_2PL": "Conservative 2PL",
    "Conservative 2PL": "Conservative 2PL",
}
PROTOCOL_ORDER = ["OCC", "Conservative 2PL"]
MACHINE_ORDER = ["mac", "windows"]

MACHINE_STYLE = {
    "mac": {
        "label": "Mac M3 Pro (12-core, 36 GB)",
        "color": "#0072B2",
        "filled": True,
    },
    "windows": {
        "label": "Windows i9-11900H (8-core/16-thread, 16 GB)",
        "color": "#D55E00",
        "filled": False,
    },
}
PROTOCOL_STYLE = {
    "OCC": {"marker": "^", "linestyle": "-"},
    "Conservative 2PL": {"marker": "X", "linestyle": "--"},
}

RT_FILE_RE = re.compile(
    r"rt_w(?P<workload>\d+)_(?P<protocol>.+?)_t(?P<threads>\d+)_c(?P<contention>\d+(?:\.\d+)?)_h(?P<hotset>\d+)\.csv$"
)


def default_results_dir() -> Path:
    here = Path(__file__).resolve().parent
    parents = list(here.parents)
    if len(parents) >= 5:
        return parents[4] / "results"
    return here / "results"


def parse_args() -> argparse.Namespace:
    results_dir = default_results_dir()

    p = argparse.ArgumentParser(description="Compare CS 223 experiments across two machines.")
    p.add_argument("--summary-mac", default=str(results_dir / "summary_mac.csv"), help="Path to Mac summary CSV")
    p.add_argument("--summary-windows", default=str(results_dir / "summary_windows.csv"), help="Path to Windows summary CSV")
    p.add_argument("--zip-mac", default=str(results_dir / "results_mac.zip"), help="Path to Mac per-transaction ZIP")
    p.add_argument("--zip-windows", default=str(results_dir / "results_windows.zip"), help="Path to Windows per-transaction ZIP")
    p.add_argument("--out", default=str(results_dir / "analysis_compare"), help="Output directory")

    p.add_argument("--plot-all-slices", action="store_true", default=True,
                   help="Generate all hotset slices instead of just representative defaults (default: on)")
    p.add_argument("--representative-slices", action="store_true",
                   help="Use only representative slices for required line plots")

    p.add_argument("--dist-threads", type=int, default=None, help="Threads setting for response-time distribution plots")
    p.add_argument("--dist-contention", type=float, default=None, help="Contention setting for response-time distribution plots")
    p.add_argument("--dist-hotset", type=int, default=None, help="Hotset setting for response-time distribution plots")

    p.add_argument("--mac-label", default=MACHINE_STYLE["mac"]["label"], help="Legend label for Mac machine")
    p.add_argument("--windows-label", default=MACHINE_STYLE["windows"]["label"], help="Legend label for Windows machine")
    return p.parse_args()


def setup_style() -> None:
    plt.style.use("default")
    plt.rcParams.update({
        "figure.figsize": (10.8, 6.4),
        "axes.grid": True,
        "grid.alpha": 0.22,
        "grid.linestyle": ":",
        "axes.titlesize": 17,
        "axes.titleweight": "semibold",
        "axes.labelsize": 13.5,
        "legend.fontsize": 10.8,
        "legend.title_fontsize": 11.2,
        "xtick.labelsize": 11.5,
        "ytick.labelsize": 11.5,
        "lines.linewidth": 2.8,
        "axes.linewidth": 1.0,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "savefig.facecolor": "white",
        "figure.facecolor": "white",
        "font.family": "DejaVu Sans",
    })


def ensure_dirs(base: Path) -> Dict[str, Path]:
    dirs = {
        "base": base,
        "plots": base / "plots",
        "plots_req": base / "plots" / "required",
        "plots_extra": base / "plots" / "extra",
        "plots_dist": base / "plots" / "distributions",
        "plots_rich": base / "plots" / "paper_grade",
        "tables": base / "tables",
        "latex": base / "tables" / "latex",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def canonicalize_machine_paths(args: argparse.Namespace) -> None:
    windows_zip = Path(args.zip_windows)
    if not windows_zip.exists():
        typo = windows_zip.with_name("results_windoes.zip")
        if typo.exists():
            args.zip_windows = str(typo)


def clean_summary(df: pd.DataFrame, machine: str) -> pd.DataFrame:
    df = df.copy()
    required = [
        "workload", "protocol", "threads", "contention", "hotset",
        "transactions", "committed", "retries", "retry_rate",
        "throughput", "avg_response_time",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Summary for {machine} is missing columns: {missing}")

    df["protocol_raw"] = df["protocol"].astype(str)
    df["protocol"] = df["protocol_raw"].map(lambda x: PROTOCOL_LABELS.get(x, x))
    df["machine"] = machine
    df["machine_label"] = MACHINE_STYLE[machine]["label"]

    df["workload"] = df["workload"].astype(int)
    df["threads"] = df["threads"].astype(int)
    df["hotset"] = df["hotset"].astype(int)
    df["contention"] = df["contention"].astype(float)

    num_cols = ["transactions", "committed", "retries", "retry_rate", "throughput", "avg_response_time"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if df["retry_rate"].max() <= 1.0 + 1e-9:
        df["retry_rate_pct"] = df["retry_rate"] * 100.0
    else:
        df["retry_rate_pct"] = df["retry_rate"]

    df["throughput_per_thread"] = df["throughput"] / df["threads"].replace(0, np.nan)
    df["protocol"] = pd.Categorical(df["protocol"], categories=PROTOCOL_ORDER, ordered=True)
    return df.sort_values(["workload", "machine", "protocol", "threads", "contention", "hotset"]).reset_index(drop=True)


def save_plot(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=240, bbox_inches="tight")
    plt.close(fig)


def pick_middle(values: Iterable) -> Optional[float]:
    vals = sorted(set(values))
    if not vals:
        return None
    return vals[len(vals) // 2]


def set_log_scale(ax: plt.Axes, values: pd.Series, axis: str = "y", prefer_log: bool = True) -> None:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    if vals.empty or not prefer_log:
        return
    if axis == "y":
        if (vals > 0).all():
            ax.set_yscale("log")
        else:
            ax.set_yscale("symlog", linthresh=1.0)
    else:
        if (vals > 0).all():
            ax.set_xscale("log")
        else:
            ax.set_xscale("symlog", linthresh=1.0)


def series_style(machine: str, protocol: str) -> Dict[str, object]:
    color = MACHINE_STYLE[machine]["color"]
    filled = MACHINE_STYLE[machine]["filled"]
    return {
        "color": color,
        "marker": PROTOCOL_STYLE[protocol]["marker"],
        "linestyle": PROTOCOL_STYLE[protocol]["linestyle"],
        "linewidth": 2.8,
        "markersize": 11,
        "markeredgewidth": 2.1,
        "markeredgecolor": color,
        "markerfacecolor": color if filled else "white",
        "alpha": 0.98,
    }


def _machine_handles(mac_label: str, windows_label: str):
    return [
        Line2D([0], [0], color=MACHINE_STYLE["mac"]["color"], marker="o", markersize=11,
               markerfacecolor=MACHINE_STYLE["mac"]["color"], markeredgecolor=MACHINE_STYLE["mac"]["color"],
               linestyle="-", linewidth=2.8, label=mac_label),
        Line2D([0], [0], color=MACHINE_STYLE["windows"]["color"], marker="o", markersize=11,
               markerfacecolor="white", markeredgecolor=MACHINE_STYLE["windows"]["color"],
               linestyle="-", linewidth=2.8, label=windows_label),
    ]


def _protocol_handles():
    return [
        Line2D([0], [0], color="#333333", marker=PROTOCOL_STYLE[p]["marker"], linestyle=PROTOCOL_STYLE[p]["linestyle"],
               markerfacecolor="#333333", markeredgecolor="#333333", linewidth=2.8, markersize=11, label=p)
        for p in PROTOCOL_ORDER
    ]


def add_rich_legend(ax: plt.Axes, mac_label: str, windows_label: str) -> None:
    machine_handles = _machine_handles(mac_label, windows_label)
    protocol_handles = _protocol_handles()
    legend1 = ax.legend(handles=machine_handles, title="Machine", loc="upper left", frameon=True,
                        fancybox=True, framealpha=0.95, borderpad=0.8)
    ax.add_artist(legend1)
    ax.legend(handles=protocol_handles, title="Protocol", loc="upper right", frameon=True,
              fancybox=True, framealpha=0.95, borderpad=0.8)


def add_panel_legends(fig: plt.Figure, mac_label: str, windows_label: str, size_values: Optional[List[int]] = None) -> None:
    machine_legend = fig.legend(
        handles=_machine_handles(mac_label, windows_label),
        title="Machine",
        loc="upper left",
        bbox_to_anchor=(0.015, 1.01),
        ncol=1,
        frameon=True,
        fancybox=True,
        framealpha=0.96,
    )
    protocol_legend = fig.legend(
        handles=_protocol_handles(),
        title="Protocol",
        loc="upper right",
        bbox_to_anchor=(0.985, 1.01),
        ncol=1,
        frameon=True,
        fancybox=True,
        framealpha=0.96,
    )
    if size_values:
        size_handles = [
            plt.scatter([], [], s=40 + 18 * np.sqrt(v), color="#666666", alpha=0.55, edgecolors="#333333", label=f"{v} threads")
            for v in sorted(set(size_values))
        ]
        size_legend = fig.legend(
            handles=size_handles,
            title="Bubble size",
            loc="lower center",
            bbox_to_anchor=(0.5, -0.005),
            ncol=min(4, len(size_handles)),
            frameon=True,
            fancybox=True,
            framealpha=0.96,
        )
        fig.add_artist(size_legend)
    fig.add_artist(machine_legend)
    fig.add_artist(protocol_legend)


def plot_machine_protocol_lines(
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str,
        xlabel: str,
        ylabel: str,
        path: Path,
        mac_label: str,
        windows_label: str,
        log_y: bool = True,
) -> None:
    fig, ax = plt.subplots(figsize=(10.8, 6.6))
    for machine in MACHINE_ORDER:
        mdf = df[df["machine"] == machine]
        if mdf.empty:
            continue
        for protocol in PROTOCOL_ORDER:
            sub = mdf[mdf["protocol"] == protocol].sort_values(x)
            if sub.empty:
                continue
            ax.plot(sub[x], sub[y], **series_style(machine, protocol))
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if log_y:
        set_log_scale(ax, df[y], axis="y", prefer_log=True)
    add_rich_legend(ax, mac_label, windows_label)
    save_plot(fig, path)


def plot_required_slices(df: pd.DataFrame, dirs: Dict[str, Path], plot_all_slices: bool, mac_label: str, windows_label: str) -> List[str]:
    notes: List[str] = []

    workloads = sorted(df["workload"].unique())
    all_threads = sorted(df["threads"].unique())
    all_contentions = sorted(df["contention"].unique())
    all_hotsets = sorted(df["hotset"].unique())

    rep_threads = int(pick_middle(all_threads))
    rep_contention = float(pick_middle(all_contentions))
    rep_hotset = int(pick_middle(all_hotsets))

    hotsets_for_threads = all_hotsets if plot_all_slices else [rep_hotset]
    hotsets_for_contention = all_hotsets if plot_all_slices else [rep_hotset]

    for workload in workloads:
        wdf = df[df["workload"] == workload]

        for hotset in hotsets_for_threads:
            sub = wdf[(wdf["contention"] == rep_contention) & (wdf["hotset"] == hotset)]
            if sub.empty:
                notes.append(f"Skipped workload {workload} thread-slice plots for contention={rep_contention}, hotset={hotset}: no rows.")
                continue
            plot_machine_protocol_lines(
                sub, "threads", "throughput",
                f"Workload {workload}: Throughput vs Threads\ncontention={rep_contention}, hotset={hotset}",
                "Threads", "Throughput (committed txns/sec)",
                dirs["plots_req"] / f"w{workload}_throughput_vs_threads_c{rep_contention:.2f}_h{hotset}.png",
                mac_label, windows_label, log_y=True,
                )
            plot_machine_protocol_lines(
                sub, "threads", "avg_response_time",
                f"Workload {workload}: Avg Response Time vs Threads\ncontention={rep_contention}, hotset={hotset}",
                "Threads", "Average response time (ms)",
                dirs["plots_req"] / f"w{workload}_latency_vs_threads_c{rep_contention:.2f}_h{hotset}.png",
                mac_label, windows_label, log_y=True,
                )
            plot_machine_protocol_lines(
                sub, "threads", "throughput_per_thread",
                f"Workload {workload}: Throughput per Thread vs Threads\ncontention={rep_contention}, hotset={hotset}",
                "Threads", "Throughput per thread",
                dirs["plots_extra"] / f"w{workload}_throughput_per_thread_vs_threads_c{rep_contention:.2f}_h{hotset}.png",
                mac_label, windows_label, log_y=True,
                )

        for hotset in hotsets_for_contention:
            sub = wdf[(wdf["threads"] == rep_threads) & (wdf["hotset"] == hotset)]
            if sub.empty:
                notes.append(f"Skipped workload {workload} contention-slice plots for threads={rep_threads}, hotset={hotset}: no rows.")
                continue
            plot_machine_protocol_lines(
                sub, "contention", "throughput",
                f"Workload {workload}: Throughput vs Contention\nthreads={rep_threads}, hotset={hotset}",
                "Contention probability p", "Throughput (committed txns/sec)",
                dirs["plots_req"] / f"w{workload}_throughput_vs_contention_t{rep_threads}_h{hotset}.png",
                mac_label, windows_label, log_y=True,
                )
            plot_machine_protocol_lines(
                sub, "contention", "avg_response_time",
                f"Workload {workload}: Avg Response Time vs Contention\nthreads={rep_threads}, hotset={hotset}",
                "Contention probability p", "Average response time (ms)",
                dirs["plots_req"] / f"w{workload}_latency_vs_contention_t{rep_threads}_h{hotset}.png",
                mac_label, windows_label, log_y=True,
                )
            plot_machine_protocol_lines(
                sub, "contention", "retries",
                f"Workload {workload}: Retries vs Contention\nthreads={rep_threads}, hotset={hotset}",
                "Contention probability p", "Retries (count)",
                dirs["plots_req"] / f"w{workload}_retries_vs_contention_t{rep_threads}_h{hotset}.png",
                mac_label, windows_label, log_y=True,
                )
            plot_machine_protocol_lines(
                sub, "contention", "retry_rate_pct",
                f"Workload {workload}: Retry Rate vs Contention\nthreads={rep_threads}, hotset={hotset}",
                "Contention probability p", "Retry rate (%)",
                dirs["plots_req"] / f"w{workload}_retryrate_vs_contention_t{rep_threads}_h{hotset}.png",
                mac_label, windows_label, log_y=False,
                )

        sub_hot = wdf[(wdf["threads"] == rep_threads) & (wdf["contention"] == rep_contention)]
        if not sub_hot.empty:
            plot_machine_protocol_lines(
                sub_hot, "hotset", "throughput",
                f"Workload {workload}: Throughput vs Hotset\nthreads={rep_threads}, contention={rep_contention}",
                "Hotset size", "Throughput (committed txns/sec)",
                dirs["plots_extra"] / f"w{workload}_throughput_vs_hotset_t{rep_threads}_c{rep_contention:.2f}.png",
                mac_label, windows_label, log_y=True,
                )
            plot_machine_protocol_lines(
                sub_hot, "hotset", "avg_response_time",
                f"Workload {workload}: Avg Response Time vs Hotset\nthreads={rep_threads}, contention={rep_contention}",
                "Hotset size", "Average response time (ms)",
                dirs["plots_extra"] / f"w{workload}_latency_vs_hotset_t{rep_threads}_c{rep_contention:.2f}.png",
                mac_label, windows_label, log_y=True,
                )

    notes.append(f"Representative defaults for fixed-parameter plots: threads={rep_threads}, contention={rep_contention}, hotset={rep_hotset}.")
    notes.append("Color encodes machine, marker shape and line style encode protocol, and hollow markers distinguish Windows from Mac.")
    return notes


def pivot_heatmap(df: pd.DataFrame, index: str, columns: str, values: str) -> pd.DataFrame:
    return df.pivot_table(index=index, columns=columns, values=values, aggfunc="mean")


def plot_heatmaps(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df["workload"].unique()):
        for machine in MACHINE_ORDER:
            for protocol in PROTOCOL_ORDER:
                sub = df[(df["workload"] == workload) & (df["machine"] == machine) & (df["protocol"] == protocol)]
                if sub.empty:
                    continue
                for hotset in sorted(sub["hotset"].unique()):
                    hdf = sub[sub["hotset"] == hotset]
                    thr = pivot_heatmap(hdf, "threads", "contention", "throughput")
                    lat = pivot_heatmap(hdf, "threads", "contention", "avg_response_time")
                    retry = pivot_heatmap(hdf, "threads", "contention", "retry_rate_pct")
                    for metric_name, mat, cmap, label in [
                        ("throughput", thr, "viridis", "Throughput"),
                        ("latency", lat, "magma_r", "Avg response time (ms)"),
                        ("retry_rate", retry, "plasma", "Retry rate (%)"),
                    ]:
                        if mat.empty:
                            continue
                        fig, ax = plt.subplots(figsize=(8.6, 6.6))
                        im = ax.imshow(mat.values, aspect="auto", cmap=cmap, origin="lower")
                        ax.set_xticks(range(len(mat.columns)))
                        ax.set_xticklabels([f"{c:.2f}" for c in mat.columns])
                        ax.set_yticks(range(len(mat.index)))
                        ax.set_yticklabels([str(i) for i in mat.index])
                        ax.set_xlabel("Contention probability p")
                        ax.set_ylabel("Threads")
                        ax.set_title(
                            f"Workload {workload}: {metric_name.replace('_', ' ').title()} heatmap\n"
                            f"{MACHINE_STYLE[machine]['label']}, {protocol}, hotset={hotset}"
                        )
                        for i in range(mat.shape[0]):
                            for j in range(mat.shape[1]):
                                val = mat.iloc[i, j]
                                if pd.notna(val):
                                    txt = f"{val:.1f}" if abs(val) < 1000 else f"{val:.0f}"
                                    ax.text(j, i, txt, ha="center", va="center", color="white" if metric_name != "latency" else "black", fontsize=9)
                        cbar = fig.colorbar(im, ax=ax)
                        cbar.set_label(label)
                        save_plot(fig, dirs["plots_extra"] / f"w{workload}_{machine}_{protocol.replace(' ', '_')}_{metric_name}_heatmap_h{hotset}.png")


def plot_pareto_panels(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    for workload in sorted(df["workload"].unique()):
        wdf = df[df["workload"] == workload].copy()
        if wdf.empty:
            continue
        hotsets = sorted(wdf["hotset"].unique())
        n = len(hotsets)
        cols = min(3, n)
        rows = math.ceil(n / cols)
        fig, axes = plt.subplots(rows, cols, figsize=(6.2 * cols, 5.4 * rows), squeeze=False)
        axes = axes.flatten()
        for ax, hotset in zip(axes, hotsets):
            hdf = wdf[wdf["hotset"] == hotset]
            sizes = 30 + 6 * np.sqrt(hdf["retry_rate_pct"].fillna(0).clip(lower=0) + 1)
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    sub = hdf[(hdf["machine"] == machine) & (hdf["protocol"] == protocol)]
                    if sub.empty:
                        continue
                    style = series_style(machine, protocol)
                    ax.scatter(
                        sub["avg_response_time"],
                        sub["throughput"],
                        s=(30 + 6 * np.sqrt(sub["retry_rate_pct"].fillna(0).clip(lower=0) + 1)).to_numpy(),
                        c=style["color"],
                        marker=style["marker"],
                        facecolors=style["markerfacecolor"],
                        edgecolors=style["markeredgecolor"],
                        linewidths=1.6,
                        alpha=0.9,
                    )
                    top = sub.nlargest(2, "throughput")
                    for _, row in top.iterrows():
                        ax.annotate(f"t{int(row['threads'])}/c{row['contention']:.2f}", (row["avg_response_time"], row["throughput"]),
                                    textcoords="offset points", xytext=(5, 4), fontsize=8)
            ax.set_title(f"Hotset = {hotset}")
            ax.set_xlabel("Avg response time (ms)")
            ax.set_ylabel("Throughput")
            set_log_scale(ax, hdf["avg_response_time"], axis="x", prefer_log=True)
            set_log_scale(ax, hdf["throughput"], axis="y", prefer_log=True)
        for ax in axes[len(hotsets):]:
            ax.axis("off")
        axes[0].set_title(f"Workload {workload}: Throughput vs Latency (size = retry rate)\nHotset = {hotsets[0]}")
        add_rich_legend(axes[0], mac_label, windows_label)
        save_plot(fig, dirs["plots_rich"] / f"w{workload}_pareto_panels.png")


def add_baseline_speedup(df: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for (workload, machine, protocol, contention, hotset), sub in df.groupby(["workload", "machine", "protocol", "contention", "hotset"], observed=False):
        sub = sub.sort_values("threads").copy()
        baseline_rows = sub[sub["threads"] == sub["threads"].min()]
        if baseline_rows.empty:
            continue
        baseline = baseline_rows["throughput"].iloc[0]
        if pd.isna(baseline) or baseline == 0:
            continue
        sub["speedup"] = sub["throughput"] / baseline
        sub["efficiency"] = sub["speedup"] / sub["threads"]
        parts.append(sub)
    if parts:
        return pd.concat(parts, ignore_index=True)
    return df.assign(speedup=np.nan, efficiency=np.nan)


def plot_speedup_efficiency(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    sdf = add_baseline_speedup(df)
    for workload in sorted(sdf["workload"].unique()):
        wdf = sdf[sdf["workload"] == workload]
        rep_contention = float(pick_middle(wdf["contention"].unique()))
        rep_hotset = int(pick_middle(wdf["hotset"].unique()))
        sub = wdf[(wdf["contention"] == rep_contention) & (wdf["hotset"] == rep_hotset)]
        if sub.empty:
            continue
        for metric, ylabel in [("speedup", "Speedup vs smallest thread count"), ("efficiency", "Parallel efficiency")]:
            fig, ax = plt.subplots(figsize=(10.6, 6.4))
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    psub = sub[(sub["machine"] == machine) & (sub["protocol"] == protocol)].sort_values("threads")
                    if psub.empty:
                        continue
                    ax.plot(psub["threads"], psub[metric], **series_style(machine, protocol))
            if metric == "speedup":
                xvals = sorted(sub["threads"].unique())
                if xvals:
                    ax.plot(xvals, np.array(xvals) / min(xvals), color="gray", linestyle=":", linewidth=2, label="Ideal linear speedup")
            ax.set_title(f"Workload {workload}: {ylabel}\ncontention={rep_contention}, hotset={rep_hotset}")
            ax.set_xlabel("Threads")
            ax.set_ylabel(ylabel)
            if metric == "efficiency":
                ax.set_ylim(bottom=0)
            else:
                set_log_scale(ax, sub[metric], axis="y", prefer_log=True)
            add_rich_legend(ax, mac_label, windows_label)
            save_plot(fig, dirs["plots_rich"] / f"w{workload}_{metric}_c{rep_contention:.2f}_h{rep_hotset}.png")


def plot_protocol_machine_bars(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    summary = (
        df.groupby(["workload", "machine", "protocol"], observed=False)
        .agg(mean_throughput=("throughput", "mean"), mean_latency=("avg_response_time", "mean"), mean_retry=("retry_rate_pct", "mean"))
        .reset_index()
    )
    for workload in sorted(summary["workload"].unique()):
        wdf = summary[summary["workload"] == workload]
        fig, axes = plt.subplots(1, 3, figsize=(17.5, 5.3))
        metrics = [
            ("mean_throughput", "Mean throughput", True),
            ("mean_latency", "Mean avg response time (ms)", True),
            ("mean_retry", "Mean retry rate (%)", False),
        ]
        for ax, (col, title, logy) in zip(axes, metrics):
            labels = []
            vals = []
            colors = []
            hatches = []
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    sub = wdf[(wdf["machine"] == machine) & (wdf["protocol"] == protocol)]
                    if sub.empty:
                        continue
                    labels.append(f"{machine}\n{protocol.replace('Conservative ', '')}")
                    vals.append(float(sub[col].iloc[0]))
                    colors.append(MACHINE_STYLE[machine]["color"])
                    hatches.append("" if protocol == "OCC" else "//")
            bars = ax.bar(range(len(vals)), vals, color=colors, edgecolor="black", linewidth=1.0)
            for bar, hatch in zip(bars, hatches):
                bar.set_hatch(hatch)
                bar.set_alpha(0.75)
            ax.set_xticks(range(len(vals)))
            ax.set_xticklabels(labels)
            ax.set_title(title)
            if logy:
                set_log_scale(ax, pd.Series(vals), axis="y", prefer_log=True)
        axes[0].set_ylabel("Value")
        legend_handles = [
            Patch(facecolor=MACHINE_STYLE["mac"]["color"], edgecolor="black", label="Mac"),
            Patch(facecolor=MACHINE_STYLE["windows"]["color"], edgecolor="black", label="Windows"),
            Patch(facecolor="white", edgecolor="black", hatch="", label="OCC"),
            Patch(facecolor="white", edgecolor="black", hatch="//", label="Conservative 2PL"),
        ]
        axes[0].legend(handles=legend_handles, loc="best", frameon=True)
        fig.suptitle(f"Workload {workload}: mean metric summary across all configurations", y=1.03, fontsize=16, fontweight="semibold")
        save_plot(fig, dirs["plots_rich"] / f"w{workload}_mean_metric_bars.png")




def plot_threads_hotset_facets(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    metrics = [
        ("throughput", "Throughput (committed txns/sec)", True, "throughput_facet_grid"),
        ("avg_response_time", "Average response time (ms)", True, "latency_facet_grid"),
        ("retry_rate_pct", "Retry rate (%)", False, "retryrate_facet_grid"),
    ]
    for workload in sorted(df["workload"].unique()):
        wdf = df[df["workload"] == workload]
        contentions = sorted(wdf["contention"].unique())
        hotsets = sorted(wdf["hotset"].unique())
        rows = len(contentions)
        cols = len(hotsets)
        for metric, ylabel, log_y, stem in metrics:
            fig, axes = plt.subplots(rows, cols, figsize=(4.7 * cols, 3.9 * rows), sharex=True, squeeze=False)
            for i, contention in enumerate(contentions):
                for j, hotset in enumerate(hotsets):
                    ax = axes[i, j]
                    sdf = wdf[(wdf["contention"] == contention) & (wdf["hotset"] == hotset)]
                    for machine in MACHINE_ORDER:
                        for protocol in PROTOCOL_ORDER:
                            sub = sdf[(sdf["machine"] == machine) & (sdf["protocol"] == protocol)].sort_values("threads")
                            if sub.empty:
                                continue
                            ax.plot(sub["threads"], sub[metric], **series_style(machine, protocol))
                    if log_y:
                        set_log_scale(ax, sdf[metric], axis="y", prefer_log=True)
                    if i == 0:
                        ax.set_title(f"hotset={hotset}")
                    if j == 0:
                        ax.set_ylabel(f"p={contention:.2f}\n{ylabel}")
                    else:
                        ax.set_ylabel("")
                    if i == rows - 1:
                        ax.set_xlabel("Threads")
                    ax.grid(True, alpha=0.2)
            fig.suptitle(f"Workload {workload}: {ylabel} across contention × hotset slices", y=1.03, fontsize=17, fontweight="semibold")
            add_panel_legends(fig, mac_label, windows_label)
            fig.tight_layout(rect=[0.03, 0.04, 0.97, 0.93])
            fig.savefig(dirs["plots_rich"] / f"w{workload}_{stem}.png", dpi=260, bbox_inches="tight")
            plt.close(fig)


def _ratio_heatmap(ax: plt.Axes, mat: pd.DataFrame, title: str, cbar_label: str, cmap: str = "coolwarm") -> None:
    if mat.empty:
        ax.axis("off")
        return
    vals = mat.to_numpy(dtype=float)
    safe = np.where(np.isfinite(vals) & (vals > 0), vals, np.nan)
    log2vals = np.log2(safe)
    finite = log2vals[np.isfinite(log2vals)]
    vmax = max(0.25, np.nanmax(np.abs(finite))) if finite.size else 1.0
    im = ax.imshow(log2vals, aspect="auto", cmap=cmap, origin="lower", vmin=-vmax, vmax=vmax)
    ax.set_xticks(range(len(mat.columns)))
    ax.set_xticklabels([f"{c:.2f}" for c in mat.columns])
    ax.set_yticks(range(len(mat.index)))
    ax.set_yticklabels([str(i) for i in mat.index])
    ax.set_xlabel("Contention probability p")
    ax.set_ylabel("Threads")
    ax.set_title(title)
    for r in range(mat.shape[0]):
        for c in range(mat.shape[1]):
            v = mat.iloc[r, c]
            if pd.notna(v) and np.isfinite(v) and v > 0:
                ax.text(c, r, f"{v:.2f}×", ha="center", va="center", fontsize=8.5, color="black")
    cb = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cb.set_label(cbar_label)


def plot_ratio_heatmaps(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df["workload"].unique()):
        wdf = df[df["workload"] == workload]
        for hotset in sorted(wdf["hotset"].unique()):
            hdf = wdf[wdf["hotset"] == hotset]
            fig, axes = plt.subplots(2, 2, figsize=(14.5, 9.2))
            for j, protocol in enumerate(PROTOCOL_ORDER):
                sub = hdf[hdf["protocol"] == protocol]
                thr = sub.pivot_table(index="threads", columns="contention", values="throughput", aggfunc="mean")
                lat = sub.pivot_table(index="threads", columns="contention", values="avg_response_time", aggfunc="mean")
                thr_ratio = (sub[sub["machine"] == "mac"].pivot_table(index="threads", columns="contention", values="throughput", aggfunc="mean") /
                             sub[sub["machine"] == "windows"].pivot_table(index="threads", columns="contention", values="throughput", aggfunc="mean"))
                lat_gain = (sub[sub["machine"] == "windows"].pivot_table(index="threads", columns="contention", values="avg_response_time", aggfunc="mean") /
                            sub[sub["machine"] == "mac"].pivot_table(index="threads", columns="contention", values="avg_response_time", aggfunc="mean"))
                _ratio_heatmap(axes[0, j], thr_ratio, f"{protocol}: throughput ratio (Mac / Windows)", "log2 ratio")
                _ratio_heatmap(axes[1, j], lat_gain, f"{protocol}: latency advantage (Windows / Mac)", "log2 ratio")
            fig.suptitle(f"Workload {workload}: machine advantage maps by protocol (hotset={hotset})", y=1.02, fontsize=17, fontweight="semibold")
            fig.tight_layout(rect=[0.02, 0.02, 0.98, 0.96])
            fig.savefig(dirs["plots_rich"] / f"w{workload}_machine_ratio_heatmaps_h{hotset}.png", dpi=260, bbox_inches="tight")
            plt.close(fig)

            fig, axes = plt.subplots(2, 2, figsize=(14.5, 9.2))
            for j, machine in enumerate(MACHINE_ORDER):
                sub = hdf[hdf["machine"] == machine]
                occ_thr = sub[sub["protocol"] == "OCC"].pivot_table(index="threads", columns="contention", values="throughput", aggfunc="mean")
                twopl_thr = sub[sub["protocol"] == "Conservative 2PL"].pivot_table(index="threads", columns="contention", values="throughput", aggfunc="mean")
                occ_lat = sub[sub["protocol"] == "OCC"].pivot_table(index="threads", columns="contention", values="avg_response_time", aggfunc="mean")
                twopl_lat = sub[sub["protocol"] == "Conservative 2PL"].pivot_table(index="threads", columns="contention", values="avg_response_time", aggfunc="mean")
                thr_ratio = occ_thr / twopl_thr
                lat_gain = twopl_lat / occ_lat
                _ratio_heatmap(axes[0, j], thr_ratio, f"{MACHINE_STYLE[machine]['label']}: throughput ratio (OCC / 2PL)", "log2 ratio")
                _ratio_heatmap(axes[1, j], lat_gain, f"{MACHINE_STYLE[machine]['label']}: latency advantage (2PL / OCC)", "log2 ratio")
            fig.suptitle(f"Workload {workload}: protocol advantage maps by machine (hotset={hotset})", y=1.02, fontsize=17, fontweight="semibold")
            fig.tight_layout(rect=[0.02, 0.02, 0.98, 0.96])
            fig.savefig(dirs["plots_rich"] / f"w{workload}_protocol_ratio_heatmaps_h{hotset}.png", dpi=260, bbox_inches="tight")
            plt.close(fig)


def plot_bubble_grids(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    metrics = [
        ("throughput", "Throughput (bubble size ∝ throughput)", True, "throughput_bubble_grid"),
        ("avg_response_time", "Avg response time (bubble size ∝ latency)", True, "latency_bubble_grid"),
    ]
    for workload in sorted(df["workload"].unique()):
        wdf = df[df["workload"] == workload]
        hotsets = sorted(wdf["hotset"].unique())
        for metric, title_label, log_scale, stem in metrics:
            cols = min(3, len(hotsets))
            rows = math.ceil(len(hotsets) / cols)
            fig, axes = plt.subplots(rows, cols, figsize=(5.2 * cols, 4.3 * rows), squeeze=False)
            axes = axes.flatten()
            for ax, hotset in zip(axes, hotsets):
                hdf = wdf[wdf["hotset"] == hotset].copy()
                metric_vals = hdf[metric].astype(float).clip(lower=0)
                if metric == "throughput":
                    size_base = 40 + 14 * np.sqrt(metric_vals / max(metric_vals.max(), 1)) * 18
                else:
                    size_base = 40 + 14 * np.sqrt(metric_vals / max(metric_vals.max(), 1)) * 18
                for machine in MACHINE_ORDER:
                    for protocol in PROTOCOL_ORDER:
                        sub = hdf[(hdf["machine"] == machine) & (hdf["protocol"] == protocol)].copy()
                        if sub.empty:
                            continue
                        sub_sizes = 40 + 18 * np.sqrt(sub[metric].clip(lower=0) / max(metric_vals.max(), 1)) * 6
                        style = series_style(machine, protocol)
                        ax.scatter(sub["contention"], sub["threads"], s=sub_sizes, c=style["color"], marker=style["marker"],
                                   facecolors=style["markerfacecolor"], edgecolors=style["markeredgecolor"], linewidths=1.7, alpha=0.72)
                        for _, row in sub.iterrows():
                            ax.annotate(f"{row[metric]:.0f}" if metric == "throughput" else f"{row[metric]:.1f}",
                                        (row["contention"], row["threads"]), textcoords="offset points", xytext=(4, 3), fontsize=7.5, alpha=0.85)
                ax.set_title(f"hotset={hotset}")
                ax.set_xlabel("Contention probability p")
                ax.set_ylabel("Threads")
                if log_scale and metric == "throughput":
                    pass
            for ax in axes[len(hotsets):]:
                ax.axis("off")
            fig.suptitle(f"Workload {workload}: {title_label}", y=1.02, fontsize=17, fontweight="semibold")
            add_panel_legends(fig, MACHINE_STYLE['mac']['label'], MACHINE_STYLE['windows']['label'], size_values=sorted(df['threads'].unique().tolist()))
            fig.tight_layout(rect=[0.02, 0.05, 0.98, 0.95])
            fig.savefig(dirs["plots_rich"] / f"w{workload}_{stem}.png", dpi=260, bbox_inches="tight")
            plt.close(fig)


def plot_delta_lollipop(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df["workload"].unique()):
        cmp_df = df.pivot_table(index=["workload", "threads", "contention", "hotset", "protocol"], columns="machine", values=["throughput", "avg_response_time"], observed=False)
        cmp_df.columns = [f"{a}_{b}" for a, b in cmp_df.columns]
        cmp_df = cmp_df.reset_index()
        sub = cmp_df[cmp_df["workload"] == workload].copy()
        if sub.empty or "throughput_mac" not in sub or "throughput_windows" not in sub:
            continue
        sub["throughput_gap_pct"] = 100 * (sub["throughput_mac"] - sub["throughput_windows"]) / sub[["throughput_mac", "throughput_windows"]].mean(axis=1)
        sub["latency_gap_pct"] = 100 * (sub["avg_response_time_windows"] - sub["avg_response_time_mac"]) / sub[["avg_response_time_mac", "avg_response_time_windows"]].mean(axis=1)
        sub = sub.sort_values(["protocol", "hotset", "contention", "threads"]).reset_index(drop=True)
        sub["idx"] = np.arange(len(sub))
        fig, axes = plt.subplots(2, 1, figsize=(15, 8), sharex=True)
        for ax, col, title in [
            (axes[0], "throughput_gap_pct", "Mac advantage in throughput (+ means Mac higher)"),
            (axes[1], "latency_gap_pct", "Mac advantage in latency (+ means Mac lower latency)"),
        ]:
            ax.axhline(0, color="#666666", linewidth=1.1)
            for protocol in PROTOCOL_ORDER:
                psub = sub[sub["protocol"] == protocol]
                marker = PROTOCOL_STYLE[protocol]["marker"]
                ax.vlines(psub["idx"], 0, psub[col], color="#999999", linewidth=1.1, alpha=0.7)
                ax.scatter(psub["idx"], psub[col], s=90, marker=marker, c=np.where(psub[col] >= 0, MACHINE_STYLE['mac']['color'], MACHINE_STYLE['windows']['color']), edgecolors="#222222", linewidths=0.9, alpha=0.85)
            ax.set_ylabel("Percent gap")
            ax.set_title(title)
        tick_labels = [f"{row.protocol.split()[0]}\nt{int(row.threads)} c{row.contention:.2f} h{int(row.hotset)}" for row in sub.itertuples()]
        axes[1].set_xticks(sub["idx"])
        axes[1].set_xticklabels(tick_labels, rotation=90)
        fig.suptitle(f"Workload {workload}: machine deltas across all configurations", y=1.02, fontsize=17, fontweight="semibold")
        fig.tight_layout(rect=[0.02, 0.03, 0.98, 0.96])
        fig.savefig(dirs["plots_rich"] / f"w{workload}_machine_delta_lollipop.png", dpi=260, bbox_inches="tight")
        plt.close(fig)


def _normalize_series(s: pd.Series, higher_is_better: bool = True) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.dropna().empty:
        return pd.Series(np.nan, index=s.index)
    lo = float(s.min())
    hi = float(s.max())
    if hi - lo < 1e-12:
        out = pd.Series(0.5, index=s.index)
    else:
        out = (s - lo) / (hi - lo)
    return out if higher_is_better else 1.0 - out


def _short_protocol(protocol: str) -> str:
    return 'OCC' if protocol == 'OCC' else '2PL'


def plot_contour_panels(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    metrics = [
        ('throughput', 'Throughput', 'viridis', True, 'throughput_contour_panels'),
        ('avg_response_time', 'Average response time (ms)', 'magma_r', True, 'latency_contour_panels'),
        ('retry_rate_pct', 'Retry rate (%)', 'plasma', False, 'retry_contour_panels'),
    ]
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload]
        for hotset in sorted(wdf['hotset'].unique()):
            hdf = wdf[wdf['hotset'] == hotset]
            for metric, label, cmap, logz, stem in metrics:
                fig, axes = plt.subplots(2, 2, figsize=(14.8, 10.2), sharex=True, sharey=True)
                for ax, machine, protocol in zip(axes.flatten(), [m for m in MACHINE_ORDER for _ in PROTOCOL_ORDER], PROTOCOL_ORDER * 2):
                    sub = hdf[(hdf['machine'] == machine) & (hdf['protocol'] == protocol)]
                    mat = sub.pivot_table(index='threads', columns='contention', values=metric, aggfunc='mean')
                    if mat.empty:
                        ax.axis('off')
                        continue
                    vals = mat.to_numpy(dtype=float)
                    if logz and np.isfinite(vals).any() and np.nanmin(vals) > 0:
                        vals = np.log10(vals)
                        cbar_label = f'log10 {label}'
                    else:
                        cbar_label = label
                    im = ax.imshow(vals, origin='lower', aspect='auto', cmap=cmap)
                    ax.set_xticks(range(len(mat.columns)))
                    ax.set_xticklabels([f'{c:.2f}' for c in mat.columns])
                    ax.set_yticks(range(len(mat.index)))
                    ax.set_yticklabels([str(int(t)) for t in mat.index])
                    ax.set_title(f"{MACHINE_STYLE[machine]['label'].split(' (')[0]} | {_short_protocol(protocol)}")
                    ax.set_xlabel('Contention probability p')
                    ax.set_ylabel('Threads')
                    cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
                    cb.set_label(cbar_label)
                fig.suptitle(f'Workload {workload}: {label} contour-style panels (hotset={hotset})', y=1.01, fontsize=17, fontweight='semibold')
                fig.tight_layout(rect=[0.02, 0.02, 0.98, 0.97])
                fig.savefig(dirs['plots_rich'] / f'w{workload}_{stem}_h{hotset}.png', dpi=260, bbox_inches='tight')
                plt.close(fig)


def plot_machine_shift_arrows(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload]
        for hotset in sorted(wdf['hotset'].unique()):
            hdf = wdf[wdf['hotset'] == hotset]
            fig, axes = plt.subplots(1, 2, figsize=(15.2, 6.2), squeeze=False)
            plotted = False
            for ax, protocol in zip(axes.flatten(), PROTOCOL_ORDER):
                sub = hdf[hdf['protocol'] == protocol]
                left = sub[sub['machine'] == 'windows'][['threads', 'contention', 'throughput', 'avg_response_time']].rename(columns={'throughput': 'thr_win', 'avg_response_time': 'lat_win'})
                right = sub[sub['machine'] == 'mac'][['threads', 'contention', 'throughput', 'avg_response_time']].rename(columns={'throughput': 'thr_mac', 'avg_response_time': 'lat_mac'})
                merged = left.merge(right, on=['threads', 'contention'])
                if merged.empty:
                    ax.axis('off')
                    continue
                plotted = True
                color = '#444444'
                for _, row in merged.iterrows():
                    ax.annotate('', xy=(row['lat_mac'], row['thr_mac']), xytext=(row['lat_win'], row['thr_win']),
                                arrowprops=dict(arrowstyle='->', lw=1.8, color=color, alpha=0.65))
                ax.scatter(merged['lat_win'], merged['thr_win'], s=90 + 8 * merged['threads'], facecolors='white',
                           edgecolors=MACHINE_STYLE['windows']['color'], linewidths=2.0, marker=PROTOCOL_STYLE[protocol]['marker'], label=windows_label)
                ax.scatter(merged['lat_mac'], merged['thr_mac'], s=90 + 8 * merged['threads'], facecolors=MACHINE_STYLE['mac']['color'],
                           edgecolors=MACHINE_STYLE['mac']['color'], linewidths=1.8, marker=PROTOCOL_STYLE[protocol]['marker'], label=mac_label)
                for _, row in merged.iterrows():
                    ax.annotate(f"t{int(row['threads'])}\nc{row['contention']:.2f}", (row['lat_mac'], row['thr_mac']), textcoords='offset points', xytext=(4, 3), fontsize=7.5)
                ax.set_title(f'{_short_protocol(protocol)}: Windows → Mac shift')
                ax.set_xlabel('Average response time (ms)')
                ax.set_ylabel('Throughput')
                set_log_scale(ax, pd.concat([merged['lat_win'], merged['lat_mac']]), axis='x', prefer_log=True)
                set_log_scale(ax, pd.concat([merged['thr_win'], merged['thr_mac']]), axis='y', prefer_log=True)
                add_rich_legend(ax, mac_label, windows_label)
            if plotted:
                fig.suptitle(f'Workload {workload}: machine shift vectors in the throughput-latency plane (hotset={hotset})', y=1.02, fontsize=17, fontweight='semibold')
                fig.tight_layout(rect=[0.02, 0.02, 0.98, 0.96])
                fig.savefig(dirs['plots_rich'] / f'w{workload}_machine_shift_vectors_h{hotset}.png', dpi=260, bbox_inches='tight')
            plt.close(fig)


def plot_protocol_shift_arrows(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload]
        for hotset in sorted(wdf['hotset'].unique()):
            hdf = wdf[wdf['hotset'] == hotset]
            fig, axes = plt.subplots(1, 2, figsize=(15.2, 6.2), squeeze=False)
            plotted = False
            for ax, machine in zip(axes.flatten(), MACHINE_ORDER):
                sub = hdf[hdf['machine'] == machine]
                left = sub[sub['protocol'] == 'Conservative 2PL'][['threads', 'contention', 'throughput', 'avg_response_time']].rename(columns={'throughput': 'thr_2pl', 'avg_response_time': 'lat_2pl'})
                right = sub[sub['protocol'] == 'OCC'][['threads', 'contention', 'throughput', 'avg_response_time']].rename(columns={'throughput': 'thr_occ', 'avg_response_time': 'lat_occ'})
                merged = left.merge(right, on=['threads', 'contention'])
                if merged.empty:
                    ax.axis('off')
                    continue
                plotted = True
                color = MACHINE_STYLE[machine]['color']
                face = color if MACHINE_STYLE[machine]['filled'] else 'white'
                for _, row in merged.iterrows():
                    ax.annotate('', xy=(row['lat_occ'], row['thr_occ']), xytext=(row['lat_2pl'], row['thr_2pl']),
                                arrowprops=dict(arrowstyle='->', lw=1.8, color=color, alpha=0.7))
                ax.scatter(merged['lat_2pl'], merged['thr_2pl'], s=90 + 8 * merged['threads'], facecolors=face,
                           edgecolors=color, linewidths=2.0, marker=PROTOCOL_STYLE['Conservative 2PL']['marker'])
                ax.scatter(merged['lat_occ'], merged['thr_occ'], s=90 + 8 * merged['threads'], facecolors=face,
                           edgecolors=color, linewidths=2.0, marker=PROTOCOL_STYLE['OCC']['marker'])
                for _, row in merged.iterrows():
                    ax.annotate(f"t{int(row['threads'])}\nc{row['contention']:.2f}", (row['lat_occ'], row['thr_occ']), textcoords='offset points', xytext=(4, 3), fontsize=7.5)
                ax.set_title(f"{MACHINE_STYLE[machine]['label'].split(' (')[0]}: 2PL → OCC shift")
                ax.set_xlabel('Average response time (ms)')
                ax.set_ylabel('Throughput')
                set_log_scale(ax, pd.concat([merged['lat_2pl'], merged['lat_occ']]), axis='x', prefer_log=True)
                set_log_scale(ax, pd.concat([merged['thr_2pl'], merged['thr_occ']]), axis='y', prefer_log=True)
                add_rich_legend(ax, mac_label, windows_label)
            if plotted:
                fig.suptitle(f'Workload {workload}: protocol shift vectors in the throughput-latency plane (hotset={hotset})', y=1.02, fontsize=17, fontweight='semibold')
                fig.tight_layout(rect=[0.02, 0.02, 0.98, 0.96])
                fig.savefig(dirs['plots_rich'] / f'w{workload}_protocol_shift_vectors_h{hotset}.png', dpi=260, bbox_inches='tight')
            plt.close(fig)


def plot_parallel_metric_profiles(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    metrics = ['throughput', 'throughput_per_thread', 'avg_response_time', 'retry_rate_pct']
    labels = ['Throughput ↑', 'Throughput/thread ↑', 'Latency ↓', 'Retry rate ↓']
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload].copy()
        agg = wdf.groupby(['machine', 'protocol'], observed=False)[metrics].mean().reset_index()
        if agg.empty:
            continue
        norm = pd.DataFrame({
            'throughput': _normalize_series(agg['throughput'], True),
            'throughput_per_thread': _normalize_series(agg['throughput_per_thread'], True),
            'avg_response_time': _normalize_series(agg['avg_response_time'], False),
            'retry_rate_pct': _normalize_series(agg['retry_rate_pct'], False),
        })
        fig, ax = plt.subplots(figsize=(10.8, 6.4))
        x = np.arange(len(metrics))
        for idx, row in agg.iterrows():
            machine = row['machine']
            protocol = row['protocol']
            style = series_style(machine, protocol)
            y = norm.loc[idx, metrics].to_numpy(dtype=float)
            ax.plot(x, y, **style)
            ax.fill_between(x, y, alpha=0.08, color=style['color'])
            ax.annotate(f"{machine}/{_short_protocol(protocol)}", (x[-1], y[-1]), textcoords='offset points', xytext=(6, 0), fontsize=9)
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylim(-0.02, 1.05)
        ax.set_ylabel('Normalized score')
        ax.set_title(f'Workload {workload}: normalized metric profile by machine and protocol')
        add_rich_legend(ax, mac_label, windows_label)
        save_plot(fig, dirs['plots_rich'] / f'w{workload}_parallel_metric_profile.png')


def plot_winrate_panels(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload]
        fig, axes = plt.subplots(1, 3, figsize=(17.2, 5.2))

        prot_rows = []
        for machine in MACHINE_ORDER:
            sub = wdf[wdf['machine'] == machine].pivot_table(index=['threads', 'contention', 'hotset'], columns='protocol', values=['throughput', 'avg_response_time', 'retry_rate_pct'], aggfunc='mean')
            if sub.empty:
                continue
            thr_occ = sub[('throughput', 'OCC')]
            thr_2pl = sub[('throughput', 'Conservative 2PL')]
            lat_occ = sub[('avg_response_time', 'OCC')]
            lat_2pl = sub[('avg_response_time', 'Conservative 2PL')]
            ret_occ = sub[('retry_rate_pct', 'OCC')]
            ret_2pl = sub[('retry_rate_pct', 'Conservative 2PL')]
            prot_rows.append({'group': machine, 'metric': 'Throughput', 'OCC': int((thr_occ > thr_2pl).sum()), '2PL': int((thr_2pl > thr_occ).sum())})
            prot_rows.append({'group': machine, 'metric': 'Latency', 'OCC': int((lat_occ < lat_2pl).sum()), '2PL': int((lat_2pl < lat_occ).sum())})
            prot_rows.append({'group': machine, 'metric': 'Retries', 'OCC': int((ret_occ < ret_2pl).sum()), '2PL': int((ret_2pl < ret_occ).sum())})
        prot_df = pd.DataFrame(prot_rows)
        if not prot_df.empty:
            x = np.arange(len(prot_df))
            axes[0].bar(x, prot_df['OCC'], color='#4daf4a', edgecolor='black', label='OCC wins')
            axes[0].bar(x, prot_df['2PL'], bottom=prot_df['OCC'], color='#984ea3', edgecolor='black', label='2PL wins')
            axes[0].set_xticks(x)
            axes[0].set_xticklabels([f"{r.group}\n{r.metric}" for r in prot_df.itertuples()], rotation=0)
            axes[0].set_title('Protocol wins by machine')
            axes[0].legend(loc='upper left')

        mach_rows = []
        for protocol in PROTOCOL_ORDER:
            sub = wdf[wdf['protocol'] == protocol].pivot_table(index=['threads', 'contention', 'hotset'], columns='machine', values=['throughput', 'avg_response_time', 'retry_rate_pct'], aggfunc='mean')
            if sub.empty:
                continue
            thr_mac = sub[('throughput', 'mac')]
            thr_win = sub[('throughput', 'windows')]
            lat_mac = sub[('avg_response_time', 'mac')]
            lat_win = sub[('avg_response_time', 'windows')]
            ret_mac = sub[('retry_rate_pct', 'mac')]
            ret_win = sub[('retry_rate_pct', 'windows')]
            mach_rows.append({'group': _short_protocol(protocol), 'metric': 'Throughput', 'Mac': int((thr_mac > thr_win).sum()), 'Windows': int((thr_win > thr_mac).sum())})
            mach_rows.append({'group': _short_protocol(protocol), 'metric': 'Latency', 'Mac': int((lat_mac < lat_win).sum()), 'Windows': int((lat_win < lat_mac).sum())})
            mach_rows.append({'group': _short_protocol(protocol), 'metric': 'Retries', 'Mac': int((ret_mac < ret_win).sum()), 'Windows': int((ret_win < ret_mac).sum())})
        mach_df = pd.DataFrame(mach_rows)
        if not mach_df.empty:
            x = np.arange(len(mach_df))
            axes[1].bar(x, mach_df['Mac'], color=MACHINE_STYLE['mac']['color'], edgecolor='black', label='Mac wins')
            axes[1].bar(x, mach_df['Windows'], bottom=mach_df['Mac'], color=MACHINE_STYLE['windows']['color'], edgecolor='black', label='Windows wins')
            axes[1].set_xticks(x)
            axes[1].set_xticklabels([f"{r.group}\n{r.metric}" for r in mach_df.itertuples()])
            axes[1].set_title('Machine wins by protocol')
            axes[1].legend(loc='upper right')

        summary = (
            wdf.groupby(['machine', 'protocol'], observed=False)
            .agg(throughput=('throughput', 'mean'), latency=('avg_response_time', 'mean'), retry=('retry_rate_pct', 'mean'))
            .reset_index()
        )
        summary['score'] = (_normalize_series(summary['throughput'], True) + _normalize_series(summary['latency'], False) + _normalize_series(summary['retry'], False)) / 3.0
        x = np.arange(len(summary))
        colors = [MACHINE_STYLE[m]['color'] for m in summary['machine']]
        bars = axes[2].bar(x, summary['score'], color=colors, edgecolor='black')
        for bar, protocol in zip(bars, summary['protocol']):
            if protocol != 'OCC':
                bar.set_hatch('//')
        axes[2].set_xticks(x)
        axes[2].set_xticklabels([f"{m}\n{_short_protocol(p)}" for m, p in zip(summary['machine'], summary['protocol'])])
        axes[2].set_title('Composite normalized score')
        axes[2].set_ylim(0, 1.05)

        fig.suptitle(f'Workload {workload}: win-rate and dominance summary', y=1.02, fontsize=17, fontweight='semibold')
        save_plot(fig, dirs['plots_rich'] / f'w{workload}_winrate_panels.png')


def plot_top_config_panels(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload].copy()
        wdf['score'] = (
                               _normalize_series(wdf['throughput'], True) +
                               _normalize_series(wdf['throughput_per_thread'], True) +
                               _normalize_series(wdf['avg_response_time'], False) +
                               _normalize_series(wdf['retry_rate_pct'], False)
                       ) / 4.0
        top = wdf.sort_values('score', ascending=False).head(12).reset_index(drop=True)
        if top.empty:
            continue
        fig, ax = plt.subplots(figsize=(12.4, 7.4))
        y = np.arange(len(top))[::-1]
        colors = [MACHINE_STYLE[m]['color'] for m in top['machine']]
        bars = ax.barh(y, top['score'], color=colors, edgecolor='black', alpha=0.8)
        for bar, protocol in zip(bars, top['protocol']):
            if protocol != 'OCC':
                bar.set_hatch('//')
        ax.set_yticks(y)
        ax.set_yticklabels([f"{r.machine} | {_short_protocol(r.protocol)} | t{int(r.threads)} c{r.contention:.2f} h{int(r.hotset)}" for r in top.itertuples()])
        ax.set_xlabel('Composite normalized score')
        ax.set_title(f'Workload {workload}: top configurations by combined throughput / latency / retry score')
        for yi, row in zip(y, top.itertuples()):
            ax.text(row.score + 0.01, yi, f"thr={row.throughput:.0f}, lat={row.avg_response_time:.2f}, retry={row.retry_rate_pct:.1f}%", va='center', fontsize=8.7)
        save_plot(fig, dirs['plots_rich'] / f'w{workload}_top_config_panels.png')


def plot_rank_heatmaps(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload].copy()
        wdf['score'] = (
                               _normalize_series(wdf['throughput'], True) +
                               _normalize_series(wdf['avg_response_time'], False) +
                               _normalize_series(wdf['retry_rate_pct'], False)
                       ) / 3.0
        for hotset in sorted(wdf['hotset'].unique()):
            hdf = wdf[wdf['hotset'] == hotset]
            fig, axes = plt.subplots(2, 2, figsize=(14.2, 9.6), sharex=True, sharey=True)
            for ax, machine, protocol in zip(axes.flatten(), [m for m in MACHINE_ORDER for _ in PROTOCOL_ORDER], PROTOCOL_ORDER * 2):
                sub = hdf[(hdf['machine'] == machine) & (hdf['protocol'] == protocol)].copy()
                if sub.empty:
                    ax.axis('off')
                    continue
                sub['rank'] = sub['score'].rank(ascending=False, method='dense')
                mat = sub.pivot_table(index='threads', columns='contention', values='rank', aggfunc='mean')
                im = ax.imshow(mat.to_numpy(dtype=float), origin='lower', aspect='auto', cmap='YlGn_r')
                ax.set_xticks(range(len(mat.columns)))
                ax.set_xticklabels([f'{c:.2f}' for c in mat.columns])
                ax.set_yticks(range(len(mat.index)))
                ax.set_yticklabels([str(int(t)) for t in mat.index])
                ax.set_title(f"{MACHINE_STYLE[machine]['label'].split(' (')[0]} | {_short_protocol(protocol)}")
                ax.set_xlabel('Contention probability p')
                ax.set_ylabel('Threads')
                for i in range(mat.shape[0]):
                    for j in range(mat.shape[1]):
                        if pd.notna(mat.iloc[i, j]):
                            ax.text(j, i, f"#{int(round(mat.iloc[i, j]))}", ha='center', va='center', fontsize=9)
                cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
                cb.set_label('Rank (1 = best)')
            fig.suptitle(f'Workload {workload}: composite-score rank heatmaps (hotset={hotset})', y=1.02, fontsize=17, fontweight='semibold')
            fig.tight_layout(rect=[0.02, 0.02, 0.98, 0.96])
            fig.savefig(dirs['plots_rich'] / f'w{workload}_rank_heatmaps_h{hotset}.png', dpi=260, bbox_inches='tight')
            plt.close(fig)


def plot_tradeoff_quadrants(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload].copy()
        wdf['thr_norm'] = _normalize_series(wdf['throughput'], True)
        wdf['lat_norm'] = _normalize_series(wdf['avg_response_time'], False)
        wdf['retry_norm'] = _normalize_series(wdf['retry_rate_pct'], False)
        wdf['size'] = 70 + 150 * wdf['throughput_per_thread'].fillna(0) / max(float(wdf['throughput_per_thread'].max()), 1.0)
        fig, ax = plt.subplots(figsize=(10.8, 7.2))
        for machine in MACHINE_ORDER:
            for protocol in PROTOCOL_ORDER:
                sub = wdf[(wdf['machine'] == machine) & (wdf['protocol'] == protocol)]
                if sub.empty:
                    continue
                style = series_style(machine, protocol)
                ax.scatter(sub['lat_norm'], sub['thr_norm'], s=sub['size'], c=sub['retry_norm'], cmap='viridis',
                           marker=style['marker'], edgecolors=style['markeredgecolor'], facecolors=style['markerfacecolor'], linewidths=1.8, alpha=0.9)
        ax.axhline(0.5, color='#999999', linestyle=':', linewidth=1.4)
        ax.axvline(0.5, color='#999999', linestyle=':', linewidth=1.4)
        ax.set_xlabel('Normalized latency goodness (higher = lower latency)')
        ax.set_ylabel('Normalized throughput goodness')
        ax.set_title(f'Workload {workload}: tradeoff quadrant map\nsize = throughput/thread, color = retry goodness')
        add_rich_legend(ax, mac_label, windows_label)
        save_plot(fig, dirs['plots_rich'] / f'w{workload}_tradeoff_quadrant_map.png')


def plot_pairwise_metric_scatter(df: pd.DataFrame, dirs: Dict[str, Path], mac_label: str, windows_label: str) -> None:
    pairs = [
        ('throughput', 'avg_response_time'),
        ('throughput', 'retry_rate_pct'),
        ('throughput_per_thread', 'avg_response_time'),
        ('throughput_per_thread', 'retry_rate_pct'),
    ]
    labels = {
        'throughput': 'Throughput',
        'avg_response_time': 'Avg response time (ms)',
        'retry_rate_pct': 'Retry rate (%)',
        'throughput_per_thread': 'Throughput / thread',
    }
    for workload in sorted(df['workload'].unique()):
        wdf = df[df['workload'] == workload]
        fig, axes = plt.subplots(2, 2, figsize=(14.0, 10.5))
        for ax, (xcol, ycol) in zip(axes.flatten(), pairs):
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    sub = wdf[(wdf['machine'] == machine) & (wdf['protocol'] == protocol)]
                    if sub.empty:
                        continue
                    style = series_style(machine, protocol)
                    ax.scatter(sub[xcol], sub[ycol], s=85 + 8 * sub['threads'], c=style['color'], marker=style['marker'],
                               facecolors=style['markerfacecolor'], edgecolors=style['markeredgecolor'], linewidths=1.7, alpha=0.78)
            ax.set_xlabel(labels[xcol])
            ax.set_ylabel(labels[ycol])
            if xcol in ('throughput', 'avg_response_time', 'throughput_per_thread'):
                set_log_scale(ax, wdf[xcol], axis='x', prefer_log=True)
            if ycol in ('throughput', 'avg_response_time', 'throughput_per_thread'):
                set_log_scale(ax, wdf[ycol], axis='y', prefer_log=True)
        fig.suptitle(f'Workload {workload}: pairwise metric scatter matrix', y=1.02, fontsize=17, fontweight='semibold')
        add_panel_legends(fig, mac_label, windows_label, size_values=sorted(wdf['threads'].unique()))
        fig.tight_layout(rect=[0.03, 0.06, 0.97, 0.94])
        fig.savefig(dirs['plots_rich'] / f'w{workload}_pairwise_metric_scatter.png', dpi=260, bbox_inches='tight')
        plt.close(fig)


def plot_ridgeline_distributions(rt_df: pd.DataFrame, dirs: Dict[str, Path], dist_threads: int, dist_contention: float, dist_hotset: int) -> None:
    if rt_df.empty:
        return
    for workload in sorted(rt_df['workload'].unique()):
        wdf = rt_df[rt_df['workload'] == workload]
        for template in sorted(wdf['template'].unique()):
            tdf = wdf[wdf['template'] == template]
            if tdf.empty:
                continue
            fig, ax = plt.subplots(figsize=(11.2, 7.6))
            combos = [(m, p) for m in MACHINE_ORDER for p in PROTOCOL_ORDER]
            offsets = np.arange(len(combos)) * 1.15
            all_x = tdf['response_time_ms'].dropna().to_numpy()
            if len(all_x) < 2:
                plt.close(fig)
                continue
            xmin = max(np.min(all_x[all_x > 0]) if np.any(all_x > 0) else np.min(all_x), 1e-6)
            xmax = np.max(all_x)
            xs = np.logspace(np.log10(xmin), np.log10(xmax), 220) if np.all(all_x > 0) else np.linspace(np.min(all_x), np.max(all_x), 220)
            bin_edges = np.geomspace(xmin, xmax, 36) if np.all(all_x > 0) else np.linspace(np.min(all_x), np.max(all_x), 36)
            for idx, (machine, protocol) in enumerate(combos):
                vals = tdf[(tdf['machine'] == machine) & (tdf['protocol'] == protocol)]['response_time_ms'].dropna().to_numpy()
                if len(vals) < 2:
                    continue
                hist, edges = np.histogram(vals, bins=bin_edges, density=True)
                mids = np.sqrt(edges[:-1] * edges[1:]) if np.all(vals > 0) else (edges[:-1] + edges[1:]) / 2
                dens = np.interp(xs, mids, hist, left=0, right=0)
                if dens.max() > 0:
                    dens = dens / dens.max() * 0.9
                base = offsets[idx]
                color = MACHINE_STYLE[machine]['color']
                face = color if MACHINE_STYLE[machine]['filled'] else 'white'
                ax.fill_between(xs, base, base + dens, color=face, edgecolor=color, linewidth=1.6, alpha=0.5 if face != 'white' else 1.0)
                ax.plot(xs, base + dens, color=color, linestyle=PROTOCOL_STYLE[protocol]['linestyle'], linewidth=2.0)
                ax.text(xs[0], base + 0.05, f"{machine}/{_short_protocol(protocol)}", fontsize=9, va='bottom')
            ax.set_yticks([])
            ax.set_xlabel('Response time (ms)')
            ax.set_title(f'Workload {workload}: ridgeline response-time distributions for {template}\nthreads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}')
            if np.all(all_x > 0):
                ax.set_xscale('log')
            save_plot(fig, dirs['plots_dist'] / f'w{workload}_{template}_ridgeline_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png')


def plot_tail_latency_bars(rt_stats: pd.DataFrame, dirs: Dict[str, Path], dist_threads: int, dist_contention: float, dist_hotset: int) -> None:
    if rt_stats.empty:
        return
    for workload in sorted(rt_stats['workload'].unique()):
        wdf = rt_stats[rt_stats['workload'] == workload]
        for tail_col, title in [('median', 'Median response time'), ('p95', 'P95 response time'), ('p99', 'P99 response time')]:
            fig, ax = plt.subplots(figsize=(12.8, 6.8))
            templates = sorted(wdf['template'].unique())
            width = 0.18
            x = np.arange(len(templates))
            offsets = [-1.5, -0.5, 0.5, 1.5]
            combos = [(m, p) for m in MACHINE_ORDER for p in PROTOCOL_ORDER]
            for off, (machine, protocol) in zip(offsets, combos):
                vals = []
                for template in templates:
                    sub = wdf[(wdf['template'] == template) & (wdf['machine'] == machine) & (wdf['protocol'] == protocol)]
                    vals.append(float(sub[tail_col].mean()) if not sub.empty else np.nan)
                color = MACHINE_STYLE[machine]['color']
                face = color if MACHINE_STYLE[machine]['filled'] else 'white'
                bars = ax.bar(x + off * width, vals, width=width, color=face, edgecolor=color, linewidth=1.8, label=f'{machine}/{_short_protocol(protocol)}')
                if protocol != 'OCC':
                    for b in bars:
                        b.set_hatch('//')
            ax.set_xticks(x)
            ax.set_xticklabels(templates)
            ax.set_ylabel('Response time (ms)')
            ax.set_title(f'Workload {workload}: {title} by template\nthreads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}')
            set_log_scale(ax, wdf[tail_col], axis='y', prefer_log=True)
            ax.legend(loc='upper left', ncol=2, fontsize=9)
            save_plot(fig, dirs['plots_dist'] / f'w{workload}_{tail_col}_bars_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png')


def make_tables(df: pd.DataFrame, dirs: Dict[str, Path]) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}

    overview = pd.DataFrame({
        "metric": [
            "num_rows", "num_workloads", "machines", "protocols", "threads_values", "contention_values", "hotset_values"
        ],
        "value": [
            len(df),
            df["workload"].nunique(),
            ", ".join(sorted(df["machine"].unique())),
            ", ".join(map(str, df["protocol"].dropna().unique().tolist())),
            ", ".join(map(str, sorted(df["threads"].unique()))),
            ", ".join(f"{x:.2f}" for x in sorted(df["contention"].unique())),
            ", ".join(map(str, sorted(df["hotset"].unique()))),
        ],
    })
    tables["dataset_overview"] = overview

    summary = (
        df.groupby(["workload", "machine", "protocol"], observed=False)
        .agg(
            runs=("throughput", "size"),
            mean_throughput=("throughput", "mean"),
            median_throughput=("throughput", "median"),
            max_throughput=("throughput", "max"),
            mean_avg_response_ms=("avg_response_time", "mean"),
            median_avg_response_ms=("avg_response_time", "median"),
            min_avg_response_ms=("avg_response_time", "min"),
            mean_retries=("retries", "mean"),
            mean_retry_rate_pct=("retry_rate_pct", "mean"),
            mean_throughput_per_thread=("throughput_per_thread", "mean"),
        )
        .reset_index()
    )
    tables["workload_machine_protocol_summary"] = summary

    best_configs = (
        df.sort_values(["workload", "machine", "throughput", "avg_response_time"], ascending=[True, True, False, True])
        .groupby(["workload", "machine"], as_index=False)
        .head(8)
        .reset_index(drop=True)
    )
    tables["best_throughput_configs_per_machine"] = best_configs

    protocol_cmp = df.pivot_table(
        index=["workload", "machine", "threads", "contention", "hotset"],
        columns="protocol",
        values=["throughput", "avg_response_time", "retry_rate_pct", "throughput_per_thread"],
        observed=False,
    )
    protocol_cmp.columns = [f"{metric}_{protocol}" for metric, protocol in protocol_cmp.columns]
    protocol_cmp = protocol_cmp.reset_index()
    if {"throughput_OCC", "throughput_Conservative 2PL"}.issubset(protocol_cmp.columns):
        protocol_cmp["throughput_diff_occ_minus_2pl"] = protocol_cmp["throughput_OCC"] - protocol_cmp["throughput_Conservative 2PL"]
        protocol_cmp["throughput_winner"] = np.where(
            protocol_cmp["throughput_OCC"] > protocol_cmp["throughput_Conservative 2PL"], "OCC",
            np.where(protocol_cmp["throughput_OCC"] < protocol_cmp["throughput_Conservative 2PL"], "Conservative 2PL", "Tie")
        )
    if {"avg_response_time_OCC", "avg_response_time_Conservative 2PL"}.issubset(protocol_cmp.columns):
        protocol_cmp["latency_diff_occ_minus_2pl_ms"] = protocol_cmp["avg_response_time_OCC"] - protocol_cmp["avg_response_time_Conservative 2PL"]
        protocol_cmp["latency_winner"] = np.where(
            protocol_cmp["avg_response_time_OCC"] < protocol_cmp["avg_response_time_Conservative 2PL"], "OCC",
            np.where(protocol_cmp["avg_response_time_OCC"] > protocol_cmp["avg_response_time_Conservative 2PL"], "Conservative 2PL", "Tie")
        )
    if {"retry_rate_pct_OCC", "retry_rate_pct_Conservative 2PL"}.issubset(protocol_cmp.columns):
        protocol_cmp["retry_diff_occ_minus_2pl_pct"] = protocol_cmp["retry_rate_pct_OCC"] - protocol_cmp["retry_rate_pct_Conservative 2PL"]
        protocol_cmp["retry_winner"] = np.where(
            protocol_cmp["retry_rate_pct_OCC"] < protocol_cmp["retry_rate_pct_Conservative 2PL"], "OCC",
            np.where(protocol_cmp["retry_rate_pct_OCC"] > protocol_cmp["retry_rate_pct_Conservative 2PL"], "Conservative 2PL", "Tie")
        )
    tables["protocol_comparison_within_machine"] = protocol_cmp

    machine_cmp = df.pivot_table(
        index=["workload", "protocol", "threads", "contention", "hotset"],
        columns="machine",
        values=["throughput", "avg_response_time", "retry_rate_pct", "throughput_per_thread"],
        observed=False,
    )
    machine_cmp.columns = [f"{metric}_{machine}" for metric, machine in machine_cmp.columns]
    machine_cmp = machine_cmp.reset_index()
    if {"throughput_mac", "throughput_windows"}.issubset(machine_cmp.columns):
        machine_cmp["throughput_diff_mac_minus_windows"] = machine_cmp["throughput_mac"] - machine_cmp["throughput_windows"]
        machine_cmp["throughput_machine_winner"] = np.where(
            machine_cmp["throughput_mac"] > machine_cmp["throughput_windows"], "mac",
            np.where(machine_cmp["throughput_mac"] < machine_cmp["throughput_windows"], "windows", "Tie")
        )
    if {"avg_response_time_mac", "avg_response_time_windows"}.issubset(machine_cmp.columns):
        machine_cmp["latency_diff_mac_minus_windows_ms"] = machine_cmp["avg_response_time_mac"] - machine_cmp["avg_response_time_windows"]
        machine_cmp["latency_machine_winner"] = np.where(
            machine_cmp["avg_response_time_mac"] < machine_cmp["avg_response_time_windows"], "mac",
            np.where(machine_cmp["avg_response_time_mac"] > machine_cmp["avg_response_time_windows"], "windows", "Tie")
        )
    if {"retry_rate_pct_mac", "retry_rate_pct_windows"}.issubset(machine_cmp.columns):
        machine_cmp["retry_diff_mac_minus_windows_pct"] = machine_cmp["retry_rate_pct_mac"] - machine_cmp["retry_rate_pct_windows"]
        machine_cmp["retry_machine_winner"] = np.where(
            machine_cmp["retry_rate_pct_mac"] < machine_cmp["retry_rate_pct_windows"], "mac",
            np.where(machine_cmp["retry_rate_pct_mac"] > machine_cmp["retry_rate_pct_windows"], "windows", "Tie")
        )
    tables["machine_comparison_within_protocol"] = machine_cmp

    win_counts = []
    if not protocol_cmp.empty:
        for workload in sorted(df["workload"].unique()):
            for machine in MACHINE_ORDER:
                sub = protocol_cmp[(protocol_cmp["workload"] == workload) & (protocol_cmp["machine"] == machine)]
                if sub.empty:
                    continue
                win_counts.append({
                    "workload": workload,
                    "machine": machine,
                    "throughput_occ_wins": int((sub.get("throughput_winner") == "OCC").sum()),
                    "throughput_2pl_wins": int((sub.get("throughput_winner") == "Conservative 2PL").sum()),
                    "latency_occ_wins": int((sub.get("latency_winner") == "OCC").sum()),
                    "latency_2pl_wins": int((sub.get("latency_winner") == "Conservative 2PL").sum()),
                })
    if win_counts:
        tables["protocol_win_counts"] = pd.DataFrame(win_counts)

    for name, table in tables.items():
        table.to_csv(dirs["tables"] / f"{name}.csv", index=False)
        try:
            table.to_latex(
                dirs["latex"] / f"{name}.tex",
                index=False,
                float_format=lambda x: f"{x:.3f}" if isinstance(x, (float, np.floating)) else str(x),
                )
        except Exception:
            pass

    return tables


def build_markdown_summary(df: pd.DataFrame, tables: Dict[str, pd.DataFrame], notes: List[str], out_path: Path) -> None:
    lines: List[str] = []
    lines.append("# CS 223 Two-Machine Comparison Summary\n")
    lines.append("## Machines")
    lines.append(f"- **mac**: {MACHINE_STYLE['mac']['label']}")
    lines.append(f"- **windows**: {MACHINE_STYLE['windows']['label']}\n")

    lines.append("## Dataset overview")
    lines.append(f"- Rows: **{len(df)}**")
    lines.append(f"- Workloads: **{df['workload'].nunique()}**")
    lines.append(f"- Threads: **{', '.join(map(str, sorted(df['threads'].unique())))}**")
    lines.append(f"- Contention levels: **{', '.join(f'{x:.2f}' for x in sorted(df['contention'].unique()))}**")
    lines.append(f"- Hotset sizes: **{', '.join(map(str, sorted(df['hotset'].unique())))}**\n")

    prot = tables.get("protocol_comparison_within_machine", pd.DataFrame())
    mach = tables.get("machine_comparison_within_protocol", pd.DataFrame())
    wps = tables.get("workload_machine_protocol_summary", pd.DataFrame())

    if not prot.empty:
        lines.append("## OCC vs Conservative 2PL within each machine")
        for workload in sorted(df["workload"].unique()):
            lines.append(f"### Workload {workload}")
            for machine in MACHINE_ORDER:
                sub = prot[(prot["workload"] == workload) & (prot["machine"] == machine)]
                if sub.empty:
                    continue
                thr = sub.get("throughput_winner", pd.Series(dtype=str)).value_counts().to_dict()
                lat = sub.get("latency_winner", pd.Series(dtype=str)).value_counts().to_dict()
                ret = sub.get("retry_winner", pd.Series(dtype=str)).value_counts().to_dict()
                lines.append(f"- {machine}: throughput winners {thr}; latency winners {lat}; retry winners {ret}")
            lines.append("")

    if not mach.empty:
        lines.append("## Machine vs machine within each protocol")
        for workload in sorted(df["workload"].unique()):
            lines.append(f"### Workload {workload}")
            for protocol in PROTOCOL_ORDER:
                sub = mach[(mach["workload"] == workload) & (mach["protocol"] == protocol)]
                if sub.empty:
                    continue
                thr = sub.get("throughput_machine_winner", pd.Series(dtype=str)).value_counts().to_dict()
                lat = sub.get("latency_machine_winner", pd.Series(dtype=str)).value_counts().to_dict()
                ret = sub.get("retry_machine_winner", pd.Series(dtype=str)).value_counts().to_dict()
                lines.append(f"- {protocol}: throughput winners {thr}; latency winners {lat}; retry winners {ret}")
            lines.append("")

    if not wps.empty:
        lines.append("## Workload / machine / protocol averages")
        try:
            lines.append(wps.to_markdown(index=False))
            lines.append("")
        except Exception:
            pass

    if notes:
        lines.append("## Notes")
        for note in notes:
            lines.append(f"- {note}")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def parse_rt_manifest(zip_path: Path, machine: str) -> pd.DataFrame:
    rows = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            m = RT_FILE_RE.match(Path(name).name)
            if not m:
                continue
            meta = m.groupdict()
            rows.append({
                "zip_name": name,
                "workload": int(meta["workload"]),
                "protocol": PROTOCOL_LABELS.get(meta["protocol"], meta["protocol"]),
                "threads": int(meta["threads"]),
                "contention": float(meta["contention"]),
                "hotset": int(meta["hotset"]),
                "machine": machine,
            })
    return pd.DataFrame(rows)


def read_rt_slice(zip_path: Path, machine: str, threads: int, contention: float, hotset: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    manifest = parse_rt_manifest(zip_path, machine)
    if manifest.empty:
        return pd.DataFrame(), pd.DataFrame()

    manifest = manifest[
        (manifest["threads"] == threads) &
        (manifest["contention"] == contention) &
        (manifest["hotset"] == hotset)
        ].copy()
    if manifest.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows = []
    stats_rows = []
    with zipfile.ZipFile(zip_path) as zf:
        for _, meta in manifest.iterrows():
            try:
                rt = pd.read_csv(io.BytesIO(zf.read(meta["zip_name"])))
            except Exception:
                continue
            if "response_time_ms" not in rt.columns:
                continue
            if "template" not in rt.columns:
                rt["template"] = "Transaction"
            rt = rt[["template", "response_time_ms"]].copy()
            rt["workload"] = int(meta["workload"])
            rt["protocol"] = str(meta["protocol"])
            rt["threads"] = int(meta["threads"])
            rt["contention"] = float(meta["contention"])
            rt["hotset"] = int(meta["hotset"])
            rt["machine"] = machine
            rows.append(rt)

            grouped = rt.groupby("template")["response_time_ms"]
            s = grouped.agg(["count", "mean", "median", "std", "min", "max"]).reset_index()
            s["p95"] = grouped.quantile(0.95).values
            s["p99"] = grouped.quantile(0.99).values
            s["workload"] = int(meta["workload"])
            s["protocol"] = str(meta["protocol"])
            s["threads"] = int(meta["threads"])
            s["contention"] = float(meta["contention"])
            s["hotset"] = int(meta["hotset"])
            s["machine"] = machine
            stats_rows.append(s)

    if rows:
        return pd.concat(rows, ignore_index=True), pd.concat(stats_rows, ignore_index=True)
    return pd.DataFrame(), pd.DataFrame()


def pick_distribution_defaults(df: pd.DataFrame, args: argparse.Namespace) -> Tuple[int, float, int]:
    threads = args.dist_threads if args.dist_threads is not None else int(max(df["threads"].unique()))
    contention = args.dist_contention if args.dist_contention is not None else float(max(df["contention"].unique()))
    hotset = args.dist_hotset if args.dist_hotset is not None else int(min(df["hotset"].unique()))
    return threads, contention, hotset


def empirical_cdf(values: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    x = np.sort(values)
    y = np.arange(1, len(x) + 1) / len(x)
    return x, y


def _group_position_map(templates: List[str]) -> Dict[Tuple[str, str, str], float]:
    positions: Dict[Tuple[str, str, str], float] = {}
    pos = 1.0
    for template in templates:
        for machine in MACHINE_ORDER:
            for protocol in PROTOCOL_ORDER:
                positions[(template, machine, protocol)] = pos
                pos += 0.85
            pos += 0.25
        pos += 0.65
    return positions


def plot_distribution_violins(rt_df: pd.DataFrame, dirs: Dict[str, Path], dist_threads: int, dist_contention: float, dist_hotset: int, mac_label: str, windows_label: str) -> None:
    for workload in sorted(rt_df["workload"].unique()):
        wdf = rt_df[rt_df["workload"] == workload]
        if wdf.empty:
            continue
        templates = sorted(wdf["template"].dropna().unique())
        pos_map = _group_position_map(templates)
        data = []
        positions = []
        colors = []
        facecolors = []
        labels = []
        for template in templates:
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    sub = wdf[(wdf["template"] == template) & (wdf["machine"] == machine) & (wdf["protocol"] == protocol)]["response_time_ms"].dropna().to_numpy()
                    if len(sub) == 0:
                        continue
                    data.append(sub)
                    positions.append(pos_map[(template, machine, protocol)])
                    colors.append(MACHINE_STYLE[machine]["color"])
                    facecolors.append(MACHINE_STYLE[machine]["color"] if MACHINE_STYLE[machine]["filled"] else "white")
                    labels.append(f"{template}\n{machine}\n{'OCC' if protocol == 'OCC' else '2PL'}")
        if not data:
            continue

        fig, ax = plt.subplots(figsize=(max(13, 1.15 * len(data) + 6), 7.0))
        parts = ax.violinplot(data, positions=positions, widths=0.7, showmeans=False, showmedians=True, showextrema=False)
        for body, color, face in zip(parts["bodies"], colors, facecolors):
            body.set_edgecolor(color)
            body.set_linewidth(1.5)
            body.set_facecolor(face)
            body.set_alpha(0.55 if face != "white" else 1.0)
        if "cmedians" in parts:
            parts["cmedians"].set_color("black")
            parts["cmedians"].set_linewidth(1.6)

        for x, arr, color, face in zip(positions, data, colors, facecolors):
            q1, med, q3 = np.percentile(arr, [25, 50, 75])
            ax.scatter([x], [med], s=28, c="black", zorder=3)
            ax.vlines(x, q1, q3, color="black", linewidth=2.0, zorder=3)

        ax.set_title(
            f"Workload {workload}: Response-time violin plots\n"
            f"threads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}"
        )
        ax.set_ylabel("Response time (ms)")
        ax.set_xticks(positions)
        ax.set_xticklabels(labels, rotation=35, ha="right")
        vals = pd.Series(np.concatenate(data))
        set_log_scale(ax, vals, axis="y", prefer_log=True)
        add_rich_legend(ax, mac_label, windows_label)
        save_plot(fig, dirs["plots_dist"] / f"w{workload}_violin_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png")


def plot_distributions(rt_df: pd.DataFrame, rt_stats: pd.DataFrame, dirs: Dict[str, Path], dist_threads: int, dist_contention: float, dist_hotset: int, notes: List[str], mac_label: str, windows_label: str) -> None:
    if rt_df.empty:
        notes.append("No per-transaction logs were parsed for the selected slice, so distribution plots were skipped.")
        return

    notes.append(f"Distribution plots use threads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}.")

    for workload in sorted(rt_df["workload"].unique()):
        wdf = rt_df[rt_df["workload"] == workload]
        if wdf.empty:
            continue
        templates = sorted(wdf["template"].dropna().unique())

        for template in templates:
            tdf = wdf[wdf["template"] == template]
            if tdf.empty:
                continue

            fig, ax = plt.subplots(figsize=(10.8, 6.6))
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    sub = tdf[(tdf["machine"] == machine) & (tdf["protocol"] == protocol)]
                    if sub.empty:
                        continue
                    x, y = empirical_cdf(sub["response_time_ms"].to_numpy())
                    style = series_style(machine, protocol)
                    ax.plot(x, y, color=style["color"], linestyle=style["linestyle"], linewidth=2.5)
            ax.set_title(
                f"Workload {workload}: Response-time CDF for {template}\n"
                f"threads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}"
            )
            ax.set_xlabel("Response time (ms)")
            ax.set_ylabel("CDF")
            positive = tdf["response_time_ms"].dropna()
            if not positive.empty and (positive > 0).all():
                ax.set_xscale("log")
            add_rich_legend(ax, mac_label, windows_label)
            save_plot(fig, dirs["plots_dist"] / f"w{workload}_{template}_cdf_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png")

        fig, ax = plt.subplots(figsize=(max(13, 2.0 * len(templates) * 4), 7.0))
        labels = []
        data = []
        positions = []
        pos = 1
        colors = []
        for template in templates:
            for machine in MACHINE_ORDER:
                for protocol in PROTOCOL_ORDER:
                    sub = wdf[
                        (wdf["template"] == template) &
                        (wdf["machine"] == machine) &
                        (wdf["protocol"] == protocol)
                        ]["response_time_ms"].dropna().to_numpy()
                    if len(sub) == 0:
                        continue
                    labels.append(f"{template}\n{machine}\n{'OCC' if protocol == 'OCC' else '2PL'}")
                    data.append(sub)
                    positions.append(pos)
                    colors.append((machine, protocol))
                    pos += 1
                pos += 0.35
            pos += 0.8

        if data:
            bp = ax.boxplot(data, positions=positions, tick_labels=labels, showfliers=False, patch_artist=True)
            for i, patch in enumerate(bp["boxes"]):
                machine, protocol = colors[i]
                face = MACHINE_STYLE[machine]["color"] if MACHINE_STYLE[machine]["filled"] else "white"
                patch.set_facecolor(face)
                patch.set_edgecolor(MACHINE_STYLE[machine]["color"])
                patch.set_linewidth(1.5)
                patch.set_alpha(0.45 if face != "white" else 1.0)
                if protocol != "OCC":
                    patch.set_hatch("//")
            for key in ["medians", "whiskers", "caps"]:
                for artist in bp[key]:
                    artist.set_color("black")
            ax.set_title(
                f"Workload {workload}: Response-time boxplots\n"
                f"threads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}"
            )
            ax.set_ylabel("Response time (ms)")
            vals = pd.Series(np.concatenate(data))
            set_log_scale(ax, vals, axis="y", prefer_log=True)
            plt.setp(ax.get_xticklabels(), rotation=35, ha="right")
            add_rich_legend(ax, mac_label, windows_label)
            save_plot(fig, dirs["plots_dist"] / f"w{workload}_boxplot_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png")
        else:
            plt.close(fig)

    plot_distribution_violins(rt_df, dirs, dist_threads, dist_contention, dist_hotset, mac_label, windows_label)

    if not rt_stats.empty:
        rt_stats = rt_stats.sort_values(["workload", "template", "machine", "protocol"]).reset_index(drop=True)
        out_csv = dirs["tables"] / f"distribution_stats_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.csv"
        rt_stats.to_csv(out_csv, index=False)
        try:
            rt_stats.to_latex(
                dirs["latex"] / f"distribution_stats_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.tex",
                index=False,
                float_format=lambda x: f"{x:.3f}" if isinstance(x, (float, np.floating)) else str(x),
                )
        except Exception:
            pass


def load_summaries(args: argparse.Namespace) -> pd.DataFrame:
    paths = {
        "mac": Path(args.summary_mac),
        "windows": Path(args.summary_windows),
    }
    missing = [f"{machine}: {path}" for machine, path in paths.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing summary CSV files. Expected one per machine.\n" + "\n".join(missing) +
            "\nYou can override them with --summary-mac and --summary-windows."
        )

    frames = []
    for machine, path in paths.items():
        frames.append(clean_summary(pd.read_csv(path), machine))
    return pd.concat(frames, ignore_index=True)


def load_rt_for_both(args: argparse.Namespace, dist_threads: int, dist_contention: float, dist_hotset: int) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    notes: List[str] = []
    all_rt = []
    all_stats = []
    zip_map = {
        "mac": Path(args.zip_mac),
        "windows": Path(args.zip_windows),
    }
    for machine, path in zip_map.items():
        if not path.exists():
            notes.append(f"Per-transaction ZIP not found for {machine}: {path}")
            continue
        rt_df, rt_stats = read_rt_slice(path, machine, dist_threads, dist_contention, dist_hotset)
        if rt_df.empty:
            notes.append(f"No matching response-time files parsed for {machine} at threads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}.")
            continue
        all_rt.append(rt_df)
        all_stats.append(rt_stats)

    if all_rt:
        return pd.concat(all_rt, ignore_index=True), pd.concat(all_stats, ignore_index=True), notes
    return pd.DataFrame(), pd.DataFrame(), notes


def main() -> None:
    args = parse_args()
    canonicalize_machine_paths(args)
    if args.representative_slices:
        args.plot_all_slices = False
    setup_style()

    MACHINE_STYLE["mac"]["label"] = args.mac_label
    MACHINE_STYLE["windows"]["label"] = args.windows_label

    out_dir = Path(args.out)
    dirs = ensure_dirs(out_dir)

    summary_df = load_summaries(args)
    notes: List[str] = []

    notes.extend(plot_required_slices(
        summary_df,
        dirs,
        plot_all_slices=args.plot_all_slices,
        mac_label=args.mac_label,
        windows_label=args.windows_label,
    ))
    plot_heatmaps(summary_df, dirs)
    plot_pareto_panels(summary_df, dirs, args.mac_label, args.windows_label)
    plot_threads_hotset_facets(summary_df, dirs, args.mac_label, args.windows_label)
    plot_ratio_heatmaps(summary_df, dirs)
    plot_bubble_grids(summary_df, dirs)
    plot_delta_lollipop(summary_df, dirs)
    plot_contour_panels(summary_df, dirs)
    plot_machine_shift_arrows(summary_df, dirs, args.mac_label, args.windows_label)
    plot_protocol_shift_arrows(summary_df, dirs, args.mac_label, args.windows_label)
    plot_parallel_metric_profiles(summary_df, dirs, args.mac_label, args.windows_label)
    plot_winrate_panels(summary_df, dirs)
    plot_top_config_panels(summary_df, dirs)
    plot_rank_heatmaps(summary_df, dirs)
    plot_tradeoff_quadrants(summary_df, dirs, args.mac_label, args.windows_label)
    plot_pairwise_metric_scatter(summary_df, dirs, args.mac_label, args.windows_label)
    plot_speedup_efficiency(summary_df, dirs, args.mac_label, args.windows_label)
    plot_protocol_machine_bars(summary_df, dirs)
    tables = make_tables(summary_df, dirs)

    dist_threads, dist_contention, dist_hotset = pick_distribution_defaults(summary_df, args)
    rt_df, rt_stats, rt_notes = load_rt_for_both(args, dist_threads, dist_contention, dist_hotset)
    notes.extend(rt_notes)
    plot_distributions(rt_df, rt_stats, dirs, dist_threads, dist_contention, dist_hotset, notes, args.mac_label, args.windows_label)
    plot_ridgeline_distributions(rt_df, dirs, dist_threads, dist_contention, dist_hotset)
    plot_tail_latency_bars(rt_stats, dirs, dist_threads, dist_contention, dist_hotset)

    build_markdown_summary(summary_df, tables, notes, out_dir / "analysis_summary.md")

    print(f"Done. Outputs written to: {out_dir.resolve()}")
    print(f"Required plots: {dirs['plots_req'].resolve()}")
    print(f"Extra plots:    {dirs['plots_extra'].resolve()}")
    print(f"Dist plots:     {dirs['plots_dist'].resolve()}")
    print(f"Paper-grade:    {dirs['plots_rich'].resolve()}")
    print(f"Tables:         {dirs['tables'].resolve()}")


if __name__ == "__main__":
    main()
