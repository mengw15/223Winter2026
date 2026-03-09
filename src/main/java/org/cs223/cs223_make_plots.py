#!/usr/bin/env python3
"""
CS 223 experiment analysis script.

Reads an aggregate summary CSV and an optional ZIP containing per-transaction
response-time CSVs, then generates:
  - Required comparison plots for OCC vs Conservative 2PL
  - Retry / retry-rate plots
  - Heatmaps for throughput and latency
  - Response-time distribution plots from per-transaction logs
  - CSV / LaTeX summary tables
  - A small markdown summary file with key comparisons

Expected summary.csv columns:
    workload, protocol, threads, contention, hotset,
    transactions, committed, retries, retry_rate,
    throughput, avg_response_time

Expected ZIP entry pattern:
    rt_w{workload}_{protocol}_t{threads}_c{contention}_h{hotset}.csv
with columns like:
    template, response_time_ms

Usage:
    python cs223_make_plots.py

Default paths are resolved relative to this script and target:
    ../../../../../results/summary.csv
    ../../../../../results/results.zip
    ../../../../../results/analysis_out

You can still override them explicitly, for example:
    python cs223_make_plots.py --summary summary.csv --zip results.zip --out analysis_out
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

try:
    import seaborn as sns
    HAS_SEABORN = True
except Exception:
    HAS_SEABORN = False


PROTOCOL_LABELS = {
    "OCC": "OCC",
    "TWO_PL": "Conservative 2PL",
    "2PL": "Conservative 2PL",
    "CONSERVATIVE_2PL": "Conservative 2PL",
    "Conservative 2PL": "Conservative 2PL",
}
PROTOCOL_ORDER = ["OCC", "Conservative 2PL"]


def find_project_root() -> Path:
    """Best-effort project root discovery based on this script's location."""
    script_dir = Path(__file__).resolve().parent
    for candidate in [script_dir, *script_dir.parents]:
        if (candidate / "results").exists():
            return candidate

    # Fallback for the expected layout:
    # <project>/src/main/java/org/cs223/cs223_make_plots.py
    parents = list(script_dir.parents)
    if len(parents) >= 5:
        return parents[4]
    return script_dir


def default_paths() -> Tuple[Path, Path, Path]:
    root = find_project_root()
    results_dir = root / "results"
    return (
        results_dir / "summary.csv",
        results_dir / "results.zip",
        results_dir / "analysis_out",
    )


def parse_args() -> argparse.Namespace:
    default_summary, default_zip, default_out = default_paths()

    p = argparse.ArgumentParser(description="Generate CS 223 plots/tables from experiment outputs.")
    p.add_argument("--summary", default=str(default_summary), help="Path to summary.csv")
    p.add_argument("--zip", dest="zip_path", default=str(default_zip), help="Path to results.zip with per-transaction response times")
    p.add_argument("--out", default=str(default_out), help="Output directory")

    # Turn the optional extras on by default so a plain run generates the full bundle.
    p.set_defaults(plot_all_slices=True)
    p.add_argument("--plot-all-slices", dest="plot_all_slices", action="store_true", help="Generate every fixed-parameter slice")
    p.add_argument("--representative-slices", dest="plot_all_slices", action="store_false", help="Generate only representative fixed-parameter slices")

    p.add_argument("--dist-threads", type=int, default=None, help="Threads setting for response-time distribution plots")
    p.add_argument("--dist-contention", type=float, default=None, help="Contention setting for response-time distribution plots")
    p.add_argument("--dist-hotset", type=int, default=None, help="Hotset setting for response-time distribution plots")
    p.add_argument("--style", choices=["default", "seaborn"], default="seaborn", help="Plot style")
    return p.parse_args()


def setup_style(style: str) -> None:
    if style == "seaborn" and HAS_SEABORN:
        sns.set_theme(style="whitegrid", context="talk")
    else:
        plt.style.use("default")
        plt.rcParams.update({
            "figure.figsize": (10, 6),
            "axes.grid": True,
            "grid.alpha": 0.3,
            "axes.titlesize": 16,
            "axes.labelsize": 13,
            "legend.fontsize": 11,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
        })


def ensure_dirs(base: Path) -> Dict[str, Path]:
    dirs = {
        "base": base,
        "plots": base / "plots",
        "plots_req": base / "plots" / "required",
        "plots_extra": base / "plots" / "extra",
        "plots_dist": base / "plots" / "distributions",
        "tables": base / "tables",
        "latex": base / "tables" / "latex",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def clean_summary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    required = [
        "workload", "protocol", "threads", "contention", "hotset",
        "transactions", "committed", "retries", "retry_rate",
        "throughput", "avg_response_time",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"summary.csv is missing columns: {missing}")

    df["protocol_raw"] = df["protocol"].astype(str)
    df["protocol"] = df["protocol_raw"].map(lambda x: PROTOCOL_LABELS.get(x, x))
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

    df["protocol"] = pd.Categorical(df["protocol"], categories=PROTOCOL_ORDER, ordered=True)
    return df.sort_values(["workload", "protocol", "threads", "contention", "hotset"]).reset_index(drop=True)


def pick_middle(values: Iterable) -> Optional[float]:
    vals = sorted(set(values))
    if not vals:
        return None
    return vals[len(vals) // 2]


def save_plot(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_line_compare(
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str,
        xlabel: str,
        ylabel: str,
        path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    for protocol in PROTOCOL_ORDER:
        sub = df[df["protocol"] == protocol].sort_values(x)
        if sub.empty:
            continue
        ax.plot(sub[x], sub[y], marker="o", linewidth=2.2, label=protocol)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(title="Protocol")
    save_plot(fig, path)


def plot_required_slices(df: pd.DataFrame, dirs: Dict[str, Path], plot_all_slices: bool) -> List[str]:
    notes = []
    workloads = sorted(df["workload"].unique())

    all_threads = sorted(df["threads"].unique())
    all_contentions = sorted(df["contention"].unique())
    all_hotsets = sorted(df["hotset"].unique())

    rep_threads = int(pick_middle(all_threads))
    rep_contention = float(pick_middle(all_contentions))
    rep_hotset = int(pick_middle(all_hotsets))

    for workload in workloads:
        wdf = df[df["workload"] == workload]

        hotsets_for_threads = all_hotsets if plot_all_slices else [rep_hotset]
        for hotset in hotsets_for_threads:
            sub = wdf[(wdf["contention"] == rep_contention) & (wdf["hotset"] == hotset)]
            if not sub.empty:
                plot_line_compare(
                    sub, "threads", "throughput",
                    f"Workload {workload}: Throughput vs Threads (contention={rep_contention}, hotset={hotset})",
                    "Threads", "Throughput (committed txns/sec)",
                    dirs["plots_req"] / f"w{workload}_throughput_vs_threads_c{rep_contention:.2f}_h{hotset}.png",
                    )
                plot_line_compare(
                    sub, "threads", "avg_response_time",
                    f"Workload {workload}: Avg Response Time vs Threads (contention={rep_contention}, hotset={hotset})",
                    "Threads", "Average response time (ms)",
                    dirs["plots_req"] / f"w{workload}_latency_vs_threads_c{rep_contention:.2f}_h{hotset}.png",
                    )
            else:
                notes.append(f"Skipped workload {workload} throughput/latency vs threads for contention={rep_contention}, hotset={hotset}: no rows.")

        hotsets_for_contention = all_hotsets if plot_all_slices else [rep_hotset]
        for hotset in hotsets_for_contention:
            sub = wdf[(wdf["threads"] == rep_threads) & (wdf["hotset"] == hotset)]
            if not sub.empty:
                plot_line_compare(
                    sub, "contention", "throughput",
                    f"Workload {workload}: Throughput vs Contention (threads={rep_threads}, hotset={hotset})",
                    "Contention probability p", "Throughput (committed txns/sec)",
                    dirs["plots_req"] / f"w{workload}_throughput_vs_contention_t{rep_threads}_h{hotset}.png",
                    )
                plot_line_compare(
                    sub, "contention", "avg_response_time",
                    f"Workload {workload}: Avg Response Time vs Contention (threads={rep_threads}, hotset={hotset})",
                    "Contention probability p", "Average response time (ms)",
                    dirs["plots_req"] / f"w{workload}_latency_vs_contention_t{rep_threads}_h{hotset}.png",
                    )
                plot_line_compare(
                    sub, "contention", "retries",
                    f"Workload {workload}: Retries vs Contention (threads={rep_threads}, hotset={hotset})",
                    "Contention probability p", "Retries (count)",
                    dirs["plots_req"] / f"w{workload}_retries_vs_contention_t{rep_threads}_h{hotset}.png",
                    )
                plot_line_compare(
                    sub, "contention", "retry_rate_pct",
                    f"Workload {workload}: Retry Rate vs Contention (threads={rep_threads}, hotset={hotset})",
                    "Contention probability p", "Retry rate (%)",
                    dirs["plots_req"] / f"w{workload}_retryrate_vs_contention_t{rep_threads}_h{hotset}.png",
                    )
            else:
                notes.append(f"Skipped workload {workload} throughput/latency/retry vs contention for threads={rep_threads}, hotset={hotset}: no rows.")

    notes.append(
        f"Representative defaults used for required plots: threads={rep_threads}, contention={rep_contention}, hotset={rep_hotset}."
    )
    return notes


def plot_all_heatmaps(df: pd.DataFrame, dirs: Dict[str, Path]) -> None:
    for workload in sorted(df["workload"].unique()):
        for protocol in PROTOCOL_ORDER:
            pdf = df[(df["workload"] == workload) & (df["protocol"] == protocol)]
            if pdf.empty:
                continue
            for hotset in sorted(pdf["hotset"].unique()):
                sub = pdf[pdf["hotset"] == hotset]
                if sub.empty:
                    continue
                thr = sub.pivot(index="threads", columns="contention", values="throughput")
                lat = sub.pivot(index="threads", columns="contention", values="avg_response_time")

                for metric_name, mat, cmap in [
                    ("throughput", thr, "viridis"),
                    ("latency", lat, "magma_r"),
                ]:
                    fig, ax = plt.subplots(figsize=(8.5, 6.5))
                    im = ax.imshow(mat.values, aspect="auto", cmap=cmap, origin="lower")
                    ax.set_xticks(range(len(mat.columns)))
                    ax.set_xticklabels([f"{c:.2f}" for c in mat.columns])
                    ax.set_yticks(range(len(mat.index)))
                    ax.set_yticklabels([str(i) for i in mat.index])
                    ax.set_xlabel("Contention probability p")
                    ax.set_ylabel("Threads")
                    ax.set_title(f"Workload {workload}: {metric_name.title()} heatmap\n{protocol}, hotset={hotset}")
                    cbar = fig.colorbar(im, ax=ax)
                    cbar.set_label("Throughput" if metric_name == "throughput" else "Avg response time (ms)")
                    save_plot(fig, dirs["plots_extra"] / f"w{workload}_{protocol.replace(' ', '_')}_{metric_name}_heatmap_h{hotset}.png")


def make_summary_tables(df: pd.DataFrame, dirs: Dict[str, Path]) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}

    dataset_overview = pd.DataFrame({
        "metric": [
            "num_rows", "num_workloads", "protocols", "threads_values",
            "contention_values", "hotset_values"
        ],
        "value": [
            len(df),
            df["workload"].nunique(),
            ", ".join(map(str, df["protocol"].dropna().unique().tolist())),
            ", ".join(map(str, sorted(df["threads"].unique()))),
            ", ".join(f"{x:.2f}" for x in sorted(df["contention"].unique())),
            ", ".join(map(str, sorted(df["hotset"].unique()))),
        ]
    })
    tables["dataset_overview"] = dataset_overview

    workload_protocol_summary = (
        df.groupby(["workload", "protocol"], observed=False)
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
        )
        .reset_index()
    )
    tables["workload_protocol_summary"] = workload_protocol_summary

    best_throughput = (
        df.sort_values(["workload", "throughput", "avg_response_time"], ascending=[True, False, True])
        .groupby("workload", as_index=False)
        .head(5)
        .reset_index(drop=True)
    )
    tables["best_throughput_configs"] = best_throughput

    best_latency = (
        df.sort_values(["workload", "avg_response_time", "throughput"], ascending=[True, True, False])
        .groupby("workload", as_index=False)
        .head(5)
        .reset_index(drop=True)
    )
    tables["best_latency_configs"] = best_latency

    pivot_thr = df.pivot_table(
        index=["workload", "threads", "contention", "hotset"],
        columns="protocol",
        values="throughput",
        observed=False,
    ).reset_index()
    pivot_lat = df.pivot_table(
        index=["workload", "threads", "contention", "hotset"],
        columns="protocol",
        values="avg_response_time",
        observed=False,
    ).reset_index()
    pivot_ret = df.pivot_table(
        index=["workload", "threads", "contention", "hotset"],
        columns="protocol",
        values="retry_rate_pct",
        observed=False,
    ).reset_index()

    merged = pivot_thr.merge(pivot_lat, on=["workload", "threads", "contention", "hotset"], suffixes=("_throughput", "_latency"))
    merged = merged.merge(pivot_ret, on=["workload", "threads", "contention", "hotset"], suffixes=("", "_retry"))

    cols = merged.columns.tolist()
    # Robust column names after pivots/merges.
    def get_col(base: str, metric: str) -> Optional[str]:
        candidate = f"{base}_{metric}"
        if candidate in merged.columns:
            return candidate
        if base in merged.columns and metric == "retry":
            return base
        return None

    occ_thr = get_col("OCC", "throughput")
    pl_thr = get_col("Conservative 2PL", "throughput")
    occ_lat = get_col("OCC", "latency")
    pl_lat = get_col("Conservative 2PL", "latency")
    occ_ret = get_col("OCC", "retry")
    pl_ret = get_col("Conservative 2PL", "retry")

    paired = merged.copy()
    if occ_thr and pl_thr:
        paired["throughput_diff_occ_minus_2pl"] = paired[occ_thr] - paired[pl_thr]
        paired["throughput_pct_over_2pl"] = np.where(paired[pl_thr] != 0, 100.0 * paired["throughput_diff_occ_minus_2pl"] / paired[pl_thr], np.nan)
        paired["throughput_winner"] = np.where(paired[occ_thr] > paired[pl_thr], "OCC", np.where(paired[occ_thr] < paired[pl_thr], "Conservative 2PL", "Tie"))
    if occ_lat and pl_lat:
        paired["latency_diff_occ_minus_2pl_ms"] = paired[occ_lat] - paired[pl_lat]
        paired["latency_pct_over_2pl"] = np.where(paired[pl_lat] != 0, 100.0 * paired["latency_diff_occ_minus_2pl_ms"] / paired[pl_lat], np.nan)
        paired["latency_winner"] = np.where(paired[occ_lat] < paired[pl_lat], "OCC", np.where(paired[occ_lat] > paired[pl_lat], "Conservative 2PL", "Tie"))
    if occ_ret and pl_ret:
        paired["retry_rate_diff_occ_minus_2pl_pct"] = paired[occ_ret] - paired[pl_ret]
        paired["retry_winner"] = np.where(paired[occ_ret] < paired[pl_ret], "OCC", np.where(paired[occ_ret] > paired[pl_ret], "Conservative 2PL", "Tie"))

    tables["paired_protocol_comparison"] = paired

    for name, table in tables.items():
        csv_path = dirs["tables"] / f"{name}.csv"
        table.to_csv(csv_path, index=False)
        try:
            latex_path = dirs["latex"] / f"{name}.tex"
            table.to_latex(latex_path, index=False, float_format=lambda x: f"{x:.3f}" if isinstance(x, (float, np.floating)) else str(x))
        except Exception:
            pass

    return tables


def build_markdown_summary(df: pd.DataFrame, tables: Dict[str, pd.DataFrame], notes: List[str], out_path: Path) -> None:
    paired = tables.get("paired_protocol_comparison", pd.DataFrame())
    wps = tables.get("workload_protocol_summary", pd.DataFrame())

    lines: List[str] = []
    lines.append("# CS 223 Experiment Summary\n")
    lines.append("## Dataset overview")
    lines.append(f"- Rows in summary.csv: **{len(df)}**")
    lines.append(f"- Workloads: **{df['workload'].nunique()}**")
    lines.append(f"- Protocols: **{', '.join(map(str, df['protocol'].dropna().unique().tolist()))}**")
    lines.append(f"- Threads tested: **{', '.join(map(str, sorted(df['threads'].unique())))}**")
    lines.append(f"- Contention levels tested: **{', '.join(f'{x:.2f}' for x in sorted(df['contention'].unique()))}**")
    lines.append(f"- Hotset sizes tested: **{', '.join(map(str, sorted(df['hotset'].unique())))}**\n")

    if not paired.empty:
        lines.append("## Pairwise OCC vs Conservative 2PL comparison")
        for workload in sorted(df["workload"].unique()):
            sub = paired[paired["workload"] == workload]
            if sub.empty:
                continue
            thr_counts = sub.get("throughput_winner", pd.Series(dtype=str)).value_counts(dropna=False).to_dict()
            lat_counts = sub.get("latency_winner", pd.Series(dtype=str)).value_counts(dropna=False).to_dict()
            ret_counts = sub.get("retry_winner", pd.Series(dtype=str)).value_counts(dropna=False).to_dict()
            lines.append(f"### Workload {workload}")
            lines.append(f"- Throughput winner counts: {thr_counts}")
            lines.append(f"- Latency winner counts: {lat_counts}")
            lines.append(f"- Retry-rate winner counts: {ret_counts}")
            if "throughput_pct_over_2pl" in sub.columns:
                lines.append(f"- Mean OCC throughput delta vs 2PL: **{sub['throughput_pct_over_2pl'].mean():.2f}%**")
            if "latency_pct_over_2pl" in sub.columns:
                lines.append(f"- Mean OCC latency delta vs 2PL: **{sub['latency_pct_over_2pl'].mean():.2f}%**")
            if "retry_rate_diff_occ_minus_2pl_pct" in sub.columns:
                lines.append(f"- Mean OCC retry-rate delta vs 2PL: **{sub['retry_rate_diff_occ_minus_2pl_pct'].mean():.2f} percentage points**\n")

    if not wps.empty:
        lines.append("## Workload/protocol averages")
        lines.append(wps.to_markdown(index=False))
        lines.append("")

    if notes:
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")

    out_path.write_text("\n".join(lines), encoding="utf-8")


RT_FILE_RE = re.compile(r"rt_w(?P<workload>\d+)_(?P<protocol>.+?)_t(?P<threads>\d+)_c(?P<contention>\d+(?:\.\d+)?)_h(?P<hotset>\d+)\.csv$")


def parse_rt_manifest(zip_path: Path) -> pd.DataFrame:
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
            })
    return pd.DataFrame(rows)


def read_rt_slice(zip_path: Path, threads: int, contention: float, hotset: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    manifest = parse_rt_manifest(zip_path)
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


def plot_distributions(rt_df: pd.DataFrame, rt_stats: pd.DataFrame, dirs: Dict[str, Path], dist_threads: int, dist_contention: float, dist_hotset: int, notes: List[str]) -> None:
    if rt_df.empty:
        notes.append("No per-transaction response-time files were found, so distribution plots were skipped.")
        return

    notes.append(
        f"Distribution plots use threads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}. Override with --dist-threads/--dist-contention/--dist-hotset if needed."
    )

    chosen = rt_df.copy()

    for workload in sorted(chosen["workload"].unique()):
        wdf = chosen[chosen["workload"] == workload]
        if wdf.empty:
            continue
        templates = sorted(wdf["template"].dropna().unique())

        for template in templates:
            fig, ax = plt.subplots(figsize=(10, 6))
            tdf = wdf[wdf["template"] == template]
            for protocol in PROTOCOL_ORDER:
                sub = tdf[tdf["protocol"] == protocol]
                if sub.empty:
                    continue
                x, y = empirical_cdf(sub["response_time_ms"].to_numpy())
                ax.plot(x, y, linewidth=2, label=protocol)
            ax.set_title(f"Workload {workload}: Response-time CDF for {template}\nthreads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}")
            ax.set_xlabel("Response time (ms)")
            ax.set_ylabel("CDF")
            ax.legend(title="Protocol")
            save_plot(fig, dirs["plots_dist"] / f"w{workload}_{template}_cdf_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png")

        fig, ax = plt.subplots(figsize=(max(10, 2.8 * len(templates)), 6.5))
        order = []
        box_data = []
        for template in templates:
            for protocol in PROTOCOL_ORDER:
                sub = wdf[(wdf["template"] == template) & (wdf["protocol"] == protocol)]["response_time_ms"].dropna().to_numpy()
                if len(sub) == 0:
                    continue
                order.append(f"{template}\n{protocol}")
                box_data.append(sub)
        if box_data:
            ax.boxplot(box_data, tick_labels=order, showfliers=False)
            ax.set_title(f"Workload {workload}: Response-time distribution by template/protocol\nthreads={dist_threads}, contention={dist_contention}, hotset={dist_hotset}")
            ax.set_ylabel("Response time (ms)")
            save_plot(fig, dirs["plots_dist"] / f"w{workload}_boxplot_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.png")
        else:
            plt.close(fig)

    if not rt_stats.empty:
        chosen_stats = rt_stats.sort_values(["workload", "template", "protocol"]).copy()
        chosen_stats.to_csv(dirs["tables"] / f"distribution_stats_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.csv", index=False)
        try:
            chosen_stats.to_latex(
                dirs["latex"] / f"distribution_stats_t{dist_threads}_c{dist_contention:.2f}_h{dist_hotset}.tex",
                index=False,
                float_format=lambda x: f"{x:.3f}" if isinstance(x, (float, np.floating)) else str(x),
                )
        except Exception:
            pass


def main() -> None:
    args = parse_args()
    setup_style(args.style)
    out_dir = Path(args.out)
    dirs = ensure_dirs(out_dir)

    summary_path = Path(args.summary)
    if not summary_path.exists():
        raise FileNotFoundError(f"summary file not found: {summary_path}")

    summary_df = clean_summary(pd.read_csv(summary_path))
    notes: List[str] = []

    required_notes = plot_required_slices(summary_df, dirs, plot_all_slices=args.plot_all_slices)
    notes.extend(required_notes)
    plot_all_heatmaps(summary_df, dirs)
    tables = make_summary_tables(summary_df, dirs)

    rt_df = pd.DataFrame()
    rt_stats = pd.DataFrame()
    if args.zip_path:
        zip_path = Path(args.zip_path)
        if zip_path.exists():
            dist_threads, dist_contention, dist_hotset = pick_distribution_defaults(summary_df, args)
            rt_df, rt_stats = read_rt_slice(zip_path, dist_threads, dist_contention, dist_hotset)
            if not rt_df.empty:
                plot_distributions(rt_df, rt_stats, dirs, dist_threads, dist_contention, dist_hotset, notes)
            else:
                notes.append("results.zip was provided, but no matching per-transaction response-time files were parsed for the selected distribution slice.")
        else:
            notes.append(f"results.zip path not found: {zip_path}")
    else:
        notes.append("No results.zip supplied. Aggregate plots/tables were created, but response-time distribution plots were skipped.")

    build_markdown_summary(summary_df, tables, notes, out_dir / "analysis_summary.md")

    print(f"Done. Outputs written to: {out_dir.resolve()}")
    print(f"Required plots: {dirs['plots_req'].resolve()}")
    print(f"Extra plots:    {dirs['plots_extra'].resolve()}")
    print(f"Dist plots:     {dirs['plots_dist'].resolve()}")
    print(f"Tables:         {dirs['tables'].resolve()}")


if __name__ == "__main__":
    main()
