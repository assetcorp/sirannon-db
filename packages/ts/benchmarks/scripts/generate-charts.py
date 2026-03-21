#!/usr/bin/env python3
"""Generate SVG charts from benchmark CSV files.

Usage:
    python3 benchmarks/scripts/generate-charts.py benchmarks/results/

Reads CSV files from the given directory and writes SVG charts to
a charts/ subdirectory within it. Requires matplotlib and pandas:

    pip install matplotlib pandas
"""

import os
import sys
from glob import glob
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
except ImportError:
    print('Missing dependencies. Install them with:')
    print('  pip install matplotlib pandas')
    sys.exit(1)


COLORS = {
    'sirannon': '#2563eb',
    'postgres': '#64748b',
    'accent': '#f59e0b',
}


def find_csvs(results_dir: str) -> dict[str, list[str]]:
    """Group CSV files by type."""
    groups: dict[str, list[str]] = {
        'comparison': [],
        'scaling': [],
        'engine': [],
        'feature': [],
        'per_run': [],
    }
    for path in sorted(glob(os.path.join(results_dir, '*.csv'))):
        name = os.path.basename(path)
        if 'per-run' in name:
            groups['per_run'].append(path)
            continue
        if 'scaling' in name or 'concurrency' in name:
            groups['scaling'].append(path)
        elif 'engine-' in name and 'scaling' not in name:
            groups['engine'].append(path)
        elif is_feature_csv(path):
            groups['feature'].append(path)
        else:
            groups['comparison'].append(path)
    return groups


def is_feature_csv(csv_path: str) -> bool:
    """Check if a CSV uses the feature benchmark format."""
    try:
        df = pd.read_csv(csv_path, nrows=1)
        return 'benchmarkType' in df.columns and df['benchmarkType'].iloc[0] == 'feature'
    except Exception:
        return False


def format_ops(ops: float) -> str:
    """Format ops/sec for chart labels."""
    if ops >= 1_000_000:
        return f'{ops / 1_000_000:.1f}M'
    if ops >= 1_000:
        return f'{ops / 1_000:.1f}K'
    return f'{ops:.0f}'


def generate_speedup_chart(csv_path: str, charts_dir: str) -> None:
    """Horizontal bar chart of speedup ratios with CI error bars when available."""
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f'  Warning: skipping {csv_path}: {e}')
        return

    required = {'workload', 'speedup'}
    if not required.issubset(df.columns):
        print(f'  Warning: skipping {csv_path}: missing columns {required - set(df.columns)}')
        return

    label_col = 'workload'
    if 'dataSize' in df.columns:
        df['label'] = df['workload'] + ' (' + df['dataSize'].apply(lambda x: f'{x:,}') + ' rows)'
        label_col = 'label'

    fig, ax = plt.subplots(figsize=(10, max(4, len(df) * 0.45)))

    y_pos = range(len(df))
    bars = ax.barh(y_pos, df['speedup'], color=COLORS['sirannon'], height=0.6)

    if 'ciLower' in df.columns and 'ciUpper' in df.columns:
        ci_lower = df['ciLower']
        ci_upper = df['ciUpper']
        has_ci = ci_lower.notna() & ci_upper.notna()
        if has_ci.any():
            xerr_low = (df['speedup'] - ci_lower).clip(lower=0)
            xerr_high = (ci_upper - df['speedup']).clip(lower=0)
            for i, (bar, has) in enumerate(zip(bars, has_ci)):
                if has:
                    ax.errorbar(
                        df['speedup'].iloc[i], i,
                        xerr=[[xerr_low.iloc[i]], [xerr_high.iloc[i]]],
                        fmt='none', color='black', capsize=3, linewidth=1,
                    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(df[label_col], fontsize=9)
    ax.set_xlabel('Speedup (Sirannon / Postgres)')
    ax.axvline(x=1.0, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    ax.set_title('Sirannon vs Postgres: Speedup by Workload')
    ax.invert_yaxis()

    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + 0.05, bar.get_y() + bar.get_height() / 2,
            f'{width:.1f}x', va='center', fontsize=8,
        )

    plt.tight_layout()
    stem = Path(csv_path).stem
    out_path = os.path.join(charts_dir, f'{stem}-speedup.svg')
    fig.savefig(out_path, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f'  Created {out_path}')


def generate_scaling_chart(csv_path: str, charts_dir: str) -> None:
    """Line chart of ops/sec vs concurrency for event-loop and worker-threads."""
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f'  Warning: skipping {csv_path}: {e}')
        return

    required = {'model', 'concurrency', 'sirannonOpsPerSec', 'postgresOpsPerSec'}
    if not required.issubset(df.columns):
        print(f'  Warning: skipping {csv_path}: missing columns {required - set(df.columns)}')
        return

    models = df['model'].unique()
    workloads = df['workload'].unique() if 'workload' in df.columns else ['all']

    fig, axes = plt.subplots(1, len(models), figsize=(7 * len(models), 5), squeeze=False)

    line_styles = ['-', '--', ':', '-.']

    for col_idx, model in enumerate(models):
        ax = axes[0][col_idx]
        model_df = df[df['model'] == model]

        for wl_idx, wl in enumerate(workloads):
            if 'workload' in model_df.columns:
                wl_df = model_df[model_df['workload'] == wl].sort_values('concurrency')
            else:
                wl_df = model_df.sort_values('concurrency')

            ls = line_styles[wl_idx % len(line_styles)]
            ax.plot(
                wl_df['concurrency'], wl_df['sirannonOpsPerSec'],
                marker='o', color=COLORS['sirannon'], linestyle=ls,
                label=f'Sirannon ({wl})', markersize=4,
            )
            ax.plot(
                wl_df['concurrency'], wl_df['postgresOpsPerSec'],
                marker='s', color=COLORS['postgres'], linestyle=ls,
                label=f'Postgres ({wl})', markersize=4,
            )

        ax.set_xlabel('Concurrency')
        ax.set_ylabel('ops/sec')
        ax.set_title(model.replace('-', ' ').title())
        ax.legend(fontsize=7, loc='best')
        ax.set_xscale('log', base=2)
        ax.grid(True, alpha=0.3)

    plt.suptitle('Concurrency Scaling', fontsize=13, y=1.02)
    plt.tight_layout()

    stem = Path(csv_path).stem
    out_path = os.path.join(charts_dir, f'{stem}-scaling.svg')
    fig.savefig(out_path, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f'  Created {out_path}')


def generate_latency_chart(csv_path: str, charts_dir: str) -> None:
    """Grouped bar chart comparing P50 and P99 latencies between engines."""
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f'  Warning: skipping {csv_path}: {e}')
        return

    required = {'workload', 'sirannonP50Ns', 'sirannonP99Ns', 'postgresP50Ns', 'postgresP99Ns'}
    if not required.issubset(df.columns):
        return

    label_col = 'workload'
    if 'dataSize' in df.columns:
        df['label'] = df['workload'] + ' (' + df['dataSize'].apply(lambda x: f'{x:,}') + ')'
        label_col = 'label'

    ns_to_us = 1 / 1_000
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, max(4, len(df) * 0.4)))

    x = range(len(df))
    width = 0.35

    ax1.barh([i - width/2 for i in x], df['sirannonP50Ns'] * ns_to_us, width, label='Sirannon', color=COLORS['sirannon'])
    ax1.barh([i + width/2 for i in x], df['postgresP50Ns'] * ns_to_us, width, label='Postgres', color=COLORS['postgres'])
    ax1.set_yticks(x)
    ax1.set_yticklabels(df[label_col], fontsize=8)
    ax1.set_xlabel('P50 Latency (us)')
    ax1.set_title('Median Latency (P50)')
    ax1.legend(fontsize=8)
    ax1.invert_yaxis()

    ax2.barh([i - width/2 for i in x], df['sirannonP99Ns'] * ns_to_us, width, label='Sirannon', color=COLORS['sirannon'])
    ax2.barh([i + width/2 for i in x], df['postgresP99Ns'] * ns_to_us, width, label='Postgres', color=COLORS['postgres'])
    ax2.set_yticks(x)
    ax2.set_yticklabels(df[label_col], fontsize=8)
    ax2.set_xlabel('P99 Latency (us)')
    ax2.set_title('Tail Latency (P99)')
    ax2.legend(fontsize=8)
    ax2.invert_yaxis()

    plt.suptitle('Latency Comparison', fontsize=13, y=1.02)
    plt.tight_layout()

    stem = Path(csv_path).stem
    out_path = os.path.join(charts_dir, f'{stem}-latency.svg')
    fig.savefig(out_path, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f'  Created {out_path}')


def generate_feature_chart(csv_path: str, charts_dir: str) -> None:
    """Horizontal bar chart for Sirannon-only feature benchmarks."""
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f'  Warning: skipping {csv_path}: {e}')
        return

    required = {'workload', 'opsPerSec'}
    if not required.issubset(df.columns):
        print(f'  Warning: skipping {csv_path}: missing columns {required - set(df.columns)}')
        return

    fig, ax = plt.subplots(figsize=(10, max(3, len(df) * 0.6)))

    y_pos = range(len(df))
    bars = ax.barh(y_pos, df['opsPerSec'], color=COLORS['sirannon'], height=0.5)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(df['workload'], fontsize=9)
    ax.set_xlabel('ops/sec')

    category = Path(csv_path).stem.rsplit('-', 6)[0]
    ax.set_title(f'Sirannon Feature: {category}')
    ax.invert_yaxis()
    ax.grid(True, axis='x', alpha=0.3)

    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + max(df['opsPerSec']) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            format_ops(width),
            va='center', fontsize=9,
        )

    plt.tight_layout()
    stem = Path(csv_path).stem
    out_path = os.path.join(charts_dir, f'{stem}-feature.svg')
    fig.savefig(out_path, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f'  Created {out_path}')

    if 'p50Ns' in df.columns and 'p99Ns' in df.columns:
        non_zero = df[(df['p50Ns'] > 0) | (df['p99Ns'] > 0)]
        if len(non_zero) > 0:
            generate_feature_latency_chart(non_zero, csv_path, charts_dir)


def generate_feature_latency_chart(df: pd.DataFrame, csv_path: str, charts_dir: str) -> None:
    """P50/P99 bar chart for feature benchmarks that have latency data."""
    ns_to_us = 1 / 1_000
    fig, ax = plt.subplots(figsize=(10, max(3, len(df) * 0.6)))

    x = np.arange(len(df))
    width = 0.35

    ax.barh(x - width/2, df['p50Ns'] * ns_to_us, width, label='P50', color=COLORS['sirannon'])
    ax.barh(x + width/2, df['p99Ns'] * ns_to_us, width, label='P99', color=COLORS['accent'])

    ax.set_yticks(x)
    ax.set_yticklabels(df['workload'], fontsize=9)
    ax.set_xlabel('Latency (us)')

    category = Path(csv_path).stem.rsplit('-', 6)[0]
    ax.set_title(f'Sirannon Feature Latency: {category}')
    ax.legend(fontsize=8)
    ax.invert_yaxis()
    ax.grid(True, axis='x', alpha=0.3)

    plt.tight_layout()
    stem = Path(csv_path).stem
    out_path = os.path.join(charts_dir, f'{stem}-feature-latency.svg')
    fig.savefig(out_path, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f'  Created {out_path}')


def generate_per_run_boxplot(csv_path: str, charts_dir: str) -> None:
    """Box plot showing ops/sec distribution across runs for each workload."""
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f'  Warning: skipping {csv_path}: {e}')
        return

    required = {'workload', 'sirannonOpsPerSec', 'postgresOpsPerSec'}
    if not required.issubset(df.columns):
        print(f'  Warning: skipping {csv_path}: missing columns {required - set(df.columns)}')
        return

    if 'dataSize' in df.columns:
        df['label'] = df['workload'] + ' (' + df['dataSize'].apply(lambda x: f'{x:,}') + ')'
    else:
        df['label'] = df['workload']

    workloads = df['label'].unique()
    n_workloads = len(workloads)

    fig, axes = plt.subplots(1, n_workloads, figsize=(5 * n_workloads, 5), squeeze=False)

    for idx, wl in enumerate(workloads):
        ax = axes[0][idx]
        wl_df = df[df['label'] == wl]

        sirannon_data = wl_df['sirannonOpsPerSec'].values
        postgres_data = wl_df['postgresOpsPerSec'].values

        bp = ax.boxplot(
            [sirannon_data, postgres_data],
            tick_labels=['Sirannon', 'Postgres'],
            patch_artist=True,
            widths=0.5,
        )

        bp['boxes'][0].set_facecolor(COLORS['sirannon'])
        bp['boxes'][0].set_alpha(0.7)
        bp['boxes'][1].set_facecolor(COLORS['postgres'])
        bp['boxes'][1].set_alpha(0.7)

        for box in bp['boxes']:
            box.set_edgecolor('black')
            box.set_linewidth(0.8)
        for whisker in bp['whiskers']:
            whisker.set_linewidth(0.8)
        for median in bp['medians']:
            median.set_color('black')
            median.set_linewidth(1.5)

        ax.set_ylabel('ops/sec')
        ax.set_title(wl, fontsize=10)
        ax.grid(True, axis='y', alpha=0.3)

        ax.text(
            1, sirannon_data.mean(),
            f'  {format_ops(sirannon_data.mean())}',
            va='center', fontsize=8, color=COLORS['sirannon'],
        )
        ax.text(
            2, postgres_data.mean(),
            f'  {format_ops(postgres_data.mean())}',
            va='center', fontsize=8, color=COLORS['postgres'],
        )

    category = Path(csv_path).stem.replace('-per-run', '').rsplit('-', 6)[0]
    plt.suptitle(f'{category}: Per-Run Distribution ({len(wl_df)} runs)', fontsize=13, y=1.02)
    plt.tight_layout()

    stem = Path(csv_path).stem
    out_path = os.path.join(charts_dir, f'{stem}-boxplot.svg')
    fig.savefig(out_path, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f'  Created {out_path}')


def main() -> None:
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <results-directory>')
        sys.exit(1)

    results_dir = sys.argv[1]
    if not os.path.isdir(results_dir):
        print(f'Directory not found: {results_dir}')
        sys.exit(1)

    charts_dir = os.path.join(results_dir, 'charts')
    os.makedirs(charts_dir, exist_ok=True)

    groups = find_csvs(results_dir)
    total = sum(len(v) for v in groups.values())

    if total == 0:
        print(f'No CSV files found in {results_dir}.')
        print('Run benchmarks first to generate CSV data, e.g.:')
        print('  pnpm bench:micro')
        sys.exit(1)

    print(f'Found {total} CSV file(s) in {results_dir}\n')

    for csv_path in groups['comparison']:
        print(f'Processing comparison: {os.path.basename(csv_path)}')
        generate_speedup_chart(csv_path, charts_dir)
        generate_latency_chart(csv_path, charts_dir)

    for csv_path in groups['engine']:
        print(f'Processing engine: {os.path.basename(csv_path)}')
        generate_speedup_chart(csv_path, charts_dir)
        generate_latency_chart(csv_path, charts_dir)

    for csv_path in groups['scaling']:
        print(f'Processing scaling: {os.path.basename(csv_path)}')
        generate_scaling_chart(csv_path, charts_dir)
        generate_latency_chart(csv_path, charts_dir)

    for csv_path in groups['feature']:
        print(f'Processing feature: {os.path.basename(csv_path)}')
        generate_feature_chart(csv_path, charts_dir)

    for csv_path in groups['per_run']:
        print(f'Processing per-run: {os.path.basename(csv_path)}')
        generate_per_run_boxplot(csv_path, charts_dir)

    create_stable_names(charts_dir)
    print(f'\nCharts written to {charts_dir}/')


STABLE_NAME_MAP = {
    'micro-point-select': {
        'speedup': 'point-select-speedup.svg',
        'latency': 'point-select-latency.svg',
    },
    'micro-point-select-per-run': {
        'boxplot': 'point-select-boxplot.svg',
    },
    'micro-batch-update': {
        'speedup': 'batch-update-speedup.svg',
    },
    'micro-batch-update-per-run': {
        'boxplot': 'batch-update-boxplot.svg',
    },
    'micro-bulk-insert': {
        'speedup': 'bulk-insert-speedup.svg',
    },
    'ycsb-a': {
        'speedup': 'ycsb-a-speedup.svg',
        'latency': 'ycsb-a-latency.svg',
    },
    'ycsb-a-per-run': {
        'boxplot': 'ycsb-a-boxplot.svg',
    },
    'oltp-tpc-c-lite': {
        'speedup': 'tpc-c-lite-speedup.svg',
        'latency': 'tpc-c-lite-latency.svg',
    },
    'oltp-tpc-c-lite-per-run': {
        'boxplot': 'tpc-c-lite-boxplot.svg',
    },
    'pool-sweep': {
        'speedup': 'pool-sweep-speedup.svg',
    },
    'cdc-latency': {
        'feature': 'cdc-throughput.svg',
    },
    'cold-start': {
        'feature': 'cold-start.svg',
    },
    'connection-pool': {
        'feature': 'connection-pool.svg',
    },
    'multi-tenant': {
        'feature': 'multi-tenant.svg',
    },
}


SCALE_STABLE_NAMES = {
    'micro-point-select': 'point-select-scale-speedup.svg',
    'micro-batch-update': 'batch-update-scale-speedup.svg',
    'ycsb-a': 'ycsb-a-scale-speedup.svg',
}


def create_stable_names(charts_dir: str) -> None:
    """Copy the latest timestamped chart for each category to a stable name.

    BENCHMARKS.md references these stable names so the embedded images
    survive chart regeneration without manual renaming.

    For benchmarks with both statistical (10-run) and scale (2-run, more data sizes)
    CSVs, the scale run gets a separate '-scale-speedup' stable name. The main
    stable name maps to the run with the most data sizes (scale run), while the
    statistical run's per-run boxplots get their own stable names.
    """
    import shutil

    svg_files = sorted(glob(os.path.join(charts_dir, '*.svg')))

    best: dict[tuple[str, str], str] = {}
    for svg_path in svg_files:
        name = os.path.basename(svg_path)
        if '-2026' not in name:
            continue
        for prefix, chart_types in STABLE_NAME_MAP.items():
            if not name.startswith(prefix + '-2026'):
                continue
            for chart_type in chart_types:
                if name.endswith(f'-{chart_type}.svg'):
                    key = (prefix, chart_type)
                    if key not in best or svg_path > best[key]:
                        best[key] = svg_path

    if not best:
        return

    print('\nCreating stable-named charts:')
    for (prefix, chart_type), src_path in sorted(best.items()):
        stable_name = STABLE_NAME_MAP[prefix][chart_type]
        dest_path = os.path.join(charts_dir, stable_name)
        shutil.copy2(src_path, dest_path)
        print(f'  {stable_name} <- {os.path.basename(src_path)}')

    all_speedups: dict[str, list[str]] = {}
    for svg_path in svg_files:
        name = os.path.basename(svg_path)
        if '-2026' not in name or not name.endswith('-speedup.svg'):
            continue
        for prefix in SCALE_STABLE_NAMES:
            if name.startswith(prefix + '-2026'):
                all_speedups.setdefault(prefix, []).append(svg_path)

    for prefix, paths in all_speedups.items():
        if len(paths) >= 2:
            sorted_paths = sorted(paths)
            scale_path = sorted_paths[-1]
            stable_name = SCALE_STABLE_NAMES[prefix]
            dest_path = os.path.join(charts_dir, stable_name)
            shutil.copy2(scale_path, dest_path)
            print(f'  {stable_name} <- {os.path.basename(scale_path)}')


if __name__ == '__main__':
    main()
