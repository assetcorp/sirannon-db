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
    import pandas as pd
except ImportError:
    print('Missing dependencies. Install them with:')
    print('  pip install matplotlib pandas')
    sys.exit(1)


COLORS = {
    'sirannon': '#2563eb',
    'postgres': '#64748b',
}


def find_csvs(results_dir: str) -> dict[str, list[str]]:
    """Group CSV files by type (comparison, scaling, engine)."""
    groups: dict[str, list[str]] = {
        'comparison': [],
        'scaling': [],
        'engine': [],
    }
    for path in sorted(glob(os.path.join(results_dir, '*.csv'))):
        name = os.path.basename(path)
        if 'per-run' in name:
            continue
        if 'scaling' in name:
            groups['scaling'].append(path)
        elif 'engine-' in name and 'scaling' not in name:
            groups['engine'].append(path)
        else:
            groups['comparison'].append(path)
    return groups


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
        df['label'] = df['workload'] + ' (' + df['dataSize'].astype(str) + ' rows)'
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
        df['label'] = df['workload'] + ' (' + df['dataSize'].astype(str) + ')'
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

    print(f'\nCharts written to {charts_dir}/')


if __name__ == '__main__':
    main()
