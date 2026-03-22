#!/usr/bin/env python3
"""检查 swap_lin CSV 实际行数和时间范围"""
import os, sys
os.environ['QC_MERGE_SWAP_PATH'] = '/home/ubuntu/数据获取/data/swap_lin'
repo_root_str = '/home/ubuntu'
sys.path.insert(0, repo_root_str)
from pathlib import Path
from apps.crypto_screener.app.series_source import _default_merge_dirs, _pick_existing_csv, read_merge_csv_tail
import pandas as pd

repo_root = Path(repo_root_str)
_sw, _ = _default_merge_dirs(repo_root)
csvs = sorted(_sw.glob('*.csv'))[:5]  # 取前5个看看

for csv_path in csvs:
    try:
        # 读全部行
        df = read_merge_csv_tail(csv_path, tail=99999)
        if df.empty:
            print(f'{csv_path.name}: empty')
            continue
        print(f'{csv_path.name}: {len(df)} 行  {df["candle_begin_time"].iloc[0]} -> {df["candle_begin_time"].iloc[-1]}')
    except Exception as e:
        print(f'{csv_path.name}: ERROR {e}')

# 专门找一个行数多的文件
print('\n找行数最多的文件...')
best = None
best_lines = 0
for p in _sw.glob('*.csv'):
    try:
        with open(p, 'rb') as f:
            lines = sum(1 for _ in f)
        if lines > best_lines:
            best_lines = lines
            best = p
    except Exception:
        pass
if best:
    print(f'最多行: {best.name}  {best_lines} 行')
    df = read_merge_csv_tail(best, tail=99999)
    print(f'  读取: {len(df)} 行  {df["candle_begin_time"].iloc[0]} -> {df["candle_begin_time"].iloc[-1]}')
