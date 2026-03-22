#!/usr/bin/env python3
"""检查服务器 CSV 数据路径"""
import os
from pathlib import Path

candidates = [
    '/home/ubuntu/数据获取/data/swap_lin',
    '/home/ubuntu/数据获取/data/spot_lin',
    '/home/ubuntu/数据获取/data/swap_merge',
    '/home/ubuntu/数据获取/data/spot_merge',
    '/home/ubuntu/数据获取/data/data_center/kline/swap',
    '/home/ubuntu/数据获取/data/data_center/kline/spot',
]

for p in candidates:
    pp = Path(p)
    exists = pp.exists()
    print(f'{p}  exists={exists}', end='')
    if exists:
        csvs = list(pp.glob('*.csv'))
        zips = list(pp.glob('*.zip'))
        print(f'  csv={len(csvs)}  zip={len(zips)}', end='')
        if csvs:
            # 检查第一个 CSV 的行数和列名
            import pandas as pd
            try:
                df = pd.read_csv(csvs[0], nrows=3)
                print(f'  cols={list(df.columns)}  示例={csvs[0].name}', end='')
            except Exception as e:
                print(f'  read_err={e}', end='')
    print()

# 检查 series pkl 里记录的实际 merge_dir
import pickle
pkl_dir = Path('/home/ubuntu/数据获取/data/preprocessed_hourly/pkl_cache')
for mkt in ('swap', 'spot'):
    fp = pkl_dir / f'series_{mkt}.pkl'
    if fp.exists():
        with fp.open('rb') as f:
            d = pickle.load(f)
        meta = d.get('meta', {})
        print(f'\nseries_{mkt}.pkl meta:')
        print(f'  merge_dir: {meta.get("merge_dir")}')
        print(f'  data_center_dir: {meta.get("data_center_dir")}')
        print(f'  tail: {meta.get("tail")}')
        print(f'  generated_at: {meta.get("generated_at")}')
        # 检查 merge_dir 是否存在
        md = Path(meta.get('merge_dir', ''))
        if md.exists():
            csvs = list(md.glob('*.csv'))
            print(f'  merge_dir exists! csv={len(csvs)}')
            if csvs:
                import pandas as pd
                df = pd.read_csv(csvs[0], nrows=5)
                print(f'  cols: {list(df.columns)}')
                print(f'  行数示例（{csvs[0].name}）: 需要进一步确认')
                # 实际行数
                with open(csvs[0]) as ff:
                    lines = sum(1 for _ in ff)
                print(f'  行数: {lines}')
        else:
            print(f'  merge_dir 不存在: {md}')
