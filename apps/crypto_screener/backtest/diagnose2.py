#!/usr/bin/env python3
"""
服务器数据结构诊断 — 在服务器上运行：
    python apps/crypto_screener/backtest/diagnose2.py
"""
import sys, os, pickle
from pathlib import Path

PKL_DIR = Path('/home/ubuntu/数据获取/data/preprocessed_hourly/pkl_cache')

print('=' * 60)
print('【1】检查 series_swap.pkl 结构')
fp = PKL_DIR / 'series_swap.pkl'
if not fp.exists():
    print('NOT FOUND:', fp); sys.exit(1)

with fp.open('rb') as f:
    data = pickle.load(f)

print(f'  类型: {type(data)}')
if isinstance(data, dict):
    print(f'  顶层 keys 数量: {len(data)}')
    sample_keys = list(data.keys())[:5]
    print(f'  示例 keys: {sample_keys}')
    # 看第一个值的结构
    first_key = list(data.keys())[0]
    first_val = data[first_key]
    print(f'  第一个值类型: {type(first_val)}')
    if hasattr(first_val, 'columns'):
        print(f'  columns: {list(first_val.columns)[:10]}')
        print(f'  shape: {first_val.shape}')
        print(f'  index sample: {first_val.index[:3].tolist()}')
    elif isinstance(first_val, dict):
        print(f'  dict keys: {list(first_val.keys())[:10]}')
elif isinstance(data, list):
    print(f'  list 长度: {len(data)}')
    print(f'  第一个元素类型: {type(data[0])}')
else:
    print(f'  未知结构: {data}')

print()
print('【2】检查 factors_swap.pkl 结构')
fp2 = PKL_DIR / 'factors_swap.pkl'
if not fp2.exists():
    print('NOT FOUND:', fp2)
else:
    with fp2.open('rb') as f:
        data2 = pickle.load(f)
    print(f'  类型: {type(data2)}')
    if isinstance(data2, dict):
        print(f'  顶层 keys: {list(data2.keys())[:10]}')
        # 检查 factors 子字典
        if 'factors' in data2:
            fac = data2['factors']
            print(f'  factors 数量: {len(fac)}')
            first_sym = list(fac.keys())[0]
            print(f'  示例符号: {first_sym}  keys: {list(fac[first_sym].keys())[:8]}')

print()
print('【3】检查 CSV merge 路径')
for p in [
    Path('/home/ubuntu/数据获取/data/merge_swap'),
    Path('/home/ubuntu/数据获取/data/swap_merge'),
    Path('/home/ubuntu/数据获取/merge/swap'),
    Path('/home/ubuntu/apps/crypto_screener/data/merge_swap'),
]:
    print(f'  {p}  exists={p.exists()}')
    if p.exists():
        csvs = list(p.glob('*.csv'))
        print(f'    CSV 文件数: {len(csvs)}  示例: {[c.name for c in csvs[:3]]}')

print()
print('【4】检查 web/data/latest.json')
for lp in [
    Path('/home/ubuntu/apps/crypto_screener/web/data/latest.json'),
    Path('/home/ubuntu/apps/web/data/latest.json'),
]:
    print(f'  {lp}  exists={lp.exists()}')
    if lp.exists():
        import json
        j = json.loads(lp.read_text(encoding='utf-8'))
        results = j.get('results', [])
        print(f'  results 数量: {len(results)}')
        if results:
            print(f'  示例: {results[0]}')

print()
print('=' * 60)
print('诊断完成，请把输出结果发给开发者')
