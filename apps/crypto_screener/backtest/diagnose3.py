#!/usr/bin/env python3
"""检查 series_swap.pkl 的 symbols 字段结构"""
import pickle
from pathlib import Path

PKL_DIR = Path('/home/ubuntu/数据获取/data/preprocessed_hourly/pkl_cache')
fp = PKL_DIR / 'series_swap.pkl'
with fp.open('rb') as f:
    data = pickle.load(f)

symbols = data.get('symbols', {})
print(f'symbols 类型: {type(symbols)}')
print(f'symbols 数量: {len(symbols)}')

# 取第一个币种
first_sym = list(symbols.keys())[0]
first_val = symbols[first_sym]
print(f'\n示例符号: {first_sym}')
print(f'  值类型: {type(first_val)}')
if isinstance(first_val, dict):
    print(f'  keys: {list(first_val.keys())}')
    for k, v in first_val.items():
        if isinstance(v, list):
            print(f'  {k}: list len={len(v)}  前3={v[:3]}')
        else:
            print(f'  {k}: {v}')
elif hasattr(first_val, 'columns'):
    print(f'  DataFrame columns: {list(first_val.columns)}')
    print(f'  shape: {first_val.shape}')
    print(first_val.head(3))

print(f'\n总 meta: {data.get("meta")}')
print(f'tail: {data.get("tail")}')
