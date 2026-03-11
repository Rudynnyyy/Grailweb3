
import pandas as pd
import numpy as np
import pickle
import os
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import time
encodings = ['gbk', 'utf-8'] # 旧顺序import shutil


def load_all_source_data():
    # 数据源路径
    spot_source_dir = r"D:\量化交易\数据\spot_lin"
    swap_source_dir = r"D:\量化交易\数据\swap_lin"

    spot_files = glob.glob(os.path.join(spot_source_dir, "*.csv")) if os.path.exists(spot_source_dir) else []
    swap_files = glob.glob(os.path.join(swap_source_dir, "*.csv")) if os.path.exists(swap_source_dir) else []

    # 简单去重处理
    def deduplicate_files(files):
        """根据文件名去重"""
        seen_symbols = set()
        unique_files = []

        for file_path in files:
            filename = os.path.basename(file_path)
            symbol = filename.replace('.csv', '')

            if symbol not in seen_symbols:
                seen_symbols.add(symbol)
                unique_files.append(file_path)

        return unique_files

    spot_files = deduplicate_files(spot_files)
    swap_files = deduplicate_files(swap_files)

    return spot_files, swap_files


def analyze_symbol_trading_period(csv_file):
    """分析币种的实际交易期间"""
    try:
        # 读取CSV数据
        df = None
        encodings = ['utf-8', 'gbk', 'latin-1', 'cp1252']

        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, skiprows=1, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue

        if df is None or len(df) == 0:
            return None

        # 解析时间
        df['candle_begin_time'] = pd.to_datetime(df.iloc[:, 0])

        # 检查价格和交易量
        if len(df.columns) >= 6:
            close_col = df.iloc[:, 4]  # close价格
            volume_col = df.iloc[:, 5]  # volume

            # 找到有效交易数据（价格>0且交易量>0）
            valid_mask = (close_col > 0) & (volume_col > 0)
            valid_data = df[valid_mask]

            if len(valid_data) == 0:
                return None

            # 计算实际交易期间
            actual_start = valid_data['candle_begin_time'].min()
            actual_end = valid_data['candle_begin_time'].max()
            total_valid_hours = len(valid_data)
            data_quality = len(valid_data) / len(df)

            # 计算连续性评分
            if len(valid_data) > 1:
                time_diffs = valid_data['candle_begin_time'].diff().dropna()
                continuous_hours = (time_diffs == pd.Timedelta(hours=1)).sum()
                continuity_score = continuous_hours / len(time_diffs) if len(time_diffs) > 0 else 0
            else:
                continuity_score = 0

            return {
                'start_time': actual_start,
                'end_time': actual_end,
                'total_hours': total_valid_hours,
                'data_quality': data_quality,
                'continuity_score': continuity_score,
                'source_total': len(df)
            }

    except Exception as e:
        return None


def create_output_directories(base_dir):
    """创建输出目录结构"""
    # 清理旧数据
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    dirs_to_create = [
        base_dir,
        f"{base_dir}/split",
        f"{base_dir}/split/spot",
        f"{base_dir}/split/swap",
        f"{base_dir}/output"
    ]

    for dir_path in dirs_to_create:
        os.makedirs(dir_path, exist_ok=True)

    return base_dir


def process_single_symbol_unlimited(args):
    """无白名单限制的高质量处理单个币种"""
    csv_file, is_spot, base_dir = args

    try:
        filename = os.path.basename(csv_file)
        symbol = filename.replace('.csv', '')

        # 1. 分析实际交易期间
        trading_info = analyze_symbol_trading_period(csv_file)
        if not trading_info:
            return None, f"{symbol}: 无有效交易数据"

        # 2. 更严格的数据质量检查（因为不限制白名单）
        if trading_info['data_quality'] < 0.7:  # 降低到70%但仍保持质量控制
            return None, f"{symbol}: 数据质量太低 ({trading_info['data_quality']:.2%})"

        if trading_info['total_hours'] < 12:  # 降低到12小时但仍保持基本要求
            return None, f"{symbol}: 交易时间太短 ({trading_info['total_hours']}小时)"

        # 3. 连续性检查 - 新增质量控制
        if trading_info['continuity_score'] < 0.3:  # 连续性低于30%的排除
            return None, f"{symbol}: 数据连续性太差 ({trading_info['continuity_score']:.2%})"

        # 4. 读取并处理CSV数据
        df = None
        encodings = ['utf-8', 'gbk', 'latin-1', 'cp1252']

        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, skiprows=1, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            return None, f"{symbol}: 无法解码文件"

        # 5. 标准化列名
        expected_columns = [
            'candle_begin_time', 'open', 'high', 'low', 'close', 'volume',
            'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume', 'Spread', 'symbol', 'avg_price_1m', 'avg_price_5m'
        ]

        if is_spot:
            expected_columns.append('placeholder')
        else:
            expected_columns.append('fundingRate')

        if len(df.columns) >= len(expected_columns):
            df.columns = expected_columns[:len(df.columns)]

        # 6. 动态时间范围处理（官方策略）
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])

        # 只保留有效交易期间的数据
        close_col = df['close'] if 'close' in df.columns else df.iloc[:, 4]
        volume_col = df['volume'] if 'volume' in df.columns else df.iloc[:, 5]

        # 更严格的有效数据筛选
        valid_mask = (close_col > 0) & (volume_col > 0)

        # 额外检查：排除明显异常的价格数据
        if 'high' in df.columns and 'low' in df.columns:
            # 排除高低价差异过大的异常数据
            price_range_ratio = (df['high'] - df['low']) / df['close']
            valid_mask = valid_mask & (price_range_ratio < 0.5)  # 日内波动不超过50%

        df_valid = df[valid_mask].copy()

        if len(df_valid) == 0:
            return None, f"{symbol}: 无有效交易数据"

        # 7. 按小时重采样并填充
        df_valid = df_valid.sort_values('candle_begin_time').reset_index(drop=True)

        # 创建完整的小时时间序列（仅覆盖实际交易期间）
        start_time = df_valid['candle_begin_time'].min().floor('h')
        end_time = df_valid['candle_begin_time'].max().ceil('h')
        full_time_index = pd.date_range(start_time, end_time, freq='h')

        # 创建完整数据框
        full_df = pd.DataFrame({'candle_begin_time': full_time_index})
        full_df = full_df.merge(df_valid, on='candle_begin_time', how='left')

        # 8. 智能数据填充策略
        numeric_fields = ['open', 'high', 'low', 'close', 'volume', 'quote_volume',
                          'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']

        for field in numeric_fields:
            if field in full_df.columns:
                # 多层填充策略
                # 1. 前向填充
                full_df[field] = full_df[field].ffill()
                # 2. 后向填充
                full_df[field] = full_df[field].bfill()
                # 3. 线性插值填充（对于价格数据）
                if field in ['open', 'high', 'low', 'close']:
                    full_df[field] = full_df[field].interpolate(method='linear')
                # 4. 剩余NaN用0填充
                full_df[field] = full_df[field].fillna(0.0)

        # 9. 添加标准字段
        full_df['symbol'] = symbol

        # 计算avg_price_1m（关键字段）
        if 'avg_price_1m' not in full_df.columns:
            full_df['avg_price_1m'] = (full_df['open'] + full_df['high'] + full_df['low'] + full_df['close']) / 4

        # 处理avg_price_5m
        if 'avg_price_5m' not in full_df.columns:
            full_df['avg_price_5m'] = full_df['avg_price_1m']

        # 处理funding相关字段
        if is_spot:
            full_df['funding_fee'] = 0
        else:
            if 'fundingRate' in full_df.columns:
                funding_rate = full_df['fundingRate'].fillna(0.0)
                funding_rate = np.clip(funding_rate, -0.1, 0.1)
                full_df['funding_fee'] = funding_rate
            else:
                full_df['funding_fee'] = 0.0

        # 10. 添加元数据字段
        full_df['是否交易'] = 1  # 由于只包含交易期间，所有数据都是交易数据
        full_df['first_candle_time'] = trading_info['start_time']
        full_df['last_candle_time'] = trading_info['end_time']
        full_df['symbol_spot'] = symbol if is_spot else ''
        full_df['symbol_swap'] = symbol if not is_spot else ''
        full_df['is_spot'] = 1 if is_spot else 0

        # 11. 最终列顺序（确保包含avg_price_1m）
        final_columns = [
            'candle_begin_time', 'symbol', 'open', 'high', 'close', 'low',
            'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume', 'funding_fee', 'avg_price_1m', 'avg_price_5m',
            '是否交易', 'first_candle_time', 'last_candle_time', 'symbol_spot', 'symbol_swap', 'is_spot'
        ]

        # 确保所有列都存在
        for col in final_columns:
            if col not in full_df.columns:
                if col in ['symbol', 'symbol_spot', 'symbol_swap']:
                    full_df[col] = symbol if col == 'symbol' else (
                        symbol if (col == 'symbol_spot' and is_spot) or (col == 'symbol_swap' and not is_spot) else '')
                elif col in ['first_candle_time', 'last_candle_time']:
                    full_df[col] = trading_info['start_time'] if col == 'first_candle_time' else trading_info[
                        'end_time']
                elif col == 'avg_price_1m':
                    full_df[col] = (full_df['open'] + full_df['high'] + full_df['low'] + full_df['close']) / 4
                else:
                    full_df[col] = 1 if col == '是否交易' or col == 'is_spot' and is_spot else 0

        full_df = full_df[final_columns]

        # 12. 保存到split目录
        split_subdir = 'spot' if is_spot else 'swap'
        split_file_path = f"{base_dir}/split/{split_subdir}/{symbol}.csv"
        full_df.to_csv(split_file_path, index=False)

        return {
            'symbol': symbol,
            'data': full_df,
            'is_spot': is_spot,
            'actual_start': trading_info['start_time'],
            'actual_end': trading_info['end_time'],
            'total_records': len(full_df),
            'data_quality': trading_info['data_quality'],
            'continuity_score': trading_info['continuity_score'],
            'time_span_hours': len(full_df)
        }, None

    except Exception as e:
        return None, f"{symbol}: {str(e)}"


def load_previous_data(pre_storage_path):
    """加载之前的处理数据"""
    spot_pkl = os.path.join(pre_storage_path, "spot_dict.pkl")
    swap_pkl = os.path.join(pre_storage_path, "swap_dict.pkl")
    
    spot_data = {}
    swap_data = {}
    
    if os.path.exists(spot_pkl):
        try:
            with open(spot_pkl, 'rb') as f:
                spot_data = pickle.load(f)
        except Exception:
            pass
            
    if os.path.exists(swap_pkl):
        try:
            with open(swap_pkl, 'rb') as f:
                swap_data = pickle.load(f)
        except Exception:
            pass
            
    return spot_data, swap_data


def process_single_symbol_incremental(args):
    """增量处理单个币种"""
    csv_file, is_spot, base_dir, old_df = args
    
    # 如果没有旧数据，使用全量处理
    if old_df is None:
        return process_single_symbol_unlimited((csv_file, is_spot, base_dir))
        
    try:
        filename = os.path.basename(csv_file)
        symbol = filename.replace('.csv', '')
        
        # 1. 读取CSV数据
        df_new = None
        encodings = ['utf-8', 'gbk', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                df_new = pd.read_csv(csv_file, skiprows=1, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
                
        if df_new is None:
            # 读取失败，返回旧数据
            return {
                'symbol': symbol,
                'data': old_df,
                'is_spot': is_spot,
                'actual_start': old_df['first_candle_time'].iloc[0] if not old_df.empty else pd.Timestamp.now(),
                'actual_end': old_df['last_candle_time'].iloc[-1] if not old_df.empty else pd.Timestamp.now(),
                'total_records': len(old_df),
                'data_quality': 1.0, 
                'continuity_score': 1.0,
                'time_span_hours': len(old_df)
            }, f"{symbol}: 读取源文件失败，保留旧数据"
            
        # 2. 筛选增量数据
        df_new['candle_begin_time'] = pd.to_datetime(df_new.iloc[:, 0])
        
        # 确保old_df的时间列是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(old_df['candle_begin_time']):
            old_df['candle_begin_time'] = pd.to_datetime(old_df['candle_begin_time'])
            
        last_time = old_df['candle_begin_time'].max()
        
        # 筛选大于旧数据最后时间的数据
        new_data_mask = df_new['candle_begin_time'] > last_time
        df_valid = df_new[new_data_mask].copy()
        
        # 如果没有新数据，直接返回旧数据
        if len(df_valid) == 0:
            # 保存旧数据到新目录
            split_subdir = 'spot' if is_spot else 'swap'
            split_file_path = f"{base_dir}/split/{split_subdir}/{symbol}.csv"
            old_df.to_csv(split_file_path, index=False)
            
            return {
                'symbol': symbol,
                'data': old_df,
                'is_spot': is_spot,
                'actual_start': old_df['first_candle_time'].iloc[0] if not old_df.empty else pd.Timestamp.now(),
                'actual_end': last_time,
                'total_records': len(old_df),
                'data_quality': 1.0,
                'continuity_score': 1.0,
                'time_span_hours': len(old_df)
            }, None
            
        # 3. 处理增量数据
        # 标准化列名
        expected_columns = [
            'candle_begin_time', 'open', 'high', 'low', 'close', 'volume',
            'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume', 'Spread', 'symbol', 'avg_price_1m', 'avg_price_5m'
        ]
        
        if is_spot:
            expected_columns.append('placeholder')
        else:
            expected_columns.append('fundingRate')
            
        if len(df_valid.columns) >= len(expected_columns):
            df_valid.columns = expected_columns[:len(df_valid.columns)]
            
        # 再次筛选有效数据（类似于process_single_symbol_unlimited）
        close_col = df_valid['close'] if 'close' in df_valid.columns else df_valid.iloc[:, 4]
        volume_col = df_valid['volume'] if 'volume' in df_valid.columns else df_valid.iloc[:, 5]
        valid_mask = (close_col > 0) & (volume_col > 0)
        
        if 'high' in df_valid.columns and 'low' in df_valid.columns:
            price_range_ratio = (df_valid['high'] - df_valid['low']) / df_valid['close']
            valid_mask = valid_mask & (price_range_ratio < 0.5)
            
        df_valid = df_valid[valid_mask].copy()
        
        if len(df_valid) == 0:
            # 新数据都无效，返回旧数据
            split_subdir = 'spot' if is_spot else 'swap'
            split_file_path = f"{base_dir}/split/{split_subdir}/{symbol}.csv"
            old_df.to_csv(split_file_path, index=False)
            
            return {
                'symbol': symbol,
                'data': old_df,
                'is_spot': is_spot,
                'actual_start': old_df['first_candle_time'].iloc[0] if not old_df.empty else pd.Timestamp.now(),
                'actual_end': last_time,
                'total_records': len(old_df),
                'data_quality': 1.0,
                'continuity_score': 1.0,
                'time_span_hours': len(old_df)
            }, None
            
        # 4. 重采样和填充
        df_valid = df_valid.sort_values('candle_begin_time').reset_index(drop=True)
        
        # 创建新数据的完整时间索引
        start_time = last_time + pd.Timedelta(hours=1)
        end_time = df_valid['candle_begin_time'].max().ceil('h')
        
        if start_time > end_time:
            # 异常情况，时间错乱
            full_time_index = pd.DatetimeIndex([])
            full_df_new = pd.DataFrame()
        else:
            full_time_index = pd.date_range(start_time, end_time, freq='h')
            full_df_new = pd.DataFrame({'candle_begin_time': full_time_index})
            full_df_new = full_df_new.merge(df_valid, on='candle_begin_time', how='left')
            
            # 填充
            numeric_fields = ['open', 'high', 'low', 'close', 'volume', 'quote_volume',
                              'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
            
            for field in numeric_fields:
                if field in full_df_new.columns:
                    full_df_new[field] = full_df_new[field].ffill().bfill()
                    if field in ['open', 'high', 'low', 'close']:
                        full_df_new[field] = full_df_new[field].interpolate(method='linear')
                    full_df_new[field] = full_df_new[field].fillna(0.0)
            
            # 添加标准字段
            full_df_new['symbol'] = symbol
            if 'avg_price_1m' not in full_df_new.columns:
                full_df_new['avg_price_1m'] = (full_df_new['open'] + full_df_new['high'] + full_df_new['low'] + full_df_new['close']) / 4
            if 'avg_price_5m' not in full_df_new.columns:
                full_df_new['avg_price_5m'] = full_df_new['avg_price_1m']
                
            if is_spot:
                full_df_new['funding_fee'] = 0
            else:
                if 'fundingRate' in full_df_new.columns:
                    funding_rate = full_df_new['fundingRate'].fillna(0.0)
                    funding_rate = np.clip(funding_rate, -0.1, 0.1)
                    full_df_new['funding_fee'] = funding_rate
                else:
                    full_df_new['funding_fee'] = 0.0
                    
            full_df_new['是否交易'] = 1
            full_df_new['first_candle_time'] = old_df['first_candle_time'].iloc[0] if 'first_candle_time' in old_df.columns else start_time
            full_df_new['symbol_spot'] = symbol if is_spot else ''
            full_df_new['symbol_swap'] = symbol if not is_spot else ''
            full_df_new['is_spot'] = 1 if is_spot else 0
            
            # 5. 合并数据
            # 确保列一致
            final_columns = [
                'candle_begin_time', 'symbol', 'open', 'high', 'close', 'low',
                'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                'taker_buy_quote_asset_volume', 'funding_fee', 'avg_price_1m', 'avg_price_5m',
                '是否交易', 'first_candle_time', 'last_candle_time', 'symbol_spot', 'symbol_swap', 'is_spot'
            ]
            
            # 补齐列
            for col in final_columns:
                if col not in full_df_new.columns:
                    if col == 'last_candle_time':
                        full_df_new[col] = end_time
                    else:
                        # 从old_df获取默认值或设为0
                        if col in old_df.columns:
                            # 取第一行的值作为默认值（针对metadata）
                            full_df_new[col] = old_df[col].iloc[0]
                        else:
                            full_df_new[col] = 0
                            
            full_df_new = full_df_new[final_columns]
            
            # 更新old_df的last_candle_time
            old_df['last_candle_time'] = end_time
            if col not in old_df.columns:
                 # 确保old_df也有这些列
                 for c in final_columns:
                     if c not in old_df.columns:
                         old_df[c] = 0 # 简单处理
            
            old_df = old_df[final_columns]
            
            # 合并
            combined_df = pd.concat([old_df, full_df_new], ignore_index=True)
            
            # 保存
            split_subdir = 'spot' if is_spot else 'swap'
            split_file_path = f"{base_dir}/split/{split_subdir}/{symbol}.csv"
            combined_df.to_csv(split_file_path, index=False)
            
            return {
                'symbol': symbol,
                'data': combined_df,
                'is_spot': is_spot,
                'actual_start': combined_df['first_candle_time'].iloc[0],
                'actual_end': end_time,
                'total_records': len(combined_df),
                'data_quality': 1.0,
                'continuity_score': 1.0,
                'time_span_hours': len(combined_df)
            }, None

    except Exception as e:
        return None, f"{symbol} Incremental: {str(e)}"


def process_all_symbols_incremental(spot_files, swap_files, base_dir, pre_storage_path=None):

    # 加载旧数据
    spot_old_data = {}
    swap_old_data = {}
    if pre_storage_path:
        spot_old_data, swap_old_data = load_previous_data(pre_storage_path)

    # 准备任务
    tasks = []
    
    # 现货任务
    for file in spot_files:
        filename = os.path.basename(file)
        symbol = filename.replace('.csv', '')
        old_df = spot_old_data.get(symbol)
        tasks.append((file, True, base_dir, old_df))
        
    # 合约任务
    for file in swap_files:
        filename = os.path.basename(file)
        symbol = filename.replace('.csv', '')
        old_df = swap_old_data.get(symbol)
        tasks.append((file, False, base_dir, old_df))

    results = []
    errors = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_task = {executor.submit(process_single_symbol_incremental, task): task for task in tasks}

        for i, future in enumerate(as_completed(future_to_task)):
            result, error = future.result()

            if result:
                results.append(result)
            if error:
                errors.append(error)

            if (i + 1) % 100 == 0:
                print(f"进度: {i + 1}/{len(tasks)} 个币种")

    return results


def create_unlimited_pivot_tables(data_dict, is_spot=True):
    if not data_dict:
        return {}

    # 包含vwap1m字段以匹配官方数据
    if is_spot:
        pivot_fields = ['open', 'close', 'vwap1m']
    else:
        pivot_fields = ['open', 'close', 'funding_rate', 'vwap1m']

    # 找到所有币种的时间范围
    all_times = set()
    for symbol, df in data_dict.items():
        all_times.update(pd.to_datetime(df['candle_begin_time']))

    # 创建全局时间索引
    if all_times:
        global_start = min(all_times)
        global_end = max(all_times)
        global_time_index = pd.date_range(global_start, global_end, freq='h')
    else:
        global_time_index = pd.DatetimeIndex([])

    pivots = {}

    def create_single_unlimited_pivot(field):
        """创建单个无限制动态透视表"""

        # 字段映射
        if field == 'funding_rate':
            source_field = 'funding_fee'
        elif field == 'vwap1m':
            source_field = 'avg_price_1m'  # vwap1m透视表使用avg_price_1m数据
        else:
            source_field = field

        # 创建透视表数据
        pivot_data = {}

        for symbol, df in data_dict.items():
            # 创建该币种在全局时间范围内的数据
            symbol_series = pd.Series(index=global_time_index, dtype=float)

            if source_field in df.columns:
                # 将币种数据映射到全局时间索引
                df_indexed = df.set_index('candle_begin_time')[source_field]
                symbol_series.loc[df_indexed.index] = df_indexed.values

            # 对于没有数据的时间点，填充NaN（而非0）
            pivot_data[symbol] = symbol_series.values

        pivot_df = pd.DataFrame(pivot_data, index=global_time_index)

        return field, pivot_df

    # 并行创建透视表
    with ThreadPoolExecutor(max_workers=min(4, len(pivot_fields))) as executor:
        future_to_field = {executor.submit(create_single_unlimited_pivot, field): field for field in pivot_fields}

        for future in as_completed(future_to_field):
            field, pivot_df = future.result()
            pivots[field] = pivot_df

    return pivots


def create_unlimited_pkl_files(results, base_dir):

    # 分离现货和合约数据
    spot_data = {}
    swap_data = {}

    for result in results:
        symbol = result['symbol']
        data = result['data']
        data.reset_index(drop=True, inplace=True)

        if result['is_spot']:
            spot_data[symbol] = data
        else:
            swap_data[symbol] = data

    def save_data_dict(data, filename):
        """保存数据字典"""
        with open(filename, 'wb') as f:
            pickle.dump(data, f)
        return filename

    def save_pivot_dict(data, is_spot, filename):
        """保存透视表字典"""
        pivots = create_unlimited_pivot_tables(data, is_spot=is_spot)
        with open(filename, 'wb') as f:
            pickle.dump(pivots, f)
        return filename

    # 并行保存文件
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        # 保存数据字典
        futures.append(executor.submit(save_data_dict, spot_data, f"{base_dir}/spot_dict.pkl"))
        futures.append(executor.submit(save_data_dict, swap_data, f"{base_dir}/swap_dict.pkl"))

        # 保存透视表
        futures.append(executor.submit(save_pivot_dict, spot_data, True, f"{base_dir}/market_pivot_spot.pkl"))
        futures.append(executor.submit(save_pivot_dict, swap_data, False, f"{base_dir}/market_pivot_swap.pkl"))

        # 等待完成
        for future in as_completed(futures):
            future.result()

    return spot_data, swap_data


def main():

    pre_storage_path = r"D:\量化交易\数据\combine"   # 原始数据位置
    after_storage_path = r"D:\量化交易\数据\new_combine" # 更新后数据保存的位置
    
    spot_files, swap_files = load_all_source_data()
    if not spot_files and not swap_files:
        return

    base_dir = create_output_directories(after_storage_path)
    results = process_all_symbols_incremental(spot_files, swap_files, base_dir, pre_storage_path)
    if not results:
        return
    create_unlimited_pkl_files(results, base_dir)

    # 删除split和output文件夹
    if os.path.exists(f"{base_dir}/split"):
        shutil.rmtree(f"{base_dir}/split")
    if os.path.exists(f"{base_dir}/output"):
        shutil.rmtree(f"{base_dir}/output")


if __name__ == "__main__":
    main()
