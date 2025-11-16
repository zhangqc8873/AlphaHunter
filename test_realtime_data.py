#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试实时数据获取功能
用于调试实时价格和涨跌额显示问题
"""

import sys
from pathlib import Path
import time

# 添加 src 目录到 Python 路径
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT / "src"))

from alphahunter.data_fetch import get_realtime_spot
from alphahunter.processing import clean_spot_df
import pandas as pd


def test_realtime_data():
    """测试实时数据获取"""
    print("=" * 60)
    print("测试实时数据获取功能")
    print("=" * 60)
    
    # 1. 获取实时数据（带重试机制）
    print("\n1. 获取实时行情数据...")
    max_retries = 3
    retry_delay = 2
    spot = None
    
    for attempt in range(max_retries):
        try:
            print(f"   尝试第 {attempt + 1}/{max_retries} 次...")
            spot = get_realtime_spot(use_cache=False)
            if spot is None or spot.empty:
                print(f"   ⚠ 获取的数据为空")
                if attempt < max_retries - 1:
                    print(f"   等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                    continue
            else:
                print(f"   ✓ 成功获取 {len(spot)} 条实时数据")
                break
        except Exception as e:
            error_msg = str(e)
            print(f"   ✗ 错误: {error_msg}")
            if attempt < max_retries - 1:
                print(f"   等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print(f"\n✗✗✗ 所有重试都失败了！")
                print("\n可能的原因：")
                print("  1. 网络连接问题 - 请检查网络连接")
                print("  2. akshare API 限频 - 请稍后再试")
                print("  3. 数据源服务器问题 - 请稍后再试")
                print("\n建议：")
                print("  - 稍等几分钟后再次运行")
                print("  - 检查是否能访问东方财富网站")
                print("  - 尝试更新 akshare: pip install --upgrade akshare")
                return
    
    if spot is None or spot.empty:
        print("\n✗ 未能获取到有效数据")
        return
    
    # 2. 显示列名
    print("\n2. 可用列名:")
    print(f"   共 {len(spot.columns)} 列")
    print(f"   列名: {list(spot.columns)}")
    
    # 3. 清洗数据
    print("\n3. 清洗数据...")
    try:
        spot_clean = clean_spot_df(spot)
        print(f"✓ 数据清洗完成，剩余 {len(spot_clean)} 条记录")
        print(f"   清洗后列名: {list(spot_clean.columns)}")
    except Exception as e:
        print(f"✗ 数据清洗失败: {e}")
        spot_clean = spot
    
    # 4. 检查关键列
    print("\n4. 检查关键列:")
    key_cols = ["代码", "名称", "最新价", "现价", "价格", "涨跌额", "涨跌值", "涨跌", "pct_chg", "涨跌幅"]
    for col in key_cols:
        if col in spot_clean.columns:
            print(f"   ✓ {col:10s} - 存在")
        else:
            print(f"   ✗ {col:10s} - 不存在")
    
    # 5. 显示示例数据
    print("\n5. 示例数据（前3条）:")
    if "代码" in spot_clean.columns:
        # 选择重要的列显示
        display_cols = ["代码"]
        for col in ["名称", "最新价", "现价", "价格", "涨跌额", "涨跌值", "涨跌幅", "pct_chg"]:
            if col in spot_clean.columns:
                display_cols.append(col)
        
        sample = spot_clean[display_cols].head(3)
        print(sample.to_string(index=False))
    else:
        print("   ⚠ 缺少'代码'列，无法显示示例")
    
    # 6. 检查价格和涨跌额数据类型
    print("\n6. 数据类型检查:")
    type_cols = ["最新价", "现价", "价格", "涨跌额", "涨跌值", "pct_chg"]
    for col in type_cols:
        if col in spot_clean.columns:
            dtype = spot_clean[col].dtype
            null_count = spot_clean[col].isnull().sum()
            print(f"   {col:10s}: dtype={dtype}, 空值数={null_count}/{len(spot_clean)}")
    
    # 7. 测试涨跌额计算
    print("\n7. 测试涨跌额计算:")
    price_col = None
    for col in ["最新价", "现价", "价格"]:
        if col in spot_clean.columns:
            price_col = col
            break
    
    if price_col and "pct_chg" in spot_clean.columns:
        print(f"   使用 {price_col} 和 pct_chg 计算涨跌额")
        try:
            spot_clean["涨跌额_计算"] = (
                pd.to_numeric(spot_clean[price_col], errors='coerce') * 
                pd.to_numeric(spot_clean["pct_chg"], errors='coerce') / 100
            ).round(2)
            print(f"   ✓ 涨跌额计算成功")
            print(f"\n   示例（前3条）:")
            calc_cols = ["代码", price_col, "pct_chg", "涨跌额_计算"]
            if "涨跌额" in spot_clean.columns:
                calc_cols.append("涨跌额")
            print(spot_clean[calc_cols].head(3).to_string(index=False))
        except Exception as e:
            print(f"   ✗ 涨跌额计算失败: {e}")
    else:
        print(f"   ⚠ 缺少必要列（价格列={price_col}, pct_chg={'存在' if 'pct_chg' in spot_clean.columns else '不存在'}）")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test_realtime_data()
