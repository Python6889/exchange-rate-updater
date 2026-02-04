#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
沪港通汇率自动更新程序 - GitHub Actions版本
"""

import os
import sys
import akshare as ak
import pandas as pd
import requests
from datetime import datetime

# ==================== 配置部分 ====================
# 从环境变量获取配置（GitHub Actions Secrets）
APP_ID = os.environ.get('APP_ID')
APP_SECRET = os.environ.get('APP_SECRET')
TABLE_TOKEN = os.environ.get('TABLE_TOKEN')
SHEET_ID = os.environ.get('SHEET_ID')

# ==================== 核心函数 ====================

def get_feishu_token():
    """获取飞书API的访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    
    try:
        response = requests.post(url, headers=headers, json=data)
        result = response.json()
        
        if result.get("code") == 0:
            print("✅ 成功获取飞书访问令牌")
            return result["tenant_access_token"]
        else:
            print(f"❌ 获取令牌失败: {result}")
            return None
    except Exception as e:
        print(f"❌ 获取令牌时出错: {e}")
        return None

def get_settlement_exchange_rate():
    """使用akshare获取沪港通结算汇率数据"""
    try:
        print("正在从akshare获取沪港通结算汇率数据...")
        df = ak.stock_sgt_settlement_exchange_rate_sse()
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取结算汇率数据，共 {len(df)} 条记录")
            return df
        else:
            print("⚠️ 结算汇率数据为空")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ 获取结算汇率数据失败: {e}")
        return pd.DataFrame()

def get_reference_exchange_rate():
    """使用akshare获取沪港通参考汇率数据"""
    try:
        print("正在从akshare获取沪港通参考汇率数据...")
        df = ak.stock_sgt_reference_exchange_rate_sse()
        
        if df is not None and not df.empty:
            print(f"✅ 成功获取参考汇率数据，共 {len(df)} 条记录")
            return df
        else:
            print("⚠️ 参考汇率数据为空")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ 获取参考汇率数据失败: {e}")
        return pd.DataFrame()

def merge_and_select_exchange_data(settlement_df, reference_df):
    """
    合并结算汇率和参考汇率数据，优先使用结算汇率
    """
    print("🔄 合并结算汇率和参考汇率数据...")
    
    # 如果两个数据都为空
    if settlement_df.empty and reference_df.empty:
        print("⚠️ 结算汇率和参考汇率数据都为空")
        return pd.DataFrame()
    
    # 如果只有结算汇率数据
    if not settlement_df.empty and reference_df.empty:
        print("📊 只使用结算汇率数据")
        settlement_df['汇率类型'] = '结算汇率'
        return settlement_df
    
    # 如果只有参考汇率数据
    if settlement_df.empty and not reference_df.empty:
        print("📊 只使用参考汇率数据")
        reference_df['汇率类型'] = '参考汇率'
        return reference_df
    
    # 如果两个数据都有，进行合并
    print("📊 合并两种汇率数据...")
    
    # 标准化日期列 - 假设第一列是日期
    settlement_df = settlement_df.copy()
    reference_df = reference_df.copy()
    
    # 重命名第一列为'交易日期'
    settlement_df.rename(columns={settlement_df.columns[0]: '交易日期'}, inplace=True)
    reference_df.rename(columns={reference_df.columns[0]: '交易日期'}, inplace=True)
    
    # 确保交易日期是字符串类型
    settlement_df['交易日期'] = settlement_df['交易日期'].astype(str)
    reference_df['交易日期'] = reference_df['交易日期'].astype(str)
    
    # 添加汇率类型列
    settlement_df['汇率类型'] = '结算汇率'
    reference_df['汇率类型'] = '参考汇率'
    
    # 获取结算汇率的所有日期
    settlement_dates = set(settlement_df['交易日期'].unique())
    print(f"结算汇率有 {len(settlement_dates)} 个不同日期")
    
    # 获取参考汇率的所有日期
    reference_dates = set(reference_df['交易日期'].unique())
    print(f"参考汇率有 {len(reference_dates)} 个不同日期")
    
    # 找出只在参考汇率中存在的日期
    reference_only_dates = reference_dates - settlement_dates
    print(f"只在参考汇率中存在的日期: {len(reference_only_dates)} 个")
    
    # 从参考汇率中筛选出结算汇率没有的日期
    reference_only_df = reference_df[reference_df['交易日期'].isin(reference_only_dates)].copy()
    
    # 合并数据：结算汇率 + 参考汇率中结算汇率没有的日期
    merged_df = pd.concat([settlement_df, reference_only_df], ignore_index=True)
    
    # 按交易日期排序
    merged_df.sort_values('交易日期', ascending=False, inplace=True)
    merged_df.reset_index(drop=True, inplace=True)
    
    print(f"✅ 合并后数据: {len(merged_df)} 条记录")
    print(f"其中结算汇率: {len(settlement_df)} 条")
    print(f"其中参考汇率: {len(reference_only_df)} 条")
    
    return merged_df

def read_existing_feishu_data(token):
    """从飞书表格读取现有数据"""
    try:
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values/{SHEET_ID}"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        print("正在从飞书表格读取现有数据...")
        response = requests.get(url, headers=headers)
        result = response.json()
        
        if result.get("code") == 0:
            data = result.get("data", {})
            values = data.get("valueRange", {}).get("values", [])
            
            if not values or len(values) <= 1:
                print("📭 表格中没有数据或只有表头")
                return []
            
            # 从第二行开始是数据（第一行是表头）
            existing_data = values[1:]
            print(f"📊 读取到 {len(existing_data)} 条现有数据")
            return existing_data
        else:
            print(f"❌ 读取数据失败: {result}")
            return []
    except Exception as e:
        print(f"❌ 读取数据时出错: {e}")
        return []

def identify_new_data(df, existing_data):
    """识别新增数据"""
    if df.empty:
        print("⚠️ 没有新数据可处理")
        return pd.DataFrame()
    
    if not existing_data:
        print("📝 表格为空，所有数据都是新增数据")
        return df
    
    try:
        # 获取现有数据中的日期（假设第一列是日期）
        existing_dates = []
        for row in existing_data:
            if row:
                existing_dates.append(str(row[0]))
        
        print(f"📅 现有数据中的日期数量: {len(existing_dates)}")
        
        # 获取新数据中的日期
        new_dates = df['交易日期'].astype(str).tolist()
        
        # 找出新增的日期
        new_date_indices = []
        for i, date in enumerate(new_dates):
            if str(date) not in existing_dates:
                new_date_indices.append(i)
        
        print(f"🔍 找到 {len(new_date_indices)} 条新增数据")
        
        if new_date_indices:
            # 提取新增数据
            new_data_df = df.iloc[new_date_indices].copy()
            
            # 显示新增数据的日期和类型
            if '交易日期' in new_data_df.columns and '汇率类型' in new_data_df.columns:
                print("新增数据详情:")
                for idx, row in new_data_df.iterrows():
                    print(f"  日期: {row['交易日期']}, 类型: {row['汇率类型']}")
            
            return new_data_df
        else:
            print("✅ 没有发现新增数据，所有数据都已存在")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ 识别新增数据时出错: {e}")
        return pd.DataFrame()

def prepare_update_data(new_data_df):
    """准备要更新到飞书的数据格式"""
    if new_data_df.empty:
        print("⚠️ 没有新增数据需要更新")
        return []
    
    try:
        # 添加更新时间列
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_data_df = new_data_df.copy()
        new_data_df['更新时间'] = current_time
        
        # 将数据转换为列表格式
        data_rows = []
        for _, row in new_data_df.iterrows():
            row_data = []
            for value in row:
                if pd.isna(value):
                    row_data.append("")
                else:
                    row_data.append(str(value))
            data_rows.append(row_data)
        
        print(f"✅ 准备完成 {len(data_rows)} 行新增数据")
        return data_rows
    except Exception as e:
        print(f"❌ 准备数据时出错: {e}")
        return []

def append_to_feishu(feishu_data, token):
    """将新增数据追加到飞书表格"""
    if not feishu_data:
        print("⚠️ 没有数据需要追加")
        return True
    
    try:
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values_append"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        request_data = {
            "valueRange": {
                "range": SHEET_ID,
                "values": feishu_data
            }
        }
        
        print(f"正在追加 {len(feishu_data)} 行新数据到飞书表格...")
        response = requests.post(url, headers=headers, json=request_data)
        result = response.json()
        
        if result.get("code") == 0:
            updates = result.get("data", {}).get("updates", {})
            print(f"✅ 成功追加数据到飞书表格！")
            print(f"更新范围：{updates.get('updatedRange', '未知')}")
            print(f"更新行数：{updates.get('updatedRows', '未知')}")
            return True
        else:
            print(f"❌ 追加数据失败: {result}")
            return False
    except Exception as e:
        print(f"❌ 追加数据时出错: {e}")
        return False

def incremental_update():
    """主函数：实现增量更新"""
    print("=" * 60)
    print("沪港通结算汇率数据 - 增量更新")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # 检查环境变量
        if not all([APP_ID, APP_SECRET, TABLE_TOKEN, SHEET_ID]):
            print("❌ 环境变量未设置完整")
            print("请在GitHub仓库的Settings -> Secrets and variables -> Actions中设置:")
            print("APP_ID, APP_SECRET, TABLE_TOKEN, SHEET_ID")
            return False
        
        # 步骤1：获取飞书访问令牌
        print("\n1️⃣ 获取飞书访问令牌")
        token = get_feishu_token()
        if not token:
            print("❌ 无法获取飞书访问令牌")
            return False
        
        # 步骤2：从飞书表格读取现有数据
        print("\n2️⃣ 读取飞书表格中的现有数据")
        existing_data = read_existing_feishu_data(token)
        
        # 步骤3：获取最新的结算汇率和参考汇率数据
        print("\n3️⃣ 获取最新的汇率数据")
        settlement_df = get_settlement_exchange_rate()
        reference_df = get_reference_exchange_rate()
        
        # 步骤4：合并数据，优先使用结算汇率
        print("\n4️⃣ 合并汇率数据（优先使用结算汇率）")
        df = merge_and_select_exchange_data(settlement_df, reference_df)
        
        if df.empty:
            print("❌ 无法获取有效数据")
            return False
        
        # 步骤5：识别新增数据
        print("\n5️⃣ 识别新增数据")
        new_data_df = identify_new_data(df, existing_data)
        
        if new_data_df.empty:
            print("✅ 没有发现新增数据，无需更新")
            return True
        
        # 步骤6：准备数据格式
        print("\n6️⃣ 准备数据格式")
        feishu_data = prepare_update_data(new_data_df)
        
        if not feishu_data:
            print("❌ 数据准备失败")
            return False
        
        # 步骤7：追加数据到飞书表格
        print("\n7️⃣ 追加新增数据到飞书表格")
        success = append_to_feishu(feishu_data, token)
        
        if success:
            print("\n" + "=" * 60)
            print(f"🎉 增量更新成功！新增 {len(feishu_data)} 条数据")
            print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            return True
        else:
            print("\n❌ 增量更新失败")
            return False
            
    except Exception as e:
        print(f"❌ 程序执行过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== 主程序入口 ====================
if __name__ == "__main__":
    print("开始执行沪港通汇率更新程序...")
    result = incremental_update()
    
    if result:
        print("程序执行成功！")
        sys.exit(0)  # 成功退出
    else:
        print("程序执行失败！")
        sys.exit(1)  # 失败退出