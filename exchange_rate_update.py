#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
沪港通结算汇率自动更新程序 - GitHub Actions版本
仅使用结算汇率数据，实现增量更新
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
            
            # 显示数据基本信息
            print(f"数据列名：{df.columns.tolist()}")
            print(f"数据日期范围：{df.iloc[0, 0]} 到 {df.iloc[-1, 0]}")
            
            return df
        else:
            print("⚠️ 结算汇率数据为空")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ 获取结算汇率数据失败: {e}")
        return pd.DataFrame()

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
                return [], []  # 返回空的数据和表头
            
            # 第一行是表头
            headers = values[0]
            # 从第二行开始是数据
            existing_data = values[1:]
            
            print(f"📊 读取到 {len(existing_data)} 条现有数据")
            print(f"表头：{headers}")
            
            return existing_data, headers
        else:
            print(f"❌ 读取数据失败: {result}")
            return [], []
    except Exception as e:
        print(f"❌ 读取数据时出错: {e}")
        return [], []

def prepare_new_data(df):
    """准备新的结算汇率数据"""
    if df is None or df.empty:
        print("⚠️ 没有新数据可处理")
        return pd.DataFrame()
    
    try:
        # 添加更新时间列
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = df.copy()
        
        # 如果还没有更新时间列，则添加
        if '更新时间' not in df.columns:
            df['更新时间'] = current_time
        
        # 确保数据按日期排序（最新的在前面）
        # 假设第一列是日期列
        date_column = df.columns[0]
        df = df.sort_values(by=date_column, ascending=False).reset_index(drop=True)
        
        print(f"✅ 准备完成 {len(df)} 条结算汇率数据")
        return df
    except Exception as e:
        print(f"❌ 准备数据时出错: {e}")
        return pd.DataFrame()

def identify_new_data(df, existing_data, existing_headers):
    """
    比较新获取的数据和现有数据，找出新增的数据
    
    参数:
    df: 新获取的DataFrame数据
    existing_data: 飞书表格中现有的数据（二维列表）
    existing_headers: 飞书表格中的表头
    
    返回:
    new_data_df: 新增数据的DataFrame
    """
    if df is None or df.empty:
        print("⚠️ 没有新数据可处理")
        return pd.DataFrame()
    
    if not existing_data:  # 如果表格中没有数据，所有数据都是新的
        print("📝 表格为空，所有数据都是新增数据")
        return df
    
    try:
        # 将现有数据转换为DataFrame以便比较
        # 假设现有数据的第一列是日期（通常是交易日期）
        existing_dates = []
        for row in existing_data:
            if row:  # 确保行不为空
                existing_dates.append(str(row[0]))  # 第一列是日期
        
        print(f"📅 现有数据中的日期数量: {len(existing_dates)}")
        if existing_dates:
            print(f"现有数据的最新日期: {existing_dates[0]}")
            print(f"现有数据的最早日期: {existing_dates[-1]}")
        
        # 获取新数据中的日期列（假设是第一列）
        if df.empty:
            print("❌ 新数据为空")
            return pd.DataFrame()
        
        # 获取日期列（尝试找到日期列名，否则用第一列）
        date_column = None
        for col in df.columns:
            if '日期' in col or 'date' in col.lower():
                date_column = col
                break
        
        if not date_column:
            date_column = df.columns[0]
            print(f"⚠️ 未找到明确的日期列，使用第一列: {date_column}")
        
        new_dates = df[date_column].astype(str).tolist()
        print(f"新数据中的最新日期: {new_dates[0] if new_dates else '无'}")
        print(f"新数据中的最早日期: {new_dates[-1] if new_dates else '无'}")
        
        # 找出新增的日期
        new_date_indices = []
        for i, date in enumerate(new_dates):
            if str(date) not in existing_dates:
                new_date_indices.append(i)
        
        print(f"🔍 找到 {len(new_date_indices)} 条新增数据")
        
        if new_date_indices:
            # 提取新增数据
            new_data_df = df.iloc[new_date_indices].copy()
            print(f"新增数据日期: {new_data_df[date_column].head().tolist()}")
            return new_data_df
        else:
            print("✅ 没有发现新增数据，所有数据都已存在")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ 识别新增数据时出错: {e}")
        print("将尝试将所有数据作为新增数据处理")
        return df

def prepare_update_data(new_data_df, existing_headers):
    """
    准备要更新到飞书的数据格式
    
    参数:
    new_data_df: 新增数据的DataFrame
    existing_headers: 飞书表格中现有的表头
    
    返回:
    feishu_data: 飞书API所需的二维列表格式
    """
    if new_data_df is None or new_data_df.empty:
        print("⚠️ 没有新增数据需要更新")
        return []
    
    try:
        # 确保有更新时间列
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_data_df = new_data_df.copy()
        
        # 如果还没有更新时间列，则添加
        if '更新时间' not in new_data_df.columns:
            new_data_df['更新时间'] = current_time
        
        # 将数据转换为列表格式
        data_rows = []
        for _, row in new_data_df.iterrows():
            # 将每行数据转换为列表
            row_data = []
            for value in row:
                # 处理NaN值，飞书不接受NaN
                if pd.isna(value):
                    row_data.append("")
                else:
                    # 转换为字符串格式
                    row_data.append(str(value))
            data_rows.append(row_data)
        
        print(f"✅ 准备完成 {len(data_rows)} 行新增数据")
        return data_rows
    except Exception as e:
        print(f"❌ 准备数据时出错: {e}")
        return []

def append_to_feishu(feishu_data, token):
    """
    将新增数据追加到飞书表格
    
    参数:
    feishu_data: 要追加的数据（二维列表，不包含表头）
    token: 飞书访问令牌
    """
    if not feishu_data:
        print("⚠️ 没有数据需要追加")
        return True
    
    try:
        # 飞书表格追加数据的API
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values_append"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # 构建请求数据
        # 注意：这里values只包含数据行，不包含表头
        request_data = {
            "valueRange": {
                "range": SHEET_ID,  # 工作表ID
                "values": feishu_data
            }
        }
        
        # 发送请求
        print(f"正在追加 {len(feishu_data)} 行新数据到飞书表格...")
        response = requests.post(url, headers=headers, json=request_data)
        result = response.json()
        
        # 检查响应
        if result.get("code") == 0:
            updates = result.get("data", {}).get("updates", {})
            print(f"✅ 成功追加数据到飞书表格！")
            print(f"更新范围：{updates.get('updatedRange', '未知')}")
            print(f"更新行数：{updates.get('updatedRows', '未知')}")
            return True
        else:
            print(f"❌ 追加数据失败: {result}")
            
            # 尝试其他方法：如果追加失败，尝试普通更新
            print("尝试使用更新接口...")
            return update_to_feishu(feishu_data, token)
    except Exception as e:
        print(f"❌ 追加数据时出错: {e}")
        return False

def update_to_feishu(feishu_data, token):
    """
    备用更新方法：使用普通更新接口
    """
    try:
        # 首先获取当前数据行数
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values/{SHEET_ID}"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers)
        result = response.json()
        
        if result.get("code") == 0:
            data = result.get("data", {})
            values = data.get("valueRange", {}).get("values", [])
            current_row_count = len(values) if values else 1  # 至少1行（表头）
            
            # 计算起始行号（从1开始）
            start_row = current_row_count + 1
            
            # 更新URL（指定具体范围）
            update_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values"
            
            # 构建更新范围，例如："9ee237!A10:C20"
            end_row = start_row + len(feishu_data) - 1
            col_count = len(feishu_data[0]) if feishu_data else 1
            end_col = chr(65 + col_count - 1) if col_count <= 26 else 'Z'  # A-Z列
            
            range_str = f"{SHEET_ID}!A{start_row}:{end_col}{end_row}"
            
            update_data = {
                "valueRanges": [{
                    "range": range_str,
                    "values": feishu_data
                }]
            }
            
            update_response = requests.put(update_url, headers=headers, json=update_data)
            update_result = update_response.json()
            
            if update_result.get("code") == 0:
                print(f"✅ 成功更新数据到飞书表格！")
                print(f"更新范围：{range_str}")
                return True
            else:
                print(f"❌ 更新数据失败: {update_result}")
                return False
        else:
            print(f"❌ 无法获取当前数据行数: {result}")
            return False
    except Exception as e:
        print(f"❌ 更新数据时出错: {e}")
        return False

def incremental_update():
    """
    主函数：实现增量更新，只添加新数据
    """
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
        existing_data, existing_headers = read_existing_feishu_data(token)
        
        # 步骤3：获取最新的结算汇率数据
        print("\n3️⃣ 获取最新的结算汇率数据")
        df = get_settlement_exchange_rate()
        
        if df is None or df.empty:
            print("❌ 无法获取结算汇率数据")
            return False
        
        # 步骤4：准备新数据
        print("\n4️⃣ 准备新数据")
        prepared_df = prepare_new_data(df)
        
        if prepared_df.empty:
            print("❌ 数据准备失败")
            return False
        
        # 步骤5：识别新增数据
        print("\n5️⃣ 识别新增数据")
        new_data_df = identify_new_data(prepared_df, existing_data, existing_headers)
        
        if new_data_df.empty:
            print("✅ 没有发现新增数据，无需更新")
            print("=" * 60)
            return True
        
        # 步骤6：准备数据格式
        print("\n6️⃣ 准备数据格式")
        feishu_data = prepare_update_data(new_data_df, existing_headers)
        
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
            print("\n" + "=" * 60)
            print("❌ 增量更新失败")
            print("=" * 60)
            return False
            
    except Exception as e:
        print(f"❌ 程序执行过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def full_sync():
    """
    完整同步：清空旧数据，重新导入所有数据（谨慎使用）
    """
    print("=" * 60)
    print("沪港通结算汇率数据 - 完整同步")
    print("=" * 60)
    
    # 获取飞书访问令牌
    token = get_feishu_token()
    if not token:
        return False
    
    # 获取数据
    df = get_settlement_exchange_rate()
    if df is None or df.empty:
        return False
    
    # 准备数据（包含表头）
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.copy()
    
    # 检查是否已经有'更新时间'列
    if '更新时间' not in df.columns:
        df['更新时间'] = current_time
    
    # 转换为列表（包含表头）
    headers = df.columns.tolist()
    data_rows = []
    for _, row in df.iterrows():
        row_data = [str(row[col]) if not pd.isna(row[col]) else "" for col in headers]
        data_rows.append(row_data)
    
    feishu_data = [headers] + data_rows
    
    # 清空表格
    print("\n清空表格...")
    clear_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values_clear"
    headers_auth = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    clear_data = {
        "range": SHEET_ID
    }
    
    clear_response = requests.post(clear_url, headers=headers_auth, json=clear_data)
    
    # 写入所有数据
    print("写入所有数据...")
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{TABLE_TOKEN}/values"
    update_data = {
        "valueRanges": [{
            "range": SHEET_ID,
            "values": feishu_data
        }]
    }
    
    response = requests.put(url, headers=headers_auth, json=update_data)
    result = response.json()
    
    if result.get("code") == 0:
        print(f"✅ 完整同步成功！共 {len(data_rows)} 条数据")
        return True
    else:
        print(f"❌ 完整同步失败: {result}")
        return False

# ==================== 主程序入口 ====================
if __name__ == "__main__":
    print("开始执行沪港通结算汇率更新程序...")
    
    # 在GitHub Actions中总是执行增量更新
    result = incremental_update()
    
    if result:
        print("程序执行成功！")
        sys.exit(0)  # 成功退出
    else:
        print("程序执行失败！")
        sys.exit(1)  # 失败退出
