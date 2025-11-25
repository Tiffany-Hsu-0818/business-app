import time
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import plotly.express as px

# 嘗試匯入 yfinance
try:
    import yfinance as yf
except ImportError:
    st.error("❌ 缺少必要套件！請確保 requirements.txt 有包含 yfinance")
    st.stop()

# ==========================================
# 📍 設定區
# ==========================================
SPREADSHEET_KEY = '1Q1-JbHje0E-8QB0pu83OHN8jCPY8We9l2j1_7eZ8yas'

# ==========================================
# ☁️ Google Sheets 連線
# ==========================================
def get_google_sheet_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    if "gcp_service_account" in st.secrets:
        try:
            key_dict = json.loads(st.secrets["gcp_service_account"]["json_content"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
            return gspread.authorize(creds)
        except Exception as e:
            st.error(f"雲端 Secrets 讀取失敗: {e}")
            st.stop()

    local_key_file = r'C:\Users\User\Desktop\業務登記表\service_account.json'
    if os.path.exists(local_key_file):
        creds = ServiceAccountCredentials.from_json_keyfile_name(local_key_file, scope)
        return gspread.authorize(creds)
    
    st.error("❌ 找不到金鑰！")
    st.stop()

def clean_headers(headers):
    cleaned = []
    seen = {}
    for i, col in enumerate(headers):
        c = str(col).strip()
        if not c: c = f"未命名_{i}"
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cleaned.append(c)
    return cleaned

def smart_append_to_gsheet(data_dict):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0)
        headers = ws.row_values(1)
        row_to_append = [""] * len(headers)
        
        for col_name, value in data_dict.items():
            try:
                idx = next(i for i, h in enumerate(headers) if str(h).strip() == col_name)
                row_to_append[idx] = value
            except StopIteration:
                pass
                
        ws.append_row(row_to_append, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"寫入失敗: {e}")
        return False

def get_latest_next_id():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0)
        col_values = ws.col_values(1)
        ids = [int(x) for x in col_values if str(x).isdigit()]
        return max(ids) + 1 if ids else 1
    except:
        return 1

@st.cache_data(ttl=60)
def load_data_from_gsheet():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        
        # 讀取公司
        try:
            ws_c = sh.get_worksheet(1)
            if ws_c:
                data = ws_c.get_all_records()
                df = pd.DataFrame(data)
                cd = {col: [str(x).strip() for x in df[col].values if str(x).strip()] for col in df.columns}
            else: cd = {}
        except: cd = {}

        # 讀取歷史紀錄
        try:
            ws_f = sh.get_worksheet(0)
            if ws_f:
                data = ws_f.get_all_values()
                if len(data) > 0:
                    headers = clean_headers(data[0])
                    df_b = pd.DataFrame(data[1:], columns=headers)
                else:
                    df_b = pd.DataFrame()
            else: df_b = pd.DataFrame()
        except: df_b = pd.DataFrame()
             
        return cd, df_b
    except Exception as e:
        st.error(f"連線失敗: {e}")
        return {}, pd.DataFrame()

def get_yahoo_rate(target_currency, query_date, inverse=False):
    ticker_symbol = f"{target_currency}TWD=X"
    check_date = query_date
    for _ in range(5):
        try:
            start_d = check_date.strftime("%Y-%m-%d")
            end_d = (check_date + timedelta(days=1)).strftime("%Y-%m-%d")
            df = yf.download(ticker_symbol, start=start_d, end=end_d, progress=False)
            if not df.empty:
                try: raw_rate = float(df['Close'].iloc[0])
                except: raw_rate = float(df['Close'].values[0])
                if inverse: return 1 / raw_rate, check_date, None
                else: return raw_rate, check_date, None
        except: pass
        check_date -= timedelta(days=1)
    return None, None, "無法取得匯率"

# ⭐ 新增：台灣日期翻譯機 (解決 114/01/01 看不懂的問題)
def parse_taiwan_date(date_str):
    if pd.isna(date_str) or date_str == "":
        return pd.NaT
    
    s = str(date_str).strip()
    
    # 嘗試標準格式 (2025-01-01)
    try:
        return pd.to_datetime(s)
    except:
        pass
    
    # 嘗試民國年格式 (114/01/01, 114.01.01)
    try:
        # 把 . 換成 /
        s = s.replace(".", "/")
        parts = s.split('/')
        
        # 如果是 3 個部分 (年/月/日) 且年份是 2位或3位數 (例如 114)
        if len(parts) == 3 and len(parts[0]) <= 3:
            year = int(parts[0]) + 1911
            return pd.to_datetime(f"{year}-{parts[1]}-{parts[2]}")
    except:
        pass
        
    return pd.NaT

# ==========================================
# 🚀 主程式
# ==========================================
def main():
    st.set_page_config(page_title="雲端業務系統", layout="wide", page_icon="☁️")
    st.title("☁️ 雲端業務專案登記系統")
    
    if st.button("🔄 重新整理資料"):
        st.cache_data.clear()
        st.rerun()
        
    company_dict, df_business = load_data_from_gsheet()
    next_id = get_latest_next_id()

    menu = st.sidebar.radio("選單", ["新增業務登記", "查看歷史資料"])

    if menu == "新增業務登記":
        st.subheader(f"📋 建立新專案 (新編號: {next_id})")
        if 'ex_res' not in st.session_state: st.session_state['ex_res'] = ""

        c1, c2 = st.columns(2)
        with c1:
            input_date = st.date_input("填表日期", datetime.today())
            
            cat_options = list(company_dict.keys()) + ["➕ 新增類別..."]
            selected_cat = st.selectbox("客戶類別", cat_options)
            if selected_cat == "➕ 新增類別...":
                final_cat = st.text_input("請輸入新類別名稱")
                client_options = ["➕ 新增客戶..."]
            else:
                final_cat = selected_cat
                client_options = company_dict.get(selected_cat, []) + ["➕ 新增客戶..."]

            selected_client = st.selectbox("客戶名稱", client_options)
            if selected_client == "➕ 新增客戶...":
                final_client = st.text_input("請輸入新客戶名稱")
            else:
                final_client = selected_client

        with c2:
            project_no = st.text_input("案號 / 產品名稱")
            price = st.number_input("完稅價格", min_value=0, step=1000)

        st.markdown("---")
        d1, d2, d3 = st.columns(3)
        with d1: 
            has_delivery = st.checkbox("已有預定交期?", value=False)
            if has_delivery:
                ex_del = st.date_input("🚚 預定交期", datetime.today())
            else:
                ex_del = None

        with d2: 
            has_inv = st.checkbox("已有發票日期?", value=False)
            if has_inv:
                inv_d = st.date_input("🧾 發票日期", datetime.today())
            else:
                inv_d = None

        with d3:
            has_pay = st.checkbox("已有收款日期?", value=False)
            if has_pay:
                pay_d = st.date_input("💰 收款日期", datetime.today())
            else:
                pay_d = None

        st.markdown("---")
        st.write("💱 **進出口匯率**")
        final_ex = st.text_input("匯率內容 (請使用下方小工具查詢)", value=st.session_state['ex_res'])
        
        st.markdown("---")
        remark = st.text_area("備註")
        
        submit = st.button("☁️ 上傳到雲端", type="primary")

        with st.expander("🔍 匯率查詢小工具", expanded=False):
            c_e1, c_e2, c_e3, c_e4 = st.columns([2, 2, 2, 2])
            with c_e1: q_date = st.date_input("查詢日期", datetime.today())
            with c_e2: q_curr = st.selectbox("外幣", ["USD", "EUR", "JPY", "CNY", "GBP"])
            with c_e3: is_inverse = st.checkbox("反轉 (台幣:外幣=1:?)", value=False)
            with c_e4:
                st.write("")
                if st.button("開始查詢"):
                    with st.spinner("連線中..."):
                        rate_val, found_d, err_msg = get_yahoo_rate(q_curr, q_date, is_inverse)
                        if rate_val:
                            d_str = found_d.strftime('%Y/%m/%d')
                            if is_inverse: desc = f"{d_str} 1 TWD = {rate_val:.5f} {q_curr}"
                            else: desc = f"{d_str} 1 {q_curr} = {rate_val:.3f} TWD"
                            st.session_state['ex_res'] = desc
                            st.success("成功！")
                            time.sleep(0.5)
                            st.rerun()
                        else: st.error(f"失敗：{err_msg}")

        if submit:
            if not final_client or price == 0:
                st.error("❌ 資料不完整：請確認客戶名稱與金額")
            else:
                ds_str = input_date.strftime("%Y-%m-%d")
                eds_str = ex_del.strftime("%Y-%m-%d") if has_delivery and ex_del else ""
                ids_str = inv_d.strftime("%Y-%m-%d") if has_inv and inv_d else ""
                pds_str = pay_d.strftime("%Y-%m-%d") if has_pay and pay_d else ""

                data_to_save = {
                    "編號": next_id,
                    "日期": ds_str,
                    "客戶類別": final_cat,
                    "客戶名稱": final_client,
                    "案號": project_no,
                    "完稅價格": price,
                    "預定交期": eds_str,
                    "發票日期": ids_str,
                    "收款日期": pds_str,
                    "進出口匯率": final_ex,
                    "備註": remark,
                    "階段性款項": "" 
                }
                
                if smart_append_to_gsheet(data_to_save):
                    st.success(f"✅ 成功！編號：{next_id}")
                    if selected_client == "➕ 新增客戶...":
                        st.info(f"💡 新客戶「{final_client}」已記錄。")
                    
                    st.session_state['ex_res'] = ""
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()

    elif menu == "📊 數據戰情室":
        st.title("📊 數據戰情室")
        
        if df_business.empty:
            st.info("目前尚無資料。")
        else:
            try:
                df_clean = df_business.copy()
                
                # 1. 金額轉數字
                price_col = next((c for c in df_clean.columns if '價格' in c or '金額' in c), None)
                if price_col:
                    df_clean[price_col] = df_clean[price_col].astype(str).str.replace(',', '').replace('', '0')
                    df_clean[price_col] = pd.to_numeric(df_clean[price_col], errors='coerce').fillna(0)
                
                # 2. 日期轉 datetime (使用自製的台灣日期翻譯機)
                date_col = next((c for c in df_clean.columns if '日期' in c), None)
                if date_col:
                    # 這裡使用 apply 來逐行翻譯日期
                    df_clean['converted_date'] = df_clean[date_col].apply(parse_taiwan_date)
                    
                    # 移除真的無法辨識的 (例如全空白或亂碼)
                    df_clean = df_clean.dropna(subset=['converted_date'])
                    
                    # 3. 提取年份
                    df_clean['Year'] = df_clean['converted_date'].dt.year
                    
                    if not df_clean.empty:
                        all_years = sorted(df_clean['Year'].unique().astype(int), reverse=True)
                        selected_year = st.selectbox("📅 請選擇年份", all_years)
                        
                        df_final = df_clean[df_clean['Year'] == selected_year]
                        
                        st.markdown(f"### 📊 {selected_year} 年度總覽")
                        
                        total_rev = df_final[price_col].sum()
                        total_count = len(df_final)
                        
                        k1, k2, k3 = st.columns(3)
                        k1.metric("總營業額", f"${total_rev:,.0f}")
                        k2.metric("總案件數", f"{total_count} 件")
                        if total_count > 0:
                            k3.metric("平均客單價", f"${total_rev/total_count:,.0f}")
                        
                        st.divider()

                        c1, c2 = st.columns(2)
                        with c1:
                            st.subheader("📈 客戶類別佔比")
                            cat_col = next((c for c in df_final.columns if '類別' in c), None)
                            if cat_col:
                                fig_pie = px.pie(df_final, names=cat_col, values=price_col, hole=0.4)
                                st.plotly_chart(fig_pie, use_container_width=True)

                        with c2:
                            st.subheader("📅 每月業績趨勢")
                            df_monthly = df_final.resample('M', on='converted_date')[price_col].sum().reset_index()
                            if not df_monthly.empty:
                                df_monthly['Month_Str'] = df_monthly['converted_date'].dt.strftime('%Y-%m')
                                fig_bar = px.bar(df_monthly, x='Month_Str', y=price_col, 
                                                 title="月營收分佈", labels={'Month_Str':'月份', price_col:'金額'})
                                st.plotly_chart(fig_bar, use_container_width=True)
                            else:
                                st.info("該年份無足夠資料繪製趨勢圖")

                        with st.expander(f"檢視 {selected_year} 年詳細資料表格"):
                            display_cols = [c for c in df_final.columns if c not in ['converted_date', 'Year']]
                            st.dataframe(df_final[display_cols], use_container_width=True)
                    else:
                        st.warning("日期欄位解析後無有效資料。請檢查 Google Sheet 的日期格式。")
                        # 顯示原始資料幫助除錯
                        with st.expander("查看原始資料 (除錯用)"):
                            st.dataframe(df_business)
                else:
                    st.error("找不到「日期」欄位，無法進行時間分析。")

            except Exception as e:
                st.error(f"數據分析發生錯誤: {e}")
                st.dataframe(df_business)

if __name__ == "__main__":
    main()