import time
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

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

# ⭐ 新增功能：不快取，即時抓取目前最大的編號
def get_latest_next_id():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0) # 業務表單
        
        # 只抓第一欄 (編號欄) 的資料，速度快
        col_values = ws.col_values(1)
        
        # 過濾出純數字的編號 (排除標題 '編號' 或空白)
        ids = []
        for val in col_values:
            if str(val).isdigit():
                ids.append(int(val))
        
        if ids:
            return max(ids) + 1
        else:
            return 1 # 如果沒資料，從 1 開始
            
    except Exception:
        return 1

# 讀取整張表單資料 (維持快取，避免看歷史資料時卡頓)
@st.cache_data(ttl=60)
def load_data_from_gsheet():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        
        try:
            ws_c = sh.get_worksheet(1)
            if ws_c:
                dc = ws_c.get_all_values()
                if len(dc)>0:
                    hc = clean_headers(dc[0])
                    dfc = pd.DataFrame(dc[1:], columns=hc)
                    cd = {col: [x.strip() for x in dfc[col].values if x.strip()] for col in dfc.columns if [x for x in dfc[col].values if x.strip()]}
                else: cd = {}
            else: cd = {}
        except: cd = {}

        try:
            ws_f = sh.get_worksheet(0)
            if ws_f:
                df = ws_f.get_all_values()
                if len(df)>0:
                    hf = clean_headers(df[0])
                    df_b = pd.DataFrame(df[1:], columns=hf)
                else: df_b = pd.DataFrame()
            else: df_b = pd.DataFrame()
        except: df_b = pd.DataFrame()
             
        return cd, df_b
    except Exception as e:
        st.error(f"連線失敗: {e}")
        return {}, pd.DataFrame()

def append_to_gsheet(rows):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        sh.get_worksheet(0).append_rows(rows, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"寫入失敗: {e}")
        return False

# ==========================================
# 💱 匯率查詢
# ==========================================
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

# ==========================================
# 🚀 主程式
# ==========================================
def main():
    st.set_page_config(page_title="雲端業務系統", layout="wide", page_icon="☁️")
    st.title("☁️ 雲端業務專案登記系統")
    
    if st.button("🔄 重新整理資料"):
        st.cache_data.clear()
        st.rerun()
        
    # 1. 載入選單資料 (有快取)
    company_dict, df_business = load_data_from_gsheet()

    # 2. ⭐ 取得最新的編號 (無快取，保證即時) ⭐
    next_id = get_latest_next_id()

    menu = st.sidebar.radio("選單", ["新增業務登記", "查看歷史資料"])

    if menu == "新增業務登記":
        st.subheader(f"📋 建立新專案")
        if 'ex_res' not in st.session_state: st.session_state['ex_res'] = ""

        c1, c2 = st.columns(2)
        with c1:
            input_date = st.date_input("填表日期", datetime.today())
            
            cat_options = list(company_dict.keys()) + ["➕ 新增類別..."]
            selected_cat = st.selectbox("客戶類別", cat_options)
            
            if selected_cat == "➕ 新增類別...":
                final_cat = st.text_input("請輸入新類別名稱", placeholder="例如：醫療器材")
                client_options = ["➕ 新增客戶..."]
            else:
                final_cat = selected_cat
                client_options = company_dict.get(selected_cat, []) + ["➕ 新增客戶..."]

            selected_client = st.selectbox("客戶名稱", client_options)
            if selected_client == "➕ 新增客戶...":
                final_client = st.text_input("請輸入新客戶名稱", placeholder="例如：台積電")
            else:
                final_client = selected_client

        with c2:
            project_no = st.text_input("案號 / 產品名稱")
            price = st.number_input("完稅價格", min_value=0, step=1000)

        st.markdown("---")
        
        # 日期開關設定
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
                rows = []
                ds = input_date.strftime("%Y-%m-%d")
                
                eds = ex_del.strftime("%Y-%m-%d") if has_delivery and ex_del else ""
                ids = inv_d.strftime("%Y-%m-%d") if has_inv and inv_d else ""
                pds = pay_d.strftime("%Y-%m-%d") if has_pay and pay_d else ""

                # 這裡只寫入一行，階段欄位留白
                row_data = [
                    next_id,            # 0: 編號 (這會是最新的)
                    ds,                 # 1: 日期
                    final_cat,          # 2: 類別
                    final_client,       # 3: 客戶
                    project_no,         # 4: 案號
                    "",                 # 5: 空
                    "",                 # 6: 階段 (強制空白)
                    "",                 # 7: 空
                    price,              # 8: 完稅價格
                    eds,                # 9: 預定交期
                    "",                 # 10: 空
                    ids,                # 11: 發票日期
                    "",                 # 12: 空
                    pds,                # 13: 收款日期
                    final_ex,           # 14: 匯率
                    "",                 # 15: 空
                    remark              # 16: 備註
                ]
                rows.append(row_data)
                
                if append_to_gsheet(rows):
                    st.success(f"✅ 成功！編號：{next_id}")
                    if selected_client == "➕ 新增客戶...":
                        st.info(f"💡 新客戶「{final_client}」已記錄。")
                    
                    st.session_state['ex_res'] = ""
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()

    elif menu == "查看歷史資料":
        st.subheader("📊 雲端資料")
        st.dataframe(df_business)

if __name__ == "__main__":
    main()