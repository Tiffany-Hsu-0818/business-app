import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import time

# 嘗試匯入 yfinance，這是抓匯率的神器
try:
    import yfinance as yf
except ImportError:
    st.error("❌ 缺少必要套件！請在黑視窗輸入： pip install yfinance")
    st.stop()

# ==========================================
# 📍 設定區
# ==========================================
KEY_FILE = r'C:\Users\User\Desktop\業務登記表\service_account.json'
SPREADSHEET_KEY = '1Q1-JbHje0E-8QB0pu83OHN8jCPY8We9l2j1_7eZ8yas'

# ==========================================
# ☁️ Google Sheets 連線
# ==========================================
def get_google_sheet_client():
    if not os.path.exists(KEY_FILE):
        st.error(f"❌ 找不到金鑰檔案！請確認路徑：\n{KEY_FILE}")
        st.stop()
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)
    client = gspread.authorize(creds)
    return client

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

def load_data_from_gsheet():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        
        # 讀取公司 (Tab 2)
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

        # 讀取表單 (Tab 1)
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
# 💱 匯率核心 (Yahoo Finance 版)
# ==========================================
def get_yahoo_rate(target_currency, query_date, inverse=False):
    """
    使用 Yahoo Finance 查詢 TWD 匯率
    代碼規則：例如 美金對台幣 = "USDTWD=X"
    """
    # 建立 Yahoo 財經代碼
    ticker_symbol = f"{target_currency}TWD=X"
    
    rate = None
    found_date = None
    error_msg = ""
    
    # 嘗試往前找 5 天 (因為股市假日沒開盤)
    check_date = query_date
    for _ in range(5):
        try:
            # 下載該日期的資料
            # Yahoo 的 end date 是不包含的，所以要 +1 天
            start_d = check_date.strftime("%Y-%m-%d")
            end_d = (check_date + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # download 會回傳一個 DataFrame
            df = yf.download(ticker_symbol, start=start_d, end=end_d, progress=False)
            
            if not df.empty:
                # 取得收盤價 (Close)，並轉為浮點數
                # 新版 yfinance 格式有時會多一層，用 iloc 取值最穩
                raw_rate = float(df['Close'].iloc[0])
                
                if inverse:
                    # 反轉：1 TWD = ? 外幣
                    final_rate = 1 / raw_rate
                else:
                    # 正常：1 外幣 = ? TWD
                    final_rate = raw_rate
                    
                return final_rate, check_date, None
                
        except Exception as e:
            error_msg = str(e)
            
        # 沒找到，日期減 1 天繼續試
        check_date -= timedelta(days=1)
        
    return None, None, f"無法取得 {target_currency} 對台幣的匯率 (已追朔5天)。"

# ==========================================
# 🚀 主程式
# ==========================================
def main():
    st.set_page_config(page_title="雲端業務系統", layout="wide", page_icon="☁️")
    st.title("☁️ 雲端業務專案登記系統")
    
    if st.button("🔄 重新整理資料"):
        st.cache_data.clear()
        
    company_dict, df_business = load_data_from_gsheet()

    next_id = 1
    if not df_business.empty:
        try:
            ids = pd.to_numeric(df_business.iloc[:, 0], errors='coerce').dropna()
            if not ids.empty: next_id = int(ids.max()) + 1
        except: pass

    menu = st.sidebar.radio("選單", ["新增業務登記", "查看歷史資料"])

    if menu == "新增業務登記":
        st.subheader("📋 建立新專案")
        
        if 'ex_res' not in st.session_state: st.session_state['ex_res'] = ""

        with st.form("cloud_form"):
            c1, c2 = st.columns(2)
            with c1:
                input_date = st.date_input("填表日期", datetime.today())
                cat = st.selectbox("客戶類別", list(company_dict.keys())) if company_dict else ""
                comps = company_dict.get(cat, []) if company_dict else []
                client = st.selectbox("客戶名稱", comps)
            with c2:
                proj = st.text_input("案號 / 產品名稱")
                price = st.number_input("完稅價格", min_value=0, step=1000)

            st.markdown("---")
            d1, d2, d3 = st.columns(3)
            with d1: ex_del = st.date_input("🚚 預定交期", datetime.today())
            with d2: 
                has_inv = st.checkbox("已有發票日期?")
                inv_d = st.date_input("🧾 發票日期", datetime.today()) if has_inv else None
            with d3:
                has_pay = st.checkbox("已有收款日期?")
                pay_d = st.date_input("💰 收款日期", datetime.today()) if has_pay else None

            st.markdown("---")
            st.write("💱 **進出口匯率 (請先在下方查詢)**")
            final_ex = st.text_input("匯率內容", value=st.session_state['ex_res'])
            
            st.markdown("---")
            remark = st.text_area("備註")
            submit = st.form_submit_button("☁️ 上傳到雲端", type="primary")

        # --- 匯率小工具 ---
        with st.expander("🔍 匯率查詢小工具", expanded=True):
            c_e1, c_e2, c_e3, c_e4 = st.columns([2, 2, 2, 2])
            with c_e1: q_date = st.date_input("查詢日期", datetime.today())
            with c_e2: q_curr = st.selectbox("外幣", ["USD", "EUR", "JPY", "CNY", "GBP"])
            with c_e3: 
                is_inverse = st.checkbox("反轉 (台幣:外幣=1:?)", value=False)
                
            with c_e4:
                st.write("")
                if st.button("開始查詢"):
                    # 初始化變數
                    rate_val = None
                    found_d = None
                    err_msg = None
                    
                    with st.spinner("連線 Yahoo 財經資料庫..."):
                        rate_val, found_d, err_msg = get_yahoo_rate(q_curr, q_date, is_inverse)
                        
                        if rate_val:
                            d_str = found_d.strftime('%Y/%m/%d')
                            
                            if is_inverse:
                                # 1 台幣 = ? 外幣 (小數點通常比較多)
                                desc = f"{d_str} 1 TWD = {rate_val:.5f} {q_curr}"
                            else:
                                # 1 外幣 = ? 台幣 (一般小數點2-3位)
                                desc = f"{d_str} 1 {q_curr} = {rate_val:.3f} TWD"
                            
                            st.session_state['ex_res'] = desc
                            st.success(f"查詢成功！({d_str})")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error(f"失敗：{err_msg}")

        if submit:
            if not client or price == 0:
                st.error("❌ 請確認客戶名稱與價格")
            else:
                stages = ["交貨", "製造", "運輸", "安裝", "尾款"]
                rows = []
                ds = input_date.strftime("%Y-%m-%d")
                eds = ex_del.strftime("%Y-%m-%d")
                ids = inv_d.strftime("%Y-%m-%d") if has_inv else ""
                pds = pay_d.strftime("%Y-%m-%d") if has_pay else ""

                for i, s in enumerate(stages):
                    rows.append([
                        next_id if i==0 else "", ds if i==0 else "",
                        cat if i==0 else "", client if i==0 else "",
                        proj if i==0 else "", "", s, "",
                        price if i==0 else "", eds if i==0 else "",
                        "", ids if i==0 else "", "",
                        pds if i==0 else "",
                        final_ex if i==0 else "", 
                        "", remark if i==0 else ""
                    ])
                
                if append_to_gsheet(rows):
                    st.success(f"✅ 成功！編號：{next_id}")
                    st.session_state['ex_res'] = ""
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

    elif menu == "查看歷史資料":
        st.subheader("📊 雲端資料")
        st.dataframe(df_business)

if __name__ == "__main__":
    main()