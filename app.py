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

# ⭐ 新增：智慧寫入功能
# 這會自動去對應 Google Sheet 的標題，不再怕欄位順序變動
def smart_append_to_gsheet(data_dict):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0) # 業務表單
        
        # 1. 抓取目前所有的標題 (第一列)
        headers = ws.row_values(1)
        
        # 2. 準備一個全空的列表，長度跟標題一樣
        row_to_append = [""] * len(headers)
        
        # 3. 依照標題名稱，把資料填入正確的位置
        # 這樣就算中間刪除了空白欄，或者欄位互換，都能填對！
        for col_name, value in data_dict.items():
            # 尋找標題在哪一欄 (模糊比對，移除前後空白)
            try:
                # 找出對應的 index
                # 使用 strip() 避免標題有空白鍵導致找不到
                idx = next(i for i, h in enumerate(headers) if str(h).strip() == col_name)
                row_to_append[idx] = value
            except StopIteration:
                # 如果找不到該標題，就不填 (不會報錯)
                pass
                
        # 4. 寫入資料
        ws.append_row(row_to_append, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"寫入失敗: {e}")
        return False

# 讀取最新編號 (維持原樣)
def get_latest_next_id():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0)
        col_values = ws.col_values(1) # 第一欄
        ids = [int(x) for x in col_values if str(x).isdigit()]
        return max(ids) + 1 if ids else 1
    except:
        return 1

# 載入資料 (維持快取)
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
                # 轉成字典
                cd = {col: [str(x).strip() for x in df[col].values if str(x).strip()] for col in df.columns}
            else: cd = {}
        except: cd = {}

        # 讀取歷史紀錄
        try:
            ws_f = sh.get_worksheet(0)
            if ws_f:
                data = ws_f.get_all_records()
                df_b = pd.DataFrame(data)
            else: df_b = pd.DataFrame()
        except: df_b = pd.DataFrame()
             
        return cd, df_b
    except Exception as e:
        st.error(f"連線失敗: {e}")
        return {}, pd.DataFrame()

# 匯率查詢
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
        
        # ⭐⭐ 日期與開關 (預設全部 False) ⭐⭐
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
        final_ex = st.text_input("匯率內容", value=st.session_state['ex_res'])
        
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
                # 準備資料 (處理日期字串)
                ds_str = input_date.strftime("%Y-%m-%d")
                eds_str = ex_del.strftime("%Y-%m-%d") if has_delivery and ex_del else ""
                ids_str = inv_d.strftime("%Y-%m-%d") if has_inv and inv_d else ""
                pds_str = pay_d.strftime("%Y-%m-%d") if has_pay and pay_d else ""

                # ⭐⭐ 關鍵：建立資料字典 (Data Dictionary) ⭐⭐
                # 這裡的 Key (左邊的字) 必須跟您 Google Sheet 的第一列標題 一模一樣！
                # 程式會自動去對應位置，所以不會再填錯格了
                
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
                    "階段性款項": "" # 強制留白 (不寫交貨)
                }
                
                if smart_append_to_gsheet(data_to_save):
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