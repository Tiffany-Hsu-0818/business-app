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
    pass # 允許在沒有 yfinance 的情況下執行，但功能會受限

# ==========================================
# 📍 設定區
# ==========================================
SPREADSHEET_KEY = '1Q1-JbHje0E-8QB0pu83OHN8jCPY8We9l2j1_7eZ8yas'

# 初始化 Session State
if 'current_page' not in st.session_state: st.session_state['current_page'] = "📝 新增業務登記"
if 'edit_mode' not in st.session_state: st.session_state['edit_mode'] = False
if 'edit_data' not in st.session_state: st.session_state['edit_data'] = {}
if 'ex_res' not in st.session_state: st.session_state['ex_res'] = ""
if 'inv_list' not in st.session_state: st.session_state['inv_list'] = []
if 'pay_list' not in st.session_state: st.session_state['pay_list'] = []

# ==========================================
# ☁️ Google Sheets 連線與工具函式
# ==========================================
def get_google_sheet_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    for attempt in range(3):
        try:
            if "gcp_service_account" in st.secrets:
                key_dict = json.loads(st.secrets["gcp_service_account"]["json_content"])
                creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
            else:
                local_key_file = r'service_account.json' # 請確保路徑正確或使用 secrets
                if os.path.exists(local_key_file):
                    creds = ServiceAccountCredentials.from_json_keyfile_name(local_key_file, scope)
                else:
                    # 本地找不到就試著用預設路徑 (相容舊程式碼)
                    local_key_file_old = r'C:\Users\User\Desktop\業務登記表\service_account.json'
                    if os.path.exists(local_key_file_old):
                        creds = ServiceAccountCredentials.from_json_keyfile_name(local_key_file_old, scope)
                    else:
                        st.error("❌ 找不到金鑰檔案 (service_account.json)！")
                        st.stop()
            return gspread.authorize(creds)
        except Exception as e:
            if "503" in str(e):
                time.sleep(2)
                continue
            st.error(f"連線失敗: {e}")
            st.stop()
    st.error("❌ Google 伺服器忙線中")
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

def parse_taiwan_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == "": return pd.NaT
    s = str(date_str).split(',')[0].strip().replace(".", "/")
    try:
        parts = s.split('/')
        if len(parts) == 2:
            this_year = datetime.now().year
            return pd.to_datetime(f"{this_year}-{parts[0]}-{parts[1]}")
        elif len(parts) == 3:
            year_val = int(parts[0])
            if year_val < 1911: year_val += 1911
            return pd.to_datetime(f"{year_val}-{parts[1]}-{parts[2]}")
        else: return pd.to_datetime(s)
    except: return pd.NaT

@st.cache_data(ttl=60)
def load_data_from_gsheet():
    for attempt in range(3):
        try:
            client = get_google_sheet_client()
            sh = client.open_by_key(SPREADSHEET_KEY)
            
            # 讀取公司名單
            try:
                ws_c = sh.get_worksheet(1)
                if ws_c:
                    data = ws_c.get_all_values()
                    if len(data) > 1:
                        headers = clean_headers(data[0])
                        df = pd.DataFrame(data[1:], columns=headers)
                        df = df.replace(r'^\s*$', pd.NA, regex=True).dropna(how='all')
                        # 轉為字典 {Category: [Client1, Client2...]}
                        cd = {col: [str(x).strip() for x in df[col].values if pd.notna(x) and str(x).strip()] for col in df.columns}
                    else: cd = {}
                else: cd = {}
            except: cd = {}

            # 讀取業務紀錄
            try:
                ws_f = sh.get_worksheet(0)
                if ws_f:
                    all_values = ws_f.get_all_values()
                    header_idx = -1
                    # 尋找標題列
                    for i, row in enumerate(all_values[:10]):
                        r_str = [str(r).strip() for r in row]
                        if "編號" in r_str and "日期" in r_str:
                            header_idx = i
                            break
                    
                    if header_idx != -1 and len(all_values) > header_idx + 1:
                        headers = clean_headers(all_values[header_idx])
                        df_b = pd.DataFrame(all_values[header_idx+1:], columns=headers)
                        # 過濾掉沒有編號的空行
                        if '編號' in df_b.columns:
                            df_b = df_b[pd.to_numeric(df_b['編號'], errors='coerce').notna()]
                        else: df_b = pd.DataFrame()
                    else: df_b = pd.DataFrame()
                else: df_b = pd.DataFrame()
            except: df_b = pd.DataFrame()
            
            return cd, df_b
        except Exception as e:
            if "503" in str(e): time.sleep(2); continue
            return {}, pd.DataFrame()
    return {}, pd.DataFrame()

# ==========================================
# 🛠️ 資料處理邏輯 (包含功能 2: 類別移動)
# ==========================================

def update_company_category_in_sheet(client_name, new_category):
    """
    功能 2 核心：檢查客戶是否已存在，若存在但類別不同，則移動之。
    若不存在，則新增。
    """
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(1) # 假設公司名單在第二個分頁
        
        # 取得目前所有資料
        all_cols = ws.get_all_values()
        if not all_cols: return False, "讀取失敗"
        
        headers = [h.strip() for h in all_cols[0]]
        
        # 1. 尋找新類別所在的欄位 Index (如果沒有就新增欄)
        if new_category in headers:
            new_col_idx = headers.index(new_category) + 1
        else:
            new_col_idx = len(headers) + 1
            ws.update_cell(1, new_col_idx, new_category)
            headers.append(new_category)

        # 2. 檢查該客戶是否已經存在於某個欄位中
        found_row, found_col = None, None
        existing_category = None
        
        # 遍歷尋找 (為了精確比對，不使用 API find，改用本地遍歷)
        for c_idx, col_name in enumerate(headers):
            # 取得該欄所有值
            col_vals = [row[c_idx] for row in all_cols if len(row) > c_idx]
            if client_name in col_vals:
                r_idx = col_vals.index(client_name)
                # Google Sheet index 從 1 開始
                found_row = r_idx + 1 
                found_col = c_idx + 1
                existing_category = col_name
                break
        
        # 3. 邏輯判斷
        if found_row and found_col:
            # 客戶已存在
            if existing_category == new_category:
                return True, "客戶類別未變動"
            else:
                # 客戶存在但類別不同 -> 移動 (先刪舊的，再加新的)
                ws.update_cell(found_row, found_col, "") # 清空舊位置
                # 找到新欄位的第一個空位
                new_col_values = ws.col_values(new_col_idx)
                next_row = len(new_col_values) + 1
                # 如果原本位置只是變成空字串，可能中間會有洞，但這裡簡化處理直接 append 到新欄位
                ws.update_cell(next_row, new_col_idx, client_name)
                return True, f"已將客戶從「{existing_category}」移動至「{new_category}」"
        else:
            # 客戶不存在 -> 新增
            new_col_values = ws.col_values(new_col_idx)
            next_row = len(new_col_values) + 1
            ws.update_cell(next_row, new_col_idx, client_name)
            return True, f"已新增客戶至「{new_category}」"

    except Exception as e:
        return False, f"更新公司名單失敗: {e}"

def smart_save_record(data_dict, is_update=False):
    """
    統一處理新增與更新 (功能 3 支援)
    """
    for attempt in range(3):
        try:
            client = get_google_sheet_client()
            sh = client.open_by_key(SPREADSHEET_KEY)
            ws = sh.get_worksheet(0)
            
            all_values = ws.get_all_values()
            headers = []
            header_row_idx = 0
            
            # 找標題
            for i, row in enumerate(all_values[:10]):
                r_str = [str(r).strip() for r in row]
                if "編號" in r_str and "日期" in r_str:
                    headers = row
                    header_row_idx = i
                    break
            if not headers: return False, "找不到標題列"

            # 準備要寫入的資料 list
            row_to_write = [""] * len(headers)
            for col_name, value in data_dict.items():
                try:
                    idx = next(i for i, h in enumerate(headers) if str(h).strip() == col_name)
                    row_to_write[idx] = str(value)
                except StopIteration: pass

            target_id = str(data_dict.get("編號"))

            if is_update:
                # 更新模式：尋找 ID 所在的列
                try:
                    id_col_idx = headers.index("編號")
                    id_list = ws.col_values(id_col_idx + 1)
                    # 尋找 ID (跳過標題前)
                    try:
                        row_index = id_list.index(target_id) + 1
                        # 更新整列
                        # 取得該列原本資料，避免沒填的欄位被清空 (Optional, 視需求)
                        # 這裡直接覆蓋
                        ws.update(f"A{row_index}", [row_to_write], value_input_option='USER_ENTERED')
                        return True, f"編號 {target_id} 更新成功"
                    except ValueError:
                        return False, "找不到原始編號，無法更新"
                except Exception as ex:
                    return False, str(ex)
            else:
                # 新增模式：直接 Append
                ws.append_row(row_to_write, value_input_option='USER_ENTERED')
                return True, f"編號 {target_id} 新增成功"

        except Exception as e:
            if "503" in str(e): time.sleep(2); continue
            return False, f"寫入失敗: {e}"
    return False, "連線逾時"

def calculate_next_id(df_all, target_year):
    if df_all.empty: return 1
    if '編號' not in df_all.columns: return 1
    # 簡單邏輯：取最大值+1 (不分年份，若要分年份需先濾)
    # 這裡假設編號是連續整數
    try:
        ids = pd.to_numeric(df_all['編號'], errors='coerce').dropna()
        if ids.empty: return 1
        return int(ids.max()) + 1
    except: return 1

def get_yahoo_rate(target_currency, query_date, inverse=False):
    # 簡易實作，避免 yfinance 報錯影響主程式
    try:
        ticker_symbol = f"{target_currency}TWD=X"
        check_date = query_date
        for _ in range(5):
            start_d = check_date.strftime("%Y-%m-%d")
            end_d = (check_date + timedelta(days=1)).strftime("%Y-%m-%d")
            df = yf.download(ticker_symbol, start=start_d, end=end_d, progress=False)
            if not df.empty:
                try: raw_rate = float(df['Close'].iloc[0])
                except: raw_rate = float(df['Close'].values[0])
                if inverse: return 1 / raw_rate, check_date, None
                else: return raw_rate, check_date, None
            check_date -= timedelta(days=1)
    except: pass
    return None, None, "無法取得匯率"

# ==========================================
# 🚀 主程式
# ==========================================
def main():
    st.set_page_config(page_title="雲端業務系統", layout="wide", page_icon="☁️")
    
    # 側邊選單邏輯 (使用 callback 切換頁面)
    with st.sidebar:
        st.title("功能選單")
        
        # 使用按鈕來切換頁面，這樣可以程式化控制
        if st.button("📝 新增業務登記", use_container_width=True):
            st.session_state['current_page'] = "📝 新增業務登記"
            st.session_state['edit_mode'] = False # 切換回新增模式
            st.session_state['edit_data'] = {}
            st.session_state['inv_list'] = []
            st.session_state['pay_list'] = []
            st.rerun()
            
        if st.button("📊 數據戰情室", use_container_width=True):
            st.session_state['current_page'] = "📊 數據戰情室"
            st.session_state['edit_mode'] = False
            st.rerun()
            
        st.markdown("---")
        if st.button("🔄 強制重新整理"):
            st.cache_data.clear()
            st.rerun()

    # 載入資料
    with st.spinner("資料載入中..."):
        company_dict, df_business = load_data_from_gsheet()

    # 準備搜尋用的清單 (功能 1)
    # 格式： "台積電 (半導體)"
    search_list = []
    client_to_cat_map = {}
    for cat, clients in company_dict.items():
        for client in clients:
            label = f"{client} ({cat})"
            search_list.append(label)
            client_to_cat_map[label] = (cat, client)
    
    # ========================================================
    # 頁面 1: 業務登記 (包含新增與編輯模式)
    # ========================================================
    if st.session_state['current_page'] == "📝 新增業務登記":
        
        # 判斷是否為編輯模式
        is_edit = st.session_state.get('edit_mode', False)
        edit_data = st.session_state.get('edit_data', {})
        
        form_title = f"📝 編輯紀錄 (No.{edit_data.get('編號')})" if is_edit else "📝 新增業務登記"
        st.subheader(form_title)
        
        if is_edit:
            st.info("💡 目前為編輯模式。修改完畢請按下方「更新資料」按鈕。")

        # 初始化表單預設值
        def_date = datetime.today()
        def_cat_idx = 0
        def_client_idx = 0
        def_project = ""
        def_price = 0
        def_remark = ""
        def_ex_res = st.session_state.get('ex_res', "")
        
        # 若為編輯模式，填入舊資料
        if is_edit and edit_data:
            try:
                if edit_data.get('日期'):
                    def_date = parse_taiwan_date(edit_data['日期'])
                    if pd.isna(def_date): def_date = datetime.today()
                
                # 設定客戶類別與名稱
                e_cat = edit_data.get('客戶類別', "")
                e_client = edit_data.get('客戶名稱', "")
                
                # 初始化列表 (如果還沒被使用者改動過)
                if not st.session_state.get('inv_list') and edit_data.get('發票日期'):
                    dates = str(edit_data['發票日期']).split(',')
                    st.session_state['inv_list'] = [parse_taiwan_date(d) for d in dates if parse_taiwan_date(d) is not pd.NaT]
                
                if not st.session_state.get('pay_list') and edit_data.get('收款日期'):
                    dates = str(edit_data['收款日期']).split(',')
                    st.session_state['pay_list'] = [parse_taiwan_date(d) for d in dates if parse_taiwan_date(d) is not pd.NaT]
                
                def_project = edit_data.get('案號', "")
                # 處理價格
                p_val = str(edit_data.get('完稅價格', "0")).replace(",", "")
                def_price = int(float(p_val)) if p_val and p_val.replace(".","").isdigit() else 0
                def_remark = edit_data.get('備註', "")
                def_ex_res = edit_data.get('進出口匯率', "")
                
            except Exception as e:
                st.error(f"載入編輯資料錯誤: {e}")

        # --- 表單 UI 開始 ---
        with st.container(border=True):
            st.markdown("### 🏢 客戶與基本資料")
            
            # --- 功能 1: 快速搜尋 (Autocomplete) ---
            # 如果是編輯模式，預設搜尋框為空或鎖定，這裡我們讓搜尋框可以覆蓋當前選擇
            col_search, _ = st.columns([3, 1])
            with col_search:
                # 使用 selectbox 達成搜尋與點選
                # 插入一個空選項以便預設不選
                options = ["🔍 請輸入關鍵字搜尋..."] + search_list
                selected_search = st.selectbox("快速搜尋客戶", options, index=0)
            
            # 判斷使用者是否透過搜尋選取了
            found_cat, found_client = None, None
            if selected_search != "🔍 請輸入關鍵字搜尋...":
                found_cat, found_client = client_to_cat_map.get(selected_search, (None, None))

            # 決定最終顯示在下方的類別與客戶
            # 優先順序: 搜尋結果 > 編輯模式舊資料 > 預設
            
            current_cat_opts = list(company_dict.keys()) + ["➕ 新增類別..."]
            
            # 計算 Selectbox 的 index
            target_cat = found_cat if found_cat else (edit_data.get('客戶類別') if is_edit else None)
            try:
                if target_cat in current_cat_opts:
                    def_cat_idx = current_cat_opts.index(target_cat)
            except: pass

            st.markdown("---")
            c1, c2 = st.columns(2)
            with c1:
                input_date = st.date_input("📅 填表日期", def_date)
                
                # 類別選擇
                selected_cat = st.selectbox("📂 客戶類別", current_cat_opts, index=def_cat_idx, key="cat_box")
                
                # 處理新增類別輸入
                if selected_cat == "➕ 新增類別...":
                    final_cat = st.text_input("✍️ 請輸入新類別名稱")
                    client_opts = ["➕ 新增客戶..."]
                else:
                    final_cat = selected_cat
                    client_opts = company_dict.get(selected_cat, []) + ["➕ 新增客戶..."]

                # 計算客戶名稱的 index
                target_client = found_client if found_client else (edit_data.get('客戶名稱') if is_edit else None)
                try:
                    if target_client in client_opts:
                        def_client_idx = client_opts.index(target_client)
                    else: def_client_idx = 0
                except: def_client_idx = 0
                
                # 客戶選擇 (如果剛剛換了類別，這裡 index 要小心，Streamlit refresh 後會重算)
                # 為了避免 index error，若 client 不在新的 opts 裡，歸零
                if def_client_idx >= len(client_opts): def_client_idx = 0
                
                selected_client = st.selectbox("👤 客戶名稱", client_opts, index=def_client_idx, key="client_box")
                
                if selected_client == "➕ 新增客戶...":
                    final_client = st.text_input("✍️ 請輸入新客戶名稱")
                else:
                    final_client = selected_client

            with c2:
                # 若是編輯模式，顯示當前 ID；若是新增，顯示預測 ID
                if is_edit:
                    current_id = edit_data.get('編號')
                    st.metric(label="✨ 編輯案件編號", value=f"No. {current_id}")
                else:
                    next_id = calculate_next_id(df_business, input_date.year)
                    st.metric(label=f"✨ {input_date.year} 新案件編號", value=f"No. {next_id}", delta="Auto")

                project_no = st.text_input("🔖 案號 / 產品名稱", value=def_project)
                price = st.number_input("💰 完稅價格 (TWD)", min_value=0, step=1000, format="%d", value=def_price)
                remark = st.text_area("📝 備註", height=100, value=def_remark)

        # --- 財務與日期區塊 ---
        with st.container(border=True):
            st.markdown("### ⏰ 時程與財務設定")
            
            # 預定交期與出貨日期處理
            d_del_def = None
            d_ship_def = None
            if is_edit:
                d_del_def = parse_taiwan_date(edit_data.get('預定交期'))
                d_ship_def = parse_taiwan_date(edit_data.get('出貨日期'))
            
            has_del_init = True if (d_del_def and not pd.isna(d_del_def)) else False
            has_ship_init = True if (d_ship_def and not pd.isna(d_ship_def)) else False

            d1, d2, d3, d4 = st.columns(4)
            with d1: 
                has_delivery = st.checkbox("已有預定交期?", value=has_del_init)
                ex_del = st.date_input("🚚 預定交期", d_del_def if has_del_init else datetime.today()) if has_delivery else None
            with d2:
                has_ship = st.checkbox("已有出貨日期?", value=has_ship_init)
                ship_d = st.date_input("🚚 出貨日期", d_ship_def if has_ship_init else datetime.today()) if has_ship else None
            
            # 發票日期 (多選邏輯)
            with d3: 
                st.markdown("🧾 **發票日期**")
                c_pick, c_add = st.columns([3, 1])
                with c_pick: new_inv_date = st.date_input("選日期", datetime.today(), key="pick_inv", label_visibility="collapsed")
                with c_add:
                    if st.button("➕", key="add_inv"):
                        if new_inv_date not in st.session_state['inv_list']:
                            st.session_state['inv_list'].append(new_inv_date)
                            st.session_state['inv_list'].sort()
                
                # 顯示列表
                if st.session_state['inv_list']:
                    st.caption("已加入:")
                    for d in st.session_state['inv_list']:
                        st.text(f"- {d.strftime('%Y-%m-%d')}")
                    if st.button("清空", key="clr_inv"):
                        st.session_state['inv_list'] = []
                        st.rerun()

            # 收款日期 (多選邏輯)
            with d4:
                st.markdown("💰 **收款日期**")
                c_pick_p, c_add_p = st.columns([3, 1])
                with c_pick_p: new_pay_date = st.date_input("選日期", datetime.today(), key="pick_pay", label_visibility="collapsed")
                with c_add_p:
                    if st.button("➕", key="add_pay"):
                        if new_pay_date not in st.session_state['pay_list']:
                            st.session_state['pay_list'].append(new_pay_date)
                            st.session_state['pay_list'].sort()
                
                if st.session_state['pay_list']:
                    st.caption("已加入:")
                    for d in st.session_state['pay_list']:
                        st.text(f"- {d.strftime('%Y-%m-%d')}")
                    if st.button("清空", key="clr_pay"):
                        st.session_state['pay_list'] = []
                        st.rerun()
            
            st.divider()
            # 匯率區塊
            col_ex_input, col_ex_btn = st.columns([3, 1])
            with col_ex_input:
                final_ex = st.text_input("匯率內容", value=def_ex_res, placeholder="匯率將顯示於此")

            with st.expander("🔍 匯率查詢小工具"):
                e1, e2, e3, e4 = st.columns(4)
                with e1: q_date = st.date_input("查詢日期", datetime.today())
                with e2: q_curr = st.selectbox("外幣", ["USD", "EUR", "JPY", "CNY", "GBP"])
                with e3: is_inverse = st.checkbox("反轉 (台幣基準)", value=False)
                with e4:
                    if st.button("🚀 查詢"):
                        r, d, m = get_yahoo_rate(q_curr, q_date, is_inverse)
                        if r:
                            desc = f"{d.strftime('%Y/%m/%d')} 1 {q_curr} = {r:.3f} TWD"
                            if is_inverse: desc = f"{d.strftime('%Y/%m/%d')} 1 TWD = {r:.5f} {q_curr}"
                            # 寫入 session 並 rerun 更新 input value
                            st.session_state['ex_res'] = desc
                            st.rerun()
                        else: st.error("查無資料")

        st.write("")
        col_sub1, col_sub2, col_sub3 = st.columns([1, 2, 1])
        with col_sub2:
            btn_label = "💾 更新資料" if is_edit else "💾 確認並上傳到雲端"
            submit = st.button(btn_label, type="primary", use_container_width=True)

        if submit:
            if not final_client:
                st.toast("❌ 資料不完整：請確認客戶名稱", icon="🚨")
            else:
                # 準備資料
                ds_str = input_date.strftime("%Y-%m-%d")
                eds_str = ex_del.strftime("%Y-%m-%d") if has_delivery and ex_del else ""
                ship_str = ship_d.strftime("%Y-%m-%d") if has_ship and ship_d else ""
                
                ids_str = ", ".join([d.strftime('%Y-%m-%d') for d in st.session_state['inv_list']])
                pds_str = ", ".join([d.strftime('%Y-%m-%d') for d in st.session_state['pay_list']])

                save_id = edit_data.get('編號') if is_edit else next_id

                data_to_save = {
                    "編號": save_id,
                    "日期": ds_str,
                    "客戶類別": final_cat,
                    "客戶名稱": final_client,
                    "案號": project_no,
                    "完稅價格": price if price > 0 else "",
                    "預定交期": eds_str,
                    "出貨日期": ship_str, 
                    "發票日期": ids_str,
                    "收款日期": pds_str,
                    "進出口匯率": final_ex,
                    "備註": remark
                }
                
                with st.spinner("資料儲存處理中..."):
                    # 1. 儲存/更新 業務紀錄
                    success, msg = smart_save_record(data_to_save, is_update=is_edit)
                    
                    if success:
                        msg_list = [msg]
                        # 2. 功能 2: 自動調整公司類別
                        # 不論新增或修改，都檢查一下公司名單狀態
                        if final_client:
                            c_success, c_msg = update_company_category_in_sheet(final_client, final_cat)
                            if c_success: msg_list.append(c_msg)
                        
                        st.balloons()
                        st.success(" | ".join(msg_list))
                        
                        # 清空狀態
                        st.session_state['ex_res'] = ""
                        st.session_state['inv_list'] = []
                        st.session_state['pay_list'] = []
                        st.session_state['edit_mode'] = False
                        st.session_state['edit_data'] = {}
                        
                        st.cache_data.clear()
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error(f"儲存失敗: {msg}")

    # ========================================================
    # 頁面 2: 數據戰情室 (包含功能 3: 點選跳轉編輯)
    # ========================================================
    elif st.session_state['current_page'] == "📊 數據戰情室":
        st.title("📊 數據戰情室")
        
        if df_business.empty:
            st.info("目前尚無資料。")
        else:
            # 資料清洗
            df_clean = df_business.copy()
            
            # 處理金額欄位
            price_col = next((c for c in df_clean.columns if '價格' in c or '金額' in c), None)
            if price_col:
                df_clean[price_col] = df_clean[price_col].astype(str).str.replace(',', '').replace('', '0')
                df_clean[price_col] = pd.to_numeric(df_clean[price_col], errors='coerce').fillna(0)
            
            # 處理日期欄位
            date_col = next((c for c in df_clean.columns if '日期' in c), None)
            if date_col:
                df_clean['parsed_date'] = df_clean[date_col].apply(parse_taiwan_date)
                df_valid = df_clean.dropna(subset=['parsed_date']).copy()
                df_valid['Year'] = df_valid['parsed_date'].dt.year
                
                all_years = sorted(df_valid['Year'].unique().astype(int), reverse=True)
                selected_year = st.selectbox("📅 請選擇年份", all_years)
                
                df_final = df_valid[df_valid['Year'] == selected_year].sort_values(by='parsed_date', ascending=False)
                
                # --- KPI ---
                total_rev = df_final[price_col].sum() if price_col else 0
                st.markdown(f"### 📊 {selected_year} 年度總覽")
                k1, k2, k3 = st.columns(3)
                k1.metric("總營業額", f"${total_rev:,.0f}")
                k2.metric("總案件數", f"{len(df_final)} 件")
                avg = total_rev/len(df_final) if len(df_final) > 0 else 0
                k3.metric("平均客單價", f"${avg:,.0f}")
                st.divider()

                # --- 功能 3: 點選編輯 ---
                st.subheader(f"📝 {selected_year} 詳細資料 (點選列可編輯)")
                st.info("💡 提示：**點選** 表格中的某一列，即可跳轉至編輯頁面修改資料。")

                # 設定表格顯示
                display_cols = [c for c in df_final.columns if c not in ['Year', 'parsed_date']]
                
                # 使用 Streamlit 的 on_select 事件
                selection = st.dataframe(
                    df_final[display_cols],
                    use_container_width=True,
                    on_select="rerun",  # 當選擇變更時重新執行
                    selection_mode="single-row", # 單行選取
                    hide_index=True
                )

                # 處理選取事件
                if selection and selection["selection"]["rows"]:
                    selected_index = selection["selection"]["rows"][0]
                    # 取得原始資料 (因為 df_final 可能被排序過，需小心 index)
                    selected_row = df_final.iloc[selected_index]
                    
                    # 轉為 dict
                    row_dict = selected_row.to_dict()
                    # 轉換 Timestamp 為字串以免報錯
                    for k, v in row_dict.items():
                        if isinstance(v, (pd.Timestamp, datetime)):
                            row_dict[k] = v.strftime('%Y-%m-%d')
                    
                    # 寫入 Session 並跳轉
                    st.session_state['edit_mode'] = True
                    st.session_state['edit_data'] = row_dict
                    st.session_state['current_page'] = "📝 新增業務登記"
                    st.rerun()

            else:
                st.error("資料表中找不到日期欄位，無法分析。")

if __name__ == "__main__":
    main()