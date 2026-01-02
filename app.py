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
    pass 

# ==========================================
# 📍 設定區
# ==========================================
SPREADSHEET_KEY = '1Q1-JbHje0E-8QB0pu83OHN8jCPY8We9l2j1_7eZ8yas'

# 🔥 強制欄位設定
TARGET_COLS = [
    "編號", "日期", "客戶類別", "客戶名稱", "案號", "完稅價格", 
    "預定交期", "出貨日期", "發票日期", "發票截收日期", "收款日期", "進出口匯率", "備註"
]

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
                local_key_file = r'service_account.json'
                if os.path.exists(local_key_file):
                    creds = ServiceAccountCredentials.from_json_keyfile_name(local_key_file, scope)
                else:
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
        c = "".join(ch for ch in c if ch.isprintable())
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cleaned.append(c)
    return cleaned

def parse_taiwan_date_strict(date_str):
    """
    🔥【絕對嚴格版日期解析】
    1. 不自動補年份。
    2. 只有完整的 YYYY/MM/DD 或 ROC/MM/DD 才算數。
    3. 避免任何簡寫日期（如 12/05）干擾年份判斷。
    """
    if pd.isna(date_str) or str(date_str).strip() == "": return pd.NaT
    s = str(date_str).split(',')[0].strip().replace(".", "/").replace("-", "/")
    try:
        parts = s.split('/')
        if len(parts) == 3:
            year_val = int(parts[0])
            if year_val < 1911: year_val += 1911
            return pd.to_datetime(f"{year_val}-{parts[1]}-{parts[2]}")
        else: 
            # ❌ 只有兩段的 (12/05) 全部丟棄，視為無效
            return pd.NaT
    except: return pd.NaT

def parse_date_for_ui(date_str):
    """UI 顯示用 (比較寬鬆，方便編輯舊資料)"""
    if pd.isna(date_str) or str(date_str).strip() == "": return pd.NaT
    s = str(date_str).split(',')[0].strip().replace(".", "/").replace("-", "/")
    try:
        parts = s.split('/')
        if len(parts) == 3:
            year_val = int(parts[0])
            if year_val < 1911: year_val += 1911
            return pd.to_datetime(f"{year_val}-{parts[1]}-{parts[2]}")
        elif len(parts) == 2:
            this_year = datetime.now().year
            return pd.to_datetime(f"{this_year}-{parts[0]}-{parts[1]}")
        else: 
            return pd.to_datetime(s)
    except: return pd.NaT

@st.cache_data(ttl=5)
def load_data_from_gsheet():
    for attempt in range(3):
        try:
            client = get_google_sheet_client()
            sh = client.open_by_key(SPREADSHEET_KEY)
            
            try:
                ws_c = sh.get_worksheet(1)
                if ws_c:
                    data = ws_c.get_all_values()
                    if len(data) > 1:
                        headers = clean_headers(data[0])
                        df = pd.DataFrame(data[1:], columns=headers)
                        df = df.replace(r'^\s*$', pd.NA, regex=True).dropna(how='all')
                        cd = {col: [str(x).strip() for x in df[col].values if pd.notna(x) and str(x).strip()] for col in df.columns}
                    else: cd = {}
                else: cd = {}
            except: cd = {}

            try:
                ws_f = sh.get_worksheet(0)
                if ws_f:
                    all_values = ws_f.get_all_values()
                    header_idx = -1
                    for i, row in enumerate(all_values[:10]):
                        r_str = [str(r).strip() for r in row]
                        # 只要有編號和日期，就是標題列
                        if "編號" in r_str and "日期" in r_str:
                            header_idx = i
                            break
                    if header_idx != -1 and len(all_values) > header_idx + 1:
                        headers = clean_headers(all_values[header_idx])
                        df_b = pd.DataFrame(all_values[header_idx+1:], columns=headers)
                        # 🔥 這裡保留原始 Index 以便抓鬼 (Excel Row = Index + header_idx + 2)
                        # 因為 pandas index 從 0 開始，header 佔 1 行，且通常 Excel 從 1 開始
                        # header_idx 是標題列在 all_values 的索引 (0-based)
                        # 真正的 Excel Row = header_idx + 1 (標題行) + index + 1 (資料行) = header_idx + index + 2
                        df_b['Thinking_Row_Index'] = df_b.index + header_idx + 2
                    else: df_b = pd.DataFrame()
                else: df_b = pd.DataFrame()
            except: df_b = pd.DataFrame()
            return cd, df_b
        except Exception as e:
            if "503" in str(e): time.sleep(2); continue
            return {}, pd.DataFrame()
    return {}, pd.DataFrame()

# ==========================================
# 🛠️ 資料處理邏輯
# ==========================================
def update_company_category_in_sheet(client_name, new_category):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(1) 
        all_cols = ws.get_all_values()
        if not all_cols: return False, "讀取失敗"
        headers = [h.strip() for h in all_cols[0]]
        
        if new_category in headers:
            new_col_idx = headers.index(new_category) + 1
        else:
            new_col_idx = len(headers) + 1
            ws.update_cell(1, new_col_idx, new_category)
            headers.append(new_category)

        found_row, found_col = None, None
        existing_category = None
        for c_idx, col_name in enumerate(headers):
            col_vals = [row[c_idx] for row in all_cols if len(row) > c_idx]
            if client_name in col_vals:
                r_idx = col_vals.index(client_name)
                found_row = r_idx + 1 
                found_col = c_idx + 1
                existing_category = col_name
                break
        
        if found_row and found_col:
            if existing_category == new_category: return True, "客戶類別未變動"
            else:
                ws.update_cell(found_row, found_col, "")
                new_col_values = ws.col_values(new_col_idx)
                next_row = len(new_col_values) + 1
                ws.update_cell(next_row, new_col_idx, client_name)
                return True, f"已將客戶從「{existing_category}」移動至「{new_category}」"
        else:
            new_col_values = ws.col_values(new_col_idx)
            next_row = len(new_col_values) + 1
            ws.update_cell(next_row, new_col_idx, client_name)
            return True, f"已新增客戶至「{new_category}」"
    except Exception as e:
        return False, f"更新公司名單失敗: {e}"

def smart_save_record(data_dict, is_update=False):
    for attempt in range(3):
        try:
            client = get_google_sheet_client()
            sh = client.open_by_key(SPREADSHEET_KEY)
            ws = sh.get_worksheet(0)
            all_values = ws.get_all_values()
            headers = []
            for i, row in enumerate(all_values[:10]):
                r_str = [str(r).strip() for r in row]
                if "編號" in r_str and "日期" in r_str:
                    headers = row
                    break
            if not headers: return False, "找不到標題列"

            row_to_write = [""] * len(headers)
            for col_name, value in data_dict.items():
                for i, h in enumerate(headers):
                    if str(h).strip() == col_name:
                        row_to_write[i] = str(value)
                        break
            
            target_id = str(data_dict.get("編號"))
            if is_update:
                try:
                    id_col_idx = headers.index("編號")
                    id_list = ws.col_values(id_col_idx + 1)
                    try:
                        row_index = id_list.index(target_id) + 1
                        ws.update(f"A{row_index}", [row_to_write], value_input_option='USER_ENTERED')
                        return True, f"編號 {target_id} 更新成功"
                    except ValueError: return False, "找不到原始編號，無法更新"
                except Exception as ex: return False, str(ex)
            else:
                ws.append_row(row_to_write, value_input_option='USER_ENTERED')
                return True, f"編號 {target_id} 新增成功"
        except Exception as e:
            if "503" in str(e): time.sleep(2); continue
            return False, f"寫入失敗: {e}"
    return False, "連線逾時"

def calculate_next_id_with_debug(df_all, target_year):
    """
    🔥 抓鬼特攻隊版 calculate_next_id
    回傳: (next_id, debug_df)
    debug_df 包含了所有被判定為該年份的資料，方便使用者檢查。
    """
    if df_all.empty: return 1, pd.DataFrame()
    
    date_col = None
    if "日期" in df_all.columns: date_col = "日期"
    else:
        candidates = [c for c in df_all.columns if '日期' in c and '發票' not in c and '收款' not in c and '出貨' not in c]
        if candidates: date_col = candidates[0]
            
    id_col = next((c for c in df_all.columns if '編號' in c), None)

    if not date_col or not id_col: return 1, pd.DataFrame()

    df_temp = df_all.copy()
    # 嚴格解析日期
    df_temp['temp_date'] = df_temp[date_col].apply(parse_taiwan_date_strict)
    df_temp['temp_year'] = df_temp['temp_date'].dt.year
    
    # 篩選年份 (找出兇手)
    df_filtered = df_temp[df_temp['temp_year'] == target_year].copy()
    
    # 為了顯示，只留重要欄位
    cols_to_show = ['Thinking_Row_Index', id_col, date_col, '客戶名稱'] if '客戶名稱' in df_temp.columns else ['Thinking_Row_Index', id_col, date_col]
    
    if df_filtered.empty:
        return 1, pd.DataFrame()
    
    try:
        df_filtered['id_num'] = pd.to_numeric(df_filtered[id_col], errors='coerce')
        max_id = df_filtered['id_num'].max()
        
        if pd.isna(max_id):
            return 1, df_filtered[cols_to_show]
        return int(max_id) + 1, df_filtered[cols_to_show].sort_values(by='id_num', ascending=False)
    except:
        return 1, df_filtered[cols_to_show]

def get_yahoo_rate(target_currency, query_date, inverse=False):
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
    
    with st.sidebar:
        st.title("功能選單")
        if st.button("📝 新增業務登記", use_container_width=True):
            st.session_state['current_page'] = "📝 新增業務登記"
            st.session_state['edit_mode'] = False
            st.session_state['edit_data'] = {}
            st.session_state['search_input'] = "" 
            
            if 'cat_box' in st.session_state: del st.session_state['cat_box']
            if 'client_box' in st.session_state: del st.session_state['client_box']
            if 'force_cat' in st.session_state: del st.session_state['force_cat']
            if 'force_client' in st.session_state: del st.session_state['force_client']
            
            st.rerun()
            
        if st.button("📊 數據戰情室", use_container_width=True):
            st.session_state['current_page'] = "📊 數據戰情室"
            st.session_state['edit_mode'] = False
            st.rerun()
            
        st.markdown("---")
        if st.button("🔄 強制重新整理"):
            st.cache_data.clear()
            st.rerun()

    with st.spinner("資料載入中..."):
        company_dict, df_business = load_data_from_gsheet()

    if st.session_state['current_page'] == "📝 新增業務登記":
        
        is_edit = st.session_state.get('edit_mode', False)
        edit_data = st.session_state.get('edit_data', {})
        form_title = f"📝 編輯紀錄 (No.{edit_data.get('編號')})" if is_edit else "📝 新增業務登記"
        
        if is_edit:
            st.success(f"✏️ 您正在編輯 **No.{edit_data.get('編號')}** 的資料。")
        else:
            st.subheader(form_title)
        
        # 預設值
        def_date = datetime.today()
        def_project = ""
        def_price = 0
        def_remark = ""
        def_ex_res = st.session_state.get('ex_res', "")
        
        if is_edit and edit_data:
            try:
                if edit_data.get('日期'):
                    def_date = parse_date_for_ui(edit_data['日期'])
                    if pd.isna(def_date): def_date = datetime.today()
                
                def_project = edit_data.get('案號', "")
                p_val = str(edit_data.get('完稅價格', "0")).replace(",", "")
                def_price = int(float(p_val)) if p_val and p_val.replace(".","").isdigit() else 0
                def_remark = edit_data.get('備註', "")
                def_ex_res = edit_data.get('進出口匯率', "")
            except: pass

        with st.container(border=True):
            st.markdown("### 🏢 客戶與基本資料")
            search_keyword = st.text_input("🔍 快速搜尋客戶", placeholder="例如：台積", key="search_input")
            
            if search_keyword:
                def normalize_text(text): return str(text).replace('臺', '台').strip()
                norm_key = normalize_text(search_keyword)
                matches = []
                for cat, clients in company_dict.items():
                    for client in clients:
                        if norm_key in normalize_text(client):
                            matches.append(f"{client} ({cat})")
                
                if len(matches) == 1:
                    target_str = matches[0]
                    st.success(f"✅ 已自動填入：{target_str}")
                    try:
                        split_idx = target_str.rfind(" (")
                        found_client = target_str[:split_idx]
                        found_cat = target_str[split_idx+2:-1]
                        if found_cat not in company_dict: company_dict[found_cat] = []
                        if found_client not in company_dict[found_cat]: company_dict[found_cat].append(found_client)
                        st.session_state['force_cat'] = found_cat
                        st.session_state['force_client'] = found_client
                        if 'cat_box' in st.session_state: del st.session_state['cat_box']
                        if 'client_box' in st.session_state: del st.session_state['client_box']
                        st.rerun()
                    except: pass
                elif len(matches) > 1: st.info(f"💡 找到 {len(matches)} 筆符合資料。")

            st.markdown("---")
            c1, c2 = st.columns(2)
            with c1:
                input_date = st.date_input("📅 填表日期", def_date)
                
                # 選單邏輯
                current_cat_opts = list(company_dict.keys()) + ["➕ 新增類別..."]
                final_cat_idx = 0
                target_cat = None
                if 'force_cat' in st.session_state: target_cat = st.session_state.pop('force_cat')
                elif is_edit and '客戶類別' in edit_data: target_cat = str(edit_data['客戶類別']).strip()
                if target_cat and target_cat not in current_cat_opts: current_cat_opts.insert(0, target_cat)
                if target_cat in current_cat_opts: final_cat_idx = current_cat_opts.index(target_cat)
                selected_cat = st.selectbox("📂 客戶類別", current_cat_opts, index=final_cat_idx, key="cat_box")
                
                if selected_cat == "➕ 新增類別...":
                    final_cat = st.text_input("✍️ 請輸入新類別名稱")
                    client_opts = ["➕ 新增客戶..."]
                else:
                    final_cat = selected_cat
                    client_opts = company_dict.get(selected_cat, []) + ["➕ 新增客戶..."]

                final_client_idx = 0
                target_client = None
                if 'force_client' in st.session_state: target_client = st.session_state.pop('force_client')
                elif is_edit and '客戶名稱' in edit_data: target_client = str(edit_data['客戶名稱']).strip()
                if target_client and target_client not in client_opts: client_opts.insert(0, target_client)
                if target_client in client_opts: final_client_idx = client_opts.index(target_client)
                selected_client = st.selectbox("👤 客戶名稱", client_opts, index=final_client_idx, key="client_box")
                
                if selected_client == "➕ 新增客戶...": final_client = st.text_input("✍️ 請輸入新客戶名稱")
                else: final_client = selected_client

            with c2:
                if is_edit:
                    current_id = edit_data.get('編號')
                    st.metric(label="✨ 編輯案件編號", value=f"No. {current_id}")
                else:
                    # 🔥 抓鬼邏輯：回傳 next_id 和 debug_df
                    next_id, debug_df = calculate_next_id_with_debug(df_business, input_date.year)
                    st.metric(label=f"✨ {input_date.year} 新案件編號", value=f"No. {next_id}", delta="Auto")
                    
                    if next_id > 1:
                        # 🔥 這裡就是抓鬼雷達
                        st.markdown(f"### 🕵️‍♂️ 資料偵探：為什麼是 {next_id}？")
                        st.error(f"因為系統在您的 Google Sheet 中，發現了以下 **{len(debug_df)} 筆** 屬於 {input_date.year} 年的資料：")
                        st.caption("👇 請看表格最左邊的 **Thinking_Row_Index**，這就是 Google Sheet 的行數。請去把它刪掉！")
                        
                        # 顯示嫌疑犯資料表
                        st.dataframe(debug_df, hide_index=True)

                project_no = st.text_input("🔖 案號 / 產品名稱", value=def_project)
                price = st.number_input("💰 完稅價格 (TWD)", min_value=0, step=1000, format="%d", value=def_price)
                remark = st.text_area("📝 備註", height=100, value=def_remark)

        # ... (時程與財務設定) ...
        with st.container(border=True):
            st.markdown("### ⏰ 時程與財務設定")
            d_del_def, d_ship_def, d_inv_deadline_def = None, None, None
            if is_edit:
                d_del_def = parse_date_for_ui(edit_data.get('預定交期'))
                d_ship_def = parse_date_for_ui(edit_data.get('出貨日期'))
                d_inv_deadline_def = parse_date_for_ui(edit_data.get('發票截收日期'))
            
            c_d1, c_d2, c_d3 = st.columns(3)
            with c_d1:
                has_del = st.checkbox("預定交期", value=bool(d_del_def and not pd.isna(d_del_def)))
                ex_del = st.date_input("d1", d_del_def if d_del_def else datetime.today(), label_visibility="collapsed") if has_del else ""
            with c_d2:
                has_ship = st.checkbox("出貨日期", value=bool(d_ship_def and not pd.isna(d_ship_def)))
                ex_ship = st.date_input("d2", d_ship_def if d_ship_def else datetime.today(), label_visibility="collapsed") if has_ship else ""
            with c_d3:
                has_inv = st.checkbox("發票截收", value=bool(d_inv_deadline_def and not pd.isna(d_inv_deadline_def)))
                ex_inv_d = st.date_input("d3", d_inv_deadline_def if d_inv_deadline_def else datetime.today(), label_visibility="collapsed") if has_inv else ""

            st.divider()
            st.write("🧾 發票與收款日期 (請於上方按鈕新增)")
            final_ex = st.text_input("匯率內容", value=def_ex_res)

        col_sub1, col_sub2, col_sub3 = st.columns([1, 2, 1])
        with col_sub2:
            btn_label = "💾 更新資料" if is_edit else "💾 確認並上傳到雲端"
            submit = st.button(btn_label, type="primary", use_container_width=True)

        if submit:
            if not final_client: st.toast("❌ 資料不完整：請確認客戶名稱", icon="🚨")
            else:
                ds_str = input_date.strftime("%Y-%m-%d")
                eds_str = ex_del.strftime("%Y-%m-%d") if has_del and ex_del else ""
                ship_str = ex_ship.strftime("%Y-%m-%d") if has_ship and ex_ship else ""
                inv_dead_str = ex_inv_d.strftime("%Y-%m-%d") if has_inv and ex_inv_d else ""
                ids_str = ", ".join([d.strftime('%Y-%m-%d') for d in st.session_state['inv_list']]) if st.session_state['inv_list'] else ""
                pds_str = ", ".join([d.strftime('%Y-%m-%d') for d in st.session_state['pay_list']]) if st.session_state['pay_list'] else ""

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
                    "發票截收日期": inv_dead_str, 
                    "收款日期": pds_str,
                    "進出口匯率": final_ex,
                    "備註": remark
                }
                
                with st.spinner("資料儲存處理中..."):
                    success, msg = smart_save_record(data_to_save, is_update=is_edit)
                    if success:
                        if final_client: update_company_category_in_sheet(final_client, final_cat)
                        st.balloons()
                        st.success(msg)
                        st.session_state['edit_mode'] = False
                        st.session_state['edit_data'] = {}
                        st.cache_data.clear()
                        time.sleep(2)
                        st.rerun()
                    else: st.error(f"儲存失敗: {msg}")

    elif st.session_state['current_page'] == "📊 數據戰情室":
        st.title("📊 數據戰情室")
        if df_business.empty: st.info("目前尚無資料。")
        else:
            df_clean = df_business.copy()
            
            date_col = None
            if "日期" in df_clean.columns: date_col = "日期"
            else:
                cands = [c for c in df_clean.columns if '日期' in c and '發票' not in c]
                if cands: date_col = cands[0]

            if date_col:
                df_clean['parsed_date'] = df_clean[date_col].apply(parse_taiwan_date_strict)
                df_valid = df_clean.dropna(subset=['parsed_date']).copy()
                df_valid['Year'] = df_valid['parsed_date'].dt.year
                
                all_years = sorted(df_valid['Year'].unique().astype(int), reverse=True)
                if 2026 not in all_years: all_years.insert(0, 2026)
                
                selected_year = st.selectbox("📅 請選擇年份", all_years)
                
                df_final = df_valid[df_valid['Year'] == selected_year].sort_values(by='parsed_date', ascending=False)
                
                valid_cols = [c for c in TARGET_COLS if c in df_final.columns]
                
                st.subheader(f"📝 {selected_year} 詳細資料")
                st.dataframe(df_final[valid_cols], use_container_width=True, hide_index=True)
            else: st.error("無日期欄位")

if __name__ == "__main__":
    main()