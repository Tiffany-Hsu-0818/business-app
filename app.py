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
    pass # 允許在沒有 yfinance 的情況下執行

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
            
            try:
                ws_c = sh.get_worksheet(1)
                cd = {}
                if ws_c:
                    data = ws_c.get_all_values()
                    if len(data) > 1:
                        headers = clean_headers(data[0])
                        df = pd.DataFrame(data[1:], columns=headers)
                        df = df.replace(r'^\s*$', pd.NA, regex=True).dropna(how='all')
                        cd = {col: [str(x).strip() for x in df[col].values if pd.notna(x) and str(x).strip()] for col in df.columns}
            except: cd = {}

            try:
                ws_f = sh.get_worksheet(0)
                df_b = pd.DataFrame()
                if ws_f:
                    all_values = ws_f.get_all_values()
                    header_idx = -1
                    for i, row in enumerate(all_values[:10]):
                        r_str = [str(r).strip() for r in row]
                        if "編號" in r_str and "日期" in r_str:
                            header_idx = i
                            break
                    if header_idx != -1 and len(all_values) > header_idx + 1:
                        headers = clean_headers(all_values[header_idx])
                        df_b = pd.DataFrame(all_values[header_idx+1:], columns=headers)
                        if '編號' in df_b.columns:
                            df_b = df_b[df_b['編號'].astype(str).str.strip() != '']
            except: df_b = pd.DataFrame()

            tax_map = {}
            rev_tax_map = {}
            try:
                ws_t = sh.get_worksheet(2)
                if ws_t:
                    t_data = ws_t.get_all_values()
                    if len(t_data) > 1:
                        for row in t_data[1:]:
                            if len(row) >= 2:
                                c_name = str(row[0]).strip()
                                c_tax = str(row[1]).strip()
                                if c_name and c_tax:
                                    tax_map[c_name] = c_tax
                                    rev_tax_map[c_tax] = c_name
            except: pass

            return cd, df_b, tax_map, rev_tax_map
        except Exception as e:
            if "503" in str(e): time.sleep(2); continue
            return {}, pd.DataFrame(), {}, {}
    return {}, pd.DataFrame(), {}, {}

# ==========================================
# 🛠️ 資料處理邏輯
# ==========================================

def update_company_category_in_sheet(client_name, new_category):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(1) 
        all_cols = ws.get_all_values()
        if not all_cols: return False
        
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
            if existing_category != new_category:
                ws.update_cell(found_row, found_col, "")
                new_col_values = ws.col_values(new_col_idx)
                next_row = len(new_col_values) + 1
                ws.update_cell(next_row, new_col_idx, client_name)
        else:
            new_col_values = ws.col_values(new_col_idx)
            next_row = len(new_col_values) + 1
            ws.update_cell(next_row, new_col_idx, client_name)
        return True
    except: return False

def update_tax_id_in_sheet(client_name, tax_id):
    if not client_name or not tax_id: return
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        try: ws = sh.get_worksheet(2)
        except: return 
        if not ws: return

        cell = None
        try: cell = ws.find(client_name, in_column=1)
        except: pass

        if cell: ws.update_cell(cell.row, 2, str(tax_id))
        else: ws.append_row([client_name, str(tax_id)])
    except: pass

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
                try:
                    idx = next(i for i, h in enumerate(headers) if str(h).strip() == col_name)
                    row_to_write[idx] = str(value)
                except StopIteration: pass

            target_id = str(data_dict.get("編號"))

            if is_update:
                try:
                    id_col_idx = headers.index("編號")
                    id_list = ws.col_values(id_col_idx + 1)
                    try:
                        row_index = id_list.index(target_id) + 1
                        ws.update(f"A{row_index}", [row_to_write], value_input_option='USER_ENTERED')
                        return True, f"編號 {target_id} 更新成功"
                    except ValueError:
                        return False, "找不到原始編號，無法更新"
                except Exception as ex:
                    return False, str(ex)
            else:
                ws.append_row(row_to_write, value_input_option='USER_ENTERED')
                return True, f"編號 {target_id} 新增成功"

        except Exception as e:
            if "503" in str(e): time.sleep(2); continue
            return False, f"寫入失敗: {e}"
    return False, "連線逾時"

def calculate_next_id(df_all, target_year):
    if df_all.empty: return 1
    date_col = next((c for c in df_all.columns if '日期' in c), None)
    if not date_col or '編號' not in df_all.columns: return 1

    try:
        df_temp = df_all[['編號', date_col]].copy()
        df_temp['id_num'] = pd.to_numeric(df_temp['編號'], errors='coerce')
        df_temp = df_temp.dropna(subset=['id_num'])

        def get_strict_year(x):
            if pd.isna(x) or str(x).strip() == "": return None
            s = str(x).strip().replace(".", "/").replace("-", "/")
            parts = s.split('/')
            if len(parts) == 3:
                try:
                    y = int(parts[0])
                    if y < 1911: y += 1911
                    return y
                except: return None
            return None

        df_temp['parsed_year'] = df_temp[date_col].apply(get_strict_year)
        df_target = df_temp[df_temp['parsed_year'] == target_year]
        if df_target.empty: return 1
        return int(df_target['id_num'].max()) + 1
    except: return 1

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
            st.session_state['inv_list'] = []
            st.session_state['pay_list'] = []
            keys_to_clear = ['force_cat', 'force_client', 'force_tax_id', 'widget_tax_id', 'tax_msg_type']
            for k in keys_to_clear:
                if k in st.session_state: del st.session_state[k]
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
        company_dict, df_business, tax_map, rev_tax_map = load_data_from_gsheet()

    # --- 定義統編欄位變更的 Callback ---
    def on_tax_id_change():
        new_tax = st.session_state['widget_tax_id'].strip()
        if not new_tax: return
        found_client = rev_tax_map.get(new_tax)
        if found_client:
            found_cat = None
            def normalize_text(text): return str(text).replace('臺', '台').strip()
            for cat, clients in company_dict.items():
                if found_client in clients or normalize_text(found_client) in [normalize_text(c) for c in clients]:
                    found_cat = cat; break
            st.session_state['force_client'] = found_client
            if found_cat: st.session_state['force_cat'] = found_cat
            st.session_state['force_tax_id'] = new_tax
            st.session_state['tax_msg_type'] = 'success'
        else:
            st.session_state['force_tax_id'] = new_tax
            st.session_state['tax_msg_type'] = 'warning'

    # ========================================================
    # 頁面 1: 業務登記
    # ========================================================
    if st.session_state['current_page'] == "📝 新增業務登記":
        
        is_edit = st.session_state.get('edit_mode', False)
        edit_data = st.session_state.get('edit_data', {})
        
        # --- 初始化 ---
        cat_options = list(company_dict.keys()) + ["➕ 新增類別..."]
        target_cat, target_client, target_tax = None, None, ""

        if 'force_cat' in st.session_state: target_cat = st.session_state['force_cat']
        elif is_edit and '客戶類別' in edit_data: target_cat = edit_data['客戶類別']
            
        if 'force_client' in st.session_state: target_client = st.session_state['force_client']
        elif is_edit and '客戶名稱' in edit_data: target_client = edit_data['客戶名稱']

        if 'force_tax_id' in st.session_state: target_tax = st.session_state['force_tax_id']
        elif is_edit and '統一編號' in edit_data: target_tax = edit_data['統一編號']
        elif target_client in tax_map: target_tax = tax_map[target_client]

        default_cat_idx, default_client_idx = 0, 0
        if target_cat:
            if target_cat not in cat_options: cat_options.insert(0, target_cat)
            default_cat_idx = cat_options.index(target_cat)
            
        current_cat_for_list = target_cat if target_cat else cat_options[0]
        if current_cat_for_list == "➕ 新增類別...": client_options = ["➕ 新增客戶..."]
        else: client_options = company_dict.get(current_cat_for_list, []) + ["➕ 新增客戶..."]

        if target_client:
            if target_client not in client_options: client_options.insert(0, target_client)
            default_client_idx = client_options.index(target_client)

        form_title = f"📝 編輯紀錄 (No.{edit_data.get('編號')})" if is_edit else "📝 新增業務登記"
        if is_edit: st.success(f"✏️ 您正在編輯 **No.{edit_data.get('編號')}** 的資料，修改完畢請按下方「更新資料」按鈕。")
        else: st.subheader(form_title)

        # 預設值變數
        def_date = datetime.today()
        def_project, def_price, def_remark, def_ex_res = "", 0, "", st.session_state.get('ex_res', "")
        
        # 🔥 補回這裡的變數初始化！
        has_inv_init, has_pay_init, has_del_init, has_ship_init = False, False, False, False
        def_inv_date, def_pay_date = datetime.today(), datetime.today()
        d_del_def = datetime.today()
        d_ship_def = datetime.today()

        if is_edit and edit_data:
            try:
                if edit_data.get('日期'): 
                    d = parse_taiwan_date(edit_data['日期'])
                    if d is not pd.NaT: def_date = d
                
                # 預定交期
                if edit_data.get('預定交期'):
                    d = parse_taiwan_date(edit_data['預定交期'])
                    if d is not pd.NaT: has_del_init = True; d_del_def = d
                
                # 出貨日期
                if edit_data.get('出貨日期'):
                    d = parse_taiwan_date(edit_data['出貨日期'])
                    if d is not pd.NaT: has_ship_init = True; d_ship_def = d

                # 發票日期
                if edit_data.get('發票日期'):
                    dates = str(edit_data['發票日期']).split(',')
                    parsed = [parse_taiwan_date(d) for d in dates if parse_taiwan_date(d) is not pd.NaT]
                    if parsed: has_inv_init, def_inv_date = True, parsed[0]; st.session_state['inv_list'] = parsed[1:]
                
                # 收款日期
                if edit_data.get('收款日期'):
                    dates = str(edit_data['收款日期']).split(',')
                    parsed = [parse_taiwan_date(d) for d in dates if parse_taiwan_date(d) is not pd.NaT]
                    if parsed: has_pay_init, def_pay_date = True, parsed[0]; st.session_state['pay_list'] = parsed[1:]
                
                def_project = edit_data.get('案號', "")
                p = str(edit_data.get('完稅價格', "0")).replace(",", "")
                def_price = int(float(p)) if p and p.replace(".","").isdigit() else 0
                def_remark = edit_data.get('備註', "")
                def_ex_res = edit_data.get('進出口匯率', "")
            except: pass

        with st.container(border=True):
            st.markdown("### 🏢 客戶與基本資料")
            
            def normalize_text(text): return str(text).replace('臺', '台').strip()
            search_keyword = st.text_input("🔍 智慧搜尋：輸入【客戶名稱】或【統一編號】(Enter)", placeholder="例如：台積 或 12345678", key="search_input")
            
            if search_keyword:
                search_val = normalize_text(search_keyword)
                if search_val.isdigit() and len(search_val) >= 8:
                    found_client_tax = rev_tax_map.get(search_val)
                    if found_client_tax:
                        st.success(f"✅ 統編識別成功！已帶入：{found_client_tax}")
                        st.session_state['force_client'] = found_client_tax
                        st.session_state['force_tax_id'] = search_val
                        for cat, clients in company_dict.items():
                            if found_client_tax in clients or normalize_text(found_client_tax) in [normalize_text(c) for c in clients]:
                                st.session_state['force_cat'] = cat; break
                        st.rerun()
                    else: st.warning("⚠️ 查無此統編，請直接填寫資料。")
                else:
                    matches = []
                    for cat, clients in company_dict.items():
                        for client in clients:
                            if search_val in normalize_text(client): matches.append(f"{client} ({cat})")
                    if len(matches) == 1:
                        t = matches[0]; st.success(f"✅ 已自動填入：{t}")
                        split_idx = t.rfind(" ("); f_c = t[:split_idx]; f_cat = t[split_idx+2:-1]
                        st.session_state['force_client'] = f_c; st.session_state['force_cat'] = f_cat
                        if f_c in tax_map: st.session_state['force_tax_id'] = tax_map[f_c]
                        st.rerun()
                    elif len(matches) > 1:
                        st.info(f"💡 找到 {len(matches)} 筆，請選擇：")
                        sel = st.selectbox("請選擇", matches, label_visibility="collapsed")
                        if sel:
                            split_idx = sel.rfind(" ("); f_c = sel[:split_idx]; f_cat = sel[split_idx+2:-1]
                            if st.session_state.get('force_client') != f_c:
                                st.session_state['force_client'] = f_c; st.session_state['force_cat'] = f_cat
                                if f_c in tax_map: st.session_state['force_tax_id'] = tax_map[f_c]
                                st.rerun()
                    else: st.warning("❌ 找不到符合的客戶")

            st.markdown("---")
            c1, c2 = st.columns(2)
            with c1:
                input_date = st.date_input("📅 填表日期", def_date)
                
                current_cat_val = st.session_state.get('cat_box')
                if current_cat_val and current_cat_val not in cat_options: cat_options.insert(0, current_cat_val)
                
                selected_cat = st.selectbox("📂 客戶類別", cat_options, index=default_cat_idx, key="cat_box")
                if selected_cat != current_cat_for_list:
                    if selected_cat == "➕ 新增類別...": client_options = ["➕ 新增客戶..."]
                    else: client_options = company_dict.get(selected_cat, []) + ["➕ 新增客戶..."]
                    default_client_idx = 0 

                if selected_cat == "➕ 新增類別...": final_cat = st.text_input("✍️ 請輸入新類別名稱")
                else: final_cat = selected_cat

                current_client_val = st.session_state.get('client_box')
                if current_client_val and current_client_val not in client_options: client_options.insert(0, current_client_val)

                selected_client = st.selectbox("👤 客戶名稱", client_options, index=default_client_idx, key="client_box")
                if selected_client == "➕ 新增客戶...": final_client = st.text_input("✍️ 請輸入新客戶名稱")
                else: final_client = selected_client

                keys_to_clear = ['force_cat', 'force_client']; 
                for k in keys_to_clear: 
                    if k in st.session_state: del st.session_state[k]

            with c2:
                if is_edit: current_id = edit_data.get('編號'); st.metric(label="✨ 編輯案件編號", value=f"No. {current_id}")
                else: next_id = calculate_next_id(df_business, input_date.year); st.metric(label=f"✨ {input_date.year} 新案件編號", value=f"No. {next_id}", delta="Auto")
                
                if 'force_tax_id' in st.session_state: current_tax_val = st.session_state['force_tax_id']
                else:
                    if 'last_client_check' not in st.session_state: st.session_state['last_client_check'] = ""
                    if final_client != st.session_state['last_client_check']:
                        current_tax_val = tax_map[final_client] if final_client in tax_map else ""
                        st.session_state['widget_tax_id'] = current_tax_val
                        st.session_state['last_client_check'] = final_client
                    else:
                        if is_edit and '統一編號' in edit_data and 'widget_tax_id' not in st.session_state:
                             st.session_state['widget_tax_id'] = edit_data['統一編號']
                        current_tax_val = st.session_state.get('widget_tax_id', "")

                if 'widget_tax_id' not in st.session_state: st.session_state['widget_tax_id'] = current_tax_val

                final_tax_id = st.text_input("🏢 統一編號 (可自動記憶)", key="widget_tax_id", on_change=on_tax_id_change, placeholder="輸入統編後按 Enter 可反查")

                if st.session_state.get('tax_msg_type') == 'success': st.success("✅ 已自動帶入對應客戶資料"); st.session_state['tax_msg_type'] = None
                elif st.session_state.get('tax_msg_type') == 'warning': st.warning("⚠️ 沒有登記該公司，請新增客戶名稱及類別"); st.session_state['tax_msg_type'] = None

                if 'force_tax_id' in st.session_state: del st.session_state['force_tax_id']

                project_no = st.text_input("🔖 案號 / 產品名稱", value=def_project)
                price = st.number_input("💰 完稅價格 (TWD)", min_value=0, step=1000, format="%d", value=def_price)

        with st.container(border=True): remark = st.text_area("📝 備註", height=80, value=def_remark)

        with st.container(border=True):
            st.markdown("### ⏰ 時程與財務設定")
            d1, d2, d3, d4 = st.columns(4)
            with d1: 
                has_delivery = st.checkbox("已有預定交期?", value=has_del_init)
                ex_del = st.date_input("🚚 預定交期", d_del_def) if has_delivery else None
            with d2:
                has_ship = st.checkbox("已有出貨日期?", value=has_ship_init)
                ship_d = st.date_input("🚚 出貨日期", d_ship_def) if has_ship else None
            with d3:
                has_invoice = st.checkbox("已有發票?", value=has_inv_init)
                if has_invoice:
                    primary_inv_date = st.date_input("🧾 發票日期", def_inv_date)
                    with st.expander("➕ 新增更多"):
                        c_pick, c_add = st.columns([3, 1])
                        with c_pick: new_inv_date = st.date_input("選日期", datetime.today(), key="pick_inv", label_visibility="collapsed")
                        with c_add:
                            if st.button("加", key="add_inv"):
                                if new_inv_date not in st.session_state['inv_list']: st.session_state['inv_list'].append(new_inv_date); st.session_state['inv_list'].sort()
                        if st.session_state['inv_list']:
                            for d in st.session_state['inv_list']: st.text(f"- {d.strftime('%Y-%m-%d')}")
                            if st.button("清", key="clr_inv"): st.session_state['inv_list'] = []; st.rerun()
            with d4:
                has_payment = st.checkbox("已有收款?", value=has_pay_init)
                if has_payment:
                    primary_pay_date = st.date_input("💰 收款日期", def_pay_date)
                    with st.expander("➕ 新增更多"):
                        c_pick_p, c_add_p = st.columns([3, 1])
                        with c_pick_p: new_pay_date = st.date_input("選日期", datetime.today(), key="pick_pay", label_visibility="collapsed")
                        with c_add_p:
                            if st.button("加", key="add_pay"):
                                if new_pay_date not in st.session_state['pay_list']: st.session_state['pay_list'].append(new_pay_date); st.session_state['pay_list'].sort()
                        if st.session_state['pay_list']:
                            for d in st.session_state['pay_list']: st.text(f"- {d.strftime('%Y-%m-%d')}")
                            if st.button("清", key="clr_pay"): st.session_state['pay_list'] = []; st.rerun()
            
            st.divider()
            col_ex_input, col_ex_btn = st.columns([3, 1])
            with col_ex_input: final_ex = st.text_input("匯率內容", value=def_ex_res, placeholder="匯率將顯示於此")
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
                            st.session_state['ex_res'] = desc; st.rerun()
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
                ds_str = input_date.strftime("%Y-%m-%d")
                eds_str = ex_del.strftime("%Y-%m-%d") if has_delivery and ex_del else ""
                ship_str = ship_d.strftime("%Y-%m-%d") if has_ship and ship_d else ""
                
                final_inv_list = []
                if has_invoice: final_inv_list.append(primary_inv_date)
                if st.session_state['inv_list']: final_inv_list.extend(st.session_state['inv_list'])
                final_inv_list = sorted(list(set(final_inv_list)))
                ids_str = ", ".join([d.strftime('%Y-%m-%d') for d in final_inv_list])

                final_pay_list = []
                if has_payment: final_pay_list.append(primary_pay_date)
                if st.session_state['pay_list']: final_pay_list.extend(st.session_state['pay_list'])
                final_pay_list = sorted(list(set(final_pay_list)))
                pds_str = ", ".join([d.strftime('%Y-%m-%d') for d in final_pay_list])

                save_id = edit_data.get('編號') if is_edit else next_id

                data_to_save = {
                    "編號": save_id,
                    "日期": ds_str,
                    "客戶類別": final_cat,
                    "客戶名稱": final_client,
                    "統一編號": final_tax_id,
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
                    success, msg = smart_save_record(data_to_save, is_update=is_edit)
                    if success:
                        msg_list = [msg]
                        if final_client:
                            update_company_category_in_sheet(final_client, final_cat)
                            if final_tax_id: update_tax_id_in_sheet(final_client, final_tax_id)
                        st.balloons()
                        st.success(" | ".join(msg_list))
                        st.session_state['ex_res'] = ""
                        st.session_state['inv_list'] = []
                        st.session_state['pay_list'] = []
                        st.session_state['edit_mode'] = False
                        st.session_state['edit_data'] = {}
                        st.session_state['search_input'] = "" 
                        if 'widget_tax_id' in st.session_state: del st.session_state['widget_tax_id']
                        st.cache_data.clear()
                        time.sleep(2)
                        st.rerun()
                    else: st.error(f"儲存失敗: {msg}")

    # ========================================================
    # 頁面 2: 數據戰情室
    # ========================================================
    elif st.session_state['current_page'] == "📊 數據戰情室":
        st.title("📊 數據戰情室")
        if df_business.empty: st.info("目前尚無資料。")
        else:
            df_clean = df_business.copy()
            price_col = next((c for c in df_clean.columns if '價格' in c or '金額' in c), None)
            if price_col:
                df_clean[price_col] = df_clean[price_col].astype(str).str.replace(',', '').replace('', '0')
                df_clean[price_col] = pd.to_numeric(df_clean[price_col], errors='coerce').fillna(0)
            
            date_col = next((c for c in df_clean.columns if '日期' in c), None)
            if date_col:
                df_clean['parsed_date'] = df_clean[date_col].apply(parse_taiwan_date)
                df_valid = df_clean.dropna(subset=['parsed_date']).copy()
                df_valid['Year'] = df_valid['parsed_date'].dt.year
                all_years = sorted(df_valid['Year'].unique().astype(int), reverse=True)
                selected_year = st.selectbox("📅 請選擇年份", all_years)
                df_final = df_valid[df_valid['Year'] == selected_year].sort_values(by='parsed_date', ascending=False)
                
                total_rev = df_final[price_col].sum() if price_col else 0
                st.markdown(f"### 📊 {selected_year} 年度總覽")
                k1, k2, k3 = st.columns(3)
                k1.metric("總營業額", f"${total_rev:,.0f}")
                k2.metric("總案件數", f"{len(df_final)} 件")
                avg = total_rev/len(df_final) if len(df_final) > 0 else 0
                k3.metric("平均客單價", f"${avg:,.0f}")
                
                st.markdown("---")
                c_chart1, c_chart2 = st.columns(2)
                with c_chart1:
                    st.subheader("📈 客戶類別佔比")
                    cat_col = next((c for c in df_final.columns if '類別' in c), None)
                    if cat_col and price_col:
                        fig_pie = px.pie(df_final, names=cat_col, values=price_col, hole=0.4)
                        st.plotly_chart(fig_pie, use_container_width=True)
                with c_chart2:
                    st.subheader("📅 每月業績趨勢")
                    if price_col and 'parsed_date' in df_final.columns:
                        df_monthly = df_final.resample('M', on='parsed_date')[price_col].sum().reset_index()
                        df_monthly['Month_Str'] = df_monthly['parsed_date'].dt.strftime('%Y-%m')
                        fig_bar = px.bar(df_monthly, x='Month_Str', y=price_col, title="月營收分佈", labels={'Month_Str':'月份', price_col:'金額'})
                        st.plotly_chart(fig_bar, use_container_width=True)
                
                st.markdown("---")
                st.subheader(f"📝 {selected_year} 詳細資料")
                st.warning("💡 **操作提示：** 請直接點選表格中的任一列，系統將自動跳轉至編輯頁面並帶入該筆資料。")

                display_cols = [c for c in df_final.columns if c not in ['Year', 'parsed_date']]
                selection = st.dataframe(df_final[display_cols], use_container_width=True, on_select="rerun", selection_mode="single-row", hide_index=True)

                if selection and selection["selection"]["rows"]:
                    selected_index = selection["selection"]["rows"][0]
                    row_dict = df_final.iloc[selected_index].to_dict()
                    for k, v in row_dict.items():
                        if isinstance(v, (pd.Timestamp, datetime)): row_dict[k] = v.strftime('%Y-%m-%d')
                    st.session_state['edit_mode'] = True
                    st.session_state['edit_data'] = row_dict
                    st.session_state['current_page'] = "📝 新增業務登記"
                    
                    if '客戶類別' in row_dict: st.session_state['force_cat'] = str(row_dict['客戶類別']).strip()
                    if '客戶名稱' in row_dict: st.session_state['force_client'] = str(row_dict['客戶名稱']).strip()
                    if '統一編號' in row_dict: st.session_state['force_tax_id'] = str(row_dict['統一編號']).strip()
                    st.session_state['search_input'] = ""
                    st.rerun()
            else: st.error("資料表中找不到日期欄位，無法分析。")

if __name__ == "__main__":
    main()