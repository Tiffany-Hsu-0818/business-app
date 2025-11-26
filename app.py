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

def save_new_company_to_sheet(new_cat, new_client):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws_company = sh.get_worksheet(1) 
        
        if not ws_company:
            return False, "找不到公司名單分頁"

        headers = ws_company.row_values(1)
        headers = [h.strip() for h in headers if h.strip()]
        
        if new_cat in headers:
            col_idx = headers.index(new_cat) + 1
            existing_clients = ws_company.col_values(col_idx)
            if new_client not in existing_clients:
                next_row = len(existing_clients) + 1
                ws_company.update_cell(next_row, col_idx, new_client)
                return True, f"已將「{new_client}」加入「{new_cat}」名單中！"
            else:
                return True, "客戶已存在名單中。"
        else:
            new_col_idx = len(headers) + 1
            ws_company.update_cell(1, new_col_idx, new_cat)
            ws_company.update_cell(2, new_col_idx, new_client)
            return True, f"已建立新類別「{new_cat}」並加入客戶！"

    except Exception as e:
        return False, f"更新名單失敗: {e}"

def smart_append_to_gsheet(data_dict):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0)
        
        # 自動尋找標題列
        all_values = ws.get_all_values()
        header_row_idx = 0
        headers = []
        for i, row in enumerate(all_values[:5]):
            row_str = [str(r).strip() for r in row]
            if "編號" in row_str and "日期" in row_str:
                header_row_idx = i
                headers = row
                break
        
        if not headers: 
            if all_values: headers = all_values[0]
            else: return False

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

def update_records_in_gsheet(edited_df):
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        ws = sh.get_worksheet(0)
        
        all_values = ws.get_all_values()
        header_row_idx = 0
        headers = []
        for i, row in enumerate(all_values[:5]):
            row_str = [str(r).strip() for r in row]
            if "編號" in row_str and "日期" in row_str:
                header_row_idx = i
                headers = row
                break
        
        if not headers: return False

        try:
            id_col_idx = headers.index("編號")
        except:
            return False

        all_col_values = ws.col_values(id_col_idx + 1)
        
        for index, row in edited_df.iterrows():
            target_id = str(row['編號'])
            try:
                row_in_list = all_col_values.index(target_id)
                actual_row_idx = row_in_list + 1
                
                row_data = []
                for h in headers:
                    val = row.get(h, "")
                    if isinstance(val, (pd.Timestamp, datetime)):
                        val = val.strftime('%Y-%m-%d')
                    if pd.isna(val): val = ""
                    row_data.append(val)
                
                if str(all_col_values[row_in_list]) == target_id:
                    ws.update(f"A{actual_row_idx}", [row_data], value_input_option='USER_ENTERED')
                
            except ValueError:
                continue
        return True
    except Exception as e:
        st.error(f"更新失敗: {e}")
        return False

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

def parse_taiwan_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == "":
        return pd.NaT
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
        else:
            return pd.to_datetime(s)
    except:
        return pd.NaT

@st.cache_data(ttl=60)
def load_data_from_gsheet():
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_KEY)
        
        # 讀取公司
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

        # 讀取表單
        try:
            ws_f = sh.get_worksheet(0)
            if ws_f:
                all_values = ws_f.get_all_values()
                header_idx = -1
                for i, row in enumerate(all_values[:5]):
                    r_str = [str(r).strip() for r in row]
                    if "編號" in r_str and "日期" in r_str:
                        header_idx = i
                        break
                
                if header_idx != -1 and len(all_values) > header_idx + 1:
                    headers = clean_headers(all_values[header_idx])
                    df_b = pd.DataFrame(all_values[header_idx+1:], columns=headers)
                    if '編號' in df_b.columns:
                        df_b = df_b[pd.to_numeric(df_b['編號'], errors='coerce').notna()]
                    else:
                        df_b = pd.DataFrame()
                else:
                    df_b = pd.DataFrame()
            else: df_b = pd.DataFrame()
        except: df_b = pd.DataFrame()
             
        return cd, df_b
    except Exception as e:
        st.error(f"連線失敗: {e}")
        return {}, pd.DataFrame()

def calculate_next_id_for_year(df_all, target_year):
    if df_all.empty: return 1
    if '編號' not in df_all.columns or '日期' not in df_all.columns: return 1
    
    df_temp = df_all[['編號', '日期']].copy()
    df_temp['parsed_date'] = df_temp['日期'].apply(parse_taiwan_date)
    df_year = df_temp[df_temp['parsed_date'].dt.year == target_year]
    
    if df_year.empty: return 1
    
    try:
        ids = pd.to_numeric(df_year['編號'], errors='coerce').dropna()
        if ids.empty: return 1
        return int(ids.max()) + 1
    except: return 1

# ==========================================
# 🚀 主程式
# ==========================================
def main():
    st.set_page_config(page_title="雲端業務系統", layout="wide", page_icon="☁️")
    
    with st.sidebar:
        st.title("功能選單")
        menu = st.radio("請選擇", ["📝 新增業務登記", "📊 數據戰情室"], index=0)
        st.markdown("---")
        if st.button("🔄 強制重新整理"):
            st.cache_data.clear()
            st.rerun()

    with st.spinner("資料載入中..."):
        company_dict, df_business = load_data_from_gsheet()

    if menu == "📝 新增業務登記":
        if 'ex_res' not in st.session_state: st.session_state['ex_res'] = ""
        if 'inv_list' not in st.session_state: st.session_state['inv_list'] = []

        with st.container(border=True):
            st.markdown("### 🏢 客戶與基本資料")
            c1, c2 = st.columns(2)
            with c1:
                input_date = st.date_input("📅 填表日期", datetime.today())
                target_year = input_date.year
                next_id = calculate_next_id_for_year(df_business, target_year)
                
                cat_options = list(company_dict.keys()) + ["➕ 新增類別..."]
                selected_cat = st.selectbox("📂 客戶類別", cat_options)
                if selected_cat == "➕ 新增類別...":
                    final_cat = st.text_input("✍️ 請輸入新類別名稱")
                    client_options = ["➕ 新增客戶..."]
                else:
                    final_cat = selected_cat
                    client_options = company_dict.get(selected_cat, []) + ["➕ 新增客戶..."]

                selected_client = st.selectbox("👤 客戶名稱", client_options)
                if selected_client == "➕ 新增客戶...":
                    final_client = st.text_input("✍️ 請輸入新客戶名稱")
                else:
                    final_client = selected_client

            with c2:
                st.metric(label=f"✨ {target_year} 年度下一個編號", value=f"No. {next_id}", delta="Auto")
                
                with st.expander("🕵️‍♂️ 編號診斷"):
                    st.write(f"系統正在檢查 {target_year} 年的舊資料...")
                    if not df_business.empty and '日期' in df_business.columns:
                        debug_df = df_business.copy()
                        debug_df['parsed_date'] = debug_df['日期'].apply(parse_taiwan_date)
                        year_data = debug_df[debug_df['parsed_date'].dt.year == target_year]
                        
                        if year_data.empty:
                            st.info(f"📭 目前沒有找到 {target_year} 年的資料，所以編號從 1 開始。")
                        else:
                            max_val = pd.to_numeric(year_data['編號'], errors='coerce').max()
                            st.success(f"✅ 找到 {len(year_data)} 筆資料，目前最大號碼是 {int(max_val)}。")
                            st.dataframe(year_data[['編號', '日期', '客戶名稱']].head())
                    else:
                        st.warning("尚未讀取到任何資料。")

                project_no = st.text_input("🔖 案號 / 產品名稱")
                price = st.number_input("💰 完稅價格 (TWD)", min_value=0, step=1000, format="%d", value=0)
                remark = st.text_area("📝 備註", height=100)

        with st.container(border=True):
            st.markdown("### ⏰ 時程與財務設定")
            
            # ⭐⭐ 改成 4 欄，加入出貨日期 ⭐⭐
            d1, d2, d3, d4 = st.columns(4)
            with d1: 
                has_delivery = st.checkbox("已有預定交期?", value=False)
                ex_del = st.date_input("🚚 預定交期", datetime.today()) if has_delivery else None
            
            with d2:
                has_ship = st.checkbox("已有出貨日期?", value=False)
                ship_d = st.date_input("🚚 出貨日期", datetime.today()) if has_ship else None

            with d3: 
                has_inv = st.checkbox("已有發票日期?", value=False)
                if has_inv:
                    c_pick, c_add = st.columns([3, 1])
                    with c_pick:
                        new_inv_date = st.date_input("選擇日期", datetime.today(), label_visibility="collapsed")
                    with c_add:
                        if st.button("➕"):
                            if new_inv_date not in st.session_state['inv_list']:
                                st.session_state['inv_list'].append(new_inv_date)
                                st.session_state['inv_list'].sort()
                    if st.session_state['inv_list']:
                        date_strs = [d.strftime('%Y-%m-%d') for d in st.session_state['inv_list']]
                        st.caption(f"已加入: {', '.join(date_strs)}")
                        if st.button("🗑️ 清除"):
                            st.session_state['inv_list'] = []
                            st.rerun()
                else:
                    if st.session_state['inv_list']:
                        st.session_state['inv_list'] = []

            with d4:
                has_pay = st.checkbox("已有收款日期?", value=False)
                pay_d = st.date_input("💰 收款日期", datetime.today()) if has_pay else None
            
            st.divider()
            st.markdown("#### 💱 進出口匯率")
            col_ex_input, col_ex_btn = st.columns([3, 1])
            with col_ex_input:
                final_ex = st.text_input("匯率內容", value=st.session_state['ex_res'], label_visibility="collapsed", placeholder="匯率將顯示於此")
            
            with st.expander("🔍 點此開啟：匯率查詢小工具"):
                e1, e2, e3, e4 = st.columns([2, 2, 2, 2])
                with e1: q_date = st.date_input("查詢日期", datetime.today())
                with e2: q_curr = st.selectbox("外幣", ["USD", "EUR", "JPY", "CNY", "GBP"])
                with e3: is_inverse = st.checkbox("反轉 (台幣基準)", value=False)
                with e4:
                    st.write("")
                    if st.button("🚀 立即查詢"):
                        with st.spinner("連線中..."):
                            rate_val, found_d, err_msg = get_yahoo_rate(q_curr, q_date, is_inverse)
                            if rate_val:
                                d_str = found_d.strftime('%Y/%m/%d')
                                if is_inverse: desc = f"{d_str} 1 TWD = {rate_val:.5f} {q_curr}"
                                else: desc = f"{d_str} 1 {q_curr} = {rate_val:.3f} TWD"
                                st.session_state['ex_res'] = desc
                                st.success("已填入！")
                                time.sleep(0.5)
                                st.rerun()
                            else: st.error(f"失敗：{err_msg}")

        st.write("")
        col_sub1, col_sub2, col_sub3 = st.columns([1, 2, 1])
        with col_sub2:
            submit = st.button("💾 確認並上傳到雲端", type="primary", use_container_width=True)

        if submit:
            if not final_client:
                st.toast("❌ 資料不完整：請確認客戶名稱", icon="🚨")
            else:
                ds_str = input_date.strftime("%Y-%m-%d")
                eds_str = ex_del.strftime("%Y-%m-%d") if has_delivery and ex_del else ""
                ship_str = ship_d.strftime("%Y-%m-%d") if has_ship and ship_d else ""
                pds_str = pay_d.strftime("%Y-%m-%d") if has_pay and pay_d else ""
                ids_str = ", ".join([d.strftime('%Y-%m-%d') for d in st.session_state['inv_list']]) if has_inv and st.session_state['inv_list'] else ""

                data_to_save = {
                    "編號": next_id,
                    "日期": ds_str,
                    "客戶類別": final_cat,
                    "客戶名稱": final_client,
                    "案號": project_no,
                    "完稅價格": price if price > 0 else "",
                    "預定交期": eds_str,
                    "出貨日期": ship_str, # 新增欄位
                    "發票日期": ids_str,
                    "收款日期": pds_str,
                    "進出口匯率": final_ex,
                    "備註": remark,
                    "階段性款項": "" 
                }
                
                if smart_append_to_gsheet(data_to_save):
                    update_msg = ""
                    if selected_cat == "➕ 新增類別..." or selected_client == "➕ 新增客戶...":
                        success, msg = save_new_company_to_sheet(final_cat, final_client)
                        if success: update_msg = f" | {msg}"

                    st.balloons()
                    st.success(f"✅ 成功建立案件：No.{next_id}{update_msg}")
                    st.session_state['ex_res'] = ""
                    st.session_state['inv_list'] = []
                    st.cache_data.clear()
                    time.sleep(3)
                    st.rerun()

    elif menu == "📊 數據戰情室":
        st.title("📊 數據戰情室")
        
        if df_business.empty:
            st.info("目前尚無資料。")
        else:
            try:
                df_clean = df_business.copy()
                
                price_col = next((c for c in df_clean.columns if '價格' in c or '金額' in c), None)
                if price_col:
                    df_clean[price_col] = df_clean[price_col].astype(str).str.replace(',', '').replace('', '0')
                    df_clean[price_col] = pd.to_numeric(df_clean[price_col], errors='coerce').fillna(0)
                
                date_col = next((c for c in df_clean.columns if '日期' in c), None)
                if date_col:
                    # ⭐⭐ 加入 '出貨日期' 到轉換清單 ⭐⭐
                    potential_date_cols = ['日期', '預定交期', '收款日期', '出貨日期'] 
                    for col in potential_date_cols:
                        if col in df_clean.columns:
                            df_clean[col] = df_clean[col].apply(parse_taiwan_date)
                    
                    df_valid = df_clean.dropna(subset=[date_col]).copy()
                    
                    if not df_valid.empty:
                        df_valid['Year'] = df_valid[date_col].dt.year
                        all_years = sorted(df_valid['Year'].unique().astype(int), reverse=True)
                        selected_year = st.selectbox("📅 請選擇年份", all_years)
                        df_final = df_valid[df_valid['Year'] == selected_year]
                        
                        st.markdown(f"### 📊 {selected_year} 年度總覽")
                        total_rev = df_final[price_col].sum()
                        total_count = len(df_final)
                        k1, k2, k3 = st.columns(3)
                        k1.metric("總營業額", f"${total_rev:,.0f}")
                        k2.metric("總案件數", f"{total_count} 件")
                        if total_count > 0: k3.metric("平均客單價", f"${total_rev/total_count:,.0f}")
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
                            df_monthly = df_final.resample('M', on=date_col)[price_col].sum().reset_index()
                            if not df_monthly.empty:
                                df_monthly['Month_Str'] = df_monthly[date_col].dt.strftime('%Y-%m')
                                fig_bar = px.bar(df_monthly, x='Month_Str', y=price_col, 
                                                 title="月營收分佈", labels={'Month_Str':'月份', price_col:'金額'})
                                st.plotly_chart(fig_bar, use_container_width=True)

                        st.markdown("---")
                        st.subheader(f"📝 編輯 {selected_year} 年度資料")
                        st.info("💡 提示：直接點擊欄位即可修改，修改完請按下方「儲存變更」按鈕。")
                        
                        display_cols = [c for c in df_final.columns if c not in ['Year', 'converted_date']]
                        
                        edited_df = st.data_editor(
                            df_final[display_cols],
                            key="data_editor",
                            num_rows="dynamic",
                            use_container_width=True,
                            column_config={
                                "編號": st.column_config.NumberColumn("編號 (鎖定)", disabled=True, format="%d"),
                                "完稅價格": st.column_config.NumberColumn("完稅價格", format="$%d"),
                                "日期": st.column_config.DateColumn("日期", format="YYYY-MM-DD"),
                                "預定交期": st.column_config.DateColumn("預定交期", format="YYYY-MM-DD"),
                                "出貨日期": st.column_config.DateColumn("出貨日期", format="YYYY-MM-DD"), # 新增
                                "收款日期": st.column_config.DateColumn("收款日期", format="YYYY-MM-DD"),
                                "發票日期": st.column_config.TextColumn("發票日期 (可多筆)"),
                            }
                        )
                        
                        if st.button("💾 儲存變更", type="primary"):
                            with st.spinner("正在更新雲端資料庫..."):
                                if update_records_in_gsheet(edited_df):
                                    st.success("✅ 更新成功！")
                                    st.cache_data.clear()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("更新失敗。")
                    else:
                        st.warning("日期解析後無資料。")
                else:
                    st.error("找不到日期欄位。")
            except Exception as e:
                st.error(f"錯誤: {e}")
                st.dataframe(df_business)

if __name__ == "__main__":
    main()