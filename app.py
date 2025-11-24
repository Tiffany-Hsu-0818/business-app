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
        sh.get_worksheet