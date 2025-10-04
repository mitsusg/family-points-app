import streamlit as st
# --- 診断用（確認したら削除してOK） ---
with st.expander("診断情報（確認後に削除してください）", expanded=True):
    st.write("Service Account:", st.secrets["gcp_service_account"]["client_email"])
    st.write("sheet_url:", st.secrets["sheet_url"])

import gspread
from datetime import date

@st.cache_resource
def get_client():
    creds = st.secrets["gcp_service_account"]
    return gspread.service_account_from_dict(creds)

@st.cache_resource
def get_sheet():
    client = get_client()
    return client.open_by_url(st.secrets["sheet_url"])

def get_or_create(wsname, header):
    sh = get_sheet()
    try:
        ws = sh.worksheet(wsname)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=wsname, rows="1000", cols="10")
        ws.append_row(header)
    return ws

# Setup
kids_ws = get_or_create("kids", ["id","name","grade","active"])
goals_ws = get_or_create("goals", ["id","title","points","active"])
check_ws = get_or_create("checkins", ["id","date","kid_id","goal_id","self_check","parent_check"])

st.title("✅ Family Points (Google Sheets版)")

role = st.radio("ロールを選択", ["子ども","親"], horizontal=True)

if role=="子ども":
    st.write("ここに子ども用チェック画面を実装（簡易版サンプル）")
else:
    st.write("ここに親用管理画面を実装（簡易版サンプル）")
