# app.py — Family Points (Google Sheets)
import time
import functools

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime
import pandas as pd

st.set_page_config(page_title="Family Points", page_icon="✅", layout="wide")

# ========= Google Sheets =========
@st.cache_resource
# ========= Google Sheets =========

# 小さなリトライ
def retry(times=3, wait=0.6, exceptions=(gspread.exceptions.APIError,)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for i in range(times):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last = e
                    time.sleep(wait * (1.5 ** i))
            raise last
        return wrapper
    return deco

@st.cache_resource
def get_client():
    info = st.secrets["gcp_service_account"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _sheet_id_from_url() -> str:
    url = st.secrets["sheet_url"]
    if "/d/" not in url:
        raise ValueError("sheet_url は Google Sheets 共有URL（/d/<ID>/...）を指定してください")
    return url.split("/d/")[1].split("/")[0]

@st.cache_resource  # ヘッダーもキーに含めキャッシュの齟齬を防ぐ
def _get_ws_cached(sheet_id: str, name: str, headers_tuple: tuple):
    client = get_client()
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=1000, cols=max(10, len(headers_tuple)))
        ws.update("1:1", [list(headers_tuple)])
        time.sleep(0.2)  # 作成直後の整合待ち
    return ws

@retry(times=3, wait=0.6)
def get_ws(name: str, headers: list[str]):
    sheet_id = _sheet_id_from_url()
    ws = _get_ws_cached(sheet_id, name, tuple(headers))
    # 1行目を検査・修復
    first = ws.row_values(1)
    if first != headers:
        ws.update("1:1", [headers])
        time.sleep(0.1)
    return ws

# 全レコードを安全に取得（ヘッダ崩れでも落ちない）
@retry(times=3, wait=0.6)
def safe_get_all_records(ws, expected_headers: list[str]) -> list[dict]:
    vals = ws.get_all_values()
    if not vals:
        ws.update("1:1", [expected_headers])
        return []
    headers = vals[0]
    rows = vals[1:]
    if headers != expected_headers:
        ws.update("1:1", [expected_headers])
        headers = expected_headers
    if not rows:
        return []
    df = pd.DataFrame(rows, columns=headers)
    df = df.applymap(lambda x: None if (x is None or str(x).strip() == "") else x)
    return df.to_dict(orient="records")

    # ヘッダが無いor崩れている場合の保険
    first = ws.row_values(1)
    if first != headers:
        ws.update("1:1", [headers])
    return ws

# タブとヘッダ
KIDS_H = ["id", "name", "grade", "active"]
GOALS_H = ["id", "title", "points", "active", "kid_id"]  # kid_id 空=共通目標
CHECKINS_H = [
    "date", "kid_id", "kid_name", "goal_id", "goal_title",
    "points", "child_checked", "parent_approved", "updated_at"
]

def ws_kids():    return get_ws("kids", KIDS_H)
def ws_goals():   return get_ws("goals", GOALS_H)
def ws_checkins():return get_ws("checkins", CHECKINS_H)

# ========= Sheets ユーティリティ =========
# ========= Sheets ユーティリティ =========
def df_kids():
    ws = ws_kids()
    recs = safe_get_all_records(ws, KIDS_H)
    df = pd.DataFrame(recs)
    if df.empty:
        return pd.DataFrame(columns=KIDS_H)
    return df[KIDS_H]

def df_goals():
    ws = ws_goals()
    recs = safe_get_all_records(ws, GOALS_H)
    df = pd.DataFrame(recs)
    if df.empty:
        df = pd.DataFrame(columns=GOALS_H)
    if "points" in df.columns:
        df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype(int)
    if "active" in df.columns:
        df["active"] = df["active"].astype(str).str.lower().isin(["true","1","yes"])
    return df[GOALS_H]

def df_checkins():
    ws = ws_checkins()
    recs = safe_get_all_records(ws, CHECKINS_H)
    df = pd.DataFrame(recs)
    if df.empty:
        df = pd.DataFrame(columns=CHECKINS_H)
    if "points" in df.columns:
        df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype(int)
    for b in ["child_checked", "parent_approved"]:
        if b in df.columns:
            df[b] = df[b].astype(str).str.lower().isin(["true","1","yes"])
    return df[CHECKINS_H]
    
def today_check_state(kid_id, goal_id):
    df = df_checkins()
    if df.empty:
        return False, False
    today_s = date.today().isoformat()  # 'YYYY-MM-DD'
    hit = df[(df["date"] == today_s) & (df["kid_id"] == kid_id) & (df["goal_id"] == goal_id)]
    if hit.empty:
        return False, False
    r = hit.iloc[0]
    return bool(r.get("child_checked", False)), bool(r.get("parent_approved", False))

def seed_if_empty():
    # 最初の一回だけの種データ
    kids = df_kids()
    goals = df_goals()
    if kids.empty:
        ws_kids().append_row(["k1", "そうた", "年中", "TRUE"])
        ws_kids().append_row(["k2", "みお",   "小1", "TRUE"])
    if goals.empty:
        # 共通目標（kid_id 空）
        ws_goals().append_row(["g1", "ランニング10分", 3, "TRUE", ""])
        ws_goals().append_row(["g2", "宿題をする",     5, "TRUE", ""])
        ws_goals().append_row(["g3", "歯みがき",       2, "TRUE", ""])

seed_if_empty()

# ========= Check-in の upsert =========
def upsert_checkin(the_date, kid_id, kid_name, goal_id, goal_title,
                   set_child=None, set_parent=None, points=0):
    ws = ws_checkins()
    df = df_checkins()
    key = (str(the_date), str(kid_id), str(goal_id))

    # 既存検索
    hit_idx = None
    if not df.empty:
        mask = (df["date"]==key[0]) & (df["kid_id"]==key[1]) & (df["goal_id"]==key[2])
        if mask.any():
            hit_idx = df[mask].index[0]

    now = datetime.now().isoformat(timespec="seconds")

    if hit_idx is None:
        # 新規
        row = {
            "date": key[0],
            "kid_id": key[1],
            "kid_name": kid_name,
            "goal_id": key[2],
            "goal_title": goal_title,
            "points": int(points),
            "child_checked": bool(set_child) if set_child is not None else False,
            "parent_approved": bool(set_parent) if set_parent is not None else False,
            "updated_at": now,
        }
        ws.append_row([row[h] for h in CHECKINS_H])
    else:
        # 更新
        r = hit_idx + 2  # 1行目ヘッダのため +2
        if set_child is not None:
            ws.update_cell(r, CHECKINS_H.index("child_checked")+1, str(bool(set_child)))
        if set_parent is not None:
            ws.update_cell(r, CHECKINS_H.index("parent_approved")+1, str(bool(set_parent)))
        ws.update_cell(r, CHECKINS_H.index("updated_at")+1, now)

def goals_for_kid(kid_id: str, viewer: str = "child"):
    """
    カンマ区切りkid_id対応 / 共通ゴール対応 / audience(任意)対応
      - kid_id 空欄 or 'all' → 全員
      - kid_id が 'k1,k2' → その子たちだけ
      - audience が 'child' / 'parent' / 'both'(既定) で出し分け
    """
    g = df_goals().copy()

    # 1) active フィルタ（true/1/yes を有効とみなす）
    g["_active"] = g.get("active", "TRUE").astype(str).str.strip().str.lower()
    g = g[g["_active"].isin(["true", "1", "yes"])].drop(columns=["_active"])

    # 2) audience（任意列）で出し分け。無ければ both 扱い
    audience_series = g.get("audience", "both").astype(str).str.strip().str.lower()
    valid_for_viewer = audience_series.isin(["both", viewer])
    g = g[valid_for_viewer]

    # 3) kid_id のカンマ区切り対応 + 全員共通（空 or 'all'）
    # 全角カンマも想定して置換 → 分割 → 配列化
    kid_id_raw = g.get("kid_id", "").astype(str).fillna("").str.replace("，", ",")
    g["_kid_ids"] = kid_id_raw.apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])

    def is_target(row_ids: list[str]) -> bool:
        if not row_ids:   # 空欄 → 全員共通
            return True
        # 'all' が含まれていれば全員共通
        if any(i.lower() == "all" for i in row_ids):
            return True
        # 指定の kid_id が含まれている
        return kid_id in row_ids

    g = g[g["_kid_ids"].apply(is_target)]

    return g.reset_index(drop=True)

def monthly_total(kid_id, target_month):
    """target_month: 'YYYY-MM'"""
    df = df_checkins()
    if df.empty: return 0
    m = df[(df["kid_id"]==kid_id)]
    m = m[m["date"].str.startswith(target_month)]
    m = m[m["child_checked"] & m["parent_approved"]]
    return int(m["points"].sum())

# ========= UI =========
st.title("✅ Family Points (Google Sheets 版)")

role = st.radio("ロールを選択", ["子ども", "親"], horizontal=True)

kids = df_kids()
kids = kids[kids.get("active","TRUE").astype(str).str.lower()=="true"].reset_index(drop=True)
kid_map = {f'{row["name"]}（{row.get("grade","")}）': row["id"] for _,row in kids.iterrows()}
if not kid_map:
    st.warning("Kids データが空です。シート 'kids' に行を追加してください。")
    st.stop()

# --- 子ども画面 ---
if role == "子ども":
    kid_label = st.selectbox("自分を選んでね", list(kid_map.keys()))
    kid_id = kid_map[kid_label]
    kid_name = kid_label.split("（")[0]

    gdf = goals_for_kid(kid_id)
    if gdf.empty:
        st.info("まだ目標が登録されていません。")
    else:
        st.subheader("今日の目標（自己チェック）")
        for _, g in gdf.iterrows():
            ch, ap = today_check_state(kid_id, g["id"])
            new_ch = st.checkbox(f'{g["title"]}（{g["points"]}点）', value=ch, key=f"kid_{g['id']}")
            if new_ch != ch:
                upsert_checkin(
                    date.today().isoformat(), kid_id, kid_name,
                    g["id"], g["title"], set_child=new_ch, points=int(g["points"])
                )
        st.success("チェックは自動保存されます。")

    # 今月の合計（親承認済みのみ）
    ym = date.today().strftime("%Y-%m")
    total = monthly_total(kid_id, ym)
    st.metric("今月の合計ポイント（承認済）", f"{total} 点")

# --- 親画面 ---
else:
    # 親ロック（簡易）
    ok = True
    try:
        required = st.secrets.get("parent_pass", "")
    except Exception:
        required = ""
    if required:
        if "parent_ok" not in st.session_state:
            st.session_state.parent_ok = False
        if not st.session_state.parent_ok:
            inp = st.text_input("親パスコードを入力してください", type="password")
            if st.button("UnLock"):
                st.session_state.parent_ok = (inp == required)
            if not st.session_state.parent_ok:
                st.stop()

    colL, colR = st.columns([1,1.4])
    with colL:
        kid_label = st.selectbox("お子さんを選択", list(kid_map.keys()))
        kid_id = kid_map[kid_label]
        kid_name = kid_label.split("（")[0]
        target_date = st.date_input("対象日", date.today())

    gdf = goals_for_kid(kid_id)
        # 追加: 未承認だけ表示のフィルタ
    show_only_pending = st.checkbox("未承認だけ表示", value=False)

    # 当日/過去日のチェック状況をまとめて取得
    df = df_checkins()
    target_iso = target_date.isoformat()

    # 当日・過去日どちらでも、表示時点の child_checked / parent_approved を算出
    # （後続のループで使い回すために辞書化）
    state_map = {}  # (goal_id) -> (child_checked, parent_approved)
    for _, g in gdf.iterrows():
        ch, ap = (False, False)
        if target_date == date.today():
            ch, ap = today_check_state(kid_id, g["id"])
        else:
            if not df.empty:
                mask = (
                    (df["date"] == target_iso) &
                    (df["kid_id"] == kid_id) &
                    (df["goal_id"] == g["id"])
                )
                if mask.any():
                    r = df[mask].iloc[0]
                    ch, ap = bool(r["child_checked"]), bool(r["parent_approved"])
        state_map[g["id"]] = (ch, ap)

    # 未承認だけ表示が ON なら gdf を絞り込む
    if show_only_pending:
        keep_ids = [gid for gid, (ch, ap) in state_map.items() if ch and not ap]
        gdf = gdf[gdf["id"].isin(keep_ids)].reset_index(drop=True)

    # 追加: 一括承認ボタン
    if st.button("表示中の目標を一括承認する"):
        for _, g in gdf.iterrows():
            ch, ap = state_map.get(g["id"], (False, False))
            # 子がチェック済みで未承認なら承認する
            if ch and not ap:
                upsert_checkin(
                    target_iso, kid_id, kid_name,
                    g["id"], g["title"],
                    set_parent=True,
                    points=int(g["points"])
                )
        st.success("一括承認しました。")
        st.experimental_rerun()

    if gdf.empty:
        st.info("まだ目標が登録されていません。")
    else:
        st.subheader(f"{kid_name} のチェック状況（{target_date.isoformat()}）")

    for _, g in gdf.iterrows():
        ch, ap = state_map.get(g["id"], (False, False))
        c1, c2, c3 = st.columns([2.5, 1.2, 1])
        c1.write(f'• {g["title"]}（{int(g["points"])}点）')
        c2.write("自己チェック" if ch else "未チェック")
        btn_text = "承認取消" if ap else "承認する"
        if c3.button(btn_text, key=f"approve_{g['id']}"):
            upsert_checkin(
                target_iso, kid_id, kid_name,   # ← target_date.isoformat() の代わりに target_iso 変数を使用
                g["id"], g["title"],
                set_parent=not ap,  # トグル
                points=int(g["points"])
            )
            st.experimental_rerun()

    # 合計表示
    ym = target_date.strftime("%Y-%m")
    total = monthly_total(kid_id, ym)
    st.metric(f"{kid_name} の {ym} 合計ポイント（承認済）", f"{total} 点")

st.caption("データは Google Sheets の 'kids' 'goals' 'checkins' を使用します。")
