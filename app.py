# app.py — Family Points (Google Sheets)
import time
import functools
import io

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime
import pandas as pd

# ============ ページ設定 ============
st.set_page_config(page_title="Family Points", page_icon="✅", layout="wide")

# ============ タイムゾーン ============
import pytz
TZ_NAME = st.secrets.get("tz", "Asia/Tokyo")
TZ = pytz.timezone(TZ_NAME)

def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")


# ============ 小さなリトライ ============
import requests
from google.auth.exceptions import TransportError

RETRIABLE = (
    gspread.exceptions.APIError,
    requests.exceptions.RequestException,
    TransportError,
    TimeoutError,
)

def retry(times=5, base_wait=0.6, factor=1.8):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for i in range(times):
                try:
                    return fn(*args, **kwargs)
                except RETRIABLE as e:
                    last = e
                    status = getattr(getattr(e, "response", None), "status_code", None)
                    # 4xx でも429はリトライ、それ以外の4xxは即時raise
                    if status and status not in (429, 500, 502, 503, 504):
                        raise
                    time.sleep(base_wait * (factor ** i))
            raise last
        return wrapper
    return deco


# ============ Google Sheets ============
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

@retry(times=3, base_wait=0.6)
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
@retry(times=5, base_wait=0.6, factor=1.8)
def safe_get_all_records(ws, expected_headers: list[str]) -> list[dict]:
    """
    1行目=ヘッダー前提で安全に全件を返す。
    空/崩れ/直後の整合不良でも落ちずに自己修復。
    """
    vals = ws.get_all_values()
    if not vals:
        ws.update("1:1", [expected_headers])
        time.sleep(0.2)
        return []

    headers = [str(h).strip() for h in (vals[0] or [])]
    rows = vals[1:]

    if headers != expected_headers:
        ws.update("1:1", [expected_headers])
        time.sleep(0.3)
        vals = ws.get_all_values()
        headers = vals[0] if vals else expected_headers
        rows = vals[1:] if len(vals) > 1 else []

    if not rows:
        return []

    n = len(headers)
    norm_rows = []
    for r in rows:
        r = list(r or [])
        if len(r) < n:
            r = r + [None] * (n - len(r))
        elif len(r) > n:
            r = r[:n]
        norm_rows.append(r)

    df = pd.DataFrame(norm_rows, columns=headers)
    df = df.applymap(lambda x: None if (x is None or str(x).strip() == "") else x)
    return df.to_dict(orient="records")


# ============ タブとヘッダ ============
KIDS_H = ["id", "name", "grade", "active"]
GOALS_H = ["id", "title", "points", "active", "kid_id"]  # kid_id 空=共通目標 or "k1,k2"
CHECKINS_H = [
    "date", "kid_id", "kid_name", "goal_id", "goal_title",
    "points", "child_checked", "parent_approved", "updated_at"
]

def ws_kids():     return get_ws("kids", KIDS_H)
def ws_goals():    return get_ws("goals", GOALS_H)
def ws_checkins(): return get_ws("checkins", CHECKINS_H)


# ============ Sheets ユーティリティ ============
@st.cache_data(ttl=20)
def df_kids():
    ws = ws_kids()
    recs = safe_get_all_records(ws, KIDS_H)
    df = pd.DataFrame(recs)
    if df.empty:
        df = pd.DataFrame(columns=KIDS_H)
    for c in KIDS_H:
        if c not in df.columns:
            df[c] = None
    # 重複ID警告（任意）
    if not df.empty and "id" in df.columns and df["id"].duplicated().any():
        dups = df[df["id"].duplicated()]["id"].unique().tolist()
        st.warning(f"⚠️ kids.id が重複しています: {dups}")
    return df[KIDS_H]

@st.cache_data(ttl=20)
def df_goals():
    ws = ws_goals()
    recs = safe_get_all_records(ws, GOALS_H)
    df = pd.DataFrame(recs)
    if df.empty:
        df = pd.DataFrame(columns=GOALS_H)
    for c in GOALS_H:
        if c not in df.columns:
            df[c] = None
    if "points" in df.columns:
        df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype(int)
    if "active" in df.columns:
        df["active"] = df["active"].astype(str).str.lower().isin(["true","1","yes"])
    # audience列が無ければ "both" を補う
    if "audience" not in df.columns:
        df["audience"] = "both"
    # 重複ID警告（任意）
    if not df.empty and "id" in df.columns and df["id"].duplicated().any():
        dups = df[df["id"].duplicated()]["id"].unique().tolist()
        st.warning(f"⚠️ goals.id が重複しています: {dups}")
    return df[GOALS_H + ["audience"]]

@st.cache_data(ttl=20)
def df_checkins():
    ws = ws_checkins()
    recs = safe_get_all_records(ws, CHECKINS_H)
    df = pd.DataFrame(recs)
    if df.empty:
        df = pd.DataFrame(columns=CHECKINS_H)
    for c in CHECKINS_H:
        if c not in df.columns:
            df[c] = None
    if "points" in df.columns:
        df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype(int)
    for b in ["child_checked", "parent_approved"]:
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

def ensure_ws_and_header(name, headers):
    ws = get_ws(name, headers)
    first = ws.row_values(1)
    if first != headers:
        ws.update("1:1", [headers])
        time.sleep(0.2)
    return ws

def seed_if_empty():
    # セッション中は1回だけ
    if st.session_state.get("_seeded_once"):
        return

    ensure_ws_and_header("kids", KIDS_H)
    ensure_ws_and_header("goals", GOALS_H)
    ensure_ws_and_header("checkins", CHECKINS_H)
    time.sleep(0.2)

    kids = df_kids()
    if kids.empty:
        ws_kids().append_row(["k1", "そうた", "年中", "TRUE"])
        ws_kids().append_row(["k2", "みお",   "小1", "TRUE"])

    goals = df_goals()
    if goals.empty:
        ws_goals().append_row(["g1", "ランニング10分", 3, "TRUE", ""])
        ws_goals().append_row(["g2", "宿題をする",     5, "TRUE", ""])
        ws_goals().append_row(["g3", "歯みがき",       2, "TRUE", ""])

    st.session_state["_seeded_once"] = True
    st.cache_data.clear()


# ============ Check-in の upsert ============
def upsert_checkin(the_date, kid_id, kid_name, goal_id, goal_title,
                   set_child=None, set_parent=None, points=0):
    ws = ws_checkins()
    df = df_checkins()
    key = (str(the_date), str(kid_id), str(goal_id))

    # 既存検索
    hit_idx = None
    if not df.empty:
        mask = (df["date"] == key[0]) & (df["kid_id"] == key[1]) & (df["goal_id"] == key[2])
        if mask.any():
            hit_idx = df[mask].index[0]

    now = now_iso()

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
        # 更新（batch_update でまとめて）
        r = hit_idx + 2  # 1行目ヘッダのため +2
        ops = []
        if set_child is not None:
            ops.append({
                "range": f"{ws.title}!{chr(65 + CHECKINS_H.index('child_checked'))}{r}",
                "values": [[str(bool(set_child))]],
            })
        if set_parent is not None:
            ops.append({
                "range": f"{ws.title}!{chr(65 + CHECKINS_H.index('parent_approved'))}{r}",
                "values": [[str(bool(set_parent))]],
            })
        ops.append({
            "range": f"{ws.title}!{chr(65 + CHECKINS_H.index('updated_at'))}{r}",
            "values": [[now]],
        })
      if ops:
          ws.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED",
             "data": ops
         })

    # 書き込み後はキャッシュをクリアして即時反映
    st.cache_data.clear()


def goals_for_kid(kid_id: str, viewer: str = "child"):
    g = df_goals().copy()

    # 1) active フィルタ
    g["_active"] = g.get("active", "TRUE").astype(str).str.strip().str.lower()
    g = g[g["_active"].isin(["true", "1", "yes"])].drop(columns=["_active"])

    # 2) audience（任意列）で出し分け。無ければ both 扱い
    import pandas as pd
    if "audience" in g.columns:
        audience_series = g["audience"].astype(str).str.strip().str.lower()
    else:
        audience_series = pd.Series(["both"] * len(g), index=g.index)

    valid_for_viewer = audience_series.isin(["both", viewer])
    g = g[valid_for_viewer]

    # 3) kid_id のカンマ区切り対応 + 全員共通（空 or 'all'）
    kid_id_raw = g.get("kid_id", "").astype(str).fillna("").str.replace("，", ",")
    g["_kid_ids"] = kid_id_raw.apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])

    def is_target(row_ids: list[str]) -> bool:
        if not row_ids:
            return True
        if any(i.lower() == "all" for i in row_ids):
            return True
        return kid_id in row_ids

    g = g[g["_kid_ids"].apply(is_target)]
    return g.reset_index(drop=True)


def monthly_total(kid_id, target_month):
    """target_month: 'YYYY-MM'"""
    df = df_checkins()
    if df.empty:
        return 0
    m = df[(df["kid_id"] == kid_id)]
    m = m[m["date"].str.startswith(target_month)]
    m = m[m["child_checked"] & m["parent_approved"]]
    return int(m["points"].sum())

def total_points_alltime(kid_id):
    """承認済みの全期間合計ポイント"""
    df = df_checkins()
    if df.empty:
        return 0
    df = df[(df["kid_id"] == kid_id) & df["child_checked"] & df["parent_approved"]]
    return int(df["points"].sum())


# ============ UI ============
st.title("✅ Family Points")

role = st.radio("ロールを選択", ["子ども", "親"], horizontal=True)

kids = df_kids()
kids = kids[kids.get("active", "TRUE").astype(str).str.lower() == "true"].reset_index(drop=True)
kid_map = {f'{row["name"]}（{row.get("grade","")}）': row["id"] for _, row in kids.iterrows()}
if not kid_map:
    st.warning("Kids データが空です。シート 'kids' に行を追加してください。")
    st.stop()

seed_if_empty()

# --- 子ども画面 ---
if role == "子ども":
    kid_label = st.selectbox("自分を選んでね", list(kid_map.keys()))
    kid_id = kid_map[kid_label]
    kid_name = kid_label.split("（")[0]

    gdf = goals_for_kid(kid_id, viewer="child")
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

    # 累計ポイントも表示
    total_all = total_points_alltime(kid_id)
    st.metric("累計ポイント（承認済）", f"{total_all} 点")


# --- 親画面 ---
else:
    # 親ロック（簡易）
    try:
        required = st.secrets.get("parent_pass", "")
    except Exception:
        required = ""
    if required:
        if "parent_ok" not in st.session_state:
            st.session_state["parent_ok"] = False
        if not st.session_state["parent_ok"]:
            inp = st.text_input("親パスコードを入力してください", type="password")
            if st.button("UnLock"):
                st.session_state["parent_ok"] = (inp == required)
            if not st.session_state["parent_ok"]:
                st.stop()

    colL, colR = st.columns([1, 1.4])
    with colL:
        kid_label = st.selectbox("お子さんを選択", list(kid_map.keys()))
        kid_id = kid_map[kid_label]
        kid_name = kid_label.split("（")[0]
        target_date = st.date_input("対象日", date.today())

    # audience のインタラクティブ切替（任意）
    with st.expander("🎛 表示オプション"):
        forced_audience = st.selectbox("audienceフィルタ", ["自動（parent）", "child", "parent", "both"], index=0)

    gdf = goals_for_kid(kid_id, viewer="parent")
    if forced_audience != "自動（parent）":
        gdf = goals_for_kid(kid_id, viewer=forced_audience)

   # --- 親画面 ---

    # 追加: 未承認だけ表示のフィルタ
    show_only_pending = st.checkbox("未承認だけ表示（全日対象）", value=False)

    df_all = df_checkins()
    target_iso = target_date.isoformat()

    state_map = {}
    for _, g in gdf.iterrows():
        ch, ap = (False, False)
        if target_date == date.today():
            ch, ap = today_check_state(kid_id, g["id"])
        else:
            if not df_all.empty:
                mask = (
                    (df_all["date"] == target_iso)
                    & (df_all["kid_id"] == kid_id)
                    & (df_all["goal_id"] == g["id"])
                )
                if mask.any():
                    r = df_all[mask].iloc[0]
                    ch, ap = bool(r["child_checked"]), bool(r["parent_approved"])
        state_map[g["id"]] = (ch, ap)

    # ✅ 全日対象の未承認リスト生成
    if show_only_pending and not df_all.empty:
        df_pending = df_all[
            (df_all["kid_id"] == kid_id)
            & (df_all["child_checked"])
            & (~df_all["parent_approved"])
        ].copy()
        if df_pending.empty:
            st.info("未承認のチェックインはありません。")
            st.stop()

        st.subheader(f"🕒 未承認のチェックイン一覧（全日）")
        for _, r in df_pending.iterrows():
            c1, c2, c3 = st.columns([2.5, 1.5, 1])
            c1.write(f'{r["goal_title"]}（{r["points"]}点）')
            c2.write(f'日付：{r["date"]}')
            if c3.button("承認する", key=f"approve_pending_{r['date']}_{r['goal_id']}"):
                upsert_checkin(
                    r["date"], kid_id, kid_name,
                    r["goal_id"], r["goal_title"],
                    set_parent=True, points=int(r["points"])
                )
                st.success(f'{r["goal_title"]}（{r["date"]}）を承認しました。')
                st.cache_data.clear()
                st.experimental_rerun()
        st.stop()

    # 一括承認
    if st.button("表示中の目標を一括承認する"):
        for _, g in gdf.iterrows():
            ch, ap = state_map.get(g["id"], (False, False))
            if ch and not ap:
                upsert_checkin(
                    target_iso, kid_id, kid_name,
                    g["id"], g["title"],
                    set_parent=True,
                    points=int(g["points"])
                )
        st.success("一括承認しました。")
        st.cache_data.clear()
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
            btn_key = f"approve_{g['id']}"

            if c3.button(btn_text, key=btn_key):
                confirm_key = f"confirm_unapprove_{g['id']}"
                if ap and not st.session_state.get(confirm_key):
                    st.session_state[confirm_key] = True
                    st.warning("本当に承認を取り消しますか？もう一度ボタンを押すと実行されます。")
                else:
                    st.session_state.pop(confirm_key, None)
                    upsert_checkin(
                        target_iso, kid_id, kid_name,
                        g["id"], g["title"],
                        set_parent=not ap,  # トグル
                        points=int(g["points"])
                    )
                    st.experimental_rerun()

    # 合計表示
    ym = target_date.strftime("%Y-%m")
    total = monthly_total(kid_id, ym)
    st.metric(f"{kid_name} の {ym} 合計ポイント（承認済）", f"{total} 点")


# ============ 便利ツール ============
def cache_daily_total(kid_id: str, ymd: str, total_points: int):
    """任意：kids シートに日別合計の列 total_YYYYMMDD を追加して保存する"""
    ws = ws_kids()
    vals = ws.get_all_values()
    headers = vals[0] if vals else []
    col_name = f"total_{ymd.replace('-', '')}"
    if col_name not in headers:
        headers.append(col_name)
        ws.update("1:1", [headers])
    rows = vals[1:]
    try:
        idx = [r[0] for r in rows].index(kid_id)  # id 列が先頭前提
        r = idx + 2
        c = headers.index(col_name) + 1
        ws.update_cell(r, c, total_points)
    except ValueError:
        pass

def df_to_csv_download(df, filename):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button("CSVをダウンロード", data=buf.getvalue(), file_name=filename, mime="text/csv")


# ============ 管理 & エクスポート ============
with st.expander("🛠 管理（メンテナンス）"):
    col_a, col_b = st.columns(2)
    if col_a.button("ヘッダ再生成 / シート検診"):
        for name, hdr in [("kids", KIDS_H), ("goals", GOALS_H), ("checkins", CHECKINS_H)]:
            ws = ensure_ws_and_header(name, hdr)
            st.success(f"{name}: ヘッダ検診 OK")
        st.cache_data.clear()
    if col_b.button("空行を除去（checkins）"):
        df = df_checkins()
        if not df.empty:
            df = df.dropna(how="all")
            ws = ws_checkins()
            ws.clear()
            ws.update("1:1", [CHECKINS_H])
            if not df.empty:
                rows = df[CHECKINS_H].astype(str).fillna("").values.tolist()
                ws.update("A2", rows)
        st.success("checkins の空行除去完了")
        st.cache_data.clear()

with st.expander("⬇️ データのエクスポート"):
    df_to_csv_download(df_kids(), "kids.csv")
    df_to_csv_download(df_goals(), "goals.csv")
    df_to_csv_download(df_checkins(), "checkins.csv")

st.caption("データは Google Sheets の 'kids' 'goals' 'checkins' を使用します。")
