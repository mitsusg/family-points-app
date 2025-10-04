
import streamlit as st
import sqlite3
from datetime import date
from contextlib import closing

DB_PATH = "goalpoints.db"

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS kids (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            grade TEXT,
            active INTEGER DEFAULT 1
        );""")
        cur.execute("""CREATE TABLE IF NOT EXISTS goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            base_points INTEGER NOT NULL DEFAULT 1,
            category TEXT,
            active INTEGER DEFAULT 1
        );""")
        cur.execute("""CREATE TABLE IF NOT EXISTS checkins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT NOT NULL,
            kid_id INTEGER NOT NULL,
            goal_id INTEGER NOT NULL,
            self_checked INTEGER DEFAULT 0,
            parent_approved INTEGER DEFAULT 0,
            UNIQUE(d, kid_id, goal_id),
            FOREIGN KEY(kid_id) REFERENCES kids(id),
            FOREIGN KEY(goal_id) REFERENCES goals(id)
        );""")
        con.commit()

def seed_if_empty():
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM kids;")
        if cur.fetchone()[0] == 0:
            cur.executemany("INSERT INTO kids (name, grade, active) VALUES (?, ?, 1);",
                            [("はると","小4"), ("みお","小2"), ("そうた","年中")])
        cur.execute("SELECT COUNT(*) FROM goals;")
        if cur.fetchone()[0] == 0:
            cur.executemany("INSERT INTO goals (title, base_points, category, active) VALUES (?, ?, ?, 1);",
                            [("宿題をする",5,"勉強"), ("歯みがき",2,"生活"), ("ランニング10分",3,"運動")])
        con.commit()

def ensure_today_checkins():
    today = date.today().isoformat()
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("SELECT id FROM kids WHERE active=1;")
        kid_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT id FROM goals WHERE active=1;")
        goal_ids = [row[0] for row in cur.fetchall()]
        for kid_id in kid_ids:
            for goal_id in goal_ids:
                cur.execute("INSERT OR IGNORE INTO checkins (d, kid_id, goal_id, self_checked, parent_approved) VALUES (?, ?, ?, 0, 0);",
                            (today, kid_id, goal_id))
        con.commit()

def list_kids(active_only=True):
    with closing(get_conn()) as con:
        cur = con.cursor()
        if active_only:
            cur.execute("SELECT id, name, grade FROM kids WHERE active=1 ORDER BY name;")
        else:
            cur.execute("SELECT id, name, grade, active FROM kids ORDER BY name;")
        return cur.fetchall()

def list_goals(active_only=True):
    with closing(get_conn()) as con:
        cur = con.cursor()
        if active_only:
            cur.execute("SELECT id, title, base_points, category FROM goals WHERE active=1 ORDER BY title;")
        else:
            cur.execute("SELECT id, title, base_points, category, active FROM goals ORDER BY title;")
        return cur.fetchall()

def update_self_checked(checkin_id, value):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("UPDATE checkins SET self_checked=? WHERE id=?;", (1 if value else 0, checkin_id))
        con.commit()

def update_parent_approved(checkin_id, value):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("UPDATE checkins SET parent_approved=? WHERE id=?;", (1 if value else 0, checkin_id))
        con.commit()

def get_child_today_checkins(kid_id):
    today = date.today().isoformat()
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT c.id, g.title, g.base_points, c.self_checked, c.parent_approved
            FROM checkins c
            JOIN goals g ON g.id = c.goal_id
            WHERE c.kid_id=? AND c.d=?
            ORDER BY g.title;
        """, (kid_id, today))
        return cur.fetchall()

def get_pending_approvals():
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT c.id, c.d, k.name, g.title, g.base_points
            FROM checkins c
            JOIN kids k ON k.id = c.kid_id
            JOIN goals g ON g.id = c.goal_id
            WHERE c.self_checked=1 AND c.parent_approved=0
            ORDER BY c.d DESC, k.name, g.title;
        """)
        return cur.fetchall()

def kid_points_total(kid_id):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(g.base_points),0)
            FROM checkins c
            JOIN goals g ON g.id = c.goal_id
            WHERE c.kid_id=? AND c.self_checked=1 AND c.parent_approved=1;
        """, (kid_id,))
        return int(cur.fetchone()[0])

def kid_points_this_month(kid_id):
    from datetime import date
    ym = date.today().strftime("%Y-%m")
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(g.base_points),0)
            FROM checkins c
            JOIN goals g ON g.id = c.goal_id
            WHERE c.kid_id=?
              AND c.self_checked=1 AND c.parent_approved=1
              AND substr(c.d,1,7)=?;
        """, (kid_id, ym))
        return int(cur.fetchone()[0])

def add_kid(name, grade):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("INSERT INTO kids (name, grade, active) VALUES (?, ?, 1);", (name, grade))
        con.commit()

def add_goal(title, base_points, category):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("INSERT INTO goals (title, base_points, category, active) VALUES (?, ?, ?, 1);", (title, base_points, category))
        con.commit()

def toggle_kid_active(kid_id, active):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("UPDATE kids SET active=? WHERE id=?;", (1 if active else 0, kid_id))
        con.commit()

def toggle_goal_active(goal_id, active):
    with closing(get_conn()) as con:
        cur = con.cursor()
        cur.execute("UPDATE goals SET active=? WHERE id=?;", (1 if active else 0, goal_id))
        con.commit()

# ------------- UI -----------------

st.set_page_config(page_title="Goal Points", page_icon="✅", layout="wide")

init_db()
seed_if_empty()
ensure_today_checkins()

st.title("✅ 親子ポイント（Python・Streamlit版）")

tab_role = st.radio("ロールを選択", ["子ども", "親"], horizontal=True)

if tab_role == "子ども":
    kids = list_kids(True)
    kid_map = {f"{name}（{grade}）": kid_id for kid_id, name, grade in kids}
    if not kid_map:
        st.info("有効な子どもがいません。親ビューで追加してください。")
    else:
        kid_label = st.selectbox("自分を選んでね", list(kid_map.keys()))
        kid_id = kid_map[kid_label]
        st.subheader("今日の目標（自己チェック）")
        rows = get_child_today_checkins(kid_id)
        if not rows:
            st.info("今日の目標はまだ作成されていません。親ビューで目標を作成してください。")
        for cid, title, base_points, self_checked, parent_approved in rows:
            cols = st.columns([6,2,2,2])
            cols[0].markdown(f"**{title}**")
            cols[1].write(f"{base_points} 点")
            new_val = cols[2].checkbox("できた！", value=bool(self_checked), key=f"self_{cid}")
            if new_val != bool(self_checked):
                update_self_checked(cid, new_val)
            cols[3].write("承認済" if parent_approved else "承認待ち")

        st.divider()
        st.subheader("ポイント集計")
        st.write(f"🎯 **今月合計**： {kid_points_this_month(kid_id)} 点")
        st.write(f"🏆 **累計**： {kid_points_total(kid_id)} 点")

else:
    st.subheader("未承認一覧（自己チェック済み）")
    pending = get_pending_approvals()
    if not pending:
        st.success("未承認はありません。")
    else:
        for cid, d, kid_name, goal_title, base_points in pending:
            cols = st.columns([2,3,3,2,2])
            cols[0].write(d)
            cols[1].markdown(f"**{kid_name}**")
            cols[2].markdown(goal_title)
            cols[3].write(f"{base_points} 点")
            if cols[4].button("承認", key=f"approve_{cid}"):
                update_parent_approved(cid, True)
                st.experimental_rerun()

    st.divider()
    st.subheader("今月ランキング")
    kids = list_kids(True)
    ranking = []
    for kid_id, name, grade in kids:
        ranking.append((name, grade, kid_points_this_month(kid_id), kid_points_total(kid_id), kid_id))
    ranking.sort(key=lambda x: (-x[2], x[0]))
    for name, grade, mpts, tpts, kid_id in ranking:
        st.write(f"**{name}**（{grade}）｜今月 {mpts} 点｜累計 {tpts} 点")

    st.divider()
    st.subheader("設定（親）")
    with st.expander("子ども管理"):
        name = st.text_input("名前を追加", key="kid_name_add")
        grade = st.text_input("学年", key="kid_grade_add")
        if st.button("子どもを追加"):
            if name.strip():
                add_kid(name.strip(), grade.strip())
                st.success("追加しました")
                st.experimental_rerun()
            else:
                st.warning("名前を入力してください")
        st.write("有効/無効切替")
        all_kids = list_kids(False)
        for row in all_kids:
            if len(row)==4:
                kid_id, nm, gr, active = row
            else:
                kid_id, nm, gr = row; active = 1
            on = st.checkbox(f"{nm}（{gr}）を有効", value=bool(active), key=f"kact_{kid_id}")
            toggle_kid_active(kid_id, on)

    with st.expander("目標管理"):
        title = st.text_input("目標名を追加", key="goal_title_add")
        base_points = st.number_input("基準ポイント", min_value=0, max_value=100, value=1, step=1, key="goal_pts_add")
        category = st.text_input("カテゴリ（任意）", key="goal_cat_add")
        if st.button("目標を追加"):
            if title.strip():
                add_goal(title.strip(), int(base_points), category.strip())
                st.success("追加しました")
                st.experimental_rerun()
            else:
                st.warning("目標名を入力してください")
        st.write("有効/無効切替")
        all_goals = list_goals(False)
        for row in all_goals:
            if len(row)==5:
                gid, title, pts, cat, active = row
            else:
                gid, title, pts, cat = row; active = 1
            on = st.checkbox(f"{title}（{pts}点）を有効", value=bool(active), key=f"gact_{gid}")
            toggle_goal_active(gid, on)

    if st.button("🔁 きょうの目標を再生成（有効な子ども×有効な目標）"):
        ensure_today_checkins()
        st.success("作成しました")
        st.experimental_rerun()
