"""OneBot v11 → QQ 官方机器人 数据迁移脚本。

读取 official_bind 插件收集的绑定映射（id_mapping.json，或导出的 CSV），
把 AstrBot 主库与 livingmemory 插件库中的 umo / platform_id 等字段
从旧值重写为新值，使历史对话、消息记录、长期记忆跟随到新会话。

用法（先停掉 AstrBot，再执行）::

    # 干跑：只打印会改哪些表、各多少行，不写库
    python migrate_to_official.py --astrbot-root E:\\QQBOT\\astrbot --dry-run

    # 确认无误后执行（自动备份每个被改动的文件）
    python migrate_to_official.py --astrbot-root E:\\QQBOT\\astrbot --execute

    # 顺带处理其他插件数据目录里的 umo 字符串（json/sqlite 通用替换）
    python migrate_to_official.py --astrbot-root E:\\QQBOT\\astrbot --execute --plugin-data

    # 再把「群成员 QQ 号 → 该成员在群内的 openid」改写进消息历史（可选）
    python migrate_to_official.py --astrbot-root E:\\QQBOT\\astrbot --dry-run --user-ids

注意：默认只迁移「umo 级」数据。群成员的个人 id 需要在官方机器人群里逐个
发送 /数据迁入 完成个人绑定（写入 id_mapping.json 的 users 表），再显式加
--user-ids 才会改写 sender_id。群内 openid 是群维度的，脚本只在对应的官方
群会话范围内改写，不会跨群串号。
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# 映射读取
# ---------------------------------------------------------------------------


def load_mappings(bind_dir: Path) -> list[dict]:
    """从插件数据目录读取绑定映射。

    优先读 id_mapping.json（官方端发码流程的落盘文件）；
    其次读旧版 bindings.json；都没有时找最新的 mapping_export_*.csv。
    """
    json_path = bind_dir / "id_mapping.json"
    if not json_path.exists():
        json_path = bind_dir / "bindings.json"
    if json_path.exists():
        data = json.loads(json_path.read_text(encoding="utf-8"))
        # v2.2.2 起群聊绑定放在 groups 区块；旧版平铺在顶层 bound
        groups = data.get("groups")
        bound = (groups.get("bound") if isinstance(groups, dict) else None) or data.get("bound") or []
        if bound:
            print(f"[映射] 从 {json_path} 读取到 {len(bound)} 条绑定")
            return bound

    csvs = sorted(bind_dir.glob("mapping_export_*.csv"))
    if csvs:
        rows = []
        with open(csvs[-1], encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                rows.append(dict(row))
        print(f"[映射] 从 {csvs[-1]} 读取到 {len(rows)} 条绑定")
        return rows

    raise SystemExit(
        f"没有找到绑定映射：{bind_dir} 下既没有 id_mapping.json/bindings.json，"
        "也没有 mapping_export_*.csv\n"
        "请先在 QQ 上完成绑定（官方 bot 发送 /数据迁入 → 粘贴给非官方 bot），"
        "或先执行 /导出映射 。"
    )


def load_user_mappings(bind_dir: Path) -> list[dict]:
    """读取「个人映射」（QQ 号 ↔ openid），没有则返回空列表。

    优先 id_mapping.json 的 users 表；其次最新的 user_mapping_*.csv。
    个人映射是可选的：没有它时脚本只做 umo 级迁移。
    """
    json_path = bind_dir / "id_mapping.json"
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = {}
        # v2.2.2 起个人映射放在 members 区块；旧版平铺在顶层 users
        members = data.get("members")
        raw = (members.get("users") if isinstance(members, dict) else None) or data.get("users") or []
        users = [u for u in raw if u.get("qq_user_id")]
        if users:
            print(f"[个人映射] 从 {json_path} 读取到 {len(users)} 条")
            return users

    csvs = sorted(bind_dir.glob("user_mapping_*.csv"))
    if csvs:
        rows: list[dict] = []
        with open(csvs[-1], encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("qq_user_id"):
                    rows.append(dict(row))
        if rows:
            print(f"[个人映射] 从 {csvs[-1]} 读取到 {len(rows)} 条")
            return rows
    return []


# ---------------------------------------------------------------------------
# SQLite 通用操作
# ---------------------------------------------------------------------------


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    except sqlite3.Error:
        return []


def rewrite_exact(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    pairs: list[tuple[str, str]],
    execute: bool,
    report: list[str],
) -> int:
    """把 table.column 中精确等于旧值的行改写为新值。

    返回受影响行数（dry-run 时为预计行数）。
    """
    if column not in table_columns(conn, table):
        return 0
    total = 0
    for old, new in pairs:
        cur = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} = ?", (old,)
        )
        n = cur.fetchone()[0]
        if not n:
            continue
        total += n
        if execute:
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE {column} = ?",
                (new, old),
            )
    if total:
        report.append(f"    {table}.{column}: {total} 行")
    return total


def rewrite_pair_row(
    conn: sqlite3.Connection,
    table: str,
    umo_column: str,
    platform_column: str | None,
    mapping: dict,
    execute: bool,
    report: list[str],
) -> int:
    """按单条映射改写一行数据：umo 列换新 umo，平台列换新平台 id。"""
    cols = table_columns(conn, table)
    if umo_column not in cols:
        return 0
    old_umo, new_umo = mapping["old_umo"], mapping["new_umo"]
    new_pid = mapping.get("new_platform_id") or new_umo.split(":")[0]

    n = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {umo_column} = ?", (old_umo,)
    ).fetchone()[0]
    if not n:
        return 0
    if execute:
        set_parts = [f"{umo_column} = ?"]
        params: list = [new_umo]
        if platform_column and platform_column in cols:
            set_parts.append(f"{platform_column} = ?")
            params.append(new_pid)
        params.append(old_umo)
        conn.execute(
            f"UPDATE {table} SET {', '.join(set_parts)} WHERE {umo_column} = ?",
            params,
        )
    report.append(f"    {table}.{umo_column}: {n} 行")
    return n


def rewrite_string_contains(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    pairs: list[tuple[str, str]],
    execute: bool,
    report: list[str],
) -> int:
    """把字段内容中包含旧 umo 的行做子串替换（用于 preferences/cron payload）。"""
    if column not in table_columns(conn, table):
        return 0
    total = 0
    for old, new in pairs:
        rows = conn.execute(
            f"SELECT rowid, {column} FROM {table} WHERE {column} LIKE ?",
            (f"%{old}%",),
        ).fetchall()
        if not rows:
            continue
        total += len(rows)
        if execute:
            for rowid, val in rows:
                if isinstance(val, str) and old in val:
                    conn.execute(
                        f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                        (val.replace(old, new), rowid),
                    )
    if total:
        report.append(f"    {table}.{column}（子串替换）: {total} 行")
    return total


# ---------------------------------------------------------------------------
# 个人 id 迁移（可选，--user-ids）
# ---------------------------------------------------------------------------


def rewrite_sender_ids(
    conn: sqlite3.Connection,
    table: str,
    scope_column: str,
    user_mapping: dict,
    execute: bool,
    report: list[str],
) -> int:
    """在指定会话范围内把 sender_id 从 QQ 号改成该成员在官方端的 openid。

    scope_column 是「会话标识」列（platform_message_history.user_id /
    livingmemory 的 session_id）。只在 scope = 该用户来源的官方会话时改写，
    避免把同一个 QQ 号在别的群里的记录一起改掉——群内 openid 是群维度的。
    """
    cols = table_columns(conn, table)
    if "sender_id" not in cols or scope_column not in cols:
        return 0
    scope = str(user_mapping.get("source_umo") or "")
    qq = str(user_mapping.get("qq_user_id") or "")
    openid = str(user_mapping.get("user_openid") or "")
    if not (scope and qq and openid):
        return 0
    n = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {scope_column} = ? AND sender_id = ?",
        (scope, qq),
    ).fetchone()[0]
    if not n:
        return 0
    if execute:
        conn.execute(
            f"UPDATE {table} SET sender_id = ? WHERE {scope_column} = ? AND sender_id = ?",
            (openid, scope, qq),
        )
    label = "群内" if user_mapping.get("openid_scope") == "group" else "私聊"
    report.append(f"    {table}.sender_id（{label} {qq}）: {n} 行")
    return n


def migrate_main_db_user_ids(
    db_path: Path, user_mappings: list[dict], execute: bool
) -> list[str]:
    report = [f"[主库-个人] {db_path}"]
    conn = connect(db_path)
    try:
        for u in user_mappings:
            rewrite_sender_ids(
                conn,
                "platform_message_history",
                "user_id",
                u,
                execute,
                report,
            )
        if execute:
            conn.commit()
    finally:
        conn.close()
    return report


def migrate_livingmemory_user_ids(
    lm_dir: Path, user_mappings: list[dict], execute: bool
) -> list[str]:
    report: list[str] = []
    main_db = lm_dir / "livingmemory.db"
    if not main_db.exists():
        return report
    report.append(f"[livingmemory-个人] {main_db}")
    conn = connect(main_db)
    try:
        for u in user_mappings:
            for table in ("memory_atoms", "graph_entries"):
                rewrite_sender_ids(conn, table, "session_id", u, execute, report)
        if execute:
            conn.commit()
    finally:
        conn.close()
    return report


# ---------------------------------------------------------------------------
# 主库迁移
# ---------------------------------------------------------------------------


def migrate_main_db(db_path: Path, mappings: list[dict], execute: bool) -> list[str]:
    report = [f"[主库] {db_path}"]
    pairs = [(m["old_umo"], m["new_umo"]) for m in mappings]

    conn = connect(db_path)
    try:
        # conversations / platform_message_history: umo + platform_id
        for m in mappings:
            rewrite_pair_row(
                conn, "conversations", "user_id", "platform_id", m, execute, report
            )
        for m in mappings:
            rewrite_pair_row(
                conn,
                "platform_message_history",
                "user_id",
                "platform_id",
                m,
                execute,
                report,
            )
        # 私聊绑定的 sender_id 也可以换（旧 sender_id == 旧 session_id）
        for m in mappings:
            if m.get("scope") != "private":
                continue
            old_sid, new_sid = m.get("old_session_id", ""), m.get("new_session_id", "")
            if not old_sid or not new_sid:
                continue
            n = conn.execute(
                "SELECT COUNT(*) FROM platform_message_history "
                "WHERE user_id = ? AND sender_id = ?",
                (m["new_umo"] if execute else m["old_umo"], old_sid),
            ).fetchone()[0]
            if n:
                report.append(f"    platform_message_history.sender_id（私聊）: {n} 行")
                if execute:
                    conn.execute(
                        "UPDATE platform_message_history SET sender_id = ? "
                        "WHERE user_id = ? AND sender_id = ?",
                        (new_sid, m["new_umo"], old_sid),
                    )
        # umo_aliases / umo_senders
        rewrite_exact(conn, "umo_aliases", "umo", pairs, execute, report)
        rewrite_exact(conn, "umo_senders", "umo", pairs, execute, report)
        for m in mappings:
            if m.get("scope") != "private":
                continue
            old_sid, new_sid = m.get("old_session_id", ""), m.get("new_session_id", "")
            if old_sid and new_sid:
                rewrite_exact(
                    conn, "umo_senders", "sender_id", [(old_sid, new_sid)], execute, report
                )
        # 会话-项目关联
        rewrite_exact(
            conn, "session_project_relations", "session_id", pairs, execute, report
        )
        # 偏好与定时任务里可能内嵌 umo 字符串
        rewrite_string_contains(conn, "preferences", "value", pairs, execute, report)
        rewrite_string_contains(conn, "cron_jobs", "payload", pairs, execute, report)
        # 平台统计：把旧平台 id 归到新平台 id
        pid_pairs = [
            (
                m.get("old_platform_id") or m["old_umo"].split(":")[0],
                m.get("new_platform_id") or m["new_umo"].split(":")[0],
            )
            for m in mappings
        ]
        rewrite_exact(conn, "platform_stats", "platform_id", pid_pairs, execute, report)

        if execute:
            conn.commit()
    finally:
        conn.close()
    return report


# ---------------------------------------------------------------------------
# livingmemory 迁移
# ---------------------------------------------------------------------------


def migrate_livingmemory(lm_dir: Path, mappings: list[dict], execute: bool) -> list[str]:
    report: list[str] = []
    pairs = [(m["old_umo"], m["new_umo"]) for m in mappings]

    main_db = lm_dir / "livingmemory.db"
    if main_db.exists():
        report.append(f"[livingmemory] {main_db}")
        conn = connect(main_db)
        try:
            rewrite_exact(conn, "memory_atoms", "session_id", pairs, execute, report)
            rewrite_exact(conn, "graph_entries", "session_id", pairs, execute, report)
            rewrite_exact(conn, "memory_profiles", "scope", pairs, execute, report)
            if execute:
                conn.commit()
        finally:
            conn.close()

    conv_db = lm_dir / "conversations.db"
    if conv_db.exists():
        report.append(f"[livingmemory] {conv_db}")
        conn = connect(conv_db)
        try:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            ]
            for t in tables:
                cols = table_columns(conn, t)
                for col in ("session_id", "umo", "user_id"):
                    if col in cols:
                        rewrite_exact(conn, t, col, pairs, execute, report)
            if execute:
                conn.commit()
        finally:
            conn.close()
    return report


# ---------------------------------------------------------------------------
# 其他插件数据（可选）
# ---------------------------------------------------------------------------


def migrate_plugin_data(
    plugin_data_dir: Path,
    mappings: list[dict],
    execute: bool,
    report: list[str],
    stamp: str,
) -> None:
    """对 plugin_data 下其他插件的 json / sqlite 做 umo 子串替换。

    只替换完整 umo 字符串（含冒号，误伤面小），不碰裸 QQ 号。
    """
    pairs = [(m["old_umo"], m["new_umo"]) for m in mappings]
    skip_dirs = {"astrbot_plugin_official_bind", "astrbot_plugin_livingmemory"}

    for path in sorted(plugin_data_dir.rglob("*")):
        if not path.is_file():
            continue
        if any(part in skip_dirs for part in path.parts):
            continue
        if path.suffix in (".db-shm", ".db-wal", ".tmp", ".bak") or ".bak_" in path.name:
            continue

        if path.suffix == ".json":
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            hits = sum(text.count(old) for old, _ in pairs)
            if hits:
                report.append(f"    {path.relative_to(plugin_data_dir)}: {hits} 处")
                if execute:
                    backup_file(path, stamp)
                    for old, new in pairs:
                        text = text.replace(old, new)
                    path.write_text(text, encoding="utf-8")
        elif path.suffix == ".db":
            try:
                conn = connect(path)
            except sqlite3.Error:
                continue
            try:
                tables = [
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                ]
                # 先干跑统计，有命中再备份并真正执行
                file_hits = 0
                for t in tables:
                    cols = table_columns(conn, t)
                    for col in ("session_id", "umo", "user_id", "scope"):
                        if col in cols:
                            file_hits += rewrite_exact(
                                conn, t, col, pairs, False, []
                            )
                if file_hits:
                    report.append(
                        f"    {path.relative_to(plugin_data_dir)}: {file_hits} 行"
                    )
                    if execute:
                        conn.close()
                        backup_file(path, stamp)
                        conn = connect(path)
                        for t in tables:
                            cols = table_columns(conn, t)
                            for col in ("session_id", "umo", "user_id", "scope"):
                                if col in cols:
                                    rewrite_exact(conn, t, col, pairs, True, [])
                        conn.commit()
            except sqlite3.Error:
                pass
            finally:
                conn.close()


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------


def backup_file(path: Path, stamp: str) -> None:
    bak = path.with_name(path.name + f".bak_{stamp}")
    shutil.copy2(path, bak)
    print(f"  已备份 -> {bak.name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--astrbot-root",
        type=Path,
        default=None,
        help="AstrBot 根目录（包含 data/ 的那层）。默认按脚本位置推断。",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="只报告，不写库（默认）")
    mode.add_argument("--execute", action="store_true", help="真正执行重写")
    parser.add_argument(
        "--plugin-data",
        action="store_true",
        help="顺带处理 plugin_data 下其他插件的 json/sqlite 数据",
    )
    parser.add_argument(
        "--user-ids",
        action="store_true",
        help="额外改写消息历史里的 sender_id（群成员 QQ号 → 该成员在群内/私聊的 openid）。"
        "需要先在群里逐个完成个人绑定",
    )
    args = parser.parse_args()
    execute = args.execute and not args.dry_run

    root = args.astrbot_root
    if root is None:
        # 脚本位于 data/plugins/<plugin>/ 时，root = 上三级
        cand = Path(__file__).resolve().parents[2]
        root = cand if (cand / "data" / "data_v4.db").exists() else None
    if root is None or not (root / "data").exists():
        print("无法确定 AstrBot 根目录，请用 --astrbot-root 指定")
        return 1

    data_dir = root / "data"
    bind_dir = data_dir / "plugin_data" / "astrbot_plugin_official_bind"
    mappings = load_mappings(bind_dir)
    if not mappings:
        print("绑定映射为空，没有需要迁移的数据。")
        return 0

    for m in mappings:
        print(f"  {m['old_umo']}  ->  {m['new_umo']}")

    main_db = data_dir / "data_v4.db"
    if execute and (main_db.with_suffix(".db-wal").exists()):
        wal_size = main_db.with_suffix(".db-wal").stat().st_size
        if wal_size > 0:
            print(
                "\n[警告] 检测到 data_v4.db-wal 非空，AstrBot 可能仍在运行。\n"
                "请先停止 AstrBot 再执行，否则改动可能丢失或损坏数据库。"
            )
            return 2

    stamp = time.strftime("%Y%m%d_%H%M%S")
    print(f"\n{'=' * 50}\n模式：{'执行' if execute else '干跑（不写库）'}\n{'=' * 50}")

    report: list[str] = []

    # 主库
    if main_db.exists():
        if execute:
            backup_file(main_db, stamp)
        report += migrate_main_db(main_db, mappings, execute)

    # livingmemory
    lm_dir = data_dir / "plugin_data" / "astrbot_plugin_livingmemory"
    if lm_dir.exists():
        if execute:
            for name in ("livingmemory.db", "conversations.db"):
                p = lm_dir / name
                if p.exists():
                    backup_file(p, stamp)
        report += migrate_livingmemory(lm_dir, mappings, execute)

    # 其他插件数据
    if args.plugin_data:
        report.append("[plugin_data] 其他插件")
        migrate_plugin_data(data_dir / "plugin_data", mappings, execute, report, stamp)

    # 个人 id（可选）：先把 umo 迁完，再在同一批库里改 sender_id
    if args.user_ids:
        user_mappings = load_user_mappings(bind_dir)
        if not user_mappings:
            report.append(
                "[个人] 没有个人映射：请先在官方机器人群里让成员发送 /数据迁入 完成个人绑定"
            )
        else:
            for u in user_mappings:
                scope = (
                    f"群 {u.get('source_group_qq')}"
                    if u.get("openid_scope") == "group"
                    else "私聊"
                )
                print(
                    f"  QQ {u.get('qq_user_id')}  ->  "
                    f"{str(u.get('user_openid', ''))[:8]}...（{scope}）"
                )
            if main_db.exists():
                report += migrate_main_db_user_ids(main_db, user_mappings, execute)
            if lm_dir.exists():
                report += migrate_livingmemory_user_ids(lm_dir, user_mappings, execute)

    print("\n".join(report))
    if not execute:
        print("\n以上为干跑结果。确认后加 --execute 真正执行。")
    else:
        print(f"\n执行完成。备份文件后缀：.bak_{stamp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
