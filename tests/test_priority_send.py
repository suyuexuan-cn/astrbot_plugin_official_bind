"""优先级发送回归测试：每群独立 msg_id + 5分钟窗口。"""
from __future__ import annotations

import sys
import time
from pathlib import Path

# 复用主测试里的 stub 与工厂
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_content_dedup as base


def make():
    p = base.make_plugin()
    p._official_msg_id_seen = {}
    p._stats.setdefault("passive_replied", 0)
    p._stats.setdefault("proactive_sent", 0)
    return p

def test_per_group_msg_id():
    p = make()
    n = time.time()
    p._official_msg_id_seen["grpA"] = ("MA", n)
    p._official_msg_id_seen["grpB"] = ("MB", n)
    assert p._official_msg_id_seen["grpA"][0] == "MA"
    assert p._official_msg_id_seen["grpB"][0] == "MB"
    # 更新A群不影响B群（模拟只恢复当前群缓存）
    p._official_msg_id_seen["grpA"] = ("MA2", n)
    assert p._official_msg_id_seen["grpB"][0] == "MB"
    print("PASS test_per_group_msg_id")

def test_window():
    p = make()
    n = time.time()
    p._official_msg_id_seen["fresh"] = ("M1", n)
    p._official_msg_id_seen["stale"] = ("M2", n - 400)
    w = p._passive_reply_window()
    f1 = n - p._official_msg_id_seen["fresh"][1] <= w
    f2 = n - p._official_msg_id_seen["stale"][1] <= w
    assert f1 is True, "5分钟内应被动回复"
    assert f2 is False, "超5分钟应主动"
    print("PASS test_window")

def test_onebot_at_official_yields():
    """非官方端 @官方机器人时应让位，使官方端独占处理。"""
    p = make()
    p._onebot_names = lambda: {"aiocqhttp"}
    p._ignore_sender_ids = lambda: {"TEST_OFFICIAL_UIN"}
    class AtEv:
        def __init__(self):
            self.message_obj = base.types.SimpleNamespace(message=[base._At("TEST_OFFICIAL_UIN")])
        def get_platform_name(self):
            return "aiocqhttp"
    assert p._onebot_at_official(AtEv()) is True
    print("PASS test_onebot_at_official_yields")

if __name__ == "__main__":
    test_per_group_msg_id()
    test_window()
    test_onebot_at_official_yields()
    print("ALL PRIORITY TESTS PASSED")