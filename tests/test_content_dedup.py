"""离线回归测试：内容对比去重 + 官方备用入口。

通过 stub astrbot API 模块导入插件，实测 _should_drop_event 三条关键路径：
1. 两端同群同内容：先到者认领，后到者被丢弃（只处理一次）
2. 只有官方端收到（@官方机器人 / 非官方掉线）：官方端认领并进管线
3. 未绑定群 / 私聊：零干预

运行：python tests/test_content_dedup.py
"""
from __future__ import annotations

import sys
import types
from pathlib import Path


# ---- stub astrbot 模块，避免真实依赖 ----
def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m

class _Filter:
    class CustomFilter:
        def filter(self, event, cfg):
            return False
    class EventMessageType:
        ALL = "ALL"
    class PermissionType:
        ADMIN = "ADMIN"
    @staticmethod
    def command(*a, **kw):
        return lambda f: f
    @staticmethod
    def custom_filter(*a, **kw):
        return lambda f: f
    @staticmethod
    def on_decorating_result(*a, **kw):
        return lambda f: f
    @staticmethod
    def permission_type(*a, **kw):
        return lambda f: f
    @staticmethod
    def event_message_type(*a, **kw):
        return lambda f: f
    @staticmethod
    def on_platform_loaded(*a, **kw):
        return lambda f: f

class _Plain:
    def __init__(self, text=""):
        self.text = text
class _At:
    def __init__(self, qq=""):
        self.qq = qq
class _MessageChain:
    def __init__(self, chain=None):
        self.chain = chain or []
    def message(self, text):
        return self
    def get_plain_text(self):
        return ""
    @classmethod
    def message_cls(cls, text):
        return cls([_Plain(text)])

_stub("astrbot")
_stub("astrbot.api", AstrBotConfig=dict, logger=types.SimpleNamespace(
    info=lambda *a, **k: None, debug=lambda *a, **k: None,
    warning=lambda *a, **k: None, error=lambda *a, **k: None,
))
_stub("astrbot.api.event", AstrMessageEvent=object, MessageChain=_MessageChain, filter=_Filter)
_stub("astrbot.api.message_components", At=_At, Image=object, Node=object,
      Plain=_Plain, Record=object, Reply=object)
_stub("astrbot.api.star", Context=object, Star=object, StarTools=object, register=lambda *a, **k: (lambda c: c))

# 导入插件
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as m


class FakeEvent:
    def __init__(self, platform="qq_official", group="G1", sender="U1",
                 text="hello", umo=None, at=False):
        self._platform, self._group, self._sender = platform, group, sender
        self._text, self._at = text, at
        self.unified_msg_origin = umo or f"{platform}:GroupMessage:{group}"
        self.message_str = text
        self.message_obj = types.SimpleNamespace(message=[_At("bot")] if at else [_Plain(text)])
        self._extra = {}
    def get_group_id(self): return self._group
    def get_platform_name(self): return self._platform
    def get_sender_id(self): return self._sender
    def get_self_id(self): return "SELF"
    def set_extra(self, k, v): self._extra[k] = v
    def get_extra(self, k, default=False): return self._extra.get(k, default)

def make_plugin():
    p = object.__new__(m.OfficialBindPlugin)
    p.config = {}
    p._salt = "s"
    p._pending, p._bound, p._users = {}, [], []
    p._learned_ignored = set()
    p._dedup_seen = {}
    p._active, p._echo_records = True, []
    p._stats = {k: 0 for k in [
        "forwarded","fallback","skipped","deduped","handed_over","echo_guarded",
        "official_dropped","self_dropped","direct_forwarded","direct_fallback",
        "direct_deduped","migrated","member_bound","member_updated",
        "cross_deduped","official_took_over","at_official_handled","at_official_yield",
    ]}
    # 绑定一个群：非官方 QQ 群 123 <-> 官方 openid GRP
    p._bound = [{
        "scope": "group", "old_umo": "aiocqhttp:GroupMessage:123",
        "new_umo": "default:GroupMessage:GRP", "qq_group_id": "123",
        "group_openid": "GRP",
    }]
    return p

def test_cross_dedup():
    """两端同群同内容：先到认领，后到丢弃。"""
    p = make_plugin()
    # 非官方先到（正常情况）
    e1 = FakeEvent("aiocqhttp", "123", "U1", "hello")
    assert p._should_drop_event(e1) is False, "非官方首条应进管线"
    assert e1.get_extra("official_bind_claimed") is True
    # 官方后到同样内容（同一用户发到两个 bot 的内容一致）
    e2 = FakeEvent("qq_official", "GRP", "U1", "hello")
    assert p._should_drop_event(e2) is True, "官方后到应被去重"
    assert p._stats["cross_deduped"] == 1

def test_at_official_goes_through():
    """@官方机器人：官方端认领并进管线。"""
    p = make_plugin()
    e = FakeEvent("qq_official", "GRP", "U1", "你好", at=True)
    assert p._should_drop_event(e) is False, "@官方应进管线"
    assert p._stats["at_official_handled"] == 1

def test_official_takeover_when_onebot_absent():
    """非官方掉线：只有官方那份到达，官方接管。"""
    p = make_plugin()
    e = FakeEvent("qq_official", "GRP", "U1", "还有人吗")
    assert p._should_drop_event(e) is False, "官方应接管"
    assert p._stats["official_took_over"] == 1

def test_private_and_unbound_untouched():
    """私聊与未绑定群：零干预。"""
    p = make_plugin()
    assert p._should_drop_event(FakeEvent("qq_official", "", "U1", "hi")) is False
    assert p._should_drop_event(FakeEvent("qq_official", "UNKNOWN", "U1", "hi")) is False

def test_cross_dedup_with_real_sender_ids():
    """真实场景：官方 sender=openid，非官方 sender=QQ号，靠 users 表归一后应能去重。"""
    p = make_plugin()
    p._users = [{
        "qq_user_id": "TEST_QQ",
        "user_openid": "TEST_OPENID",
        "openid_scope": "group", "source_group_qq": "123",
    }]
    # 非官方先到：sender 是 QQ 号
    e1 = FakeEvent("aiocqhttp", "123", "TEST_QQ", "真实测试")
    assert p._should_drop_event(e1) is False
    # 官方后到：sender 是 openid
    e2 = FakeEvent("qq_official", "GRP", "TEST_OPENID", "真实测试")
    assert p._should_drop_event(e2) is True, "两端 sender 归一后应识别为同一条消息"
    assert p._stats["cross_deduped"] == 1

def test_official_only_command_yield():
    """官方专属指令：非官方让位，官方执行。"""
    p = make_plugin()
    e = FakeEvent("aiocqhttp", "123", "U1", "/数据迁入")
    assert p._should_drop_event(e) is True, "非官方应让位"
    e2 = FakeEvent("qq_official", "GRP", "U1", "/数据迁入")
    assert p._should_drop_event(e2) is False, "官方应放行"

if __name__ == "__main__":
    tests = [test_cross_dedup, test_at_official_goes_through,
             test_official_takeover_when_onebot_absent,
             test_private_and_unbound_untouched,
             test_cross_dedup_with_real_sender_ids,
             test_official_only_command_yield]
    for t in tests:
        t()
        print(f"PASS {t.__name__}")
    print(f"ALL {len(tests)} TESTS PASSED")
