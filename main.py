"""QQ 官方机器人 ⇄ OneBot v11 桥接插件（合并版）。

本插件 = 「身份绑定 / 数据迁移」+「同群双 bot 接收去重 / 回复转发」。

一、身份绑定与数据迁移
----------------------
QQ 官方机器人使用 openid / group_openid 标识用户与群，与 OneBot v11 的
QQ 号 / 群号完全不可换算。通过「身份码」让用户自助完成两端身份绑定：

1. 用户在官方机器人（qq_official / qq_official_webhook）的群或私聊发 /数据迁入
2. 插件生成身份码（加盐 md5 截取），返回一段说明消息
3. 用户把消息发给非官方机器人（aiocqhttp/NapCat）：群数据发群、个人数据私聊
4. 非官方端识别身份码，建立 官方 umo <-> 非官方 umo 的映射并写入
   id_mapping.json，供同目录 migrate_to_official.py 重写历史数据

群聊有两种码：

- 首次在某个群发 /数据迁入 -> 「群身份码」，绑定 群号 <-> 群 openid（会话级）
- 该群绑定完成后，任何成员再发 /数据迁入 -> 「个人身份码」，额外绑定
  QQ 号 <-> 该成员的 openid（个人级），写进 users 表。
  这样群成员各自的记忆/好感度/签到等按用户区分的数据才有迁移依据。

openid 是用户维度、跨群稳定的：同一个人在官方机器人的任意群（以及私聊）
拿到的 openid 都是同一个值。因此个人绑定按 QQ 号全局唯一，一次绑定处处可用，
不需要逐群重复绑定，粘码也不限定在同一个群。

二、双 bot 同群（两个机器人在同一个群）
------------------------------------
同一条群消息会被两个适配器各收到一次，整条管线会跑两遍、回复两条。v2.4
改用「内容对比 + 先到先得」：两端消息归一为「群号 + 发送者(openid) + 文本」
指纹，窗口内先到者认领进管线、后到者静默，保证只处理一次、只回一条。

1. 管线前（Platform.commit_event 包装）：按内容指纹认领，后到者直接丢弃；
   早于 WakingCheckStage，不触发「权限不足」自动回复。非官方端自身消息
   （NapCat report-self-message）与官方代发回声同样在这里拦下。
2. 管线内（priority=9999 自定义过滤器）：补丁未挂上时的兜底，同一套指纹逻辑。
3. 发送前（bot.send_group_msg 包装）：gscore_adapter 等绕过管线直调协议 API
   的群发也转由官方 bot 代发，失败退回原路径。

- 官方专属指令（/数据迁入）：官方端执行、非官方端让位
- 非官方端 @官方机器人（At.qq ∈ ignore_sender_ids）：非官方让位，官方独占处理
- 普通消息：先到先得；非官方先到（常态）→ 非官方处理+官方代发；
            只有官方到（非官方掉线）→ 官方端接管入口自行处理
- 官方回复优先级：5 分钟内 msg_id 被动回复 → 主动群消息 → 退回非官方
- 群映射直接取内存中的绑定记录，绑定完成即刻生效
- 官方平台 id 变更（webhook ↔ websocket 互切）时自动迁移已有绑定

管理员指令：绑定列表 / 解绑 / 导出映射 / 转发状态
"""

from __future__ import annotations

import asyncio
import contextvars
import csv
import hashlib
import json
import re
import time
from collections import deque
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import At, Image, Node, Plain, Record, Reply
from astrbot.api.star import Context, Star, StarTools, register

PLUGIN_NAME = "astrbot_plugin_official_bind"

# 从消息文本中识别身份码（6-16 位十六进制，前后不能紧跟字母数字）
CODE_RE = re.compile(r"(?<![0-9a-zA-Z])([0-9a-f]{6,16})(?![0-9a-zA-Z])")

# 非官方端触发绑定的关键词：消息需包含其中之一且带身份码才会被当作绑定请求
BIND_KEYWORDS = ("身份码", "官方机器人迁移绑定", "绑定")

# 官方 bot 在 OneBot 侧显示的 QQ 号/UIN。它自己发的身份码说明、代发回执会被
# NapCat 回灌进非官方端，绝不能当成用户去完成绑定。配置里的 ignore_sender_ids
# 会与此集合合并；换成其他官方 bot 时，可在配置里追加新的 UIN。
BUILTIN_IGNORED_SENDERS = frozenset()  # 官方 bot 的 OneBot UIN 由配置/自动学习填入

DEDUP_PRIORITY = 9999  # 必须高于所有业务 handler，才能在最早期拦下重复事件

# 平台补丁登记表：key 为对象 id，插件重载时只替换 plugin 引用，不重复套娃
_COMMIT_PATCHES: dict[int, dict] = {}
_SEND_PATCHES: dict[int, dict] = {}
_OFFICIAL_SEND_PATCHES: dict[int, dict] = {}

# 转发中的重入保护：避免「官方 umo 恰好也指向 aiocqhttp」时自己转发给自己
_FORWARD_GUARD: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "official_bind_forwarding", default=False
)


def _onebot_to_chain(message) -> MessageChain | None:
    """把 OneBot v11 的消息段数组还原成 AstrBot 消息链。

    只还原官方平台能呈现的段类型；@ / 引用 / 表情 / 合并转发在官方端没有
    对应形态，直接丢弃。还原不出任何内容时返回 None，调用方应退回原路径。
    """
    if isinstance(message, str):
        return MessageChain(chain=[Plain(message)]) if message.strip() else None
    if not isinstance(message, list | tuple):
        return None

    out: list = []
    for seg in message:
        if not isinstance(seg, dict):
            continue
        seg_type = str(seg.get("type") or "")
        data = seg.get("data") or {}
        if seg_type == "text":
            text = str(data.get("text") or "")
            if text:
                out.append(Plain(text))
        elif seg_type == "image":
            file = str(data.get("file") or data.get("url") or "")
            if file:
                out.append(Image(file=file))
        elif seg_type == "record":
            file = str(data.get("file") or data.get("url") or "")
            if file:
                out.append(Record(file=file))
        # at / reply / face / node / forward 等：官方端无法呈现，丢弃
    if not out:
        return None
    return MessageChain(chain=out)


class _DedupFilter(filter.CustomFilter):
    """接收去重过滤器。

    只在确实需要拦截该事件时返回 True（激活 handler 并 stop_event），
    其余情况一律返回 False —— 不改变 AstrBot 原本的唤醒判断，
    对私聊、官方独占群等无需拦截的场景零副作用。
    """

    plugin: OfficialBindPlugin | None = None

    def filter(self, event: AstrMessageEvent, cfg) -> bool:
        """管线内兜底：与 commit 前拦截保持同一套「内容对比先到先得」语义。

        只有确实需要拦下（重复事件 / 指令让位 / 回环）才返回 True。
        commit 补丁已挂上时事件根本进不来，这里自然不会被触发；补丁失败时
        这里的去重仍保证同一条消息只跑一次。
        """
        p = _DedupFilter.plugin
        if p is None or not p._forward_enabled() or not p._dedup_enabled():
            return False
        if not event.get_group_id():
            return False  # 只管群聊，私聊不受影响

        name = event.get_platform_name()
        is_official = name in p._official_type_names()
        is_onebot = name in p._onebot_names()
        paired = (
            p._is_paired_group(event)
            if is_official
            else p._is_onebot_paired_group(event)
            if is_onebot
            else False
        )
        if not paired:
            return False

        # 官方 bot 代发的回执在本端的回声：静默
        if p._is_echo(event):
            return True

        # 官方专属指令：非官方端交出，官方端放行
        if p._is_official_only_command(event):
            return is_onebot

        # commit 补丁已认领并放行的事件：直接放行，绝不二次认领。
        if event.get_extra("official_bind_claimed"):
            return False
        # 补丁没挂上时才走到这里：管线内补做内容对比，后到者拦下。
        fp = p._fingerprint(event)
        if fp is not None:
            return not p._claim(fp)
        return False


@register(
    PLUGIN_NAME,
    "yuexuan",
    "QQ 官方机器人 ⇄ OneBot v11：身份绑定/数据迁移 + 同群双 bot 接收去重与回复转发",
    "2.5.0",
)
class OfficialBindPlugin(Star):
    """身份绑定 + 双 bot 同群桥接（官方端发码，非官方端收码并统一回复出口）。"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.data_dir: Path = StarTools.get_data_dir(PLUGIN_NAME)
        self.db_path: Path = self.data_dir / "id_mapping.json"
        self._lock = asyncio.Lock()

        # 运行时状态：盐 / 待绑定码 / 已绑定会话 / 已绑定个人
        self._salt: str = ""
        self._pending: dict[str, dict] = {}
        self._bound: list[dict] = []
        self._users: list[dict] = []
        # 从「非官方端 @ 官方 bot 发专属指令」自动学到的官方 bot OneBot 账号
        self._learned_ignored: set[str] = set()
        self._load()

        # 回环防护：最近由官方端代发出去的消息指纹 (umo, 规范化文本, 时间戳)
        self._echo_records: deque[tuple[str, str, float]] = deque(maxlen=32)
        # 最近一次「未转发」的原因，供 /转发状态 与日志排查
        self._last_skip: str = ""
        self._last_skip_at: float = 0.0
        # 最近成功代发的文本指纹 (群号, 规范化文本, 时间戳)，用于直发兜底去重
        self._recent_forwards: deque[tuple[str, str, float]] = deque(maxlen=64)
        # 跨端消息指纹去重：(canonical群号, 规范化文本) -> 认领时间（谁先到谁处理）
        self._dedup_seen: dict[tuple[str, str], float] = {}
        # 各群非官方端最近收到消息的时间（相对活跃信号，用于判断它是否还活着）
        self._last_onebot_seen: dict[str, float] = {}
        # 官方群 msg_id 及记录时间：用于「5分钟内 msg_id 被动回复」优先级
        # key = 官方群 session_id(openid)，value = (msg_id, 记录时间戳)
        self._official_msg_id_seen: dict[str, tuple[str, float]] = {}
        # 统计：代发成功 / 退回 / 跳过 / 接收去重 / 交官方 / 回环防护
        self._stats = {
            "forwarded": 0,
            "fallback": 0,
            "skipped": 0,
            "deduped": 0,
            "handed_over": 0,
            "echo_guarded": 0,
            "official_dropped": 0,
            "self_dropped": 0,
            "direct_forwarded": 0,
            "direct_fallback": 0,
            "direct_deduped": 0,
            "migrated": 0,
            "member_bound": 0,
            "member_updated": 0,
            "cross_deduped": 0,
            "official_took_over": 0,
            "at_official_handled": 0,
            "at_official_yield": 0,
            "at_official_handed": 0,
        }
        self._active = True
        _DedupFilter.plugin = self

        # 先挂平台补丁（事件入口 + 协议端直发兜底），再迁移官方平台 id。
        # 两者都失败也不影响绑定/迁移功能，最坏退化为 v2.0 的行为。
        self._patch_task: asyncio.Task | None = None
        self._install_patches()
        self._migrate_platform_ids()
        try:
            self._patch_task = asyncio.create_task(self._patch_loop())
        except RuntimeError:  # 没有运行中的事件循环时交给 on_platform_loaded
            self._patch_task = None

    async def _patch_loop(self) -> None:
        """平台实例/协议端连接可能晚于插件就绪，前 10 分钟定期补挂补丁。"""
        for _ in range(120):
            await asyncio.sleep(5)
            if not self._active:
                return
            try:
                self._install_patches()
            except BaseException:  # noqa: BLE001,S110 补挂失败下一轮再试
                pass
            if self._patches_complete():
                return

    def _patches_complete(self) -> bool:
        """官方/非官方平台是否都挂上了对应补丁。"""
        try:
            insts = list(self.context.platform_manager.platform_insts)
        except BaseException:  # noqa: BLE001 拿不到平台列表视为未完成
            return False
        for platform in insts:
            try:
                name = platform.meta().name
            except BaseException:  # noqa: BLE001,S112 跳过异常平台
                continue
            if name in self._official_type_names() or name in self._onebot_names():
                if id(platform) not in _COMMIT_PATCHES:
                    return False
                if name == "aiocqhttp":
                    try:
                        bot = platform.get_client()
                    except BaseException:  # noqa: BLE001 连接异常视为未完成
                        return False
                    if bot is not None and id(bot) not in _SEND_PATCHES:
                        return False
        return True

    # ------------------------------------------------------------------
    # 配置
    # ------------------------------------------------------------------

    def _expire_seconds(self) -> int:
        try:
            return max(1, int(self.config.get("code_expire_minutes", 15))) * 60
        except (TypeError, ValueError):
            return 15 * 60

    def _code_length(self) -> int:
        try:
            return min(16, max(6, int(self.config.get("code_length", 8))))
        except (TypeError, ValueError):
            return 8

    def _admin_only_bind(self) -> bool:
        return bool(self.config.get("admin_only_bind", False))

    def _member_bind_enabled(self) -> bool:
        """群会话绑定完成后，是否允许群成员继续绑定个人 id。"""
        return bool(self.config.get("enable_member_bind", True))

    def _onebot_names(self) -> set[str]:
        """非官方平台名：配置项 + 绑定记录里记录的真实平台 id。

        old_platform_id 就是非官方适配器当时的实际名字，用户把适配器改名
        （如 aiocqhttp → napcat）时，靠它也能继续识别，不会整条链路静默失效。
        """
        raw = str(self.config.get("onebot_platform_names", "aiocqhttp"))
        names = {s.strip() for s in raw.split(",") if s.strip()}
        for rec in list(self._bound):
            pid = str(rec.get("old_platform_id") or "").strip()
            if pid:
                names.add(pid)
        return names

    def _official_type_names(self) -> set[str]:
        """官方适配器的「类型名」，不含绑定记录里的平台 id。

        类型名由适配器决定（qq_official / qq_official_webhook），平台 id 则是
        用户在 WebUI 里填的实例名（default、QQBOT-websocket…）。判断一条事件
        属于哪一端时只能用类型名，否则平台改名就会失效。
        """
        raw = str(
            self.config.get(
                "official_platform_names", "qq_official,qq_official_webhook"
            )
        )
        return {s.strip() for s in raw.split(",") if s.strip()}

    # --- 双 bot 桥接配置 ---

    def _forward_enabled(self) -> bool:
        """同群双 bot 桥接总开关（去重 + 回复转发）；不影响身份绑定功能。"""
        return bool(self.config.get("enable_forward", True))

    def _dedup_enabled(self) -> bool:
        return bool(self.config.get("enable_dedup", True))

    def _drop_self_messages(self) -> bool:
        """丢弃非官方端收到的自身消息（NapCat report-self-message 回环）。"""
        return bool(self.config.get("drop_self_messages", True))

    def _enable_official_fallback(self) -> bool:
        """非官方端沉默时，官方端是否接管为备用消息入口。"""
        return bool(self.config.get("enable_official_fallback", True))

    def _message_dedup_window(self) -> float:
        """跨端内容对比窗口（秒）。

        官方端先于非官方端收到普通群消息时，等待这段时长再复查内容指纹：
        非官方那份到了就让位，没到才判定它掉线并自己接管。正常时零延迟。
        """
        try:
            return max(0.2, float(self.config.get("message_dedup_window_seconds", 1.5)))
        except (TypeError, ValueError):
            return 1.5

    def _intercept_direct_sends(self) -> bool:
        """拦截绕过管线、直接调用协议 API 的群发（如 gscore_adapter）。"""
        return bool(self.config.get("intercept_direct_sends", True))

    def _passive_reply_window(self) -> float:
        """msg_id 被动回复的有效窗口（秒），默认 5 分钟。

        窗口内用最近消息的 msg_id 走被动回复（不占主动消息额度）；
        超过窗口则退化为主 sent 消息。
        """
        try:
            return max(0.0, float(self.config.get("passive_reply_window_seconds", 300)))
        except (TypeError, ValueError):
            return 300.0

    def _official_only_commands(self) -> set[str]:
        """只由官方端执行的指令（如生成身份码），两端都会做对应去重。"""
        raw = str(self.config.get("official_only_commands", "数据迁入"))
        return {s.strip().lstrip("/") for s in raw.split(",") if s.strip()}

    def _ignore_sender_ids(self) -> set[str]:
        """官方 bot 在 OneBot 侧的账号：配置 + 内置保底 + 运行时自动学习。"""
        raw = str(self.config.get("ignore_sender_ids", ""))
        configured = {s.strip() for s in raw.split(",") if s.strip()}
        return configured | set(BUILTIN_IGNORED_SENDERS) | set(self._learned_ignored)

    def _learn_official_sender(self, event: AstrMessageEvent) -> None:
        """从非官方端「@官方 bot + 专属指令」自动学到它的 OneBot 账号。

        官方 bot 在官方渠道用 openid、在 OneBot 渠道用 QQ 号，两者无法换算。
        用户在非官方群 @ 官方 bot 发 /数据迁入 时，At.qq 就是它的 OneBot 账号；
        学到后即可在管线前屏蔽它的身份码说明与代发回执。
        """
        if event.get_platform_name() not in self._onebot_names():
            return
        msg_obj = getattr(event, "message_obj", None)
        comps = getattr(msg_obj, "message", None) or []
        texts = [
            str(getattr(comp, "text", "") or "")
            for comp in comps
            if isinstance(comp, Plain)
        ]
        joined = " ".join(texts).strip()
        if not joined:
            return
        if not any(
            joined.lstrip("/").startswith(cmd)
            for cmd in self._official_only_commands()
        ):
            return
        for comp in comps:
            if not isinstance(comp, At):
                continue
            qq = str(getattr(comp, "qq", "") or "").strip()
            if qq.isdigit() and qq not in self._learned_ignored:
                self._learned_ignored.add(qq)
                logger.info(f"[official_bind] 自动识别官方 bot 的 OneBot 账号：{qq}")

    @staticmethod
    def _is_streaming_result(result: object) -> bool:
        """结果是否为流式（STREAMING_RESULT/STREAMING_FINISH），按枚举名判断以免 import 私有模块。"""
        name = getattr(getattr(result, "result_content_type", None), "name", "")
        return name in {"STREAMING_RESULT", "STREAMING_FINISH"}

    def _echo_seconds(self) -> int:
        try:
            return max(0, int(self.config.get("echo_guard_seconds", 10)))
        except (TypeError, ValueError):
            return 10

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------

    def _clean_legacy_data(self, pending, bound, users):
        """一次性迁移旧格式数据：只留群聊，纠正错误 UIN，丢弃全部个人数据。

        群记录里 qq_user_id=官方 bot UIN 是回环误绑产生的。用同一条记录里的
        user_openid 去旧个人表反查正确的 QQ 号（排除被忽略的 UIN），命中即替换
        qq_user_id / bound_by_qq 与发起人昵称；个人表本身和个人身份码一律不保留。
        """
        ignored = self._ignore_sender_ids()
        correct: dict[str, tuple[str, str]] = {}
        for u in users or []:
            qq = str(u.get("qq_user_id") or "")
            oid = str(u.get("user_openid") or "")
            if qq and oid and qq not in ignored:
                correct[oid] = (qq, str(u.get("sender_name") or ""))

        cleaned_bound = []
        fixed = 0
        for rec in bound or []:
            rec = dict(rec)
            qq = str(rec.get("qq_user_id") or "")
            if qq in ignored:
                hit = correct.get(str(rec.get("user_openid") or ""))
                if hit:
                    rec["qq_user_id"] = hit[0]
                    rec["bound_by_qq"] = hit[0]
                    if hit[1]:
                        rec["bound_by_name"] = hit[1]
                    fixed += 1
            cleaned_bound.append(rec)

        cleaned_pending = {
            k: v for k, v in (pending or {}).items() if v.get("kind") != "member"
        }
        logger.warning(
            f"[official_bind] 旧数据迁移：群聊 {len(cleaned_bound)} 条全部保留"
            f"（纠正错误 UIN {fixed} 条），个人映射 {len(users or [])} 条与"
            f"个人身份码 {len(pending or {}) - len(cleaned_pending)} 个已全部去除"
        )
        return cleaned_pending, cleaned_bound, []

    def _load(self) -> None:
        """从 plugin_data 读取绑定数据；兼容旧四段式与新「群聊/个人」双区块。"""
        migrated = False
        try:
            if self.db_path.exists():
                data = json.loads(self.db_path.read_text(encoding="utf-8"))
                self._salt = str(data.get("salt") or "")
                if "groups" in data or "members" in data:
                    # 新格式：groups = 群聊会话，members = 个人映射
                    groups = data.get("groups") or {}
                    members = data.get("members") or {}
                    self._pending = dict(groups.get("pending") or {})
                    self._pending.update(dict(members.get("pending") or {}))
                    self._bound = list(groups.get("bound") or [])
                    self._users = list(members.get("users") or [])
                else:
                    # 旧格式：一次性迁移后立刻写成新格式
                    self._pending, self._bound, self._users = self._clean_legacy_data(
                        data.get("pending"), data.get("bound"), data.get("users")
                    )
                    migrated = True
        except Exception as e:  # noqa: BLE001 数据损坏不应导致插件崩溃
            logger.error(f"[official_bind] 读取绑定数据失败，将从头开始: {e}")
        if not self._salt:
            self._salt = hashlib.sha256(
                f"{time.time()}:{id(self)}".encode()
            ).hexdigest()
            migrated = True
        if migrated:
            self._save()
        self._cleanup_expired()

    def _save(self) -> None:
        """写新格式：groups（群聊会话）+ members（个人映射），pending 按 kind 归位。

        pending 里 kind=member 的是个人码，归入 members；其余（群/私聊会话码）
        归入 groups。bound 全是群聊/私聊会话绑定，统一放 groups；users 放 members。
        """
        group_pending: dict[str, dict] = {}
        member_pending: dict[str, dict] = {}
        for code, rec in self._pending.items():
            (member_pending if rec.get("kind") == "member" else group_pending)[code] = rec
        payload = {
            "salt": self._salt,
            "groups": {
                "pending": group_pending,
                "bound": self._bound,
            },
            "members": {
                "pending": member_pending,
                "users": self._users,
            },
        }
        try:
            tmp = self.db_path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self.db_path)  # 原子替换，避免写一半损坏
        except Exception as e:  # noqa: BLE001 落盘失败只记日志，不影响消息处理
            logger.error(f"[official_bind] 保存绑定数据失败: {e}")

    def _cleanup_expired(self) -> None:
        now = time.time()
        expired = [
            code
            for code, rec in self._pending.items()
            if rec.get("expires_at", 0) < now
        ]
        for code in expired:
            self._pending.pop(code, None)
        if expired:
            self._save()

    # ------------------------------------------------------------------
    # 工具函数
    # ------------------------------------------------------------------

    def _make_code(self, umo: str, seed: str = "") -> str:
        """由官方会话身份生成身份码（确定性 + 加盐，碰撞时换 nonce）。

        seed 用于区分同一会话下的不同用途（如「群成员个人码」），
        空串即会话级身份码。
        """
        length = self._code_length()
        for nonce in range(100):
            raw = f"{self._salt}|{umo}|{seed}|{nonce}"
            code = hashlib.md5(raw.encode()).hexdigest()[:length]
            existing = self._pending.get(code)
            occupant = self._find_any_by_code(code)
            if existing and existing.get("new_umo") != umo:
                continue  # 碰撞到别人，换 nonce
            if occupant and occupant.get("new_umo") != umo:
                continue
            return code
        # 理论上到不了这里；兜底返回更长摘要
        return hashlib.md5(
            f"{self._salt}|{umo}|{seed}|fallback".encode()
        ).hexdigest()[:16]

    def _find_bound_by_code(self, code: str) -> dict | None:
        for rec in self._bound:
            if rec.get("code") == code:
                return rec
        return None

    def _find_bound_by_new_umo(self, new_umo: str) -> dict | None:
        for rec in self._bound:
            if rec.get("new_umo") == new_umo:
                return rec
        return None

    def _user_by_code(self, code: str) -> dict | None:
        for rec in self._users:
            if rec.get("code") == code:
                return rec
        return None

    def _find_any_by_code(self, code: str) -> dict | None:
        """身份码是否已经被会话绑定或个人绑定占用。"""
        return self._find_bound_by_code(code) or self._user_by_code(code)

    def _find_user(
        self,
        qq_user_id: str,
        openid_scope: str = "",
        source_group_qq: str = "",
    ) -> dict | None:
        """定位一条个人映射：openid 用户级跨群稳定，按 QQ 号全局唯一。

        openid_scope / source_group_qq 仅作来源记录，不参与唯一性判定。
        """
        qq = str(qq_user_id or "")
        if not qq:
            return None
        for rec in self._users:
            if str(rec.get("qq_user_id", "")) == qq:
                return rec
        return None

    def _upsert_user(
        self,
        *,
        code: str,
        qq_user_id: str,
        user_openid: str,
        openid_scope: str,
        source_group_qq: str = "",
        source_group_openid: str = "",
        source_umo: str = "",
        old_umo: str = "",
        platform_id: str = "",
        sender_name: str = "",
    ) -> tuple[dict, bool]:
        """写入或刷新一条个人映射，返回 (记录, 是否新建)。"""
        rec = self._find_user(qq_user_id, openid_scope, source_group_qq)
        now = time.time()
        created = rec is None
        if rec is None:
            rec = {"created_at": now}
            self._users.append(rec)
        rec.update(
            {
                "code": code,
                "new_umo": source_umo,
                "qq_user_id": str(qq_user_id or ""),
                "user_openid": str(user_openid or ""),
                "openid_scope": openid_scope,
                "source_group_qq": str(source_group_qq or ""),
                "source_group_openid": str(source_group_openid or ""),
                "source_umo": source_umo,
                "old_umo": old_umo,
                "platform_id": platform_id,
                "sender_name": sender_name,
                "bound_at": rec.get("bound_at") or now,
                "updated_at": now,
            }
        )
        return rec, created

    def _drop_user(self, target: str) -> list[dict]:
        """按身份码 / QQ 号 / openid 片段删除个人映射。"""
        target = str(target or "")
        kept: list[dict] = []
        removed: list[dict] = []
        for rec in self._users:
            hit = (
                rec.get("code") == target
                or str(rec.get("qq_user_id", "")) == target
                or (target and target in str(rec.get("user_openid", "")))
            )
            (removed if hit else kept).append(rec)
        if removed:
            self._users = kept
        return removed

    @staticmethod
    def _mask(umo: str) -> str:
        """umo 打码展示，避免把 openid 完整刷到群聊里。"""
        parts = umo.split(":")
        if len(parts) >= 3 and len(parts[2]) > 8:
            parts[2] = parts[2][:4] + "..." + parts[2][-4:]
        return ":".join(parts)

    @staticmethod
    def _mask_openid(openid: str) -> str:
        """openid 打码：保留前 6 后 4，中间省略。"""
        s = str(openid or "")
        if len(s) <= 12:
            return s
        return f"{s[:6]}...{s[-4:]}"

    @staticmethod
    def _mask_qq(qq: str) -> str:
        """QQ 号打码，用于可能被他人看到的提示文案。"""
        s = str(qq or "")
        if len(s) <= 5:
            return s
        if len(s) <= 8:
            return f"{s[:2]}****{s[-2:]}"
        return f"{s[:3]}****{s[-3:]}"

    @staticmethod
    def _fmt_time(ts: float) -> str:
        return time.strftime("%m-%d %H:%M", time.localtime(ts))

    async def _notify_official(self, new_umo: str, code: str) -> bool:
        """尽力向官方会话推送绑定成功通知（失败不影响已落盘的绑定）。"""
        try:
            text = (
                "【绑定成功通知】\n"
                f"身份码 {code} 已在非官方机器人处完成绑定。\n"
                "对应关系已保存，待管理员运行迁移脚本后，"
                "历史数据就会挂到本会话上。"
            )
            await self.context.send_message(new_umo, MessageChain().message(text))
            return True
        except Exception as e:  # noqa: BLE001 通知失败不影响绑定
            logger.warning(f"[official_bind] 通知官方会话失败（不影响绑定）: {e}")
            return False

    def _build_guide(self, code: str, scope: str, sender_name: str) -> str:
        """生成给用户粘贴到非官方 bot 的说明消息。"""
        scope_text = "群聊" if scope == "group" else "私聊"
        target = (
            "请在非官方机器人所在的群里发送本段消息"
            if scope == "group"
            else "请私聊非官方机器人发送本段消息"
        )
        return (
            "【官方机器人迁移绑定】\n"
            "说明：把历史数据（对话/记忆/好感度/会话配置）关联到官方机器人\n"
            f"名称：{sender_name}（{scope_text}）\n"
            f"身份码：{code}\n"
            f"操作：{target}\n"
            f"有效期：{self._expire_seconds() // 60} 分钟"
        )

    def _build_member_guide(self, code: str, sender_name: str, qq_group: str) -> str:
        """生成「群成员个人身份码」的说明消息。"""
        return (
            "【官方机器人个人身份绑定】\n"
            "说明：把你的个人身份（QQ 号 ↔ 官方 openid）登记下来，"
            "用于迁移按用户区分的记忆/好感度/签到等数据\n"
            f"名称：{sender_name}（群成员）\n"
            f"身份码：{code}\n"
            "操作：把本段消息发给非官方机器人（群聊或私聊均可）\n"
            f"有效期：{self._expire_seconds() // 60} 分钟\n"
            "说明：官方 openid 按用户固定、跨群通用，绑定一次即可，"
            "无需在每个群重复绑定"
        )

    # ------------------------------------------------------------------
    # 群映射（直接取内存绑定记录，绑定完成即刻生效，无需文件缓存）
    # ------------------------------------------------------------------

    def _group_map(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for rec in list(self._bound):
            if rec.get("scope") != "group":
                continue
            old, new = rec.get("old_umo", ""), rec.get("new_umo", "")
            if old and new and old != new:
                mapping[old] = self._resolve_official_umo(new)
        return mapping

    def _official_group_keys(self) -> set[str]:
        """官方会话的多形态标识（umo 全文 / 群 openid / 群号）。"""
        keys: set[str] = set()
        for rec in list(self._bound):
            if rec.get("scope") != "group":
                continue
            new = self._resolve_official_umo(rec.get("new_umo", ""))
            if not new:
                continue
            keys.add(new)
            keys.add(new.rsplit(":", 1)[-1])
            for k in ("group_openid", "qq_group_id"):
                if rec.get(k):
                    keys.add(str(rec[k]))
        return keys

    # ------------------------------------------------------------------
    # 官方平台识别 / 平台 id 迁移
    # ------------------------------------------------------------------

    def _live_official_platforms(self) -> list:
        """当前已实例化的官方平台（qq_official / qq_official_webhook）。"""
        names = self._official_type_names()
        out: list = []
        try:
            insts = list(self.context.platform_manager.platform_insts)
        except BaseException:  # noqa: BLE001 拿不到平台列表时退化为「无实例」
            return out
        for platform in insts:
            try:
                if platform.meta().name in names:
                    out.append(platform)
            except BaseException:  # noqa: BLE001,S112 单个平台异常不影响其余
                continue
        return out

    def _resolve_official_umo(self, umo: str) -> str:
        """把绑定记录里的官方 umo 对齐到当前实际运行的官方平台 id。

        同一套 appid 可以配成 webhook 或 websocket 两种适配器，用户在 WebUI
        里切换后平台 id 会变（default → QQBOT-websocket），旧的绑定记录就会
        指向一个不存在的平台，代发全部失败。这里按「同类型、只换 id」修正。
        """
        parts = str(umo or "").split(":", 2)
        if len(parts) != 3:
            return umo
        pid, msg_type, session_id = parts
        live = self._live_official_platforms()
        if not live:
            return umo
        for platform in live:
            try:
                if platform.meta().id == pid:
                    return umo  # 原 id 仍在运行，无需迁移
            except BaseException:  # noqa: BLE001,S112 单个平台异常不影响判断
                continue
        if len(live) == 1:
            try:
                return f"{live[0].meta().id}:{msg_type}:{session_id}"
            except BaseException:  # noqa: BLE001 取不到 id 就保持原样
                return umo
        # 多个官方实例同时在线时无法判断该用哪个，保持原样由就绪检查退回
        return umo

    def _migrate_platform_ids(self) -> int:
        """把已绑定记录迁移到当前官方平台 id，并落盘。返回迁移条数。"""
        changed = 0
        for rec in list(self._bound):
            old = str(rec.get("new_umo") or "")
            new = self._resolve_official_umo(old)
            if new and new != old:
                rec["new_umo"] = new
                rec["new_platform_id"] = new.split(":", 1)[0]
                changed += 1
        # 个人映射里的来源 umo 同样跟着平台 id 走
        for user in list(self._users):
            old = str(user.get("source_umo") or "")
            if not old:
                continue
            new = self._resolve_official_umo(old)
            if new and new != old:
                user["source_umo"] = new
                user["platform_id"] = new.split(":", 1)[0]
                changed += 1
        if changed:
            self._save()
            self._stats["migrated"] += changed
            logger.info(
                f"[official_bind] 官方平台 id 已变更，自动迁移 {changed} 条会话/个人绑定"
            )
        return changed

    # ------------------------------------------------------------------
    # 平台补丁 1：事件进入管线前拦截
    # ------------------------------------------------------------------

    def _install_patches(self) -> None:
        """给官方端 / 非官方端的平台实例挂上补丁（幂等）。"""
        try:
            insts = list(self.context.platform_manager.platform_insts)
        except BaseException:  # noqa: BLE001 平台尚未启动时忽略，等 on_platform_loaded
            return
        for platform in insts:
            try:
                name = platform.meta().name
            except BaseException:  # noqa: BLE001,S112 单个平台异常不影响其余
                continue
            if name in self._official_type_names() or name in self._onebot_names():
                self._install_commit_guard(platform)
                self._install_send_guard(platform)
                if name in self._official_type_names():
                    self._install_official_send_guard(platform)

    def _install_commit_guard(self, platform) -> bool:
        """包装 platform.commit_event，在事件进入管线之前做丢弃判断。

        WakingCheckStage 是管线的第一个阶段，而且它对每个 handler 单独做权限
        判断、遇到权限不足会立刻发一条「权限不足」并 return。只靠管线内的
        stop_event 拦不住这一步，必须在 commit 之前就把重复事件丢掉。
        """
        key = id(platform)
        rec = _COMMIT_PATCHES.get(key)
        if rec is not None:
            rec["plugin"] = self
            return False

        orig = getattr(platform, "commit_event", None)
        if orig is None or getattr(orig, "_official_bind_patched", False):
            return False

        def _commit(event, _orig=orig, _key=key):
            rec2 = _COMMIT_PATCHES.get(_key)
            plugin = rec2.get("plugin") if rec2 else None
            if plugin is not None:
                try:
                    if plugin._should_drop_event(event):
                        return
                except BaseException as e:  # noqa: BLE001 拦截失败必须放行
                    logger.debug(f"[official_bind] 事件拦截检查失败，已放行: {e}")
            _orig(event)

        _commit._official_bind_patched = True  # type: ignore[attr-defined]
        try:
            platform.commit_event = _commit
        except BaseException as e:  # noqa: BLE001 挂不上就退化为管线内去重
            logger.warning(f"[official_bind] 无法挂载事件拦截，将退回管线内去重: {e}")
            return False
        _COMMIT_PATCHES[key] = {"platform": platform, "orig": orig, "plugin": self}
        logger.info(
            f"[official_bind] 已在管线前挂载拦截：{platform.meta().id}"
            f"({platform.meta().name})"
        )
        return True

    def _should_drop_event(self, event: AstrMessageEvent) -> bool:
        """判断一条刚被适配器解析完的事件是否应当直接丢弃。

        新版核心（内容对比）：两端同群收到同一条用户消息时内容指纹相同，
        先到者认领进管线、后到者丢弃 —— 既去重，又天然实现「谁活着谁处理」：
        非官方掉线时只剩官方那份，官方自动接管。
        """
        try:
            group_id = str(event.get_group_id() or "")
        except BaseException:  # noqa: BLE001 取不到群号说明不是群消息
            return False
        if not group_id:
            return False  # 只管群聊，私聊不受影响

        name = event.get_platform_name()
        is_official = name in self._official_type_names()
        is_onebot = name in self._onebot_names()
        # [DEBUG-dedup] 临时探针：定位真实事件字段差异，定位后删除
        try:
            self._debug(
                f"[DEBUG-dedup] name={name} is_official={is_official} "
                f"is_onebot={is_onebot} gid={event.get_group_id() or ''!s} "
                f"sender={event.get_sender_id() or ''!s} "
                f"text={str(event.message_str or '')[:60]!r} "
                f"umo={event.unified_msg_origin or ''!s} "
                f"paired_of={self._is_paired_group(event) if is_official else '-'} "
                f"paired_ob={self._is_onebot_paired_group(event) if is_onebot else '-'} "
                f"fp={self._fingerprint(event)}"
            )
        except BaseException as e:  # noqa: BLE001 探针失败不影响主流程
            logger.debug(f"[DEBUG-dedup] 探针异常: {e}")

        # 官方机器人收到群消息：INFO 记录用户/id 与内容
        if is_official:
            try:
                _name = event.get_sender_name() or ""
            except BaseException:  # noqa: BLE001 取不到昵称时用 id 兜底
                _name = ""
            _uid = str(event.get_sender_id() or "")
            _text = str(getattr(event, "message_str", "") or "")[:80]
            logger.info(
                f"[official_bind] 官方机器人收到群消息 "
                f"群{event.get_group_id() or ''} 用户{_name}/{_uid}: {_text}"
            )

        # 身份学习 / 回环防护：任何情况下都不能拿官方 bot 的 OneBot 账号
        # 去绑定用户，也不能让它的代发回执被当新消息处理。
        if is_onebot:
            self._learn_official_sender(event)
            try:
                sender_id = str(event.get_sender_id() or "")
            except BaseException:  # noqa: BLE001 取不到 id 时放行
                sender_id = ""
            if sender_id and sender_id in self._ignore_sender_ids():
                self._stats["echo_guarded"] += 1
                logger.debug(
                    "[official_bind] 管线前丢弃官方 bot 回环消息 "
                    f"{sender_id} {event.unified_msg_origin}"
                )
                return True

        # 官方端群消息一律先记 msg_id：被动回复凭据与绑定/桥接开关都无关。
        if is_official:
            self._remember_official_msg_id(event)

        if not self._active or not self._forward_enabled():
            return False

        # 只有「双 bot 已绑定群」才参与内容对比；未绑定群零干预。
        paired = (
            self._is_paired_group(event)
            if is_official
            else self._is_onebot_paired_group(event)
            if is_onebot
            else False
        )
        if not paired:
            return False

        # 非官方端：自身消息回灌（report-self-message）直接丢弃。
        if is_onebot and self._drop_self_messages():
            try:
                if str(event.get_sender_id() or "") == str(event.get_self_id() or ""):
                    self._stats["self_dropped"] += 1
                    logger.debug(
                        f"[official_bind] 管线前丢弃自身消息回环 {event.unified_msg_origin}"
                    )
                    return True
            except BaseException as e:  # noqa: BLE001 取不到 id 就交给内容对比
                logger.debug(f"[official_bind] 自收过滤跳过（取不到 id）: {e}")

        # 官方专属指令（如 /数据迁入）：只在官方端执行，非官方端交出。
        if self._is_official_only_command(event):
            if is_onebot:
                self._stats["handed_over"] += 1
                return True
            return False

        # @官方机器人的消息：非官方端主动让位，交给官方端独占处理，
        # 保证官方 bot 自己能回复（否则本地 NapCat 永远先到，官方端抢不到）。
        if is_onebot and self._onebot_at_official(event):
            self._stats["at_official_handed"] = self._stats.get("at_official_handed", 0) + 1
            self._debug(f"[official_bind] 非官方端让位 @官方机器人消息 {event.unified_msg_origin}")
            return True

        if not self._dedup_enabled():
            return False

        # 内容对比：先到先得。同群、同发送者、同文本在窗口内只处理一次。
        fp = self._fingerprint(event)
        if fp is None:
            return False
        if not self._claim(fp):
            self._stats["cross_deduped"] += 1
            hint = "官方端已认领" if is_onebot else "非官方端已认领"
            logger.debug(f"[official_bind] 内容对比去重（{hint}）{fp}")
            return True

        # 认领成功：打标记，管线内兜底过滤器据此识别「本端已认领」。
        event.set_extra("official_bind_claimed", True)
        # 记录官方群 msg_id + 时间，供「5分钟内被动回复」使用
        # 官方端：@消息由官方自行处理，普通消息视为接管。
        if is_official:
            if self._is_at_official(event):
                self._stats["at_official_handled"] += 1
                logger.info(f"[official_bind] @官方机器人，由官方端自行处理 {fp}")
            elif self._enable_official_fallback():
                self._stats["official_took_over"] += 1
                logger.info(f"[official_bind] 非官方端未认领，官方端接管入口 {fp}")
        return False

    # ------------------------------------------------------------------
    # 平台补丁 2：协议端直发兜底
    # ------------------------------------------------------------------

    def _install_official_send_guard(self, platform) -> bool:
        """包装官方平台的 send_by_session：让群消息优先走 msg_id 被动回复。

        gscore_adapter 等插件会直接 context.send_message(官方umo, ...)，绕过
        本插件的 _send_official_with_priority。而官方适配器对群消息的
        _allow_group_proactive_send 恒为 True，等于永远走主动消息——一旦
        「主动消息无权限」就整条发不出去。这里在出口统一兜一层：该群有
        5 分钟内的 msg_id 就临时关掉主动开关，强制走被动回复。
        """
        key = id(platform)
        if key in _OFFICIAL_SEND_PATCHES:
            _OFFICIAL_SEND_PATCHES[key]["plugin"] = self
            return False
        orig = getattr(platform, "send_by_session", None)
        if orig is None or getattr(orig, "_official_bind_patched", False):
            return False

        async def _send_by_session(session, message_chain, _orig=orig, _key=key):
            rec2 = _OFFICIAL_SEND_PATCHES.get(_key)
            plugin = rec2.get("plugin") if rec2 else None
            if plugin is not None and plugin._forward_enabled():
                try:
                    sid = str(getattr(session, "session_id", "") or "").rsplit("_", 1)[-1]
                    seen = plugin._official_msg_id_seen.get(sid)
                    fresh = bool(
                        seen and time.time() - seen[1] <= plugin._passive_reply_window()
                    )
                    # message_type 是 MessageType 枚举（不是字符串），统一取 .value 比较
                    _mtype = getattr(session, "message_type", "")
                    _mtype = getattr(_mtype, "value", _mtype)
                    logger.info("[GUARD] sid=" + str(sid) + " fresh=" + str(fresh) + " mtype=" + str(_mtype) + " seen=" + str(bool(seen)))
                    if fresh and _mtype == "GroupMessage":
                        last_ids = getattr(platform, "_session_last_message_id", None)
                        old_ids = dict(last_ids) if isinstance(last_ids, dict) else {}
                        old_proactive = getattr(
                            platform, "_allow_group_proactive_send", True
                        )
                        try:
                            if isinstance(last_ids, dict):
                                last_ids[sid] = seen[0]
                            platform._allow_group_proactive_send = False
                            return await _orig(session, message_chain)
                        finally:
                            platform._allow_group_proactive_send = old_proactive
                            if isinstance(last_ids, dict):
                                if sid in old_ids:
                                    last_ids[sid] = old_ids[sid]
                                else:
                                    last_ids.pop(sid, None)
                except BaseException as e:  # noqa: BLE001 守卫失败必须放行
                    logger.debug(f"[official_bind] 官方被动守卫异常，按原路径发送: {e}")
            return await _orig(session, message_chain)

        try:
            _send_by_session._official_bind_patched = True  # type: ignore[attr-defined]
            platform.send_by_session = _send_by_session
        except BaseException as e:  # noqa: BLE001 挂不上就退化为原行为
            logger.warning(f"[official_bind] 无法挂载官方被动守卫: {e}")
            return False
        _OFFICIAL_SEND_PATCHES[key] = {
            "platform": platform,
            "orig": orig,
            "wrapper": _send_by_session,
            "plugin": self,
        }
        logger.info(
            f"[official_bind] 已在官方平台挂载被动回复守卫：{platform.meta().id}"
        )
        return True

    def _install_send_guard(self, platform) -> bool:
        """包装 aiocqhttp 客户端的 send_group_msg。

        gscore_adapter 这类插件为了拿 message_id 会绕过 AstrBot 的发送管线，
        直接 `bot.send_group_msg(...)`，on_decorating_result 钩子看不到它们。
        这里在协议端出口再兜一层：双 bot 群里一律改由官方 bot 代发，失败退回。
        """
        try:
            if platform.meta().name != "aiocqhttp":
                return False
            bot = platform.get_client()
        except BaseException:  # noqa: BLE001 平台还没连上
            return False
        if bot is None:
            return False

        key = id(bot)
        rec = _SEND_PATCHES.get(key)
        if rec is not None:
            rec["plugin"] = self
            return False

        orig = getattr(bot, "send_group_msg", None)
        if orig is None:
            return False

        async def _send_group_msg(*args, **kwargs):
            rec2 = _SEND_PATCHES.get(key)
            plugin = rec2.get("plugin") if rec2 else None
            if plugin is not None:
                try:
                    handled = await plugin._try_forward_direct_group_send(
                        kwargs, args
                    )
                except BaseException as e:  # noqa: BLE001 兜底失败必须退回原路径
                    logger.warning(
                        f"[official_bind] 直发兜底异常，退回原路径: {e}"
                    )
                    handled = None
                if handled is not None:
                    return handled
            # 退回时走 bot 的“当前”属性：后加载的插件（如 send_blocker）可能包在我们
            # 外层，直接调闭包里的 orig 会绕过它的拦截。
            current = getattr(bot, "send_group_msg", None)
            if current is not None and current is not _send_group_msg:
                return await current(*args, **kwargs)
            return await orig(*args, **kwargs)

        try:
            bot.send_group_msg = _send_group_msg
        except BaseException as e:  # noqa: BLE001 挂不上就退化为只管管线内回复
            logger.warning(f"[official_bind] 无法挂载直发兜底: {e}")
            return False
        _SEND_PATCHES[key] = {
            "bot": bot,
            "orig": orig,
            "wrapper": _send_group_msg,
            "plugin": self,
        }
        logger.info("[official_bind] 已在协议端挂载直发兜底")
        return True

    async def _try_forward_direct_group_send(self, kwargs, args):
        """拦截一次直发；成功代发返回伪回执，退回则返回 None（调用原方法）。"""
        if not self._forward_enabled() or not self._intercept_direct_sends():
            return None
        if _FORWARD_GUARD.get():
            return None  # 正在代发中，避免自己转发给自己

        group_id = kwargs.get("group_id")
        if group_id is None and args:
            group_id = args[0]
        group_id = str(group_id or "")
        if not group_id.isdigit():
            return None

        new_umo = self._group_umo_by_qq_group(group_id)
        if not new_umo:
            return None

        message = kwargs.get("message")
        if message is None and len(args) > 1:
            message = args[1]
        chain = _onebot_to_chain(message)
        if chain is None:
            return None

        plain = chain.get_plain_text()
        norm = self._norm_text(plain)
        if norm and self._was_recently_forwarded(group_id, norm):
            self._stats["direct_deduped"] += 1
            return {"status": "ok", "retcode": 0}

        if not self._official_ready(new_umo):
            self._note_skip(
                "官方端未就绪（直发兜底）", f"QQ-onebot-1:GroupMessage:{group_id}"
            )
            return None

        out = self._convert_chain(chain.chain)
        if out is None:
            return None

        token = _FORWARD_GUARD.set(True)
        try:
            ok = await self._send_official_with_priority(new_umo, out)
        except BaseException as e:  # noqa: BLE001 代发异常必须退回原路径
            self._stats["direct_fallback"] += 1
            logger.warning(f"[official_bind] 直发兜底转发失败，退回原路径: {e}")
            return None
        finally:
            _FORWARD_GUARD.reset(token)

        if not ok:
            self._stats["direct_fallback"] += 1
            return None

        self._record_forwarded_text(group_id, plain)
        self._record_echo(f"aiocqhttp:GroupMessage:{group_id}", plain)
        self._stats["direct_forwarded"] += 1
        logger.info(
            f"[official_bind] 群 {group_id} 的直发消息已由官方 bot 代发 -> {new_umo}"
        )
        # 不带 message_id：上层拿不到可撤回 id，也就不会去撤回官方 bot 的消息
        return {"status": "ok", "retcode": 0}

    def _group_umo_by_qq_group(self, group_id: str) -> str | None:
        group_id = str(group_id or "")
        for old, new in self._group_map().items():
            if old.rsplit(":", 1)[-1] == group_id:
                return new
        return None

    # ------------------------------------------------------------------
    # 代发文本指纹
    # ------------------------------------------------------------------

    def _record_forwarded_text(self, group_id: str, text: str) -> None:
        norm = self._norm_text(text)
        if norm:
            self._recent_forwards.append((str(group_id or ""), norm, time.time()))

    def _was_recently_forwarded(
        self, group_id: str, norm: str, window: float = 3.0
    ) -> bool:
        """同群同文本是否刚刚已经代发过（管线钩子与直发兜底之间去重）。"""
        if not norm:
            return False
        now = time.time()
        for gid, text_norm, ts in list(self._recent_forwards):
            if now - ts > window:
                continue
            if gid == str(group_id or "") and text_norm == norm:
                return True
        return False

    # ------------------------------------------------------------------
    # 接收去重 / 回环防护
    # ------------------------------------------------------------------

    def _is_paired_group(self, event: AstrMessageEvent) -> bool:
        """官方端这条群消息是否来自「双 bot 同群」。"""
        keys = self._official_group_keys()
        if not keys:
            return False
        umo = event.unified_msg_origin
        gid = str(event.get_group_id() or "")
        return umo in keys or (bool(gid) and gid in keys)

    def _is_onebot_paired_group(self, event: AstrMessageEvent) -> bool:
        """非官方端这条群消息是否来自「双 bot 同群」。"""
        mapping = self._group_map()
        if not mapping:
            return False
        if event.unified_msg_origin in mapping:
            return True
        gid = str(event.get_group_id() or "")
        return any(old.rsplit(":", 1)[-1] == gid for old in mapping)

    def _is_official_only_command(self, event: AstrMessageEvent) -> bool:
        """是否为「只由官方端执行」的指令。

        QQ 官方群必须先 @ 机器人，不同适配器给出的 message_str 可能带
        `[At:xxx]` 前缀，这里先剥掉再比较，避免官方专属指令被误丢弃。
        """
        raw = str(event.message_str or "").strip()
        raw = re.sub(r"^(?:\[[^\]]{0,80}\]\s*)+", "", raw)
        raw = raw.lstrip("/").strip()
        if not raw:
            return False
        return any(raw.startswith(cmd) for cmd in self._official_only_commands())

    @staticmethod
    def _norm_text(text: str) -> str:
        """规范化文本用于内容对比：去空白，只取前 200 字。"""
        return re.sub(r"\s+", "", str(text or ""))[:200]

    def _canonical_group(self, event: AstrMessageEvent) -> str:
        """把两端的群标识归一到同一个群号，才能跨端对比内容。

        官方端 group_id 是 group_openid，非官方端是 QQ 群号。绑定记录里
        qq_group_id 与 group_openid 一一对应，用 gid 反查即可，绝不能依赖
        umo 前缀（webhook/websocket 互切会导致平台 id 段变化）。
        """
        gid = str(event.get_group_id() or "")
        for rec in list(self._bound):
            if rec.get("scope") != "group":
                continue
            if gid and gid == str(rec.get("group_openid") or ""):
                return str(rec.get("qq_group_id") or gid)
            if gid and gid == str(rec.get("qq_group_id") or ""):
                return gid
        return gid

    def _unified_sender(self, event: AstrMessageEvent) -> str:
        """把两端的发送者归一到同一个标识（官方 openid）。

        官方端 sender_id 已是 openid；非官方端是 QQ 号，靠 users 表反查。
        归不了就退回原 sender_id，内容对比仍然可用（只是精度略低）。
        """
        sid = str(event.get_sender_id() or "")
        if event.get_platform_name() not in self._onebot_names():
            return sid
        for u in list(self._users):
            if str(u.get("qq_user_id") or "") == sid:
                return str(u.get("user_openid") or sid)
        return sid

    def _fingerprint(self, event: AstrMessageEvent) -> tuple[str, str, str] | None:
        """跨端消息指纹：(统一群号, 统一发送者, 规范化文本)。"""
        if not event.get_group_id():
            return None
        norm = self._norm_text(event.message_str)
        if not norm:
            return None
        return (self._canonical_group(event), self._unified_sender(event), norm)

    def _claim(self, fp: tuple[str, str, str]) -> bool:
        """认领一条消息：窗口内首次认领返回 True，重复返回 False。"""
        now = time.time()
        window = self._message_dedup_window()
        for k, ts in list(self._dedup_seen.items()):
            if now - ts > window:
                self._dedup_seen.pop(k, None)
        prev = self._dedup_seen.get(fp)
        delta = (now - prev) if prev is not None else -1.0
        dup = prev is not None and delta <= window
        self._debug(
            f"[DEBUG-claim] fp={fp} window={window} prev={prev} "
            f"delta={delta:.3f} dup={dup} seen={len(self._dedup_seen)}"
        )
        if dup:
            return False
        self._dedup_seen[fp] = now
        return True

    def _claimed_by_other(self, fp: tuple[str, str, str], since: float) -> bool:
        """复查：在 since 之后是否有另一端认领了同一指纹。"""
        ts = self._dedup_seen.get(fp)
        return ts is not None and ts > since

    @staticmethod
    def _is_at_official(event: AstrMessageEvent) -> bool:
        """官方端消息是否在 @机器人（解析器会在链首放 At）。"""
        msg_obj = getattr(event, "message_obj", None)
        comps = getattr(msg_obj, "message", None) or []
        return bool(comps) and isinstance(comps[0], At)

    def _onebot_at_official(self, event: AstrMessageEvent) -> bool:
        """非官方端这条消息是否在 @官方机器人。

        官方 bot 在非官方侧以 QQ 号/UIN 呈现，存于 ignore_sender_ids
        （配置 + 内置 + 自动学习）。命中其中任一 At.qq 即视为 @官方 bot。
        """
        if event.get_platform_name() not in self._onebot_names():
            return False
        ignored = self._ignore_sender_ids()
        if not ignored:
            return False
        msg_obj = getattr(event, "message_obj", None)
        comps = getattr(msg_obj, "message", None) or []
        for comp in comps:
            if isinstance(comp, At):
                qq = str(getattr(comp, "qq", "") or "").strip()
                # 'all' / 全体成员不算 @官方机器人
                if qq and qq.lower() != "all" and qq in ignored:
                    return True
        return False

    def _debug(self, text: str) -> None:
        """调试日志：默认 debug 级别不刷屏，开启 verbose_debug 时升为 warning。"""
        if bool(self.config.get("verbose_debug", False)):
            logger.warning(text)
        else:
            logger.debug(text)

    def _record_echo(self, umo: str, text: str) -> None:
        """记录官方端代发出去的消息指纹，用于拦截本端再次收到时的回环。"""
        norm = self._norm_text(text)
        if norm and self._echo_seconds() > 0:
            self._echo_records.append((umo, norm, time.time()))

    def _is_echo(self, event: AstrMessageEvent) -> bool:
        """判断非官方端收到的群消息是否是「官方 bot 刚代发的那条」的回声。"""
        seconds = self._echo_seconds()
        sender_id = str(event.get_sender_id() or "")
        if sender_id in self._ignore_sender_ids():
            return True
        # 自身消息（NapCat 开了 report-self-message 时会把它回灌进来）
        try:
            self_id = str(event.get_self_id() or "")
        except BaseException:  # noqa: BLE001 取不到 self_id 就只靠文本指纹
            self_id = ""
        if sender_id and self_id and sender_id == self_id:
            return True
        if seconds <= 0 or not self._echo_records:
            return False

        now = time.time()
        norm = self._norm_text(event.message_str)
        if not norm:
            return False
        gid = str(event.get_group_id() or "")

        for umo, sent_norm, ts in list(self._echo_records):
            if now - ts > seconds:
                continue
            # 同一群（umo 末段为群号）才认定回声
            if gid and not umo.endswith(gid):
                continue
            if norm == sent_norm or (
                len(norm) >= 8 and (norm in sent_norm or sent_norm in norm)
            ):
                return True
        return False

    @filter.custom_filter(_DedupFilter, priority=DEDUP_PRIORITY)
    async def dedup_group_message(self, event: AstrMessageEvent):
        """最早期拦截：重复接收的官方端群消息 / 官方端代发消息的回声。

        stop_event() 会让调度器中断后续所有阶段（其余插件 handler、LLM、
        结果装饰、发送），因此这份重复事件完全静默，不会产生第二条回复。
        """
        # 打显式标记：发送前钩子据此跳过，不要依赖 is_stopped()（会被业务插件污染）
        event.set_extra("official_bind_deduped", True)
        if event.get_platform_name() in self._official_type_names():
            self._stats["deduped"] += 1
            logger.debug(
                f"[official_bind] 去重：丢弃官方端重复群消息 {event.unified_msg_origin}"
            )
        elif self._is_official_only_command(event):
            self._stats["handed_over"] += 1
            logger.debug(
                f"[official_bind] 去重：{event.unified_msg_origin} 的官方专属指令交给官方端执行"
            )
        else:
            self._stats["echo_guarded"] += 1
            logger.debug(
                f"[official_bind] 回环防护：忽略官方 bot 代发消息的回声 {event.unified_msg_origin}"
            )
        event.stop_event()

    # ------------------------------------------------------------------
    # 官方端就绪检查 / 消息链转换（回复转发）
    # ------------------------------------------------------------------

    def _note_skip(self, reason: str, umo: str = "") -> None:
        """记录一次「未转发」的原因：计入统计、留在 /转发状态、按 info 打日志。

        非官方 bot 的回复没能改由官方 bot 发出时，原因必须可见，
        否则只能看到「消息从非官方渠道发出」却不知道卡在哪一步。
        """
        self._stats["skipped"] += 1
        self._last_skip = f"{reason}（{umo}）" if umo else reason
        self._last_skip_at = time.time()
        logger.info(f"[official_bind] 未转发：{self._last_skip}")

    def _remember_official_msg_id(self, event: AstrMessageEvent) -> None:
        """记下官方群最近一条消息的 msg_id 与时间（被动回复凭据）。"""
        try:
            sid = str(event.get_session_id() or "").rsplit("_", 1)[-1]
            msg_id = str(
                getattr(getattr(event, "message_obj", None), "message_id", "") or ""
            )
            if sid and msg_id:
                self._official_msg_id_seen[sid] = (msg_id, time.time())
                logger.info("[REC] sid=" + str(sid) + " msg_id=" + str(msg_id)[:24])
        except BaseException as e:  # noqa: BLE001 记录失败不影响主流程
            logger.debug(f"[official_bind] 记录官方 msg_id 失败: {e}")

    def _official_platform_by_id(self, platform_id: str):
        try:
            for platform in self.context.platform_manager.platform_insts:
                if platform.meta().id == platform_id:
                    return platform
        except BaseException:  # noqa: BLE001 拿不到平台列表
            return None
        return None

    async def _send_official_with_priority(
        self, new_umo: str, chain: MessageChain
    ) -> bool:
        """按「被动(msg_id, 5分钟内) -> 主动」优先级发送官方消息。

        复用 AstrBot 的 send_by_session（含完整消息链转换），只在发送前
        「填 msg_id 缓存 + 关主动开关」让它走被动回复；失败或过期则恢复
        主动开关重发。返回是否发送成功。
        """
        from astrbot.core.platform.astr_message_event import MessageSesion

        session = MessageSesion.from_str(new_umo)
        sid = session.session_id.rsplit("_", 1)[-1]
        platform = self._official_platform_by_id(session.platform_name)
        if platform is None:
            return False

        rec = self._official_msg_id_seen.get(sid)
        msg_id = rec[0] if rec else ""
        fresh = bool(rec and time.time() - rec[1] <= self._passive_reply_window())

        # ① 被动回复：窗口内有 msg_id，填缓存 + 关主动开关
        if fresh and msg_id:
            try:
                last_ids = getattr(platform, "_session_last_message_id", None)
                old_ids = dict(last_ids) if isinstance(last_ids, dict) else {}
                old_proactive = getattr(platform, "_allow_group_proactive_send", True)
                if isinstance(last_ids, dict):
                    last_ids[sid] = msg_id
                platform._allow_group_proactive_send = False
                try:
                    ok = await self.context.send_message(new_umo, chain)
                    if ok:
                        self._stats["passive_replied"] = (
                            self._stats.get("passive_replied", 0) + 1
                        )
                        return True
                finally:
                    platform._allow_group_proactive_send = old_proactive
                    # 只恢复本群这一项，绝不 clear 整个字典（其他群各有自己的 msg_id）
                    if isinstance(last_ids, dict):
                        if sid in old_ids:
                            last_ids[sid] = old_ids[sid]
                        else:
                            last_ids.pop(sid, None)
            except BaseException as e:  # noqa: BLE001 被动失败转主动
                logger.info(f"[official_bind] 被动回复失败，转主动消息: {e}")

        # ② 主动消息：恢复默认开关直接发
        ok = await self.context.send_message(new_umo, chain)
        if ok:
            self._stats["proactive_sent"] = self._stats.get("proactive_sent", 0) + 1
        return ok

    def _official_ready(self, new_umo: str) -> bool:
        """检查官方适配器是否具备该群的推送条件。

        qq_official 的 send_by_session 在「无 msg_id 缓存且无群 scene 缓存」
        时会静默跳过发送；若此时贸然吞掉原回复会造成消息丢失，因此
        官方端尚未就绪时直接退回原路径。官方 bot 被 @ 过一次即就绪。
        """
        try:
            from astrbot.core.platform.astr_message_event import MessageSesion
        except ImportError:
            return False

        try:
            session = MessageSesion.from_str(new_umo)
        except BaseException:  # noqa: BLE001 解析失败一律视为未就绪，退回原路径
            return False

        # qqofficial 适配器按 rsplit("_", 1) 归一化群 session_id
        sid = session.session_id.rsplit("_", 1)[-1]

        try:
            insts = self.context.platform_manager.platform_insts
        except BaseException:  # noqa: BLE001 拿不到平台实例视为未就绪，退回原路径
            return False

        for platform in insts:
            try:
                if platform.meta().id != session.platform_name:
                    continue
                scenes = getattr(platform, "_session_scene", None) or {}
                last_ids = getattr(platform, "_session_last_message_id", None) or {}
                return scenes.get(sid) == "group" or bool(last_ids.get(sid))
            except BaseException:  # noqa: BLE001,S112 单个平台异常不应中断检查
                continue
        return False

    def _convert_chain(self, chain: list) -> MessageChain | None:
        """把 aiocqhttp 消息链转成官方平台可发送的形式。

        - Plain（非空白）/ Image / Record 保留
        - Node（合并转发节点）递归展开其内部内容
        - At / Reply 及其他组件丢弃（官方平台无法呈现 QQ 号 @ 与引用）
        - 转换后无任何可发内容时返回 None，表示应退回原路径
        """
        out: list = []

        def walk(items):
            for comp in items or []:
                if isinstance(comp, Plain):
                    if comp.text and comp.text.strip():
                        out.append(Plain(comp.text))
                elif isinstance(comp, (Image, Record)):
                    out.append(comp)
                elif isinstance(comp, Node):
                    walk(comp.content)
                elif isinstance(comp, (At, Reply)):
                    pass  # 官方平台无法呈现 QQ 号 @ 与引用，显式丢弃
                # 其余组件同样默认丢弃

        walk(chain)
        if not out:
            return None
        return MessageChain(chain=out)

    # ------------------------------------------------------------------
    # 指令：生成身份码（在官方 bot 上使用）
    # ------------------------------------------------------------------

    @filter.command("数据迁入")
    async def gen_bind_code(self, event: AstrMessageEvent):
        """生成迁移身份码：群首次 = 群会话码；群已绑定 = 群成员个人码。"""
        if event.get_platform_name() not in self._official_type_names():
            yield event.plain_result(
                "身份码只能在官方机器人上生成。\n"
                "请在官方机器人群/私聊中发送 /数据迁入 ，"
                "拿到身份码后再把说明消息发给我。"
            )
            return

        if self._admin_only_bind() and not event.is_admin():
            yield event.plain_result("本插件已开启「仅管理员可生成身份码」，请联系管理员。")
            return

        async with self._lock:
            self._cleanup_expired()
            new_umo = event.unified_msg_origin
            in_group = bool(event.get_group_id())
            scope = "group" if in_group else "private"
            sender_id = str(event.get_sender_id() or "")
            sender_name = event.get_sender_name() or sender_id
            existing = self._find_bound_by_new_umo(new_umo)

            if existing and in_group and self._member_bind_enabled():
                # 群会话已经绑定过，再发就是为「当前成员」登记个人 id
                kind = "member"
                code = self._make_code(new_umo, seed=f"member|{sender_id}")
                now = time.time()
                self._pending[code] = {
                    "kind": "member",
                    "new_umo": new_umo,
                    "scope": scope,
                    "new_platform_id": event.get_platform_id(),
                    "new_session_id": event.get_session_id(),
                    # 官方端群消息的 sender_id 就是该成员的 openid（用户级，跨群固定）
                    "sender_id": sender_id,
                    "sender_name": sender_name,
                    "group_openid": event.get_group_id() or "",
                    # 仅记录发码来源群用于展示；openid 跨群通用，粘码不再限定同群
                    "expect_qq_group": str(existing.get("qq_group_id") or ""),
                    "created_at": now,
                    "expires_at": now + self._expire_seconds(),
                }
                self._save()
                reply = self._build_member_guide(
                    code, sender_name, str(existing.get("qq_group_id") or "")
                )
            elif existing:
                yield event.plain_result(
                    "本会话已完成绑定，无需重复操作。\n"
                    f"绑定时间：{self._fmt_time(existing.get('bound_at', 0))}\n"
                    "如需更换绑定，请管理员先发送 /解绑 。"
                )
                return
            else:
                kind = "session"
                code = self._make_code(new_umo)
                now = time.time()
                self._pending[code] = {
                    "kind": "session",
                    "new_umo": new_umo,
                    "scope": scope,
                    "new_platform_id": event.get_platform_id(),
                    "new_session_id": event.get_session_id(),
                    "sender_id": sender_id,
                    "sender_name": sender_name,
                    # 群聊：官方群 openid；私聊为空
                    "group_openid": event.get_group_id() or "",
                    "created_at": now,
                    "expires_at": now + self._expire_seconds(),
                }
                self._save()
                reply = self._build_guide(code, scope, sender_name)

        logger.info(f"[official_bind] 生成身份码 {code}（{kind}） <- {new_umo}")
        yield event.plain_result(reply)

    # ------------------------------------------------------------------
    # 监听：非官方端接收身份码完成绑定
    # ------------------------------------------------------------------

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_onebot_message(self, event: AstrMessageEvent):
        """识别粘贴到非官方机器人的身份码消息并完成绑定，写入 id_mapping.json"""
        if event.get_platform_name() not in self._onebot_names():
            return
        # 双保险：管线前拦截没挂上时，也绝不让官方 bot 自己发的身份码回环
        # 被当成用户粘贴，写出「QQ=官方 bot UIN」的错误绑定。
        self._learn_official_sender(event)
        try:
            sender_id = str(event.get_sender_id() or "")
        except BaseException:  # noqa: BLE001
            sender_id = ""
        if sender_id and sender_id in self._ignore_sender_ids():
            logger.debug(
                f"[official_bind] 忽略官方 bot 的身份码回环，sender={sender_id}"
            )
            return

        text = (event.get_message_str() or "").strip()
        if not text:
            return
        if not any(kw in text for kw in BIND_KEYWORDS):
            return
        codes = CODE_RE.findall(text)
        if not codes:
            return

        # 绑定流程自身的回执不参与「回复转发」（官方会话另有 _notify_official 通知）
        event.set_extra("bind_reply_skip_forward", True)

        code = codes[0].lower()
        old_umo = event.unified_msg_origin

        async with self._lock:
            self._cleanup_expired()

            rec = self._pending.get(code)
            used_user = self._user_by_code(code)
            # 同一个人重新发码刷新 openid：码没变且 openid 一致即允许覆盖更新。
            # openid 跨群稳定，不要求贴码群与发码群相同。
            refresh = bool(
                used_user
                and rec
                and rec.get("kind") == "member"
                and str(used_user.get("user_openid", ""))
                == str(rec.get("sender_id", ""))
            )

            # 兜底0：码已被「个人绑定」用过（且不是本人刷新）
            if used_user and not refresh:
                where = (
                    f"群 {used_user.get('source_group_qq')}"
                    if used_user.get("openid_scope") == "group"
                    else "私聊"
                )
                yield event.plain_result(
                    "该身份码已完成个人绑定：\n"
                    f"QQ {self._mask_qq(used_user.get('qq_user_id', ''))} <-> "
                    f"{self._mask_openid(used_user.get('user_openid', ''))}"
                    f"（采集自官方{where}）\n"
                    f"绑定时间：{self._fmt_time(used_user.get('updated_at', 0))}\n"
                    "如需更换，请管理员发送 /解绑 后重新操作。"
                )
                event.stop_event()
                return

            # 兜底1：码已用过（会话绑定）
            used = self._find_bound_by_code(code)
            if used:
                yield event.plain_result(
                    "该身份码已完成绑定：\n"
                    f"目标：{used.get('new_umo')}\n"
                    f"绑定时间：{self._fmt_time(used.get('bound_at', 0))}\n"
                    "如需更换，请管理员发送 /解绑 后重新操作。"
                )
                event.stop_event()
                return

            # 兜底2：码不存在或已过期
            if not rec:
                yield event.plain_result(
                    "身份码无效或已过期。\n"
                    "请到官方机器人重新发送 /数据迁入 获取新码后，再粘贴到这里。"
                )
                event.stop_event()
                return

            # 兜底3：群码/私聊码发错了地方
            if rec["scope"] == "group" and not event.get_group_id():
                yield event.plain_result(
                    "这是【群聊】身份码，需要在非官方机器人所在的群里发送，"
                    "私聊发送不会生效。"
                )
                event.stop_event()
                return
            if rec["scope"] == "private" and event.get_group_id():
                yield event.plain_result(
                    "这是【个人】身份码，需要私聊非官方机器人发送，"
                    "在群里发送不会生效。"
                )
                event.stop_event()
                return

            # ---- 分支 A：群成员个人码 ----
            if rec.get("kind") == "member":
                gid = str(event.get_group_id() or "")
                qq = str(event.get_sender_id() or "")
                if not qq:
                    yield event.plain_result("拿不到你的 QQ 号，无法完成个人绑定。")
                    event.stop_event()
                    return

                # openid 用户级跨群稳定：群聊/私聊、哪个群贴码都能绑到同一条记录。
                # expect_qq_group 只是发码时记的来源群，不再用于拦截。
                self._pending.pop(code, None)
                user, created = self._upsert_user(
                    code=code,
                    qq_user_id=qq,
                    user_openid=str(rec.get("sender_id") or ""),
                    openid_scope=str(rec.get("scope") or "group"),
                    source_group_qq=str(rec.get("expect_qq_group") or gid),
                    source_group_openid=str(rec.get("group_openid") or ""),
                    source_umo=str(rec.get("new_umo") or ""),
                    old_umo=old_umo,
                    platform_id=str(rec.get("new_platform_id") or ""),
                    sender_name=event.get_sender_name() or "",
                )
                self._save()
                self._stats["member_bound" if created else "member_updated"] += 1

                verb = "个人绑定成功！" if created else "个人绑定已更新！"
                where = "群" if user.get("openid_scope") == "group" else "私聊"
                yield event.plain_result(
                    f"{verb}\n"
                    f"QQ {user.get('qq_user_id')} <-> "
                    f"{self._mask_openid(user.get('user_openid', ''))}\n"
                    f"采集来源：官方机器人{where}（来源群 {user.get('source_group_qq') or '—'}）\n"
                    "已写入 id_mapping.json 的 users 表。\n"
                    "openid 按用户固定、跨群通用，已可用于全部群的个人数据迁移。"
                )
                event.stop_event()
                return

            # 兜底4：该非官方会话已绑定过其他官方来源（仅会话级码需要）
            for old in self._bound:
                if old.get("old_umo") == old_umo:
                    self._pending.pop(code, None)
                    self._save()
                    yield event.plain_result(
                        "本会话已绑定过其他官方来源：\n"
                        f"当前绑定：{old.get('new_umo')}\n"
                        "如需更换，请管理员发送 /解绑 后重新操作。"
                    )
                    event.stop_event()
                    return

            # 完成绑定
            self._pending.pop(code, None)
            record = {
                "code": code,
                "scope": rec["scope"],
                "old_umo": old_umo,
                "new_umo": rec["new_umo"],
                "old_platform_id": event.get_platform_id(),
                "new_platform_id": rec.get("new_platform_id", ""),
                "old_session_id": event.get_session_id(),
                "new_session_id": rec.get("new_session_id", ""),
                # 显式的 id 对应关系（群聊：群号 <-> 群 openid；私聊：QQ 号 <-> openid）
                "qq_group_id": event.get_group_id() or "",
                "group_openid": rec.get("group_openid", ""),
                "qq_user_id": event.get_sender_id() or "",
                "user_openid": rec.get("sender_id", ""),
                "bound_at": time.time(),
                "bound_by_qq": event.get_sender_id(),
                "bound_by_name": event.get_sender_name() or "",
            }
            self._bound.append(record)
            # 会话绑定顺手登记发起人的个人映射：users 是用户级数据迁移的唯一来源
            self._upsert_user(
                code=code,
                qq_user_id=record["qq_user_id"],
                user_openid=record["user_openid"],
                openid_scope="group" if record["scope"] == "group" else "private",
                source_group_qq=record["qq_group_id"],
                source_group_openid=record["group_openid"],
                source_umo=record["new_umo"],
                old_umo=record["old_umo"],
                platform_id=record["new_platform_id"],
                sender_name=record["bound_by_name"],
            )
            self._save()

        logger.info(f"[official_bind] 绑定成功 {old_umo} -> {rec['new_umo']}")
        notified = await self._notify_official(rec["new_umo"], code)
        if rec["scope"] == "group":
            pair_text = (
                f"群聊：群号 {event.get_group_id()}"
                f" <-> 群openid {rec.get('group_openid', '')}"
            )
        else:
            pair_text = (
                f"用户：QQ {event.get_sender_id()}"
                f" <-> openid {rec.get('sender_id', '')}"
            )
        notify_text = (
            "已同步通知官方机器人。"
            if notified
            else "（官方平台消息时限/额度限制，本次未能主动通知官方机器人，不影响绑定。）"
        )
        yield event.plain_result(
            "绑定成功！\n"
            f"{pair_text}\n"
            f"{notify_text}\n"
            "对应关系已保存到 id_mapping.json，"
            "待管理员运行迁移脚本后，历史数据就会挂到官方会话上。\n"
            "如需按成员迁移个人数据（记忆/好感度/签到等），"
            "让每个成员在本群再发一次 /数据迁入 ，按提示把个人身份码粘贴到这里。"
        )
        event.stop_event()

    # ------------------------------------------------------------------
    # 发送前钩子：双 bot 同群时把非官方 bot 的群回复改由官方 bot 发出
    # ------------------------------------------------------------------

    @filter.on_decorating_result(priority=10)
    async def forward_group_reply(self, event: AstrMessageEvent):
        """转发成功 -> 清空原消息链；任何失败/不满足条件 -> 原链保留（退回）。"""
        if not self._forward_enabled():
            return
        # 注意：这里不能用 event.is_stopped() 判断，否则会误伤所有主动调用
        # stop_event() 的业务插件（如嫙星的每个指令都在 yield 前 stop_event），
        # 导致它们的回复全部漏转发。改用去重阶段写入的显式标记。
        if event.get_extra("official_bind_deduped"):
            return  # 已被接收去重拦下，无需转发
        if event.get_extra("bind_reply_skip_forward"):
            return  # 绑定流程回执不转发，避免与官方端通知重复
        if event.get_platform_name() not in self._onebot_names():
            return
        if not event.get_group_id():
            return  # 只处理群聊

        result = event.get_result()
        if result is None or not result.chain:
            return
        # 流式回复已由非官方端逐段发出，核心对 STREAMING_FINISH 不再二次
        # 发送；若在这里再代发一遍就是双份消息，所以整体跳过。
        if self._is_streaming_result(result):
            self._note_skip("流式回复已在原端发出，跳过代发", event.unified_msg_origin)
            return

        mapping = self._group_map()
        new_umo = mapping.get(event.unified_msg_origin)
        if not new_umo:
            self._note_skip(
                "该群没有官方绑定映射" if mapping else "还没有任何群绑定",
                event.unified_msg_origin,
            )
            return

        if not self._official_ready(new_umo):
            self._note_skip(
                "官方端未就绪（该群还没收到过官方 bot 的消息，先 @ 它一次）",
                event.unified_msg_origin,
            )
            return

        chain = self._convert_chain(result.chain)
        if chain is None:
            self._note_skip(
                "消息链没有官方端可发内容（纯 @/引用/空文本）",
                event.unified_msg_origin,
            )
            return

        try:
            ok = await self._send_official_with_priority(new_umo, chain)
        except BaseException as e:  # noqa: BLE001 任何发送异常都必须退回，不能丢消息
            self._stats["fallback"] += 1
            self._last_skip = f"官方端发送异常：{e}"
            self._last_skip_at = time.time()
            logger.warning(f"[official_bind] 官方端转发失败，退回原路径: {e}")
            logger.warning(
                "[official_bind] 注意：若已启用 astrbot_plugin_send_blocker，"
                "这条退回消息将被静默拦截、不会发出"
            )
            return

        if not ok:
            self._stats["fallback"] += 1
            self._last_skip = "未找到官方平台实例（转发未送达）"
            self._last_skip_at = time.time()
            logger.warning("[official_bind] 未找到官方平台实例，退回原路径")
            return

        # 转发成功：先记录回声指纹，再清空原结果链（框架据此跳过原路发送）
        self._record_echo(event.unified_msg_origin, result.get_plain_text())
        self._record_forwarded_text(
            str(event.get_group_id() or ""), result.get_plain_text()
        )
        result.chain = []
        self._stats["forwarded"] += 1
        logger.info(
            f"[official_bind] {event.unified_msg_origin} 的回复已由官方 bot 代发 -> {new_umo}"
        )

    # ------------------------------------------------------------------
    # 管理员指令
    # ------------------------------------------------------------------

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("绑定列表", alias={"bindlist"})
    async def list_bindings(self, event: AstrMessageEvent):
        """查看待绑定、会话绑定与个人映射列表"""
        async with self._lock:
            self._cleanup_expired()
            pending = dict(self._pending)
            bound = list(self._bound)
            users = list(self._users)

        lines = [f"待绑定（{len(pending)}）"]
        if pending:
            now = time.time()
            for code, rec in pending.items():
                left = max(0, int(rec.get("expires_at", 0) - now)) // 60
                if rec.get("kind") == "member":
                    scope = f"成员(群 {rec.get('expect_qq_group') or '?'})"
                else:
                    scope = "群" if rec.get("scope") == "group" else "私聊"
                lines.append(
                    f"  {code} | {scope} | {rec.get('sender_name', '')}"
                    f" | {self._mask(rec.get('new_umo', ''))} | 剩 {left} 分钟"
                )
        else:
            lines.append("  （无）")
        lines.append(f"会话绑定（{len(bound)}）")
        if bound:
            for rec in bound:
                scope = "群" if rec.get("scope") == "group" else "私聊"
                if scope == "群":
                    who = (
                        f"群 {rec.get('qq_group_id', '')}"
                        f" <-> {rec.get('group_openid', '')}"
                        f" | 发起人 {rec.get('qq_user_id', '')}"
                    )
                else:
                    who = (
                        f"{rec.get('qq_user_id', '')}"
                        f" <-> {rec.get('user_openid', '')}"
                    )
                lines.append(
                    f"  {rec.get('code', '')} | {scope} | {who}"
                    f" | {self._fmt_time(rec.get('bound_at', 0))}"
                )
        else:
            lines.append("  （无）")
        group_users = [u for u in users if u.get("openid_scope") == "group"]
        priv_users = [u for u in users if u.get("openid_scope") != "group"]
        lines.append(
            f"个人映射（{len(users)}：群内采集 {len(group_users)} / 私聊采集 {len(priv_users)}）"
        )
        if users:
            for u in users:
                where = (
                    f"群 {u.get('source_group_qq') or '?'}"
                    if u.get("openid_scope") == "group"
                    else "私聊"
                )
                lines.append(
                    f"  QQ {u.get('qq_user_id', '')} <-> "
                    f"{self._mask_openid(u.get('user_openid', ''))}"
                    f" | 采集自{where} | {u.get('sender_name', '')}"
                    f" | {self._fmt_time(u.get('updated_at') or u.get('bound_at', 0))}"
                )
        else:
            lines.append("  （无）")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("解绑", alias={"unbind"})
    async def unbind(self, event: AstrMessageEvent, target: str = ""):
        """解绑：参数为身份码、QQ 号或 umo 片段，如 /解绑 a1b2c3d4"""
        target = target.strip()
        if not target:
            yield event.plain_result("用法：/解绑 <身份码 / QQ 号 / umo 片段>")
            return

        async with self._lock:
            removed = None
            for i, rec in enumerate(self._bound):
                if rec.get("code") == target or target in rec.get(
                    "old_umo", ""
                ) or target in rec.get("new_umo", ""):
                    removed = self._bound.pop(i)
                    break
            # 个人映射：按身份码 / QQ 号 / openid 片段
            removed_users = self._drop_user(target) if not removed else []
            # 顺手清掉同目标的待绑定码
            pending_removed = [
                code
                for code, rec in self._pending.items()
                if rec.get("code") == target
                or target in rec.get("new_umo", "")
                or str(rec.get("sender_id", "")) == target
            ]
            for code in pending_removed:
                self._pending.pop(code, None)
            if removed or removed_users or pending_removed:
                self._save()

        if removed:
            yield event.plain_result(
                f"已解绑会话：{removed.get('old_umo')} -> "
                f"{self._mask(removed.get('new_umo', ''))}"
            )
        elif removed_users:
            names = "、".join(
                f"QQ {u.get('qq_user_id')}" for u in removed_users[:10]
            )
            yield event.plain_result(f"已解绑 {len(removed_users)} 条个人映射：{names}")
        elif pending_removed:
            yield event.plain_result(f"已撤销 {len(pending_removed)} 个待绑定身份码。")
        else:
            yield event.plain_result("没有找到匹配的绑定记录。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("导出映射", alias={"export_bind"})
    async def export_mapping(self, event: AstrMessageEvent):
        """导出会话映射与个人映射 CSV，供迁移脚本使用"""
        async with self._lock:
            bound = list(self._bound)
            users = list(self._users)

        if not bound and not users:
            yield event.plain_result("还没有任何已完成的绑定，无法导出。")
            return

        ts = time.strftime("%Y%m%d_%H%M%S")
        out = self.data_dir / f"mapping_export_{ts}.csv"
        try:
            with open(out, "w", newline="", encoding="utf-8-sig") as f:  # noqa: ASYNC230 导出文件很小，阻塞可忽略
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "code",
                        "scope",
                        "old_umo",
                        "new_umo",
                        "old_platform_id",
                        "new_platform_id",
                        "old_session_id",
                        "new_session_id",
                        "qq_group_id",
                        "group_openid",
                        "qq_user_id",
                        "user_openid",
                        "bound_at",
                        "bound_by_qq",
                        "bound_by_name",
                    ]
                )
                for rec in bound:
                    writer.writerow(
                        [
                            rec.get("code", ""),
                            rec.get("scope", ""),
                            rec.get("old_umo", ""),
                            rec.get("new_umo", ""),
                            rec.get("old_platform_id", ""),
                            rec.get("new_platform_id", ""),
                            rec.get("old_session_id", ""),
                            rec.get("new_session_id", ""),
                            rec.get("qq_group_id", ""),
                            rec.get("group_openid", ""),
                            rec.get("qq_user_id", ""),
                            rec.get("user_openid", ""),
                            int(rec.get("bound_at", 0)),
                            rec.get("bound_by_qq", ""),
                            rec.get("bound_by_name", ""),
                        ]
                    )
        except Exception as e:  # noqa: BLE001
            logger.error(f"[official_bind] 导出映射失败: {e}")
            yield event.plain_result(f"导出失败：{e}")
            return

        user_out: Path | None = None
        if users:
            user_out = self.data_dir / f"user_mapping_{ts}.csv"
            try:
                with open(  # noqa: ASYNC230 文件很小
                    user_out, "w", newline="", encoding="utf-8-sig"
                ) as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [
                            "qq_user_id",
                            "user_openid",
                            "openid_scope",
                            "source_group_qq",
                            "source_group_openid",
                            "source_umo",
                            "old_umo",
                            "platform_id",
                            "code",
                            "sender_name",
                            "bound_at",
                            "updated_at",
                        ]
                    )
                    for u in users:
                        writer.writerow(
                            [
                                u.get("qq_user_id", ""),
                                u.get("user_openid", ""),
                                u.get("openid_scope", "group"),
                                u.get("source_group_qq", ""),
                                u.get("source_group_openid", ""),
                                u.get("source_umo", ""),
                                u.get("old_umo", ""),
                                u.get("platform_id", ""),
                                u.get("code", ""),
                                u.get("sender_name", ""),
                                int(u.get("bound_at", 0)),
                                int(u.get("updated_at", 0)),
                            ]
                        )
            except Exception as e:  # noqa: BLE001
                logger.error(f"[official_bind] 导出个人映射失败: {e}")
                user_out = None

        lines = [f"已导出会话映射 {len(bound)} 条：", str(out)]
        if user_out is not None:
            lines += [f"已导出个人映射 {len(users)} 条：", str(user_out)]
        elif users:
            lines.append("个人映射导出失败，详见日志。")
        lines.append("把这两个文件与本插件数据目录放在一起，migrate_to_official.py 会自动读取。")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("转发状态")
    async def forward_status(self, event: AstrMessageEvent):
        """查看群映射、官方端就绪状态、去重配置与统计（管理员）。"""
        mapping = self._group_map()
        # 头部不用下划线：qq_official 这类名字里的 "_" 在 Markdown 里会被当成
        # 斜体标记吃掉，日志/群聊里会出现「officialbind」这种缺字。
        lines = ["[绑定桥接] 双 bot 群映射与转发状态："]
        live = self._live_official_platforms()
        live_txt = "、".join(
            f"{p.meta().id}({p.meta().name})" for p in live
        ) or "未加载"
        for old, new in mapping.items():
            gid = old.rsplit(":", 1)[-1]
            ready = (
                "就绪"
                if self._official_ready(new)
                else "未就绪(官方端还没收到过该群消息，暂退回)"
            )
            lines.append(f"  群 {gid} -> {new} [{ready}]")
        if not mapping:
            lines.append("  （暂无群绑定。请先在双 bot 群里完成绑定：/数据迁入）")
        s = self._stats
        lines.append(
            f"统计：官方代发 {s['forwarded']} | 退回 {s['fallback']}"
            f" | 跳过 {s['skipped']} | 接收去重 {s['deduped']}"
        )
        lines.append(
            f"      官方端拦下 {s['official_dropped']} | 自收回环 {s['self_dropped']}"
            f" | 交官方 {s['handed_over']} | 回环防护 {s['echo_guarded']}"
        )
        lines.append(
            f"      直发拦截 {s['direct_forwarded']} | 直发退回 {s['direct_fallback']}"
            f" | 直发去重 {s['direct_deduped']}"
        )
        lines.append(
            f"      内容对比去重 {s['cross_deduped']} | 官方接管 {s['official_took_over']}"
            f" | @官方处理 {s['at_official_handled']} | @官方让位 {s.get('at_official_handed', 0)}"
        )
        lines.append(
            f"      被动回复 {s.get('passive_replied', 0)} | 主动消息 {s.get('proactive_sent', 0)}"
            f" | 被动窗口 {self._passive_reply_window():.0f}秒"
        )
        users = list(self._users)
        g_users = sum(1 for u in users if u.get("openid_scope") == "group")
        lines.append(
            f"个人映射：{len(users)} 条（群内采集 {g_users} / 私聊采集 {len(users) - g_users}）"
            f" | 本次新增 {s['member_bound']} | 更新 {s['member_updated']}"
        )
        if self._last_skip:
            lines.append(
                f"最近未转发原因：{self._last_skip}"
                f"（{self._fmt_time(self._last_skip_at)}）"
            )
        else:
            lines.append("最近未转发原因：（暂无，说明经过本插件的群回复都已成功代发）")
        bridge = "开启" if self._forward_enabled() else "关闭"
        dedup = "开启" if self._forward_enabled() and self._dedup_enabled() else "关闭"
        lines.append(f"桥接：{bridge} | 接收去重：{dedup} | 官方平台：{live_txt}")
        lines.append(
            "自收过滤："
            f"{'开启' if self._drop_self_messages() else '关闭'}"
            " | 直发兜底："
            f"{'开启' if self._intercept_direct_sends() else '关闭'}"
            f" | 内容对比窗口：{self._message_dedup_window():.1f}秒"
        )
        lines.append(
            f"官方专属指令：{', '.join(sorted(self._official_only_commands())) or '无'}"
        )
        echo = self._echo_seconds()
        lines.append(
            f"回环防护：{'关闭' if echo <= 0 else f'{echo} 秒窗口'}"
            f" | 忽略 sender_id：{', '.join(sorted(self._ignore_sender_ids())) or '未配置'}"
        )
        if s["migrated"]:
            lines.append(
                f"平台迁移：本次运行已自动迁移 {s['migrated']} 条绑定"
                "（官方适配器 webhook/websocket 互切时自动完成）"
            )
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # 平台加载钩子 / 卸载
    # ------------------------------------------------------------------

    @filter.on_platform_loaded()
    async def on_platform_loaded(self):
        """新平台上线时补挂补丁，并同步迁移官方平台 id。

        官方适配器从 webhook 切成 websocket（或反过来）时，平台 id 会变，
        旧绑定记录必须跟着改，否则代发会全部退回到非官方 bot。
        """
        self._install_patches()
        self._migrate_platform_ids()

    def _uninstall_patches(self) -> None:
        """卸掉本实例挂的补丁，避免插件重载后旧补丁继续生效。"""
        self._active = False
        for key, rec in list(_COMMIT_PATCHES.items()):
            if rec.get("plugin") is not self:
                continue
            try:
                rec["platform"].commit_event = rec["orig"]
            except BaseException:  # noqa: BLE001,S110 平台可能已经销毁
                pass
            _COMMIT_PATCHES.pop(key, None)
        for key, rec in list(_SEND_PATCHES.items()):
            if rec.get("plugin") is not self:
                continue
            try:
                bot = rec["bot"]
                # 只移除自己挂的那一个，别误删别人后挂的补丁
                if bot.__dict__.get("send_group_msg") is rec.get("wrapper"):
                    bot.__dict__.pop("send_group_msg", None)
            except BaseException:  # noqa: BLE001,S110 客户端可能已经断开
                pass
            _SEND_PATCHES.pop(key, None)

        for key, rec in list(_OFFICIAL_SEND_PATCHES.items()):
            if rec.get("plugin") is not self:
                continue
            try:
                platform = rec["platform"]
                if getattr(platform, "send_by_session", None) is rec.get("wrapper"):
                    platform.send_by_session = rec["orig"]
            except BaseException:  # noqa: BLE001,S110 平台可能已销毁
                pass
            _OFFICIAL_SEND_PATCHES.pop(key, None)

    async def terminate(self):
        """插件卸载/停用时落盘一次并清理运行时状态。"""
        self._active = False
        if self._patch_task is not None:
            self._patch_task.cancel()
            self._patch_task = None
        try:
            self._save()
        except Exception:  # noqa: BLE001, S110 停机落盘失败也无能为力
            pass
        self._uninstall_patches()
        self._echo_records.clear()
        self._recent_forwards.clear()
        _DedupFilter.plugin = None
        logger.info("[official_bind] 插件已卸载")
