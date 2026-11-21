# QQ 官方机器人 ⇄ OneBot v11 双 bot 桥接方案 —— 完整需求规格

> 用途：交给另一个 AI / 在另一台机器上按本文复现整套架构。
> 版本基准：astrbot_plugin_official_bind v2.2.2（metadata.yaml / README.md 为准）。
> 目标效果：一个群里同时存在「QQ 官方机器人」和「OneBot v11（NapCat）非官方机器人」，消息只被处理一次，回复统一由官方机器人发出；群/个人身份可迁移，历史数据可改写。

---

## 0. 一句话目标

让官方机器人做「回复出口」，非官方机器人（OneBot v11）做「消息入口」，二者同群时通过 `astrbot_plugin_official_bind` 插件完成**身份绑定 / 数据迁移 / 接收去重 / 回复代发**，避免消息处理两遍、回复两条。

---

## 1. 业务背景与要解决的问题

1. QQ 官方机器人用 openid / group_openid 标识用户与群，OneBot v11 用 QQ 号 / 群号，二者**不可换算**，历史会话/记忆/好感度/签到等数据无法直接挂到官方机器人上。
2. 两个 bot 在同一群时，同一条消息会被两个适配器各收到一次 → 插件/LLM 跑两遍、回复两条。
3. 官方机器人被动回复受限（`msg_id` 60 分钟内有效、每消息最多回复 5 次；管理员身份 50 次），而群主动消息有频控（单关系 20 条/分钟、每群每天 1000 条）。
4. 需要一个插件统一完成：身份绑定 → 历史数据迁移 → 双 bot 去重 → 回复官方代发 → 回环防护。

---

## 2. 官方平台侧要求（必须先满足）

### 2.1 QQ 开放平台配置
- 机器人 AppID / Secret 正确（appid=`YOUR_APPID`，注意不要外泄 secret）。
- **事件订阅与回调地址**：接入方式选 Webhook 或 WebSocket 均可；事件接收选「全部接收」即可覆盖 `C2C_MESSAGE_CREATE`、`GROUP_AT_MESSAGE_CREATE`、`GROUP_MESSAGE_CREATE`、`INTERACTION_CREATE`。
  - 官方文档：`GROUP_AT_MESSAGE_CREATE`（群 @ 机器人，content 已去掉 @ 前缀）、`GROUP_MESSAGE_CREATE`（全量模式，需机器人开启「接收所有消息」）、`C2C_MESSAGE_CREATE`（私聊）。
  - Webhook 模式回调地址必须为公网可达的 HTTPS；AstrBot 开启「统一 Webhook 模式」会自动生成回调链接与端口（webhook_uuid=`YOUR_UUID`，端口由部署决定；如反向代理统一转发，按实际部署为准）。
  - 需配置服务器 **IP 白名单**。
- 官方机器人必须开通 **群聊主动消息权限**，否则无法主动代发回复与绑定通知。
- 服务范围：未认证/个人主体只能添加自己是群主或管理员的群；机器人需先被拉进目标群。

### 2.2 手机 QQ 群侧配置
- 群设置 → 机器人设置：「机器人可获取的群聊消息范围」设为 **获取群内全部消息**，并开启 **机器人主动在群聊内发言**。
- 「官方端就绪」前提：AstrBot 启动后，官方 bot 至少在该群收到过一条消息（被 @ 一次即可）；未就绪期间回复走原路径，不丢消息。
- 绑定前：官方机器人和 OneBot 机器人都要在同一个群。

### 2.3 AstrBot 侧配置
- 安装 `astrbot_plugin_official_bind`（v2.2.2），**不要同时安装已被合并的旧插件 `astrbot_plugin_reply_forward`**，否则重复注册钩子造成多次拦截。
- 适配器：
  - 官方端：`qq_official_webhook`（Webhook）或 `qq_official`（WebSocket），二者可互切，插件会自动迁移平台 id 对应的绑定。
  - 非官方端：`aiocqhttp`（OneBot v11 / NapCat）。
- 唤醒与回复策略（AstrBot `platform_settings`）：私聊 `friend_message_needs_wake_prefix=false`；群聊 @ 与指令可被唤醒；决策 AI（`ai_decision`）负责判断「只发内容不 @」时是否回复。
- 日志：`log_level=INFO`，日志路径 `data/logs/astrbot.log`（排障用）。

---

## 3. 插件功能需求

### 3.1 身份绑定与数据迁移
- **群绑定流程**：官方 bot 群/私聊中发 `/数据迁入` → 得到带身份码的说明消息 → 把该消息发到非官方 bot 所在群 → 完成绑定，写入 `id_mapping.json` 的 `groups.bound`（群号 ↔ group_openid + 两端 umo/session）。
- **个人绑定流程**（v2.2）：群会话绑定完成后，成员在官方 bot 群发 `/数据迁入` 得到「个人身份码」→ 私聊非官方 bot 粘贴 → 写入 `members.users`（QQ 号 ↔ 官方 openid）。openid 按用户固定、跨群通用、按 QQ 号全局唯一，重复绑定只更新。
- **身份码**：有效期默认 15 分钟，长度 8（6–16）；`admin_only_bind` 可限制仅管理员生成。
- **历史数据迁移**：同目录 `migrate_to_official.py` 读取 `id_mapping.json`（或 `/导出映射` 导出的 CSV），把主库 `data_v4.db`、livingmemory 等库里的旧 umo 改写成官方 umo；加 `--user-ids` 用 users 表把消息/记忆里的 `sender_id`（QQ 号）改写成官方 openid。默认**干跑只统计**，`--execute` 才写库；写前自动备份；检测到 WAL 非空（AstrBot 仍在运行）时拒绝执行。

### 3.2 同群双 bot 桥接：三层拦截

| 层 | 位置 | 拦截对象 | 原因 |
|---|---|---|---|
| 1 | `Platform.commit_event` 包装 | 官方端重复群消息、非官方端自身消息 | `WakingCheckStage` 是管线第一阶段，权限不足会立即回复并 return；必须在进管线前丢弃，否则官方端还会把插件/LLM 再跑一遍 |
| 2 | `priority=9999` 自定义过滤器 + `stop_event` | 补丁未挂上时的兜底 | 平台未就绪/补丁失败时仍能去重 |
| 3 | `bot.send_group_msg` 包装 | gscore_adapter 等绕过管线直接调协议 API 的群发 | 这类消息不经过 `on_decorating_result`，发送前钩子看不到 |

**官方端收到群消息**：
- 是官方专属指令（默认 `/数据迁入`，见 `official_only_commands`）→ 放行进管线（只此一次）；
- 其余 → 在 `commit_event` 处丢弃（不进管线、不触发权限提示）。

**非官方端收到群消息**：
- 自身消息（NapCat report-self-message 回灌）→ 丢弃；
- 官方专属指令 → 交给官方端执行；
- 官方 bot 刚代发消息的回声 → 静默（防回环）；
- 其余 → 正常处理，回复统一改由官方 bot 代发。

### 3.3 回复转发链路
- 发送前钩子 `@filter.on_decorating_result(priority=10)` + 协议端兜底：
  - 该群无绑定 / 官方端未就绪 / 内容不可转发 → 原路径发送（不干预）；
  - 否则消息链清洗（保留文本/图片/语音，丢弃 @ 与引用，展开合并转发节点）→ `context.send_message` 官方群 umo → 成功则清空原消息链（官方已代发）；失败则保留原链、非官方 bot 照常发送（**绝不丢消息**）。
- 第三方插件直发协议 API：第 3 层把 OneBot 消息段还原成消息链，同样改由官方 bot 代发，失败退回原方法。

### 3.4 回环防护
- 第 1 层按 `sender_id == self_id` 丢弃自身消息；文本指纹窗口 `echo_guard_seconds`（默认 10 秒）静默相似内容。
- 官方 bot 在 NapCat 侧的 UIN 默认加入 `ignore_sender_ids`（你自己的官方 bot UIN），命中即在管线前丢弃；插件会从「非官方端 @官方bot 发 `/数据迁入`」的 At 组件自动学习新 UIN。
- 升级时自动清理历史误绑（把官方 bot UIN 误当用户的记录），日志提示 `已清理 N 条…`，需用真实 QQ 号重新粘贴身份码。

### 3.5 平台 id 自动迁移
- 同一套 appid 在 Webhook / WebSocket 互切后平台 id 会变（如 `default` → `QQBOT-websocket`）；插件在启动/平台加载时把 `new_umo` 的平台段换成当前实际运行的官方平台 id 并落盘，无需重新绑定。

---

## 4. 数据契约

`data/plugin_data/astrbot_plugin_official_bind/id_mapping.json`（唯一数据文件，绑定完成即刻生效，无缓存延迟）：

```json
{
  "salt": "...",
  "groups": { "pending": {}, "bound": [] },
  "members": { "pending": {}, "users": [] }
}
```

- `groups`：群聊身份码 + 群号↔群openid 绑定。
- `members`：个人身份码 + QQ号↔个人openid 映射。
- 旧版四段式（pending/bound/users 平铺）启动时自动迁移：**只保留群聊绑定**，个人映射与个人身份码丢弃；群记录里误写成官方 bot UIN 的发起人按 `user_openid` 反查纠正（查不到则原样保留）；迁移脚本兼容新旧格式。

---

## 5. 配置项契约（key / 默认 / 含义）

| 配置键 | 默认 | 含义 |
|---|---|---|
| `code_expire_minutes` | 15 | 身份码有效期（分钟） |
| `code_length` | 8 | md5 截取位数（6–16） |
| `admin_only_bind` | false | 仅管理员可生成身份码 |
| `enable_member_bind` | true | 允许成员绑定个人 id |
| `onebot_platform_names` | `aiocqhttp` | 非官方 bot 适配器名（逗号分隔） |
| `official_platform_names` | `qq_official,qq_official_webhook` | 官方 bot 适配器名（逗号分隔） |
| `enable_forward` | true | 双 bot 桥接总开关（去重+转发），不影响绑定 |
| `enable_dedup` | true | 接收去重（管线内兜底） |
| `drop_official_duplicates` | true | 管线前丢弃官方端重复群消息（关键开关） |
| `drop_self_messages` | true | 丢弃非官方 bot 自身消息回灌 |
| `intercept_direct_sends` | true | 拦截绕过管线的协议端直发并转官方代发 |
| `official_only_commands` | `数据迁入` | 只在官方端执行的指令（逗号分隔） |
| `ignore_sender_ids` | 空/自动学习 | 忽略的官方 bot 在 NapCat 侧的 UIN（可多个，自动学习） |
| `echo_guard_seconds` | 10 | 回环内容指纹窗口（秒），0=关闭 |

---

## 6. 指令契约

| 指令 | 权限 | 行为 |
|---|---|---|
| `/数据迁入` | 全部（可设仅管理员） | 官方 bot 生成身份码；群首次=群码，已绑定=个人码 |
| `/绑定列表` | 管理员 | 查看待绑定/会话绑定/个人映射 |
| `/解绑 <码或QQ号或umo片段>` | 管理员 | 删除绑定/个人映射/撤销待绑定码 |
| `/导出映射` | 管理员 | 导出映射 CSV + 个人映射 `user_mapping_*.csv` |
| `/转发状态` | 管理员 | 查看群映射、官方端就绪、去重配置与统计 |

---

## 7. 验收标准（DoD）

1. 官方群消息（@ 与普通全量）能被 Webhook/WebSocket 正常推送到 AstrBot（日志中可见 OpenID 格式事件，`event_id` 形如 `GROUP_MESSAGE_CREATE:...`）。
2. 双 bot 同群时，一条普通群消息只产生**一次**插件/LLM 处理与**一条**回复；回复账号为官方机器人。
3. 官方专属指令（默认 `/数据迁入`）只执行一次（官方端放行、非官方端同步丢弃）。
4. 官方 bot 代发的消息不会在非官方端形成回环（`ignore_sender_ids` + `echo_guard_seconds` 生效）。
5. `/数据迁入` 群绑定与个人绑定均成功写入 `id_mapping.json`；重启 AstrBot 后绑定仍在（平台 id 互切自动迁移正常）。
6. `migrate_to_official.py` 默认干跑不写库；`--execute` 前有备份、WAL 非空时拒绝执行；`--user-ids` 能改写 sender_id。
7. 官方端发送失败 / 未就绪 / 内容不可转发时，回复自动退回非官方 bot 原路径，**不丢消息**。
8. 私聊不受三层拦截影响，官方 bot 私聊可正常收发。
9. gscore_adapter 等绕过管线的直发也被第 3 层拦截并由官方 bot 代发（流式输出逐段代发不漏）。
10. 日志中无重复拦截冲突（`astrbot_plugin_reply_forward` 未同时安装）。

---

## 8. 实测现象与预期对照（本实例已验证）

| 实测现象 | 是否符合预期 | 原因 |
|---|---|---|
| 群里 @ 官方机器人能回复 | ✅ | 官方专属指令/唤醒放行；或通过 OneBot 入口处理 |
| 只发内容不 @ 时「智能识别」 | ✅ | 决策 AI `ai_decision` 判断是否介入 |
| 只有一个 bot 收到时正常回复 | ✅ | 无重复，直接处理 |
| 两个 bot 都收到时只回复一个 | ✅ | `official_bind` 去重（第 1/2 层）+ 代发（第 3 层） |
| 官方端「收不到群消息」的表象 | ✅（非缺陷） | 官方群消息在 `Platform.commit_event` 前被按设计丢弃，避免处理两遍；官方端实际已收到推送 |
| 私聊可用 | ✅ | 插件只处理群聊，私聊不受影响 |

---

## 9. 复现步骤（另一个 AI / 新机器）

1. 部署 AstrBot（你的 AstrBot 源码目录），安装 `aiocqhttp`（NapCat/OneBot v11）与官方机器人适配器（Webhook 或 WebSocket）。
2. 按第 2 节完成 QQ 开放平台与手机 QQ 群配置，确认官方 bot 收到过目标群的一条消息（被 @ 一次）。
3. 安装 `astrbot_plugin_official_bind`（源码见插件目录），确认 `metadata.yaml` 版本 v2.2.2，且未安装 `astrbot_plugin_reply_forward`。
4. 配置第 5 节各开关（默认即可；如官方 bot 在 NapCat 侧 UIN 变化，更新 `ignore_sender_ids`）。
5. 在官方 bot 群发 `/数据迁入` 生成群身份码，把说明消息发到非官方 bot 所在群完成群绑定。
6. 成员在官方 bot 群发 `/数据迁入` 生成个人身份码，私聊非官方 bot 粘贴完成个人绑定。
7. 用 `/转发状态` 确认群映射与官方端就绪状态。
8. 按第 7 节 DoD 逐条验收；如要迁移历史数据，停掉 AstrBot（确保 DB WAL 为空）后运行 `migrate_to_official.py`，先干跑核对再 `--execute`。

---

## 10. 已知限制与注意事项

- 官方平台无法呈现 QQ 号 @ 和引用回复；转发时这类组件被丢弃，纯 At/引用（无正文）退回原路径。
- 开启 `use_markdown` 时，正文里成对下划线 `_` 会被 QQ 当斜体标记吞掉；自定义文案避免成对下划线。
- 群主动消息频控：单关系 20 条/分钟、每群每天 1000 条，超限发送失败并退回原路径。
- 官方 bot 的 `msg_id` 被动回复 60 分钟有效、每消息最多 5 次（管理员 50 次）。
- 之前在其他插件（如嫙星）里配置的「群聊忽略平台」可清空交给本插件统一处理（保留也不冲突，只是重复拦截）。
- 切勿在日志/文档中暴露 AppSecret、API Key 等敏感信息。

---

## 11. 参考资料

- QQ 官方文档：群@机器人消息 `GROUP_AT_MESSAGE_CREATE`、群消息全量模式 `GROUP_MESSAGE_CREATE`、发送群聊消息接口（被动回复规则）、事件订阅与通知。
- AstrBot 官方文档：QQ 官方机器人 Webhook / WebSocket 接入指南（docs.astrbot.app）。
- 插件内 `README.md`（v2.2.2）、`_conf_schema.json`、`main.py`、`metadata.yaml`、`migrate_to_official.py`。
- 排障日志：`data/logs/astrbot.log`（其中可见官方 OpenID 格式 `GROUP_MESSAGE_CREATE` 事件与插件加载记录）。
