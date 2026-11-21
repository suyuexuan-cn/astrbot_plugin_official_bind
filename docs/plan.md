# 双 bot 桥接备用入口 Implementation Plan

**Goal:** 让官方机器人在被 @ 时自行回复，并在非官方机器人掉线时作为备用消息入口接管，同时保证每条消息只处理一次、只回一条。

**Architecture:** 在 commit_event 拦截层引入跨端消息指纹去重（统一群号+规范化文本+时间窗口）与相对活跃信号（非官方端最近收消息时间）；官方端 @消息独占，普通消息在非官方活跃时丢弃、沉默超时后接管。

**Tech Stack:** Python / AstrBot Plugin API

## 任务
1. 新增配置项：enable_official_fallback / fallback_silence_seconds / message_dedup_window_seconds
2. 新增运行时状态：_dedup_seen / _last_onebot_seen
3. 新增指纹与活跃方法，重构 _should_drop_event 与 _DedupFilter.filter
4. 统计与 /转发状态 展示
5. 版本号、README、文档
6. 离线回归测试 + ruff + 打包
