# 进度

- 2026-11-21：诊断完成，开始实施备用入口改造。
# 进度

- 2026-11-21：诊断完成，开始实施备用入口改造。
- 2026-11-21：v2.3.0 完成——内容对比去重（先到先得）替代原「官方端一律丢弃」；@官方由官方自行处理；非官方掉线时官方接管。6 项回归测试 + ruff + 语法 + schema 全过，已打 zip。
- 2026-11-23：v2.4.1——流式输出守卫。核实：流式时 on_decorating_result 虽仍触发（result_decorate 168-172 仅告警）但消息链未定型、不可代发；STREAMING_FINISH 会被 respond 阶段拦截不再发送。此前代发钩子对流式链尝试代发（转换多为 skip，但若链恰好含文本会与已流式发出的内容重复）。现在 forward_group_reply 检测到 result_content_type 为 STREAMING_RESULT/STREAMING_FINISH 直接跳过并记 skip 原因。
