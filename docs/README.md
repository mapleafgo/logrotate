# 文档索引

## 使用与发布

- [使用说明](../README.md)：安装、配置、轮转、同步与关闭、升级和回滚。
- [发布验收](release.md)：正式发布前必须完成的验证。

## 实现分工

- [Writer](../rolling.go) 负责活动文件的追加、显式同步、封存和重开。
- [归档维护](../rolling_maintenance.go) 串行调度[恢复与压缩](../rolling_recovery.go)和[保留清理](../rolling_retention.go)，独立于写入队列。
- [AsyncWriter](../async.go) 独占底层 sink，使用有界 FIFO 队列，负责并发入队、同步屏障和停机排空。队列有空位才复制；底层写入返回后回收小缓冲，每个 writer 最多保留 64 个、每个不超过 4 KiB 的空闲缓冲区。关闭不隐式同步。

## 验证与调研

- [验证记录](logback-rolling-acceptance.md)：历史测试、性能数据和候选验证状态。
- [轮转机制对标](rotation-comparison.md)、[Logback 归档保留](logback-retention-reference.md)、[Logback 可靠性](logback-reliability-research.md)、[Go 文件可靠性](go-file-reliability-research.md)、[Go 日志缓冲机制](go-logging-buffer-research.md)。

## 历史设计与计划

以下记录仅供追溯，不定义当前接口或行为。

- [Logback 对标提案](superpowers/specs/2026-11-18-logback-rolling-design.md)、[异步归档维护设计](superpowers/specs/2026-11-18-async-archive-maintenance.md)。
- [旧版运维加固设计](superpowers/specs/2026-07-24-lockfree-and-ops-hardening-design.md)、[实施计划](superpowers/plans/2026-07-24-lockfree-and-ops-hardening.md)。
