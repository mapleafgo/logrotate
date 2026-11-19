# 日志轮转对标：何时切分，保留多少？

| 实现（官方依据） | 轮转触发与归档命名 | 保留规则 |
| --- | --- | --- |
| [lumberjack v2.2.1](https://github.com/natefinch/lumberjack/blob/v2.2.1/lumberjack.go) | 本次写入将使文件超过 `MaxSize` 时先轮转；单次写入本身超限则报错，不拆分。旧文件以轮转时间命名。 | `MaxBackups` 限文件数，`MaxAge` 按归档名中的轮转时间限天数；两者均为 0 时不删除。 |
| Python [RotatingFileHandler](https://docs.python.org/3/library/logging.handlers.html#rotatingfilehandler) | 按 `maxBytes` 切分；最新备份为 `.1`，已有备份编号递增。 | `backupCount` 限备份数；它或 `maxBytes` 为 0 时都不会产生轮转备份。 |
| Python [TimedRotatingFileHandler](https://docs.python.org/3/library/logging.handlers.html#timedrotatingfilehandler) | 按 `when` 和 `interval` 定周期；到期后在下一条日志写入时才轮转，文件名带日期时间。 | `backupCount` 仍按文件计数；为 0 时继续轮转，但不清理旧备份。 |
| [Logback：滚动文件策略](https://logback.qos.ch/manual/appenders-rolling.html#SizeAndTimeBasedRollingPolicy) | 大小与时间组合策略用 `%d` 标记周期；周期内达到 `maxFileSize` 则以 `%i` 编号分卷。 | 先按 `maxHistory` 保留历史周期，再按 `totalSizeCap` 异步删除最旧归档；后者需同时设置前者。 |

本页是固定版本的调研记录，不定义本库行为；当前规则见 [使用说明](../README.md)。

对本库的取舍：把“什么时候切文件”和“什么时候删文件”拆成两组配置（`Daily`/`MaxFileMiB` 与 `HistoryDays`/`TotalArchiveMiB`）。保留按自然日计而非按文件数计，因此“保留 7 天”不会在一天多卷时覆盖不足一天；归档总量是后台清理目标，不是磁盘硬配额。
