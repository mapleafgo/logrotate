# Go 日志缓冲机制调研：所有权、过载与停机

**结论：** lumberjack 同步轮转文件；zap 聚合字节；zerolog 的 diode 让生产者不等待，允许未读条目被覆盖。本库的异步写入同样让生产者与磁盘解耦，但不会覆盖已经接受的写入。下文固定各调研版本，这些版本不是本模块的依赖，本页也不定义本库行为；当前规则见[使用说明](../README.md)。

| 实现 | 缓冲所有权与容量 | 过载时的行为 |
| --- | --- | --- |
| [lumberjack v2.2.1: Write](https://github.com/natefinch/lumberjack/blob/v2.2.1/lumberjack.go#L123-L153) | 没有负载队列。调用方字节在持有 logger 锁时直接写入活动文件。 | 写者可等待锁、轮转或文件 I/O。单次写入大于文件上限时返回错误；越过上限时先轮转再写。 |
| [zap v1.27.0: BufferedWriteSyncer](https://github.com/uber-go/zap/blob/v1.27.0/zapcore/buffered_write_syncer.go#L30-L37)（[写入路径](https://github.com/uber-go/zap/blob/v1.27.0/zapcore/buffered_write_syncer.go#L108-L148)） | 互斥锁保护内部字节缓冲（默认 256 KiB）。留在缓冲中的数据被复制进缓冲；空缓冲下的大写入可以同步直写底层。 | 放不下且缓冲已有数据时，生产者先冲刷再提交新字节。没有丢弃策略，生产者可能遇到底层延迟或错误。 |
| [zerolog v1.34.0: diode.Writer](https://github.com/rs/zerolog/blob/v1.34.0/diode/diode.go#L34-L77)（[环形实现](https://github.com/rs/zerolog/blob/v1.34.0/diode/internal/diodes/many_to_one.go#L17-L62)） | 调用方切片可能来自 logger 池，因此包装层先复制到自有池化存储再发布。容量是调用方指定的条目数，不是字节数。 | 生产者不等待队列空间，后续发布可覆盖未读条目。消费者追上时会跳过整段序号缺口，而不只是被覆盖的单个槽位。缺口被发现时在读取侧回调缺失计数，生产者仍返回输入长度和 nil。[缺口处理](https://github.com/rs/zerolog/blob/v1.34.0/diode/internal/diodes/many_to_one.go#L64-L122) |
| [本库 AsyncWriter](../async.go) | 互斥锁保护 FIFO 环形队列，默认 1,024 个槽位。有空位时才复制负载，底层写入返回后回收小缓冲，`Write` 返回后调用方可复用原缓冲区。槽位数不是字节上限，也不包含已取出正在交付的那一条。 | `FullBlock`（默认）等待空位。`FullDropNewest` 以 `(0, ErrDropped)` 拒收当前记录并累加 `Dropped`，不复制被拒字节，也不驱逐已有记录。该模式同样加锁，并非无锁。 |

## 交付、错误与停机

- **lumberjack：** 后台 mill 只负责备份压缩与清理，不负责提交日志。单槽非阻塞通知会合并维护请求。Close 关闭当前文件，既不排空负载队列，也不提供 Sync 方法。[关闭文件](https://github.com/natefinch/lumberjack/blob/v2.2.1/lumberjack.go#L154-L169) · [维护 goroutine](https://github.com/natefinch/lumberjack/blob/v2.2.1/lumberjack.go#L351-L372)
- **zap：** ticker（默认 30 秒）调用 Sync，冲刷缓冲并调用被包装的 Sync。循环忽略 tick 错误；bufio 会保留写入或冲刷错误，但被包装 Sync 的瞬时错误不会被单独保存。Stop 结束周期循环，并在正常的初始化停机路径上做最后一次 Sync；它不关闭被包装的 sink，也不阻止新的 Write，因此应先停止生产者。[Sync 与 Stop](https://github.com/uber-go/zap/blob/v1.27.0/zapcore/buffered_write_syncer.go#L149-L209)
- **diode：** worker 忽略底层的写入字节数与错误。一条已消费条目的底层 Write 返回后，容量不超过 64 KiB 的缓冲可以回到池中；被覆盖或跳过的条目不走该回收路径。该上限用于池回收，不是单条上限。Close 取消并等待消费者退出，若底层实现 io.Closer 再关闭它。[poller](https://github.com/rs/zerolog/blob/v1.34.0/diode/internal/diodes/poller.go#L53-L67) 与 [waiter](https://github.com/rs/zerolog/blob/v1.34.0/diode/internal/diodes/waiter.go#L56-L75) 在未命中时都先尝试读取再检查取消；没有生产者准入闸门、交付屏障，也不保证每次成功的 Write 都被交付。[Writer 生命周期](https://github.com/rs/zerolog/blob/v1.34.0/diode/diode.go#L71-L109)
- **本库：** `Write` 成功只表示已入队，不表示已交付或已落盘。单个 worker 对每条数据调用一次 sink 写入，把无错误的短写转成 `io.ErrShortWrite`，并保留首个投递错误；`LastError` 仍可在后续成功后清空。`Sync` 插入 FIFO 屏障，即使 `FullDropNewest` 也要等待准入，返回此前首个投递错误和/或本次 sink 同步错误。`Close` 关闭准入、唤醒等待的生产者、处理完已接受的消息后关闭 sink，报告首个投递错误和/或底层关闭错误；它不设超时，也不隐式 Sync。

## 对本库设计的约束

- **准入时复制即所有权边界。** 池化可以降低分配开销，但只能在同步 sink 写入返回后回收缓冲，并对保留容量设上限；池化和按条计数都不能给出排队字节总量的上限。
- **批处理与过载是两个独立选择。** 当前 worker 对每条已接受记录发送一次 sink 写入。合并记录会改变轮转边界，也可能触发按大小轮转的 sink 的单次写入上限。
- **区分“已接受”“已交付”“已同步”。** 停机前需要显式同步时，先停止生产者，调用并检查 `Sync`，再调用并检查 `Close`；实际持久化取决于 sink 的 Sync 实现。
