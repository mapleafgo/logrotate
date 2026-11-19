# Go 文件写入可靠性核对

范围：Go 官方源码固定为 `go1.26.0`；目录持久化语义以下仅讨论 Linux。

## 标准库与 Linux

- **Write 不等于落盘。** [`(*os.File).Write`](https://github.com/golang/go/blob/go1.26.0/src/os/file.go#L208-L228) 返回实际字节数与错误，短写必有非 nil 错误；即使完整写入成功，[Linux write(2)](https://man7.org/linux/man-pages/man2/write.2.html) 也不保证数据已落盘，部分错误可延迟到后续 Write、Sync 或 Close。
- **Sync 与 Close 是两件事。** [Unix `File.Sync`](https://github.com/golang/go/blob/go1.26.0/src/os/file_posix.go) 经 [`poll.FD.Fsync`](https://github.com/golang/go/blob/go1.26.0/src/internal/poll/fd_fsync_posix.go#L10-L19) 对该描述符调用 `fsync`；`File.Close` 走[关闭路径](https://github.com/golang/go/blob/go1.26.0/src/os/file_unix.go)，不隐式 Sync。应检查 Close 的错误，但在 Linux 上不要重试底层 `close(2)`，描述符可能已被重用。[Linux close(2)](https://man7.org/linux/man-pages/man2/close.2.html)
- **文件内容和文件名分开同步。** [Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html) 明确指出，文件 fsync 不保证其目录项落盘。若要持久化创建或重命名后的目录项，还要打开并同步发生变更的父目录；跨目录 rename 要考虑源、目标两侧。
- **短命写入也不例外。** [`os.WriteFile`](https://github.com/golang/go/blob/go1.26.0/src/os/file.go#L930-L945) 是打开（创建或截断）→ Write → Close；写错误优先，仅在写成功时返回关闭错误。它既不 Sync，也不是原子替换，失败可留下部分内容。普通 `defer f.Close()` 会丢弃关闭错误；手动 Open 时可显式检查 Close，或用带命名返回值的延迟闭包 `defer func() { err = errors.Join(err, f.Close()) }()`，保留先前错误。需要持久化时另行检查 Sync。[Go 语言规范：Defer statements](https://go.dev/ref/spec#Defer_statements)

## 与本库对照

- **异步写入：** `AsyncWriter.Write` 对非空输入成功只表示已入队；后台 goroutine 把无错误的短写转成 `io.ErrShortWrite`，并保留首个投递错误直到 Close。`Sync` 是 FIFO 屏障：处理此前已入队的记录后同步 sink，返回首个投递错误与本次同步错误。`Close` 排空并关闭，合并投递错误与底层关闭错误，但不隐式 Sync。
- **活动文件：** `Writer` 的 `Write`/`Sync`/`Close` 分别转交文件写入、仅同步当前活动文件、关闭活动文件并等待归档维护。`Close` 不同步活动文件，但会合并关闭错误和未解决的维护错误；创建活动文件时不显式同步其父目录。
- **轮转：** 封存前先 Sync 再 Close 旧文件；rename 成功后、重开活动文件前同步归档目录和活动文件的父目录。目录同步失败交给后台维护重试，不作为触发轮转的 `Write` 错误返回；目录同步忽略 `EINVAL`/`ENOTSUP`，这种返回不能视为目录已落盘。

当前规则见[使用说明](../README.md)。需要在停机前确认活动文件内容时：先停产，显式 Sync 并检查结果，再 Close 并检查结果；这不能代替新建路径所需的目录同步，也不覆盖尚未入队或已丢弃的记录。


