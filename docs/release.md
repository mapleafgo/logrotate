# 发布验收

发布范围为 Linux、Windows、macOS。对同一候选版本逐项记录实际结果；任一项缺失即视为未发布。

## 原生 CI

在 [CI 矩阵](../.github/workflows/ci.yml) 的 Linux、Windows、macOS runner 上执行 `go vet -mod=readonly ./...` 与 `go test -mod=readonly -race ./...`，并在 `tests/migration` 下禁用工作区（`GOWORK=off`）运行升级与回滚测试。记录候选提交、各平台结果和 CI 运行链接。交叉编译不能替代原生运行。

## 外部模块

在仓库外新建独立 Go 模块，禁用工作区（`GOWORK=off`），不使用本地 `replace`，以 `go get github.com/mapleafgo/logrotate/v2@<候选版本>` 获取候选版本。核对解析出的版本后，用公开 API 依次执行打开、写入、切卷和关闭，并核对活动文件与归档内容。记录候选版本、Go 版本、命令及结果。仓库内测试或对本地 checkout 的验证不能替代该结果。

候选结果记入[验证记录](logback-rolling-acceptance.md#候选验证记录)。
