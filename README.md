# review-agent-infra

面向 `llm-d` 的 **100% 自托管、全链路可插拔 Coding Review Agent**（Go 全栈实现）。

## 设计文档导航

* [高层架构设计（HLD）](docs/design/hld.md)：业务目标、信任域边界、5 个可插拔插件接口、自建 `llm-d` 集群对接清单、全自托管技术选型与演进路径。
* [Go 实现方案与开发计划（Implementation Plan）](docs/design/implementation-plan.md)：Go 仓库包结构与单向依赖图、5 大插件 Go `interface`、PostgreSQL 并发队列与指纹 DDL、`client-go` gVisor Exec 沙箱、Go `harness` 与 `adapter` 伪代码、去噪发布流水线及 M1~M4 开发里程碑。

## 规划中的仓库目录

```text
review-agent-infra/
├── docs/design/              # 高层设计 (hld.md) 与 Go 实现计划 (implementation-plan.md)
├── cmd/
│   ├── controller/           # 控制面单体 (Webhook + PG 调度器 + K8s 沙箱调度 + 发布器)
│   ├── adapter/              # 沙箱外侧 Inference Adapter (HMAC 验签 + 预算熔断 + llm-d 路由)
│   ├── harness/              # 沙箱内轻量两阶段 Go Agent (静态单二进制 + 离线只读工具)
│   └── reviewctl/            # 本地 CLI 审查与历史 PR 离线回放评测工具
├── pkg/contract/             # 跨二进制共享的纯净数据契约与 5 个插件接口
├── internal/                 # 各模块具体实现 (vcs / store / sandbox / orchestrator / publisher / token / inference / harness / tools / telemetry)
└── deploy/                   # Dockerfile、K8s NetworkPolicy 与部署清单
```
