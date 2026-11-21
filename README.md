<div align="center">

# OVH Console

**面向 OVHcloud 独立服务器、Eco 系列与 VPS 的全生命周期管理与实时库存控制台**

[![Version](https://img.shields.io/badge/version-v0.4.37-2563EB?style=for-the-badge)](https://github.com/w0ven/ovh-console/releases)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![React](https://img.shields.io/badge/React-18-61DAFB?style=for-the-badge&logo=react&logoColor=111827)](https://react.dev/)
[![Docker](https://img.shields.io/badge/Docker-linux%2Famd64%20%7C%20arm64-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://github.com/w0ven/ovh-console/pkgs/container/ovh-console)
[![License](https://img.shields.io/badge/License-AGPL--3.0-10B981?style=for-the-badge)](LICENSE)

简体中文 · [English](README.en.md)

</div>

---

## 项目简介

**OVH Console** 是一套轻量级、自托管的 OVHcloud 运维与订购控制台。前端通过 `//go:embed` 直接嵌入 Go 静态单二进制，内置纯 Go SQLite 驱动，开箱即用，无需额外数据库或运行时依赖。

它同时覆盖 **库存监控与自动化订购** 与 **已购资产全生命周期运维** 两大场景，原生支持 **EU / US / CA 三大站点** 与 **多账户隔离管理**。

<details>
<summary>核心亮点一览</summary>

- **极速单文件交付**：前端静态资源内嵌进单一可执行文件，支持 `linux/amd64`、`linux/arm64` 与 `windows/amd64`，同时提供轻量级 Alpine Docker 镜像。
- **多区域与多账户原生隔离**：EU / US / CA 站点目录、币种、计费规则与库存接口自动适配；抢购队列、监控订阅与服务器控制按账户完全隔离。
- **安全优先**：全站 API Key 鉴权 + 暴力破解限流；OVH API 凭据与 Telegram Token 均采用 **AES-256-GCM** 加密落盘；下单默认不自动扣款，支付需页面二次核对。
- **双语与深色界面**：内置中/英双语（含后端错误码双语翻译）以及浅色 / 深色 / 跟随系统三种主题模式。

</details>

---

## 核心功能矩阵

| 模块 | 核心能力 |
| :--- | :--- |
| **实时库存与目录** | 直连 OVH 各区公开可用性接口（60s 新鲜期），按账户站点实时渲染红绿库存灯与准确币种价格；支持硬件配置选择器（内存 / 硬盘 / 带宽 / vRack）与后端购物车实时询价兜底 |
| **抢购队列与预上架** | 支持按「机型 × 机房 × 数量 × 账户」创建独立任务；支持预上架型号提前挂单、优惠码自动应用、Fail-Fast 严选配置（绝不降配）、分阶段耗时打点与任务随时暂停/编辑 |
| **补货监控与自动下单** | 同时支持独服（Eco / Dedicated）与 VPS（实时目录防停售失效）补货监控；检查间隔 `5–3600s` 可配；支持仅通知或触发自动下单，编辑订阅不重置历史库存状态 |
| **多通道与 Telegram 交互** | 支持 **Telegram Bot 长轮询**（无需公网 HTTPS 域名与证书）+ **自定义 Webhook**（钉钉 / 飞书 / Bark 等）多通道冗余；支持 TG 文本指令下单与带一次性防重放令牌的「一键下单」内联按钮 |
| **独服全生命周期控制** | 概览监控与 MRTG 流量图、续费价格与配置安装费展示、启动状态（救援 / 正常及是否已生效）追踪、电源与一键救援系统、官方全能力自定义重装（OS 定制项 / SSH 密钥 / 后置脚本 / 软硬 RAID / LVM / ZFS）、IPMI / KVM / SOL、BIOS、防火墙、vRack、Backup FTP、维护工单与联系人变更 |
| **VPS 全生命周期控制** | 开关机、系统重装、快照管理、在线控制台、重置密码、反向 DNS、自动备份与续费策略调整 |
| **账单、钱包与官方客服** | 账户余额钱包卡（支持余额支付、预付款充值订单生成与券码兑换）、未付款订单页内核对发起支付、账单/退款/邮件记录、官方客服工单管理与官方在线客服一键直连 |
| **隐私与安全运维** | 隐私模式一键打码 IP / MAC / 反解域名；优雅停机（SIGTERM 刷盘落库）；容器环境防误触自更新保护 |

---

## 快速开始

### 方式一：Docker Compose（推荐）

> **持久化提示**：所有数据（SQLite 数据库 `sniper.db`、自动生成的数据库加密密钥 `.env`、缓存与日志）全部保存在 `./data` 目录中。备份与迁移时请务必完整保留该目录。

1. 准备配置与环境变量：

```bash
git clone https://github.com/w0ven/ovh-console.git
cd ovh-console
cp .env.example .env
```

2. 生成随机访问密钥并写入 `.env` 中的 `API_SECRET_KEY`：

```bash
openssl rand -hex 32
```

3. 启动服务：

```bash
docker compose up -d
```

服务默认监听 `127.0.0.1:20000`（强烈建议置于 Nginx / Caddy 等反向代理与 HTTPS 之后对外提供访问）。

**升级到最新版本：**

```bash
docker compose pull && docker compose up -d
```

<details>
<summary>使用 <code>docker run</code> 直接启动</summary>

```bash
docker run -d --name ovh-console \
  -p 127.0.0.1:20000:20000 \
  -v "$PWD/data:/data" \
  -e API_SECRET_KEY="替换为你的强随机密钥" \
  -e TZ="Asia/Shanghai" \
  --restart unless-stopped \
  ghcr.io/w0ven/ovh-console:latest
```

> 容器启动入口脚本会自动将 `/data` 权限调整为 `PUID:PGID`（默认 `10001:10001`）后降权运行。

</details>

---

### 方式二：从源码编译单二进制

前置依赖：`Node.js 22+`、`Go 1.26+`（使用 `CGO_ENABLED=0` 纯 Go SQLite 驱动，无需 `gcc` 或外部 SQLite 库）。

```bash
# 1. 构建前端并输出至 server/web/
cd web
npm ci
npm run build

# 2. 编译嵌入前端的静态单二进制
cd ../server
CGO_ENABLED=0 go build -tags ui -trimpath \
  -ldflags "-s -w -X github.com/ovh-buy/server/internal/handlers.Version=$(cat ../VERSION)" \
  -o ovh-server .

# 3. 运行（默认监听 :19998）
./ovh-server
```

交叉编译示例（如 Linux ARM64）：

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags ui -trimpath \
  -ldflags "-s -w -X github.com/ovh-buy/server/internal/handlers.Version=$(cat ../VERSION)" \
  -o ovh-server-linux-arm64 .
```

---

### 方式三：本地开发模式

```bash
# 终端 1：启动 Go 后端（默认 :19998）
cd server && go run .

# 终端 2：启动 Vite 前端开发服务器（默认 :19997，自动代理 /api/*）
cd web && npm install && npm run dev
```

---

## 首次配置与接入 OVH API

打开控制台后，需要依次通过两道安全引导：

1. **访问鉴权（AuthGate）**：输入你在 `.env` 中配置的 `API_SECRET_KEY`。
2. **添加 OVH 账户（OvhCredsGate）**：选择对应子公司（如 `IE` / `FR` / `US` / `CA` 等），填入 OVH API 的 `Application Key`、`Application Secret` 与 `Consumer Key`。

申请 OVH API Token 时，建议勾选以下完整权限并将有效期（Validity）设为 **Unlimited**：

```text
GET     /*
POST    /*
PUT     /*
DELETE  /*
```

> **注意**：EU（`eu.api.ovh.com`）、US（`api.us.ovhcloud.com`）与 CA（`ca.api.ovh.com`）三大站点的 API Token 互不通用，请务必在目标账户所属站点申请。

---

## 环境变量说明

完整模板见 [`.env.example`](.env.example)，常用配置项如下：

| 变量名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `API_SECRET_KEY` | *必填* | 控制台访问密钥，所有管理接口均需通过 `X-API-Key` 校验；同一 IP 连续错误 10 次触发 5 分钟冷却 |
| `OVH_DB_KEY` | 自动生成 | 数据库敏感字段（OVH 凭据、TG Token）AES-256-GCM 加密密钥；留空时首次启动自动生成并持久化到 `/data/.env`（切勿传空字符串） |
| `BIND_ADDR` | `127.0.0.1` | Docker Compose 宿主机绑定地址，生产环境建议保持回环地址并由反代接管 TLS |
| `HOST_PORT` | `20000` | Docker 宿主机映射端口（单二进制默认监听 `19998`） |
| `TZ` | `Asia/Shanghai` | 容器运行与日志时区 |
| `PUID` / `PGID` | `10001` | 容器降权运行所使用的 UID / GID |
| `CORS_ALLOWED_ORIGINS` | 空 | 反向代理或跨域访问时按需指定的允许来源 |
| `TRUSTED_PROXIES` | 空 | 可信反向代理 CIDR（用于正确解析客户端真实 IP 与限流） |
| `TG_ALLOWED_USER_IDS` | 空 | 群聊场景下允许触发 Telegram 下单的用户 ID 白名单（逗号分隔） |

---

## 技术架构与目录结构

| 层级 | 技术选型 |
| :--- | :--- |
| **前端** | React 18 · TypeScript · Vite 7 · TanStack Router & Query · Tailwind CSS · shadcn/ui · Recharts · react-i18next |
| **后端** | Go 1.26+ · Gin · 官方 `ovh/go-ovh` SDK |
| **存储与安全** | SQLite（`modernc.org/sqlite` 纯 Go / `mattn/go-sqlite3` CGO 自动切换）· AES-256-GCM 凭据落盘加密 |
| **通知与控制** | Telegram Long Polling（内置授权 / 幂等 / 限流 / 一次性按钮安全链）· 自定义 Webhook |

```text
.
├── .github/workflows/      # CI 测试与多架构 Docker 自动构建工作流
├── docs/                   # 性能调优与抢购引擎设计文档
│   ├── PERFORMANCE.md      # 预装购物车、连接预热与派发延迟说明
│   └── SNIPING-REDESIGN.md # 抢购架构设计说明
├── server/                 # Go 后端服务（Gin + SQLite + 监控/抢购/控制引擎）
│   ├── internal/
│   │   ├── app/            # 运行时状态聚合
│   │   ├── catalog/        # 多区公开目录、区域解析与安装费计算
│   │   ├── db/             # SQLite Schema 与数据访问层
│   │   ├── handlers/       # REST API 路由与处理器
│   │   ├── monitor/        # 独立服务器补货监控
│   │   ├── notify/         # Telegram 与 Webhook 多通道通知
│   │   ├── ovh/            # 多账户 OVH API 客户端路由工厂
│   │   ├── paycheck/       # 订单支付状态巡检
│   │   ├── purchase/       # 购物车、配置选项与高并发下单链路
│   │   ├── secret/         # AES-256-GCM 凭据加密与密钥管理
│   │   ├── updater/        # 版本检查与单二进制自更新
│   │   └── vps/            # VPS 实时目录与补货监控
│   └── main.go
├── web/                    # React + TypeScript 前端工程
│   └── src/
│       ├── components/     # 布局框架、鉴权门禁与业务组件
│       ├── hooks/          # TanStack Query 数据与状态 Hooks
│       ├── i18n/           # 模块化中英文语言包与错误码映射
│       └── routes/         # TanStack 文件路由页面
├── Dockerfile              # 多阶段构建（Node 22 + Go 1.27 + Alpine 3.20）
├── docker-compose.yml      # 生产级 Compose 编排配置（含日志轮转）
└── VERSION                 # 当前应用版本号
```

---

## 数据安全与运维注意事项

- **备份完整性**：数据库文件 `data/sniper.db` 与加密密钥 `data/.env`（或外部注入的 `OVH_DB_KEY`）必须**成对备份**。如果数据库含密文但丢失密钥，程序将主动拒绝启动以避免覆盖原有密文；确认无法找回时方可通过 `OVH_DB_KEY_RESET=1` 重置后重新录入账户。
- **下单与扣款机制**：抢购命中后默认生成待付款订单并保留在队列中（标记为「已下单」防重买），不会自动扣款；可在「账户 → 订单记录」或「抢购历史」中核对含税金额与支付方式后再确认付款（支持绑卡方式及 OVH 账户余额）。
- **Telegram 单实例约束**：同一个 Bot Token 仅允许单个控制台实例拉取 `getUpdates`，多实例共用会导致轮询互踢和消息丢失。
- **更多专题文档**：
  - 后端架构说明：[server/README.md](server/README.md)
  - 预装购物车与延迟优化：[docs/PERFORMANCE.md](docs/PERFORMANCE.md)
  - 抢购引擎设计：[docs/SNIPING-REDESIGN.md](docs/SNIPING-REDESIGN.md)

---

## 开源协议

本项目基于 [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE) 开源发布。
