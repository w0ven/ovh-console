<div align="center">

# OVH Console

**Self-Hosted Stock Monitor, Automated Ordering & Lifecycle Management Console for OVHcloud**

[![Version](https://img.shields.io/badge/version-v0.4.37-2563EB?style=for-the-badge)](https://github.com/w0ven/ovh-console/releases)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![React](https://img.shields.io/badge/React-18-61DAFB?style=for-the-badge&logo=react&logoColor=111827)](https://react.dev/)
[![Docker](https://img.shields.io/badge/Docker-linux%2Famd64%20%7C%20arm64-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://github.com/w0ven/ovh-console/pkgs/container/ovh-console)
[![License](https://img.shields.io/badge/License-AGPL--3.0-10B981?style=for-the-badge)](LICENSE)

[简体中文](README.md) · English

</div>

---

## Overview

**OVH Console** is a lightweight, self-hosted web console for managing OVHcloud Dedicated Servers, Eco range, and VPS instances. The React frontend is embedded directly into a single static Go binary via `//go:embed`, paired with a pure-Go SQLite engine for zero-dependency deployment.

It unifies **real-time availability monitoring & order automation** with **full server lifecycle operations**, featuring native **EU / US / CA multi-region support** and strict **multi-account isolation**.

<details>
<summary>Key Highlights</summary>

- **Single-Binary & Lightweight Container**: Embedded frontend + pure-Go SQLite (`CGO_ENABLED=0`), shipping for `linux/amd64`, `linux/arm64`, and `windows/amd64`, plus a minimal Alpine Docker image.
- **Native Multi-Region & Multi-Account Isolation**: Automatically adapts catalogs, currencies, datacenters, and regional cart rules across EU, US, and CA endpoints. Queues, monitors, and server controls are isolated per account.
- **Security by Design**: Mandatory `API_SECRET_KEY` gate with brute-force rate limiting, **AES-256-GCM** encryption at rest for OVH API credentials and Telegram tokens, and explicit checkout confirmation before charging.
- **Bilingual & Themeable UI**: Built-in English / Chinese interface (including localized backend error codes) with Light, Dark, and System theme modes.

</details>

---

## Feature Matrix

| Area | Capabilities |
| :--- | :--- |
| **Live Catalog & Availability** | Direct queries against regional OVH public availability APIs (60s freshness) with live datacenter stock indicators, subsidiary-accurate pricing, hardware option selector (RAM / Storage / Bandwidth / vRack), and cart-backed pricing fallback |
| **Order Queue & Preorder** | Independent queue tasks per model, datacenter, quantity, and account; supports pre-listing models, promo code application, strict fail-fast hardware matching (never downgrades), per-stage latency telemetry, and pause/edit workflows |
| **Restock Monitors** | Monitors both Dedicated/Eco servers and live-catalog VPS lines with configurable polling (`5–3600s`); supports notification-only or automated ordering, and preserves stock history when editing subscriptions |
| **Notifications & Telegram Bot** | Redundant delivery via **Telegram Long Polling** (no public domain or HTTPS webhook needed) and **custom Webhooks**; supports text-command ordering and single-use, replay-protected inline quick-order buttons |
| **Dedicated Server Control** | Hardware overview & MRTG traffic charts, renewal & configuration setup fee breakdown, boot state tracking (rescue vs. normal & whether applied), one-click rescue mode, full custom OS reinstall (SSH keys, post-install scripts, HW/SW RAID, LVM, ZFS), IPMI / KVM / SOL, BIOS, firewall, vRack, Backup FTP, maintenance tickets, and contact changes |
| **VPS Control** | Power actions, OS reinstall, snapshots, web console, password reset, reverse DNS, automated backups, and renewal management |
| **Billing, Wallet & Support** | Account wallet card (balance checkout, prepaid top-up order creation, voucher code redemption), in-app unpaid order payment review, order/refund/email logs, official support tickets, and direct OVH Live Chat launch |
| **Operations & Privacy** | One-click Privacy Mode to mask IPs, MACs, and rDNS hostnames; graceful SIGTERM shutdown with WAL flush; container-aware update protection |

---

## Quick Start

### Option 1: Docker Compose (Recommended)

> **Persistence Note**: All state — SQLite database (`sniper.db`), auto-generated database encryption key (`.env`), caches, and logs — lives inside `./data`. Always back up `./data` as a whole.

1. Clone the repository and prepare your environment file:

```bash
git clone https://github.com/w0ven/ovh-console.git
cd ovh-console
cp .env.example .env
```

2. Generate a strong secret and assign it to `API_SECRET_KEY` in `.env`:

```bash
openssl rand -hex 32
```

3. Start the container:

```bash
docker compose up -d
```

By default, the service binds to `127.0.0.1:20000`. Place it behind a reverse proxy (Nginx / Caddy) with HTTPS for remote access.

**Upgrade to the latest image:**

```bash
docker compose pull && docker compose up -d
```

<details>
<summary>Run directly with <code>docker run</code></summary>

```bash
docker run -d --name ovh-console \
  -p 127.0.0.1:20000:20000 \
  -v "$PWD/data:/data" \
  -e API_SECRET_KEY="replace-with-a-strong-random-key" \
  -e TZ="UTC" \
  --restart unless-stopped \
  ghcr.io/w0ven/ovh-console:latest
```

> The entrypoint automatically adjusts `/data` ownership to `PUID:PGID` (default `10001:10001`) and drops root privileges via `su-exec`.

</details>

---

### Option 2: Build Single Binary from Source

Requirements: `Node.js 22+` and `Go 1.26+` (pure-Go SQLite via `CGO_ENABLED=0`; no `gcc` or system SQLite required).

```bash
# 1. Build frontend assets into server/web/
cd web
npm ci
npm run build

# 2. Build static Go binary with embedded UI
cd ../server
CGO_ENABLED=0 go build -tags ui -trimpath \
  -ldflags "-s -w -X github.com/ovh-buy/server/internal/handlers.Version=$(cat ../VERSION)" \
  -o ovh-server .

# 3. Run (listens on :19998 by default)
./ovh-server
```

---

### Option 3: Local Development

```bash
# Terminal 1: Go backend (default :19998)
cd server && go run .

# Terminal 2: Vite dev server (default :19997, proxies /api/* automatically)
cd web && npm install && npm run dev
```

---

## Initial Setup & OVH API Credentials

On first visit, complete the two setup gates:

1. **AuthGate**: Enter the `API_SECRET_KEY` configured in `.env`.
2. **OvhCredsGate**: Select your OVH subsidiary (`IE`, `FR`, `US`, `CA`, etc.) and provide your `Application Key`, `Application Secret`, and `Consumer Key`.

When generating your OVH API token, grant the following rights and choose **Unlimited** validity:

```text
GET     /*
POST    /*
PUT     /*
DELETE  /*
```

> **Region Note**: Credentials are not interchangeable across `eu.api.ovh.com`, `api.us.ovhcloud.com`, and `ca.api.ovh.com`. Generate tokens on the exact endpoint matching your account subsidiary.

---

## Environment Variables

See [`.env.example`](.env.example) for the complete template:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `API_SECRET_KEY` | *Required* | Access secret required in `X-API-Key`; 10 consecutive failures from the same IP trigger a 5-minute lockout |
| `OVH_DB_KEY` | Auto-generated | AES-256-GCM key encrypting OVH credentials and Telegram tokens in SQLite; auto-persisted to `/data/.env` if unset (do not pass an empty value) |
| `BIND_ADDR` | `127.0.0.1` | Host bind address in Docker Compose |
| `HOST_PORT` | `20000` | Published host port for Docker (standalone binary defaults to `19998`) |
| `TZ` | `Asia/Shanghai` | Container timezone |
| `PUID` / `PGID` | `10001` | Non-root UID/GID used inside the container |
| `CORS_ALLOWED_ORIGINS` | Empty | Allowed CORS origins when served behind a proxy |
| `TRUSTED_PROXIES` | Empty | Trusted proxy CIDRs for client IP resolution |
| `TG_ALLOWED_USER_IDS` | Empty | Comma-separated Telegram user IDs allowed to place orders in group chats |

---

## Architecture & Repository Layout

| Layer | Stack |
| :--- | :--- |
| **Frontend** | React 18 · TypeScript · Vite 7 · TanStack Router & Query · Tailwind CSS · shadcn/ui · Recharts · react-i18next |
| **Backend** | Go 1.26+ · Gin · Official `ovh/go-ovh` SDK |
| **Storage & Security** | SQLite (`modernc.org/sqlite` pure Go / `mattn/go-sqlite3` CGO) · AES-256-GCM at-rest encryption |
| **Notifications** | Telegram Long Polling (actor auth, `update_id` idempotency, rate limit, one-time buttons) · Custom Webhooks |

```text
.
├── .github/workflows/      # CI checks and multi-arch Docker image publishing
├── docs/                   # Performance tuning & sniping architecture docs
├── server/                 # Go backend (Gin + SQLite + monitor/purchase/control)
│   └── internal/
│       ├── app/            # Shared application state
│       ├── catalog/        # Multi-region catalog, region & setup-fee resolution
│       ├── db/             # SQLite schema & CRUD layer
│       ├── handlers/       # REST API handlers
│       ├── monitor/        # Dedicated server stock monitor
│       ├── notify/         # Telegram & Webhook delivery
│       ├── ovh/            # Multi-account OVH API client factory
│       ├── paycheck/       # Order payment status watcher
│       ├── purchase/       # Cart preparation & order execution pipeline
│       ├── secret/         # AES-256-GCM encryption & key management
│       ├── updater/        # Release check & standalone binary self-updater
│       └── vps/            # VPS live catalog & restock monitor
├── web/                    # React + TypeScript frontend
├── Dockerfile              # Multi-stage build (Node 22 + Go 1.27 + Alpine 3.20)
├── docker-compose.yml      # Production Compose file with log rotation
└── VERSION                 # Application version
```

---

## Security & Operational Notes

- **Pairwise Backups**: Always back up `data/sniper.db` together with `data/.env` (or your external `OVH_DB_KEY`). If encrypted records exist without the key, startup intentionally halts to protect existing ciphertexts; only use `OVH_DB_KEY_RESET=1` when intentionally discarding unrecoverable credentials.
- **Order & Payment Safety**: Automated orders stop at checkout and remain unpaid until you explicitly review and pay them in OVH or via the in-app payment modal (supporting saved payment methods and OVH prepaid balance).
- **Single Telegram Poller**: Only one instance should poll a given Telegram Bot Token at a time; concurrent pollers kick each other off `getUpdates`.
- **Additional Documentation**: [server/README.md](server/README.md) · [docs/PERFORMANCE.md](docs/PERFORMANCE.md) · [docs/SNIPING-REDESIGN.md](docs/SNIPING-REDESIGN.md)

---

## License

Licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE).
