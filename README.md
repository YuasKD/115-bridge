# 115 Bridge

> **Seamless 115 Cloud Offline Downloader for Radarr & Sonarr**  
> **让 Radarr / Sonarr 无缝接入 115 网盘离线下载的自托管桥梁**

**115 Bridge** disguises itself as a standard **qBittorrent client** (WebAPI 2.11.2) for Radarr and Sonarr. Under the hood, it offloads torrent/magnet tasks to **115 Cloud Offline Download**, pulls completed files back to local storage, verifies every file's `SHA-1` hash, and hands them off to the *Arr stack for atomic import. If a release stalls or fails, it automatically triggers Arr's queue API to blocklist the release and search for a replacement.

**115 Bridge** 对 Radarr / Sonarr 伪装成标准的 **qBittorrent 下载客户端**（WebAPI 2.11.2）。在后台，它自动将磁力/种子提交至 **115 离线下载**，待云端秒传或下载完成后高速取回本地，经过逐文件 `SHA-1` 校验并原子重命名后，交由 *Arr 队列完成导入。遇到死种或下载失败时，自动调用 Arr 接口拉黑当前版本并触发重搜。

---

## Architecture / 工作流

```text
Seerr ──[Request]──> Radarr / Sonarr ──[qBit API]──> 115 Bridge ──[Offline & Fetch]──> 115 Cloud
                            ▲                             │
                            └───[Atomic Import/Hardlink]──┘  (/data/downloads/115/<category>/)

```

* **Zero Credential Exposure**: Only 115 Bridge holds your 115 session cookie (via TV-end QR login).
* **Native Arr Experience**: Identity matching, hardlinking, renaming, quality upgrades, and notifications remain 100% handled by Radarr/Sonarr.
* **凭据隔离**：仅本容器持有 115 登录态（支持电视端扫码登录），与媒体库工具完全解耦。
* **原生体验**：影片刮削、硬链接导入、重命名、洗版升级与通知推送均完全沿用 *Arr 原生工作流。

---

## Key Features / 核心特性

* **qBittorrent WebAPI Compatibility / 高度兼容 qBittorrent 接口**
Implements the exact WebAPI subset required by Radarr/Sonarr, including end-to-end support for *Remove Completed* cleanup.
完整实现 Arr 依赖的核心接口，端到端支持“下载完成后自动移除任务”。
* **Smart 115 Cloud Offloading / 智能云端离线调度**
Supports BTIH idempotency (deduplication), instant-restore (秒传) indexing, 12-hour task timeout, quota/storage alerts, and TV-end QR code login.
支持 BTIH 幂等去重、秒传索引命中、12 小时超时控制、配额与空间告警，内置 Web 端电视扫码登录。
* **Resilient Multi-Connection Fetcher / 高可靠断点续传拉取**
2 concurrent connections per file with HTTP Range requests, checkpoint resumption, and strict `206` / `Content-Range` / `Content-Length` validation (guaranteed never to write beyond expected byte size).
单文件双线程分片拉取，支持断点续传与严格的响应头及字节边界校验，杜绝脏数据写入。
* **Strict Verification & Atomic Landing / 严格校验与原子落盘**
Verifies per-file size and `SHA-1` checksums before performing an atomic same-volume rename. Arr is never notified of completion until integrity is 100% confirmed.
逐文件核对大小与 `SHA-1`，全部通过后才执行同卷原子重命名并通知 Arr 导入，彻底避免导入损坏文件。
* **Self-Healing & Global Pause / 自愈重试与全局熔断**
Bounded monotonic retries per stage; automatically blocklists and re-searches via Arr upon permanent failure. Global issues (auth expiry, risk control, low disk space) pause the queue rather than falsely failing tasks.
分阶段有限次单调重试，彻底失败时自动联动 Arr 拉黑重搜；遇到风控、掉登录或磁盘不足等全局异常时自动挂起队列，避免误杀正常任务。
* **Modern Web UI & Telegram Alerts / 轻量面板与消息推送**
Responsive dashboard with dark/light themes, 5-second live polling, mobile card view, 7-day task history, and optional send-only Telegram notifications for QR logins and alerts.
自带响应式 Web 界面（深浅色主题、5 秒无感刷新、移动端适配、7 天历史记录），支持可选的 Telegram 单向告警与二维码推送。
* **Rock-Solid Concurrency / 单实例与数据安全**
Enforced via process `flock` + SQLite lease, backed by a SQLite WAL state machine.
通过进程文件锁与 SQLite 租约机制保证单实例运行，基于 SQLite WAL 模式持久化任务状态机。

---

## Quick Start & Deployment / 部署指南

### Deployment Options / 部署方式

* **Add to existing stack / 接入现有媒体栈**: [`compose.example.yml`](https://www.google.com/search?q=compose.example.yml)
* **Full reference stack / 完整全家桶示例** *(Seerr + Radarr + Sonarr + Prowlarr + Bridge)*: [`examples/full-stack.compose.yml`](https://www.google.com/search?q=examples/full-stack.compose.yml)
* **Detailed step-by-step guide / 详细图文部署教程**: [`examples/README.md`](https://www.google.com/search?q=examples/README.md)

### Critical Configuration Notes / 关键配置说明

1. **Path Consistency (路径一致性)**: Mount `/data` to the exact same host path and volume as Radarr/Sonarr so hardlinks work out of the box without Remote Path Mappings. (`/data` 需与 Arr 保持完全一致的挂载卷与路径，无需配置远程路径映射)。
2. **Local Config (配置目录)**: Keep `/config` on local disk/SSD for SQLite WAL performance. (`/config` 请挂载在本地磁盘以保障 SQLite 性能)。
3. **Secrets & Permissions (密钥与权限)**: Store secrets exclusively in `bridge.env` (`chmod 0600`). On NAS systems using group ACLs, set `PGID` to match your media share group. (敏感信息仅通过 `bridge.env` 传入；NAS 用户请根据共享文件夹 ACL 设置对应的 `PGID`)。
4. **First Boot (首次启动)**: Open the Web UI to scan the TV-end QR code and authenticate your 115 account. (启动后打开 Web UI 扫描电视端二维码完成 115 登录)。

### Offline / Release Tarball Installation / 离线包安装

Download the `docker save` image tarball and source tarball from [suspicious link removed]:

```bash
sha256sum -c SHA256SUMS
docker load -i 115-bridge-*.tar.gz

```

---

## Engineering Principles / 设计约束

To ensure safety and predictability, this codebase strictly adheres to the following invariants:

1. **Isolation**: Only `cloud/session.py` is permitted to import `p115client`.
2. **Non-Destructive Cloud**: Never deletes files or offline tasks on your 115 Cloud account.
3. **Monotonic Retries**: Retry counters only increment; infinite retry loops are impossible.
4. **Fault Isolation**: Global pauses (auth/quota/disk) never mark individual tasks as failed.
5. **Strict Typing**: 115 IDs are strictly handled as `str`; hashes are normalized to lowercase hex.
6. **Zero Secret Leaks**: Credentials and cookies never appear in logs, configs, or container images.
7. **Verified Cloud Completion**: Offline completion requires both `status == 2` and a non-empty `file_id`.
8. **Ephemeral Links**: Download URLs are never persisted; fetched fresh per request alongside their bound `User-Agent`.
9. **Byte-Exact Writes**: Local writer strictly enforces expected byte boundaries.
10. **Atomic Visibility**: Files move to the final Arr-watched directory only after passing all `SHA-1` checks.
11. **Offline Test Suite**: Unit and integration tests never touch the live network.

---

## Development / 本地开发

```bash
uv venv --python 3.12 && uv sync --extra dev
pytest -q                                # Run fully offline test suite
ruff check . && ruff format --check .    # Lint and format check
mypy src                                 # Static type checking

```

---

## Acknowledgements / 鸣谢

* [p115client](https://github.com/ChenyangGao/p115client) — The foundational 115 API client powering this project. 本项目基于 p115client 构建，特此感谢原作者。
* The **Radarr**, **Sonarr**, and **qBittorrent** communities for well-documented client APIs.

---

## Disclaimer / 免责声明

This project is built by a **non-professional developer** via **100% vibe coding** using **Claude Opus 5.5 + Sonnet 5.5 + Qwen4 MAX**. It is provided *"as-is"* without warranties, professional maintenance, or SLA commitments. Please review the configuration and back up your data before deploying in production.

本项目由非专业开发者完全通过 **Vibe Coding**（Claude Opus 5.5 + Sonnet 5.5 + Qwen4 MAX）构建完成。软件按“现状”提供，不包含任何专业维护或长期支持承诺。使用风险自负，请在用于生产环境前做好数据备份与配置审阅。

---

## License / 许可

Licensed under the [MIT License](https://www.google.com/search?q=LICENSE).
