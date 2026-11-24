# 115 Bridge

A self-hosted container that presents itself to Radarr/Sonarr as a **qBittorrent download client** (WebAPI 2.11.2), while actually downloading through **115 offline (cloud) download**, then fetching the files back to local storage, verifying them per-file with sha1, and handing them to Arr for import. When a release fails, it calls Arr's queue API to blocklist and re-search.

自托管容器：对 Radarr/Sonarr 伪装成 qBittorrent 下载客户端，背后走 115 离线下载，再把文件取回本地、逐文件 sha1 校验后交给 Arr 导入；失败时主动调用 Arr 队列接口拉黑并重搜。

```
Seerr ──request──> Radarr/Sonarr ──qBit API──> 115 Bridge ──offline/fetch──> 115
                        ^                          │
                        └──── import (move/hardlink)┘  /data/downloads/115/<category>/
```

Only this process holds the 115 credential (TV-end QR-login cookie). Identity matching, import, renaming, upgrades and notifications stay with Arr's standard flow.

## Features / 功能

- qBittorrent WebAPI compatible subset (the endpoints Arr actually calls); Remove Completed works end to end
- 115 offline submit & tracking: BTIH idempotency, instant-restore index, 12 h total timeout, quota & space alerts, TV QR login
- Fetch: 2 connections per file, ranged segments, resume from checkpoints, strict 206/Content-Range/Content-Length validation, never writes beyond expected size
- Verify & land: per-file size + sha1, atomic same-volume rename only after all checks pass; Arr is never told "completed" before that
- Bounded retries (per task, per stage, monotonic) + Arr blocklist/re-search; global problems (auth/risk-control/quota/space) pause instead of failing tasks
- Web UI: dark/light themes, 5 s in-place polling, mobile card layout, failed/completed history sections (7 days by default), TV QR login
- Optional Telegram push for session alerts and QR codes (send-only)
- Single-instance guarantee: process flock + SQLite lease; SQLite WAL task state machine and events

## Design constraints / 设计约束

1. Only `cloud/session.py` imports p115client
2. Never deletes files or offline tasks on 115
3. Retry counters only increase
4. Global pause is not task failure
5. 115 ids are strings; hashes are lowercase hex
6. Secrets never enter logs/config/image
7. Offline completion means `status == 2` and non-empty `file_id`
8. Download links are never stored; re-fetched per request with their UA
9. Never writes more bytes than expected
10. Rename to the final directory only after all verification passes
11. Tests never touch the real network

## Deploy / 部署

- Single container into an existing stack: [`compose.example.yml`](compose.example.yml)
- Full reference stack (Seerr + Radarr + Sonarr + Prowlarr + Bridge, versions pinned from a production deployment): [`examples/full-stack.compose.yml`](examples/full-stack.compose.yml)
- Step-by-step: [`examples/README.md`](examples/README.md)

Key points: `/data` must be the same volume and same path as Arr sees (no remote path mapping needed); `/config` on local disk; secrets only via `bridge.env` (0600); on NAS systems whose share ACL grants a specific group, set `PGID` accordingly; first start shows a QR code in the Web UI for TV-end login.

Release assets provide a `docker save` image tarball and a source tarball; verify with `sha256sum -c SHA256SUMS` then `docker load`.

## Development / 开发

```bash
uv venv --python 3.12 && uv sync --extra dev
pytest -q                      # fully offline tests
ruff check . && ruff format --check .
mypy src
```

## Acknowledgements / 鸣谢

- [p115client](https://github.com/ChenyangGao/p115client) — the 115 API client this project builds on. 本项目基于 p115client 构建，感谢该项目及其作者。
- Radarr/Sonarr/qBittorrent communities for documented client APIs.

## Disclaimer / 声明

This project is built by a **non-professional developer**, fully **vibe coding** with **Claude Opus 5.5 + Sonnet 5.5 + Qwen4 MAX**. It is provided as-is, without professional maintenance or support commitments. 本项目由非专业开发者以 fully vibe coding 方式完成（Claude Opus 5.5 + Sonnet 5.5 + Qwen4 MAX），按现状提供，无专业维护与支持承诺。使用风险自负；请自行备份并审阅后再用于生产。

## License / 许可

MIT, see [LICENSE](LICENSE).
