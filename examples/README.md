# 部署指南 / Deployment guide

两个 compose 示例，外加一个可选 procedure：

- [`../compose.example.yml`](../compose.example.yml)：已有媒体栈时，只加 Bridge 一个容器
- [`full-stack.compose.yml`](full-stack.compose.yml)：从零起一套参考栈（Seerr + Radarr + Sonarr + Prowlarr + Bridge），版本钉自真实生产部署
- [`1337x-cloudflare-bypass.md`](1337x-cloudflare-bypass.md)：可选 procedure，Prowlarr 的 1337x 被 Cloudflare 拦死时的旁路方案（FlareSolverr + byparr-proxy，2026-11-24 实测跑通）

## 前提

- x86_64 Docker 主机；115 账号（首次启动用 115 手机 App 扫 Web 页二维码登录 TV 端）
- 一个媒体卷：下载目录与媒体库同卷，Arr 与 Bridge 容器内路径一致（示例为 `/data`）；跨主机用 NFS 挂同一路径，不需要 remote path mapping
- Bridge 的 `/config` 用本地目录（SQLite、cookie、config.yml），不要放网络盘

## 步骤

1. 建目录：`/path/to/configs/115-bridge/`（或你的路径），放入 `config.yml`（参照 [`../config.example.yml`](../config.example.yml)：改 `webui.username` 与 `arr.*.url`）
2. 放密钥：复制 [`../bridge.env.example`](../bridge.env.example) 为 `bridge.env`，填 `WEBUI_PASSWORD`、`RADARR_API_KEY`、`SONARR_API_KEY`（Telegram 可选，两项同填或同空）；`chmod 600 bridge.env`
3. 取镜像：Release 附件 `sha256sum -c SHA256SUMS` 后 `docker load -i 115-bridge-image-0.1.0.tar.gz`；或源码 `docker build -t 115-bridge:0.1.0 .`
4. 改 compose 占位路径后 `docker compose up -d`
5. 浏览器开 `http://<主机>:8080/`，用 `webui.username` + `WEBUI_PASSWORD` 登录，扫码完成 115 TV 端登录
6. Radarr/Sonarr 各添加 qBittorrent 客户端：主机为 Bridge 地址、端口 8080、同上凭据，category 分别 `radarr` / `tv-sonarr`（与 config.yml 的 `categories` 一致），开启 **Remove Completed**；用 Test 验证
7. Seerr 连接 Radarr/Sonarr；Prowlarr 添加索引器并同步到两 Arr
8. 用小请求走一遍：Seerr → Arr 抓取 → Bridge 离线/取回/校验 → Arr 导入 → 下载目录自动清理

## 注意

- NAS 共享文件夹若只授权特定组，按实际 gid 设置 `PGID`（否则启动时建目录失败）
- `PUID/PGID/UMASK` 决定新文件属主与权限，需与媒体卷现有文件及 Arr 的运行身份兼容
- 若给 Telegram 设代理，必须同时设 `NO_PROXY` 覆盖局域网与 127.0.0.1
- Web 页只读（除重新生成二维码）；任务操作统一在 Arr 里做
- 115 云端文件与离线任务**永不删除**（设计约束）；本地下载份由 Remove Completed 清理
- 回滚：保留旧镜像与 compose 备份即可换回；云端数据不受影响

## 安全

- 服务只应暴露于局域网；密钥只经环境变量/0600 env 文件，不进配置 YAML、不进镜像、不进日志
- 单实例：同一 `/config` 不允许两个 Bridge 进程（flock + lease）
