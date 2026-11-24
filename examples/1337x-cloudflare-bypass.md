# 可选 procedure：1337x Cloudflare 旁路（FlareSolverr + Prowlarr + byparr-proxy）

2026-11-24 实测跑通（Prowlarr v2.3.5 + FlareSolverr v3.5.0）。给 [`full-stack.compose.yml`](full-stack.compose.yml) 这类带 Prowlarr 的栈补 1337x 索引器时用。

## 适用场景

- Prowlarr（或 Jackett）已配 FlareSolverr，1337x 等站仍报 `Unable to access 1337x.to, blocked by CloudFlare Protection`。
- FlareSolverr 日志明明 `Challenge solved!`，Prowlarr 还是失败。
- 尤其是多出口代理环境（路由器级多节点负载均衡，出口 IP 漂移）。

## 根因

1. Prowlarr 的 FS 集成（`FlareSolverr.cs`）流程：直连撞挑战页 → 交给 FS 解题 → **丢弃 FS 抓回的页面**，只取 cookie 和 UA → 用自己的 .NET HttpClient 带凭据重放 → 解析重放结果。
2. `cf_clearance` 绑定出口 IP（及浏览器 TLS 指纹）。重放客户端的出口与 FS 浏览器不一致时必被再拦。
3. 多节点代理下每条连接的出口 IP 可能不同，FS 与 Prowlarr 抽到不同节点，重放必死。实测同一主机两轮测量出口在不同国家、不同地址族之间漂移。
4. 已验证的死路，别再试：
   - 升级 Prowlarr / FlareSolverr：架构未变，无「直接用 FS 响应」选项。
   - `PUT /api/v1/indexer/{id}?forceSave=true`：只绕过保存前测试，运行时搜索走同一条重放链路，实测 0 结果。
   - 换 Jackett：同样的 cookie 重放架构，同样死。
   - 关 IPv6：只对 [Prowlarr issue #2810](https://github.com/Prowlarr/Prowlarr/issues/2810) 那种「FS 走 IPv4、Prowlarr 走 IPv6」的单一错配有效，对多节点漂移无效。
   - 1337x 全部官方镜像（1337x.st、x1337x.ws、x1337x.eu、x1337x.cc、unblockit）都在 CF 后。

## 方案

[byparr-proxy](https://github.com/guyg2232/byparr-proxy)：约 250 行纯 Python 标准库的本地 HTTP 透传代理，钉 commit `32d2565f3986f0d4f7e0673f32e3b32cf1d347dd`（2026-06-01）。

- Prowlarr 用纯 HTTP 访问代理（indexer Base URL 改为 `http://byparr-proxy:8888/`）。
- 代理调 FS 的标准 `/v1` `request.get`，**直接返回 `solution.response`**。
- 没有重放，出口漂移无所谓；Prowlarr 永远只看到干净 200，CF 检测不触发。
- **可复用现有 FlareSolverr**（API 兼容），不必部署 Byparr。
- 自带：结果页 1 小时缓存、并发请求合并、浏览器串行锁、静态资源 404、`/cat/` 测试路径 stub。

```
Prowlarr ──纯 HTTP──> byparr-proxy ──POST /v1──> FlareSolverr ──真实浏览器──> 1337x（过 CF）
         <──200 真实页面──            <──solution.response──
```

## 前提

- Docker + compose；Prowlarr 与 FlareSolverr 在同一 docker 网络。
- FS 本身能解目标站挑战。验证：`POST /v1` `request.get` 目标 URL，`status: ok` 且 `solution.response` 是真实内容。
- FS 代理 `requestTimeout` 建议 ≥120 秒（1337x 解题实测 30 到 60 秒波动，60 秒会超时误报）。

## 步骤

1. 克隆并钉哈希（放进你的媒体栈 compose 目录）：

   ```bash
   git clone https://github.com/guyg2232/byparr-proxy.git
   git -C byparr-proxy checkout 32d2565f3986f0d4f7e0673f32e3b32cf1d347dd
   ```

2. compose 加服务（先备份原 compose 文件）。只挂内网，不对宿主暴露端口：

   ```yaml
   byparr-proxy:
     build: ./byparr-proxy
     container_name: byparr-proxy
     restart: unless-stopped
     environment:
       - UPSTREAM=https://1337x.to
       - BYPARR=http://flaresolverr:8191/v1
       - TIMEOUT_MS=120000
       - PORT=8888
     networks:
       - media-net   # 换成你栈里 Prowlarr 所在的网络
   ```

3. `docker compose config --quiet && docker compose up -d byparr-proxy`。

4. 冒烟测试（从 prowlarr 容器内，首次约 15 秒含解题）：

   ```bash
   docker exec prowlarr curl -s -m 150 "http://byparr-proxy:8888/search/The%20Wandering%20Earth/1/"
   ```

   期望：200，真实标题 `Download The Wandering Earth Torrents | 1337x`，含 `/torrent/` 链接。

5. 自定义定义放进 Prowlarr 配置目录的 **`Definitions/Custom/` 子目录**：

   ```bash
   mkdir -p <prowlarr-config>/Definitions/Custom
   cp byparr-proxy/definitions/1337x-byparr.yml <prowlarr-config>/Definitions/Custom/
   ```

   坑：v2.3.5 只从 `Custom/` 读自定义定义，放 `Definitions/` 根目录会被静默忽略（那里是官方定义包 `indexers.prowlarr.com` 的解压镜像）。`Custom/` 动态读取，放好即识别，无需重启。用 `GET /api/v1/indexer/schema` 查 `definitionName == "1337x-byparr"` 确认。

6. 添加 indexer：UI 搜 "1337x (via Byparr)"，或 API 取 schema 模板改 `name`、`enable=true`、`appProfileId` 后 `POST /api/v1/indexer`。**不打 flaresolverr tag**（代理已处理，Prowlarr 不需要再走 FS 集成）。保存测试走 `/cat/` stub，会通过。

7. 等自动同步到 Radarr/Sonarr（条目名 "1337x (byparr) (Prowlarr)"，三个开关应全开）。如有旧的不可用 1337x 条目，`DELETE /api/v1/indexer/<旧id>` 删除，会级联删除 Arr 侧同步副本。注意 Prowlarr 重启后的全量同步可能把 Arr 侧残留条目重新启用，删除后要核对 Arr 侧状态。

8. 验证：
   - Prowlarr `GET /api/v1/search?query=The Wandering Earth&type=search&indexerIds=<新id>`：应出真实结果。
   - 取任一结果的 `downloadUrl` 请求：应 301 到 `magnet:?xt=urn:btih:...`。这就是 Arr 抓取后推给下载客户端的东西。

## 注意与坑

- **健康测试是合成的**：代理默认对 `/cat/` 路径返回 stub 页（每大类一条 0 做种假行，分类页 CF 防护比搜索页凶，真测会误报故障）。绿灯不代表站点实时可用，真实状态看 `docker logs byparr-proxy` 和实际搜索。`STUB_CAT_PATHS=false` 可关掉 stub。
- **首次搜索慢**：1337x 的 magnet 在详情页，每条结果过一次 FS（解题后 cookie 复用，单条约 1 到 3 秒，定义 `requestDelay: 3`）。结果页缓存 1 小时，重复搜索快。太慢可把定义里 `requestDelay` 调低，自行权衡限流风险。
- **第三方代码**：部署前审计。该 commit 纯标准库、无外部依赖、只访问配置的 BYPARR 端点，无遥测。务必钉哈希。
- **中文内容**：1337x 只认英文片名。华语电影覆盖好（流浪地球实测 66 条含 4K REMUX），华语剧集有限（庆余年 Qing Yu Nian / Joy of Life 2019 实测搜不到）。中文剧集可另配 dmhy（动漫向）或私有站。
- 代理内存约 30MB；FS 已在跑，无新增浏览器开销。

## 回滚

禁用或删除新 indexer → compose 移除服务 `docker compose up -d` → 删 `Custom/1337x-byparr.yml`。无其他副作用，FS 与其余 indexer 不受影响。

## 参考

- byparr-proxy：<https://github.com/guyg2232/byparr-proxy>（commit 32d2565）
- Prowlarr issue #2810：IPv4/IPv6 错配案例与关 IPv6 修法（多节点漂移不适用）
- FlareSolverr Troubleshooting：<https://github.com/FlareSolverr/FlareSolverr/wiki/Troubleshooting>
- 备选 trackarr：<https://github.com/chnorrz/trackarr>（Torznab shim，持久 Camoufox 浏览器，magnet 懒加载，支持 1337x/ext.to/EZTV，更重）