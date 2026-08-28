# xet-pd 设计文档：基于 xet 的 P2P 交付（HF_ENDPOINT 兼容）

> 状态：Draft v0.3 · 依赖 [github.com/wzshiming/xet](https://github.com/wzshiming/xet) · 演进式设计：S0 最小可用单元 → S5，每步独立可发布

## 1. 目标与非目标

**目标**

- **对客户端零改造**：任何 HF 生态客户端（`huggingface_hub`、`hf` CLI、含/不含 `hf_xet`、`curl`）只需 `HF_ENDPOINT=http://<本机 agent>` 即透明加速。
- **集群内 P2P 数据面**：同一内容集群内只从源站拉一次，节点间以 **xorb 局部 range（segment）** 为单位互相交付。
- **端到端可验证**：chunk hash / term range hash / 文件 sha256 多层校验，坏 peer 数据可检出并回退。
- **优雅降级**：任何中心组件（tracker）或 peer 缺席只降效率，不损正确性；源站挂时已缓存内容照常可用。

**非目标（首期）**：上传路径 P2P（写直接代理上游）；私有仓库细粒度 ACL（集群 = 单一信任域）；跨集群联邦与公网 NAT 穿透。

## 2. 依赖：xet 已提供的能力

| xet 包 | 复用点 |
|---|---|
| `mirror` | HF_ENDPOINT 门面全套：resolve/token/tree/上游代理、spool 边下边播、sha256 校验、断点续传 |
| `server` + `storage` | CAS 数据面：`/v1/reconstructions/*`（V1/V2）、`/v1/xorbs/*`（Range）、chunk offset 表 |
| `client` + `hf` | 上游 xet 下载：resolve Link 头解析、token 自动续期、term 级 range 拉取、chunk cache |
| `token` | Issuer（Mint）铸造 xet 协议握手所需 token（数据面匿名只读，不作访问控制）；Validate 留给后续 tracker 鉴权 |
| `xorb`/`shard`/`download` | 编解码、merkle、chunk hash 计算、重建响应构造 |

## 3. 核心设计

### 3.1 关键洞察：控制面 / 数据面分离

xet 协议中重建响应（terms + 每 xorb 的 fetch URL）是控制面，对 URL 的 Range GET 是数据面。官方 hub 把数据面指向 CDN；**xet-pd 只需改写这些 URL 指向持有数据的 peer**，未修改的 `hf_xet` 客户端即可"最后一跳"直连 peer，agent 不经手字节流。

### 3.2 P2P 交付单元：segment（xorb 的局部 range）

**局部而非完整 xorb 是常态**，设计必须以此为前提：

1. **上游只授权局部**：源站重建响应按 term 发放 `(xorbHash, chunkRange)` 的签名 URL，客户端**无法也无需**获取完整的上游 xorb；
2. **去重的必然结果**：一个文件往往只引用某 xorb 的散落区间（其余 chunk 属于其他文件）；
3. **部分文件下载**：客户端对 resolve/reconstruction 带 Range，只触及部分 term；
4. **边下边分享**：ingest 进行中已落盘的部分天然是局部的。

因此 P2P 单元定义为 **segment = `(xorbHash, chunkStart, chunkEnd)`**（附对应字节区间）；**完整 xorb 只是"全覆盖 segment"的特例**（仅由本地全量 ingest 重新分块产生）。

关键性质：term 划分在内容上传时由 shard 固化，**同一文件在所有 peer 处产生相同的 segment 键集合** ⇒ 索引与服务以**精确键匹配**起步即正确；区间包含/相邻合并是后续优化，不是正确性前提。

| 关注点 | 设计 |
|---|---|
| 存储 | segment store（新增，xet-pd 自有包）：对象键 `{xorbHash}/{chunkStart}-{chunkEnd}`；与完整 xorb 库并存，完整 xorb 可服务任意子区间 |
| 服务 | `/v1/xorbs/{ns}/{hash}`：完整 xorb 走内置 server（Range/整取），`X-Xet-PD-Chunks` 段键由 agent 服务；无完整 xorb 时精确/包含匹配 segment；无覆盖 → 404，调用方走回退链 |
| 索引 | tracker 记 `xorbHash → peer → [ranges]`，按 term range 求覆盖 |
| 校验 | 段内逐 chunk 解码验 chunk hash；可得 term hash 时验 range hash；文件装配后验 sha256 终审；**仅全覆盖时**才可验 xorb merkle |
| 重建元数据 | 以 fileHash 缓存上游 terms/ranges 结构（**不含**签名 URL，URL 短命；chunk↔字节区间由 xorb 创建时固化，可长期缓存），改写时按来源可用性现填 URL |

### 3.3 内容键

| 键 | 来源 | 用途 |
|---|---|---|
| `sha256` | resolve ETag / 本地计算 | 文件级集群主键（xet 与 plain 上游通用） |
| `fileHash` | 上游 `X-Xet-Hash` / 本地 ingest | 重建查询键 |
| `xorbHash` + `chunkRange`（segment） | 上游重建响应 / 本地 ingest | P2P 数据面传输与索引键 |

### 3.4 来源回退链（每个 term 独立执行）

```
term 需要 (xorbHash, chunkRange)
  1. 本地：完整 xorb 或覆盖该 range 的 segment
  2. peer：tracker lookup（或静态 peers）→ 覆盖该 range 的 peer，健康分排序逐个试
     （超时默认 2s；校验失败 → 丢弃 + 记坏分 + 上报）
  3. 源站：xet 上游走 CAS/CDN 签名 URL；plain 上游走 HTTP Range
```

两段式选择（对标 Dragonfly Load-Aware Scheduling）：tracker 按 announce 负载快照粗排 top-k，agent/客户端逐 term 按本地健康分（成功率/吞吐/RTT，熔断冷却）精排。

## 4. 演进路线：最小实现单元与严格增量

**原则**：每阶段独立可发布、可回退；后一阶段只做加法（新增包/路由/配置），不推翻前一阶段；正确性从不依赖尚未实现的阶段。

```
xet-pd/
  cmd/xet-pd/   # 单二进制：agent / tracker 子命令（S0 只有 agent）
  agent/        # 组合层：mirror/CAS 组合、peer 选择、捕获传输层（来源链）、announcer
  tracker/      # S2 起：服务端 + 客户端 + file Backend（索引落盘为文件）
  segstore/     # S3 起：segment 存储与服务
  deploy/       # compose / K8s manifests（随阶段补充）
```

### S0 — 单机透明缓存（最小实现单元）

- **做什么**：把 `xetd` 的 mirror 组合打包为 `xet-pd agent`：`mirror.Handler`（resolve/token/tree/上游代理）+ `server.Handler`（CAS）+ `storage.FileStorage` + `token.Issuer`。对下游是**匿名只读面**：一切读免鉴权（握手 token 照发但不校验）；一切写操作（xorb/shard 上传、dedup 查询、hub commit/preupload、`xet-write-token`）在 agent 边界一律 403。
- **新代码**：仅组合、配置解析与只读门（`cmd/xetd/main.go` 已示范组合），**零新协议**。
- **验收**：`hf` CLI 冷（边下边播）/ 热（xet links）/ plain（`/xet-bridge`）三态字节一致；断源站后热内容可用。
- **价值**：立即可部署的单机 HF 缓存，是后续一切的宿主。

### S1 — peer 即上游（文件级 P2P，零新协议）

- **做什么**：新增 `--peers` 静态列表。resolve miss 时并发 HEAD 各 peer 的同路径 resolve（带 `X-Xet-PD-Local-Only: 1`，**对端只答本地 ready 内容，miss 一律 404 且不回源**——防递归）；命中者当作 upstream，复用 mirror 既有 xet ingest 全流程；全 miss → 源站。
- **本质**：agent 本身就是 hub 语义服务器，所以 peer 可直接充当上游，P2P 免费获得。A 从 B 拉取时已经按 term 取 B 完整 xorb 的**字节区间**（局部传输），但前提是 B 持有完整文件。
- **新代码**：upstream 选择器、防递归头（匿名只读面使 peer 读取无需任何互认凭证）。
- **验收**：双节点，第二节点冷下载时源站流量 ≈ 0；杀 peer 自动回退源站。

### S2 — tracker（文件级索引）

- **做什么**：tracker 子命令 + `announce`/`lookup`（`sha256/fileHash → peers`），替代 S1 的 O(peers) 广播探测；announce 兼作 keepalive；索引条目落盘为文件（与 agent 的 storage/mirror 索引同风格），重启从文件即时恢复。**tracker 缺席自动退回 S1（有 `--peers` 时）或 S0 行为**。
- **验收**：N 节点探测流量对比 S1 显著下降；kill tracker 后正确性不变；重启 tracker 后 lookup 立即可用（不等 announce 周期）。

### S3 — 段级 P2P（segment 一等公民，核心增量）

- **做什么**（as built）：
  1. **segstore**：文件落盘的 segment 存储（键 `(xorbHash, chunkStart-chunkEnd)` + meta JSON，写入做逐 chunk 头结构校验、keep-first 编码一致性）；agent 的 CAS xorb 路由：本地整 xorb → 内置 server；否则用覆盖该请求的 segment 服务（单段 byte Range 或 `X-Xet-PD-Chunks: s-e` 段键头）；miss → 404，调用方走回退链；
  2. announce/lookup 升级到段粒度：`xorbs:[{hash, ranges:[{start,end}]}]`（chunk 区间，端点排他；tracker 按 peer 合并区间，lookup 只返回**完全包含**查询区间的 peer，键 `"{hash}:{start}-{end}"`）；
  3. **ingest 侧 per-term 来源链**：agent 的 mirror 客户端装上捕获传输层（`client.WithHTTPClient` 注入 RoundTripper），解析上游重建响应（V1/V2/batch）记录每个 term 的 `(url, byteRange) → (xorbHash, chunkRange, fileHash)`，term 抓取按 **本地 segment → 本地整 xorb（编码字节长度一致时）→ tracker 定位的 peer（≤2 个，标记头 + 206 + 精确长度 + 结构校验）→ 源站** 逐级服务，对下游客户端字节不可辨；cache miss 时对单个 span 补发 lookup（负缓存 ~2s），使并发冷启动能中途发现对方的 announce；
  4. **边下边分享**：源站来源的 segment 捕获后立即增量 announce；**peer 来源的 segment 进隔离区（quarantined）**——不可服务、不可 announce——待该文件 ingest 的 sha256 判定：通过则释放并 announce，失败则删除 + 冷却该 peer + 失效客户端 chunk 缓存（防止重试从缓存重放坏 chunk）。
- **有意推迟**：下游重建响应 URL 改写（p2pAdapter 包装 `download.StorageAdapter`）——在 S4 引入驱逐/仅元数据缓存之前，agent 对下游发重建响应时必然本地有全量数据，改写无触发场景；`--ingest-mode`/`--segment-cache` 同理推迟到 S4。
- **价值**：部分下载可共享；冷启动进行中即可互搬；段键使用**上游 xorb 身份**，不再依赖"本地重新分块产出相同 xorb"的确定性假设（该假设从此只影响 S1 文件级路径的效率）。
- **验收**（已由 e2e 测试固化）：A 冷下载被闸在 30% 处，B 下载同文件时该前缀源站零流量（`TestPartialCaptureSharedBetweenAgents`）；**后到**节点与在途节点重叠下载时总源站字节 < 1.2 × 文件大小（`TestConcurrentColdDownloadsShareOrigin`，实测 (N+1)/N ≈ 1.1×——前提是 leader 已 announce 的 span 可被 follower 发现）；注入结构合法但内容错误的段数据被 sha256 判定拒绝、隔离段被清除、坏 peer 被冷却、重试自动回源（`TestPeerContentMismatchPurged`）。
- **不在 S3 承诺内**：**严格同时**冷启动（两端都尚未 announce 任何 span 就开跑）仍会各自回源——announce 只在 term 落盘后发生，段级索引没有"在途归属"概念。该场景由 S4 的 lease（集群级 singleflight）覆盖，其验收即"N 节点并发拉同一文件，源站只承担 1 份流量"。

### S4 — 冷路径协调与容量治理

- **lease**：对 sha256 的集群级 singleflight，优先授予 seeder（对标 Dragonfly Seed Peer 回源）；TTL + 进度心跳，持有者崩溃后他人接替（spool 断点续传已有）。
- **容量**：文件级 LRU 驱逐 + 无引用 xorb/segment 孤儿 GC；驱逐后 announce removes，漏发靠 TTL 收敛。
- **验收**：N 节点并发拉同一文件，源站只承担 1 份流量；磁盘水位不破上限。

### S5 — 运维化与增强（按需挑选的选项池）

Prometheus 指标（local/peer/origin 字节占比、命中率、peer 健康）；`preheat`/`purge` 管理 API；mTLS；协作式冷启动（term 按 rendezvous hash 分片回源，多节点分摊出口带宽，建立在 S3 段能力上）；gossip/DHT 无 tracker 模式；私有仓库 ACL 透传；控制台（Manager 职能自 tracker 独立）。

## 5. HTTP API（标注引入阶段）

### 5.1 agent（单端口，路由优先级从上到下）

| 路由 | 语义 | 阶段 |
|---|---|---|
| `GET /internal/...` | 清单/指标/管理（仅集群内） | S3+ |
| `/v1/xorbs/{ns}/{hash}`、`/v1/reconstructions/*` | CAS 数据面（匿名只读；写路由一律 403）；S3 起 xorb 路由支持 segment 服务（byte Range / `X-Xet-PD-Chunks`） | S0 |
| `GET /xet-bridge/{sha256}` | plain 客户端字节流 | S0 |
| `GET /xet-token`、`/api/{type}s/{repo}/xet-read-token/{rev}` | 发集群 token，`casUrl` 指向本 agent | S0 |
| `GET /api/{type}s/{repo}/tree/{rev}` | 重写 per-file xet 字段防绕过 | S0 |
| `GET/HEAD /{repo}/resolve/{rev}/{path}` | 门面：探测/ingest/Link 头；S1 起识别 `X-Xet-PD-Local-Only` | S0 |
| 其余 `/api/...` | 反向代理上游（注入上游 token，剥离下游 Authorization） | S0 |

### 5.2 tracker

| 方法/路径 | 请求 → 响应 | 阶段 |
|---|---|---|
| `POST /v1/announce` | `{peer, adds:{files:[{sha256,fileHash,keys}], xorbs:[{hash, ranges:[{start,end}]}]}, removes, ttl}` → `{ok}`（keys 为 resolve 路径别名——含 commit 规范形——使冷 resolve 无需已知 sha256 即可定位 peer；ranges 为 chunk 区间、端点排他，tracker 按 peer 合并；S2 仅 files；S3 加 xorbs） | S2 |
| `POST /v1/lookup` | `{files:[sha256], keys:[resolveKey], xorbs:[{hash, range:{start,end}}]}` → `{files:{...→{fileHash,peers}}, keys:{...→{sha256,fileHash,peers}}, xorbs:{"{hash}:{start}-{end}"→[{peer,ranges}]}}`（xorbs 只返回完全包含查询区间的 peer） | S2 |
| `POST /v1/lease/acquire\|renew\|release` | `{key, peer, ttl, progress}` → `{granted, holder, progress}` | S4 |
| `POST /v1/preheat`、`POST /v1/purge` | 预热 / 集群级缓存清理 | S5 |

约定：announce 周期 5min、TTL 15min；lookup 允许陈旧（消费侧以 404/416/校验失败兜底）；索引条目落盘为文件（对象键即文件路径），重启从文件即时恢复；文件丢失只降效率，仍可由 announce 重建。

## 6. 鉴权与安全

| 边界 | 机制 |
|---|---|
| 客户端/agent → peer CAS | 匿名只读：读一律免鉴权，`token.Issuer` 只为满足 xet 协议握手而发 token（不作访问控制）；一切写操作（xorb/shard 上传、dedup 查询、hub commit/preupload、`xet-write-token`）在 agent 边界直接 403；agent 响应带 `X-Xet-PD-Agent: 1` 标记头，捕获层据此区分 peer 与源站（只收割源站字节，peer 字节走隔离区） |
| agent → 源站 | `HF_TOKEN` 仅注入上游 host（mirror `authInjector`），**绝不**下发给客户端或 peer |
| agent ↔ tracker | 同一集群 token；可选 mTLS（S5） |
| 数据完整性 | chunk hash（逐段）、term range hash（可得时）、sha256（文件终审）、xorb merkle（仅全覆盖时） |

威胁模型：重建响应不含 chunk hash，段级取数时只能做结构校验，内容真伪由文件级 sha256 终审兜底——peer 来源段先隔离（不可服务/announce），判定失败即删除、冷却坏 peer、失效本地 chunk 缓存并回源重试；恶意 peer 最坏造成一次失败下载与一轮退避重试，绝不产生可服务的坏数据；tracker 投毒同理只多几次失败尝试（候选 URL 经过 scheme/userinfo 白名单校验，防经由 URL 注入凭据头）。

## 7. 配置（标注引入阶段）

| Flag / Env | 默认 | 阶段 |
|---|---|---|
| `--listen` / `XET_PD_LISTEN` | `:8880` | S0 |
| `--upstream`、`--upstream-token` / `HF_TOKEN` | `https://huggingface.co`、— | S0 |
| `--storage-dir`、`--external-url`、`--cluster-secret(-file)` | `./xet-pd`、按请求推导、— | S0/S1 |
| `--peers` | 空 | S1 |
| `--tracker` / `XET_PD_TRACKER` | 空（= 不启用索引） | S2 |
| tracker 子命令：`--listen` / `XET_PD_TRACKER_LISTEN`、`--storage-dir`、`--cluster-secret(-file)` | `:8881`、`./xet-pd`、—（空 = announce 免鉴权） | S2 |
| `--ingest-mode`（`always\|on-demand\|never`）、`--segment-cache` | 推迟到 S4（S3 验收不需要） | S4 |
| `--capacity`、`--seeder` | 无限、false | S4 |
| `--peer-timeout`、`--max-peer-conns` | `2s`、`64` | S1 |

客户端侧唯一要求：`export HF_ENDPOINT=http://127.0.0.1:8880`。

## 8. 对标 Dragonfly（速览）

| Dragonfly | xet-pd | 差异 |
|---|---|---|
| Client (dfdaemon) | agent | 原生 HF_ENDPOINT 语义，无代理注入/TLS 劫持；最后一跳客户端可直连 peer |
| Scheduler | tracker | 只做内容索引 + lease：xorb/chunk 自寻址自校验，无需 piece 级 DAG 与进度跟踪 |
| Seed Peer | seeder / lease 持有者 | 回源者 lease 动态竞选，seeder 非必需 |
| Manager（可选） | tracker 兼职，S5 独立 | 沿用"中心组件可选，缺席只降效率"原则 |
| Piece（URL 为 task 身份） | segment（内容寻址） | 跨仓库/revision/文件名去重，Dragonfly 做不到；换 revision 缓存不失效 |

采纳：两段式负载感知调度、中心协调回源、异常 peer 多特征剔除、preheat/purge 一等管理 API。不采纳：piece 级中心调度与 DAG、通用源协议适配、RBAC/控制台（留 S5）。Dragonfly 官方的 HF 加速以 URL 为身份、需配代理证书，xet-pd 以 chunk 级内容寻址 + 原生协议消除这两个限制。

## 9. 风险与开放问题

| 风险 | 缓解 |
|---|---|
| 段索引基数大（大 repo 数万 term） | announce 按 xorb 聚合 ranges + 批量接口；索引落盘 + 热条目内存缓存，容量只受磁盘限制；必要时段合并 |
| S1 文件级路径依赖本地重分块确定性 | 仅影响该路径效率不影响正确性；S3 段键用上游 xorb 身份，天然免疫；M1 加一致性测试观测 |
| Range 服务不命中（peer 只有别的段） | 精确键匹配 + 404/416 → 回退链；包含匹配作为后续优化 |
| tracker 单点 | 缺席退化 S1/S0；索引已落盘、重启即恢复（announce 兜底重建），HA 简单多副本 |
| 私有仓库越权读（集群内） | 首期明示"集群 = 信任域"（同常见 pull-through cache）；S5 做 ACL |
| 客户端直连 peer 被网络策略拦截 | 配置退回"agent 代理数据面"（URL 全指本机 agent，内部转取） |
| 上游协议演进（V2/批量重建） | 改写集中在 `p2pAdapter` 单点，跟随 xet 库升级 |

**对上游 xet 可能的 PR**（保持 xet-pd 为纯组合层）：download 的 chunk 缓存读取时校验 chunk 内容或提供失效 API（当前缓存未验字节可跨 flight 重放，xet-pd 以按 xorb 删除缓存目录兜底）；prefetcher 失败路径的数据竞争修复（`failEntry` 与 `Close` 并发写读同一字段，flight 失败时可被 -race 捕获）；CAS 读路由鉴权选项；storage 的 xorb 写入事件回调（增量 announce 用）；server 重建构建的 StorageAdapter 外部包装注入点（S4 驱逐后启用下游 URL 改写所需）。
