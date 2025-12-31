以下内容为**冻结版（Frozen）FINAL Implementation Brief**：后续实现与讨论仅允许在此范围内展开；如需改范围，必须先显式更新本 Brief。
原始proposal见Proposal_Gruppe3_v3.pdf

---

## 1) 最终架构（Final Architecture：模块与职责边界）

系统总体：**Scanner Clients →（HTTP REST）→ Leader Server →（内部 TCP ring）→ Followers**；**UDP broadcast 仅用于动态发现 Leader HTTP endpoint**。

### A. 目录/模块（以 `DS_Project_Modules.xlsx / Modules` 为准）

**common/**

* `protocol.py`：**内部消息类型、JSON 格式、字段约定**（所有模块共享“语言”）。
* `utils.py`：公共工具（序列化、时间戳、重试/超时小工具等）。
  （Owner：Fang）

**server/**

* `main.py`：**系统入口**；初始化 Node；启动 TCP/UDP/HTTP 三类线程/loop；统一生命周期管理。
* `config.py`：配置加载（端口、Node ID 等）。
* `logger.py`：日志写入。

**api/**

* `http_server.py`：对外 HTTP REST（Scanner 提交更新 POST）。

  * 若本节点是 Leader：接收更新→落 WAL→更新内存 state→触发复制
  * 若本节点是 Follower：拒绝/重定向/返回 503 + 引导客户端重新 UDP discover（实现任选其一，但要一致）

**storage/**

* `wal.py`：**WAL（append-only JSON log）**：每次更新追加写入；作为 crash 恢复依据。
* `recovery.py`：启动时读取 WAL 重放，恢复 in-memory state；恢复节点以 follower 身份加入 ring。

**core/**

* `state.py`：内存 KV inventory + 节点角色/运行状态（leader/follower、node_id、successor 等）。
* `replication.py`：**被动复制（Passive Replication）**：Leader 将更新通过内部 TCP ring 传播给 followers；followers 应用更新到 state（并可选择性写 WAL）。
* `election.py`：**Ring-based election（按最高 Node ID）**；由 TCP 断连/心跳超时触发。

**network/**

* `ring.py`：内部 TCP 邻居长连接（仅维护 successor）；负责连接管理、消息收发、心跳、断链检测、ring repair（跳过失效节点）。
* `udp_discovery.py`：UDP broadcast：clients/servers 用于定位当前 leader 的 HTTP endpoint（无先验配置）。

**client/**

* `scanner_client.py`：模拟 Scanner：先 UDP discover leader → 再循环通过 HTTP POST 发更新。

---

## 2) 模块协作关系与依赖（Dependency & Collaboration Map）

### 2.1 强依赖（必须先定“共同语言/接口”才能并行）

1. **common/protocol.py**（内部消息字段与类型）
   → 依赖方：`network/ring.py`, `core/election.py`, `core/replication.py`, `core/state.py`

2. **storage/wal.py 的 record schema**（WAL 每条日志的 JSON 结构）
   → 依赖方：`api/http_server.py`, `core/replication.py`（是否 follower 也落日志取决于你们实现选择，但 schema 必须固定）, `storage/recovery.py`

3. **Node Runtime State 模型（state.py）**
   → 依赖方：`http_server.py`（判断 leader/follower）、`election.py`（写入 leader 结果）、`replication.py`（apply update）、`ring.py`（邻居/心跳状态）

### 2.2 可并行（通过“接口冻结”解除等待）

* `udp_discovery.py` 可独立实现（只要明确：请求/响应报文里包含哪些字段：leader_ip、leader_http_port、leader_id 等）。
* `scanner_client.py` 可独立实现（只要明确：UDP discover 返回结构 + HTTP endpoint 路径/JSON body）。
* `logger.py`、`config.py` 可独立实现。

### 2.3 运行时主链路（端到端）

Scanner POST update → `http_server.py`（leader）→ `wal.py` append → `state.py` apply → `replication.py` emit replication msg → `ring.py` send to successor → follower `ring.py` recv → `replication/state.py` apply（+可选 `wal.py`）

---

## 3) 最终技术约束（Final Technical Constraints）

来自 Proposal v3（必须满足）：

1. **语言：Python**
2. **外部接口：HTTP REST**（Scanner Clients → System）
3. **内部协调：自定义 socket middleware**

   * **TCP**：持久连接，仅 ring 邻居间通信；用于 election/replication/heartbeats
   * **UDP broadcast/multicast**：仅用于动态发现 leader HTTP endpoint
4. **拓扑：动态逻辑 ring**；每个 server **只维护 successor**（无全局成员列表）。
5. **容错：tolerant to crashes**（通过 timeout/connection failure 侦测失效；恢复节点重放 WAL 并重新加入）。

你额外的冻结约束（来自对话）：
6) **不用 Docker**（本项目不以容器化作为交付/评分点；直接 python 运行）。
7) **一致性要求从 eventual consistency 降级为 best-effort propagation**（见下一节“假设/保证”）。

---

## 4) 最终假设与保证（Assumptions & Guarantees）

### 4.1 我们“保证”的（Scope Guarantees）

* **Crash 容错（基础级）**：

  * 通过 TCP 断链或心跳超时检测疑似 crash，并触发 ring repair + election。
  * 节点重启可通过读取本地 WAL 恢复到“该节点曾经持久化过”的状态，并作为 follower 重新加入。
* **Leader discovery 可用**：clients/servers 可通过 UDP broadcast 找到当前 leader 的 HTTP endpoint（假设同一网段可广播）。

### 4.2 我们“不保证”的（Explicitly NOT Guaranteed）

* **不保证一致性收敛/最终一致（No eventual consistency guarantee）**：

  * 更新传播是 best-effort：leader 会尝试经 TCP ring 转发，但不会确保每个 follower 都收到/补齐历史缺口。
* **不保证零丢数据（End-to-end）**：

  * leader crash、网络抖动、TCP 断连导致的“传播未完成”可能造成部分节点缺失更新；系统继续运行不视为错误。
* **不保证 exactly-once / 去重**：

  * client 重试或连接抖动可能导致重复更新；除非你们显式在协议里做 `update_id` 幂等，否则默认不保证。
* **不保证分区容忍与自动合并（Partition healing / conflict resolution）**：

  * 设计中没有强一致或冲突解决协议；不做多主写入与合并。

---

## 5) 明确的 Non-goals（不做的内容）

为避免范围膨胀，以下明确不做（实施期若出现需求一律拒绝或延后）：

1. **不做 Docker / Kubernetes / Compose 交付**
2. **不做强一致/共识协议**（Raft/Paxos/2PC 等）
3. **不做“副本追赶/补齐/对账”机制**（无 anti-entropy / gossip 修复 / snapshot 安装）
4. **不做读扩展与 follower 读**（对外读写规则如需实现，默认只从 leader 服务，避免一致性讨论）
5. **不做安全/鉴权/加密**（TLS、token、ACL 等）
6. **不做复杂运维**（监控、metrics、自动部署、故障注入框架等）
7. **不做复杂客户端生态**（仅保留模拟 scanner_client）

---

## 6) Excel 复用说明（避免重复内容）

* **模块拆分与负责人**：以 `DS_Project_Modules.xlsx / Modules` 为当前冻结版本；后续任何“增删文件/迁移职责”都必须同步更新该表，避免口头分歧。


---
## 目录层级设计
.
├── common/
│   ├── protocol.py        # 内部消息协议定义（JSON schema / message types）
│   └── utils.py           # 公共工具函数（序列化、时间戳、重试等）
│   # Owner: Fang
│
├── server/
│   ├── main.py            # 系统入口；启动 TCP / UDP / HTTP 线程
│   ├── config.py          # 配置加载（端口、Node ID 等）
│   └── logger.py          # 日志系统
│   # Owners: Fang (main), Davud (config, logger)
│
├── api/
│   └── http_server.py     # 对外 HTTP REST 接口（Scanner → System）
│                          # - Leader: 接收更新、写 WAL、触发复制
│                          # - Follower: 拒绝 / redirect / 503
│   # Owner: Mohamed
│
├── storage/
│   ├── wal.py             # Write-Ahead Log（append-only JSON）
│   └── recovery.py        # 启动时 WAL replay，节点恢复并 rejoin
│   # Owner: Mohamed
│
├── core/
│   ├── state.py           # 内存库存状态 + 节点运行时状态
│   ├── replication.py    # 被动复制（Leader → Followers via ring）
│   └── election.py       # Ring-based election（最高 Node ID）
│   # Owner: Fang (election, replicatoin), Mohamed (state)
│
├── network/
│   ├── ring.py            # TCP ring 管理（successor、heartbeat、repair）
│   └── udp_discovery.py   # UDP broadcast：发现 Leader HTTP endpoint
│   # Owner: Fang (ring), Davud (udp_discovery)
│
├── client/
│   └── scanner_client.py  # 模拟 Scanner 客户端（UDP discover + HTTP POST）
│   # Owner: Davud
│
└── README.md              # 项目说明、启动方式、demo 指引
