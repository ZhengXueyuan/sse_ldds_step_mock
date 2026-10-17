# SSE STEP 行情重建工具

把 Wind 格式的**逐证券 CSV**（`行情.csv`、`逐笔委托.csv`、`逐笔成交.csv`）重建为
上交所行情网关（MDGW）风格的 **STEP / FIXT.1.1** 行情流：

* **逐笔行情**：`MsgType=UB001`，逐笔委托（新增/删除/产品状态）与逐笔成交按发生顺序混排；
* **切片行情**：`MsgType=W`，每 3 秒一档的十档快照；
* 可选按 TCP MSS（默认 1460 字节 = 1500 MTU）生成 **pcap**，把“几乎同时发生”的逐笔消息和
  “一个 3 秒切片内全部合约”的快照消息打成网络包；
* **实时网络模拟器 `serve`**：按通道监听 TCP，处理 Logon/心跳/UA001/UA002，连接建立后从头
  按交易所时间播放，支持倍率、UA002 请求-重传。

协议依据：`D:\doc\sse\IS120_上海证券交易所行情网关STEP数据接口规范0.62版_20260703.pdf`。

---

## 1. 快速开始

```bat
:: 1) 全量：逐笔 + 切片，按通道 1001-1008 输出，默认只写 .bin 流
python sse_step_rebuild.py build --data D:\data\20261015 --out D:\repo\sse_mock\out

:: 2) 全量：同时生成 pcap（每通道一条合成 TCP 连接）
python sse_step_rebuild.py build --data D:\data\20261015 --out out --emit both

:: 3) 切片额外生成“全市场一个文件”（所有合并在一条流里）
python sse_step_rebuild.py build --data D:\data\20261015 --out out --snapshot-layout both

:: 4) 先跑一小批验证
python sse_step_rebuild.py build --data D:\data\20261015 --out out_sample --codes 600000,600004,600009,600519 --max-events 2000 --max-snapshots 200

:: 5) 启动本地行情网关模拟器：1001-1008 监听 9001-9008，1 倍速，登录后从头播放
python sse_step_rebuild.py serve --data out --channels 1001-1008 --speed 1

:: 6) 快速联调：全速 + 只发通道 1001 + 关闭快照
python sse_step_rebuild.py serve --data out --channels 1001 --topspeed --no-snapshots
```

校验与查看：

```bat
python sse_step_rebuild.py verify  --out out
python sse_step_rebuild.py inspect --file out\tick\step_tick_1001.bin --limit 8
python sse_step_rebuild.py inspect --file out\snapshot\step_snapshot_1001.bin --limit 5 --max-entries 12
python sse_step_rebuild.py channels --data D:\data\20261015 --out channels.json
```

环境要求：Windows + Python 3.9+，无第三方依赖（`mmap`/`csv`/`struct` 都是标准库）。
生成 pcap 也不需要 scapy；`scapy` 只用于可选的独立复核。

---

## 2. 通道号

严格按要求：

```
channel = 1001 + (合约数字部分 mod 8)
```

例如 `600000 -> 1001`、`600004 -> 1005`、`900948 -> 1005`、`688001 -> 1002`。

全量 2356 只 `.SH` 的分布（279–310 只/通道，基本均匀）：

| 通道 | 1001 | 1002 | 1003 | 1004 | 1005 | 1006 | 1007 | 1008 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 证券数 | 296 | 310 | 281 | 304 | 282 | 295 | 279 | 309 |

`channels.json` 里有完整的通道 → 证券列表。源 CSV 不包含原通道号，本工具不做原通道
推断，直接按上述规则重新分配到 1001-1008；因此原来的跨证券 `ApplSeqNum` 顺序只在
单只证券内部可恢复，跨证券按时间归并（见第 6 节）。

---

## 3. 输出结构

```
out/
├─ manifest.json                  # 配置、逐通道统计、总量、还原口径说明
├─ channels.json                  # 通道 → 证券列表
├─ tick/
│  ├─ step_tick_1001.bin          # UB001 消息直接拼接（自定界）
│  ├─ step_tick_1001.pcap         # 可选：合成 TCP 包
│  └─ ...                         # 1002-1008
└─ snapshot/
   ├─ step_snapshot_1001.bin      # W 消息直接拼接
   ├─ step_snapshot_1001.pcap     # 可选
   ├─ ...
   └─ step_snapshot_all.bin       # 可选：--snapshot-layout global/both
```

每个 `.bin` 是**一条独立会话**：`MsgSeqNum` 从 1 开始连续递增。逐笔文件的
`ApplSeqNum(1181)` 也在本通道内从 1 连续递增。

`.bin` 可以直接按 FIX/STEP 的方式逐条解析：`8=FIXT.1.1<SOH>9=BodyLength<SOH>...10=CheckSum<SOH>`，
消息之间没有额外分隔符，`BodyLength` + `CheckSum` 自带定界与完整性校验。

### pcap

`--emit pcap|both` 时，每通道合成一条 MDGW → VSS 的 TCP 连接：

* 服务端 `10.0.0.1:9001..9008`，客户端 `10.0.0.2:50000..50007`；
* 三次握手、数据段、FIN 都有；可选 `--pcap-acks` 每两个数据段回一个客户端 ACK；
* 数据段负载 ≤ `--mss`（默认 1460），多个 STEP 消息可以出现在同一段，一条 STEP 消息也可以跨段；
* 按 TCP 序列号重组所有载荷，得到的字节流与 `.bin` **逐字节相同**（已用 scapy 验证）；
* 可直接用 Wireshark 打开，或按需用 tcpreplay 回放（注意：合成连接只含 TCP 握手 + 行情数据，
  没有 Logon/心跳等会话消息）。

逐笔的 batching 规则：同一时间戳附近、空闲间隔 ≤ `--tick-batch-ms`（默认 10ms）的消息
进入同一发送批次，批次超过 MSS 就切成整段，批次结束时把余量发出。
切片的 batching 规则：按 9:15:00 起的 3 秒窗口分组，一个窗口内该通道的全部 W 消息先入缓冲，
再按 MSS 切片；`--snapshot-layout global` 时窗口内就是全市场所有合约的 W 消息。
这正是“切片包含所有合约，再按 MTU 分片”的形态。

---

## 4. 网络行情模拟器（serve）

### 4.1 启动

```bat
:: 1001-1008 分别监听 9001-9008；登录后从文件头开始、按交易所时间 1 倍速播放
python sse_step_rebuild.py serve --data out --channels 1001-1008 --speed 1

:: 10 倍速；把长间隔（开市前、午休）压到最多 5 秒
python sse_step_rebuild.py serve --data out --channels 1001 --speed 10

:: 全速联调；只发通道 1001；不带快照
python sse_step_rebuild.py serve --data out --channels 1001 --topspeed --no-snapshots

:: 检查文件/索引/端口规划，不启动监听
python sse_step_rebuild.py serve --data out --channels 1001-1008 --check
```

端口规则：通道 `N` → `--base-port + (N - 1001)`，默认 `9001..9008`。

### 4.2 连接、登录与播放

* 默认等客户端先发 `MsgType=A`（Logon）；服务端回 Logon（`98/108/1137/1407/1408`），
  然后才开始播放；
* **每个新连接都从播放窗口的第一条消息开始**（默认就是文件头）；`--start-time` /
  `--end-time` 可限定窗口；
* 服务端把每条数据消息重新封装进本会话的 `MsgSeqNum` 序列：Logon 为 1，之后所有数据、
  心跳、UA001/UA002 应答连续递增；
* 支持 `MsgType=0`（心跳）、`1`（TestRequest，回带 112 的心跳）、`5`（Logout）、
  `2`（ResendRequest，回 `4` SequenceReset）；
* 已登录后按 `HeartBtInt`（客户端 Logon 里的 108）在空闲时发心跳；
* 逐笔频道空闲每 `--channel-heartbeat` 秒发一次 `UA001`，带 `1350 ApplLastSeqNum`；
  播放结束时发一次 `10205=Y`（EndOfChannel）；
* 播放结束后默认保持连接，继续处理心跳和 UA002；`--close-at-end` 可改为结束即断开。

### 4.3 播放速率

| 参数 | 说明 |
|---|---|
| `--speed N` | 相对交易所时间的倍率，1=实时，10=10 倍速 |
| `--topspeed` | 不等待，尽可能快（联调用） |
| `--pps N` | 固定每秒 N 条消息 |
| `--mbps N` | 固定每秒 N 兆比特（按消息字节数计） |
| `--max-gap-ms N` | 相邻消息的交易所时间差超过 N 毫秒时按 N 毫秒播放；默认 5000。设为 0 = 精确保留（开市前/午休会真的等） |
| `--start-time` / `--end-time` | `HHMMSSsss`、`93000000` 或 `09:30:00.000` |

第一条消息立即发送；之后按上表节流。倍率模式保留消息间的相对时间，只压缩超过
`--max-gap-ms` 的长间隔。

### 4.4 UA002 请求-重传

客户端发现 `ApplSeqNum` 跳号后发：

```
35=UA002, 10201=频道, 10077=1, 1182=起始ApplSeqNum, 1183=结束ApplSeqNum
```

服务端行为：

* 从 `.idx` 索引定位 `.bin` 中的原始 `UB001`，按区间重发；消息体/`ApplSeqNum` 不变，
  只换成新的会话 `MsgSeqNum`；
* 默认**不**置 `43 PossDupFlag` / `97 PossResend`：IS120 规定客户端按“频道 + 消息记录号”
  去重，应用层重传用 `ApplSeqNum` 识别即可。需要 FIX 风格标记时加 `--mark-possdup`；
* 重发完成后回 `UA002` 应答：`10076 ResendStatus`（1 全部完成 / 2 部分完成 /
  3 无权限 / 4 不可用 / 5 正在重传中 / 99 其他）；
* 每次最多重发 `--max-resend` 条（默认 200，规范建议 ≤200），超出返回状态 2；
* 同一连接同一时间只处理一个重传请求；并发请求返回状态 5；
* 重传期间暂停直播发送（同一 `stream_lock`），重传完成后恢复；
* `1183=0` 时取文件的最后一条 `ApplSeqNum`（**整个预生成交易日**，不是当前直播位置）；
  便于在回放中途补数据。需要严格“只补已发布数据”的语义时可自行按 `1350` 限制。

快照 `W` 按规范**不支持重传**；全局快照端口收到 UA002 会回状态 4。

### 4.5 索引

首次 `serve` 会在每个逐笔 `.bin` 旁生成 `.idx`：

* 索引记录每条消息的文件偏移，`ApplSeqNum` 连续，可直接 `O(1)` 定位；
* 约 8 字节/条：单通道 22M 条约 176MB；`samples/` 的 500 条几乎为 0；
* 缓存按 `.bin` 的 size/mtime 失效；`--rebuild-index` 强制重建；
* 首次启动要扫描一遍逐笔文件（单通道 22M 条约几十秒），之后启动很快。

### 4.6 快照

* 默认把本通道的 `snapshot/step_snapshot_<ch>.bin` 与逐笔按交易所时间归并后在同一连接发送；
* `--no-snapshots` 关闭；
* `--snapshot-all-port 9100` 额外监听一个端口，播放 `snapshot/step_snapshot_all.bin`
  （全市场一个流）；该端口只发 W、不支持 UA002；
* 快照消息的会话 `MsgSeqNum` 同样重编号连续。

### 4.7 限制

* 不做用户名/密码认证（协议里 Username/Password 是预留字段），任何 Logon 都接受；
* 每个 TCP 连接独立从窗口头开始，不做跨重连的会话续传；
* 只实现 UA002 应用层重传，不实现 FIX 会话层按 MsgSeqNum 的历史重放（`2` 只回 `4`）；
* `--no-logon` / `--start-on-connect` 是联调便利选项，会先发数据再回 Logon，非标准行为；
* 模拟器把整个预生成交易日视为可重传数据（见 4.4）。

---

## 5. 字段映射

### 5.1 逐笔 `UB001`

公共头：`8=FIXT.1.1, 9=BodyLength, 35=UB001, 49=MDGW, 56=VSS, 34=MsgSeqNum,
52=SendingTime(交易所时间), 347=GBK`。

| Tag | 值 | 说明 |
|---:|---|---|
| 167 | `01` | 股票/基金/指数及债券分销 |
| 10201 | 1001..1008 | 通道号 |
| 1181 | 1,2,3... | 本通道内统一连续编号（委托、撤单、状态、成交共用） |
| 1500 | `MD002` | 股票类 |
| 48 | `600000` | 产品代码 |
| 60 | `HHMMSSsss` | 交易所时间（9 位，补前导 0） |
| 150 | `0`/`4`/`S`/`F` | 新增委托 / 删除委托 / 产品状态 / 成交 |
| 44 | N14(5) | 委托价或成交价；删除/状态为 `0.00000` |
| 38 | N16 | 委托量或成交量；删除/状态为 `0` |

逐笔委托（`150=0/4`）扩展：

| Tag | 值 | 说明 |
|---:|---|---|
| 54 | `1`/`2` | 买 / 卖 |
| 117 | C18 | 订单编号（源 `交易所委托号`）；删除时是被删订单编号 |
| 10211 / 10215 | — | 源数据无会员/交易员信息，**省略**（严格 FIX 解析器不接受空值） |

逐笔成交（`150=F`）扩展：

| Tag | 值 | 说明 |
|---:|---|---|
| 10116 | C18 | 买方订单编号（源 `叫买序号`） |
| 10117 | C18 | 卖方订单编号（源 `叫卖序号`） |
| 8504 | N17(2) | 成交金额 = 价格 × 数量 |
| 10333 | `0` | 匹配成交 |

> 逐笔委托/成交的 `QuoteID`、`BidApplSeqNum`、`OfferApplSeqNum` 都是源文件里的
> **订单编号（交易所委托号）**，不是 `ApplSeqNum`。这是 IS120 2.5.4.2 的定义。

### 5.2 切片 `W`

| Tag | 值 | 说明 |
|---:|---|---|
| 167 | `01` | 证券类型 |
| 339 | `3` | 正常交易盘 |
| 75 | `20261015` | 交易日期 |
| 779 | `HHMMSSsss` | 最新更新时间 |
| 1500 | `MD002` | 股票类 |
| 48 | `600000` | 产品代码 |
| 140 | N14(5) | 前收盘 |
| 387 | N16 | 当日累计成交量 |
| 8503 | N16 | 当日累计成交笔数 |
| 8504 | N17(2) | 当日累计成交金额 |
| 268 | N5 | 下面的行情条目个数 |
| 8538 | C8 | 实时阶段及标志 |

行情条目循环（`268` 后面）：

| 269 | 270 | 271 | 290 |
|---|---|---|---|
| `2` 最新成交价 | 成交价 | — | — |
| `4` 今开盘价 | 开盘价 | — | — |
| `7` 当日最高 | 最高价 | — | — |
| `8` 当日最低 | 最低价 | — | — |
| `9` 当日成交均价 | 累计金额/累计数量 | — | — |
| `5` 今收盘价 | 收盘价 | — | — |
| `0` 买盘档位 | 申买价 | 申买量 | 0..9 |
| `1` 卖盘档位 | 申卖价 | 申卖量 | 0..9 |

说明：

* 价格为源文件的整数 **× 1/10000 元**（B 股为美元），输出固定 5 位小数；
* 数量单位为股/份，输出整数；
* 金额为元，输出 2 位小数；
* 集合竞价阶段的“虚拟未匹配量”会以 `270=0.00000, 271=非零, 290=1` 的形式保留
  （源数据买二/卖二价可能为 0 但量非 0）；
* 价格和数量都为 0 的空档位不发送；接收方可以按 10 档补 0；
* `269=9` 的当日成交均价源文件没有，按 `当日累计成交额 / 当日累计成交量` 计算；
* `269=5` 只在 15:00:00 及之后的快照中发送（收盘价 = 最后一笔成交价）。

---

## 6. 还原口径与已知限制

1. **ApplSeqNum 重编号**
   源文件是“按证券”的切片，`委托编号`/`成交编号` 是**原通道**的统一序号；同一通道内
   其他证券的消息不在这个文件里，所以单只证券的序号是稀疏的，且不同原通道之间会重叠。
   重建时按新通道把委托、撤单、状态、成交统一重编号为 1,2,3...，并保持每只证券内部的
   原始 `(时间, 原序号)` 顺序。跨证券只能按 `TransactTime` 归并（同一毫秒按证券代码
   稳定排序）——原始跨证券顺序在重映射通道后不可恢复。

2. **源数据缺少部分主动方委托**
   Wind 的逐笔委托文件并不包含所有立即成交的订单；逐笔成交里的
   `叫买序号/叫卖序号` 可能指向一个没有对应新增记录的订单编号。这是源数据本身的缺口，
   不是重建程序丢消息。工具会原样保留这些引用；`manifest.json` 中也有说明。
   典型例子：`600000.SH` 的 9:15–9:25 集合竞价委托全部缺失，但 9:25 的成交仍在。

3. **产品状态记录**
   源 `逐笔委托.csv` 里的 `委托类型=S`（`委托代码=E/I/O/J/C`）不是订单/成交。
   为“尽可能保留”，默认把它们发成 `UB001`，`150=S`，`58=<E/I/O/J/C>`。
   这是扩展：IS120 0.62 的 `150` 只定义 `0/4/F`，且 UB001 章节只写了
   `SecurityType=13`（债券非匹配成交）。如不需要，用 `--status-mode drop`。

4. **`TradingPhaseCode` 是推导值**
   源快照没有产品状态字段。按交易所时间推导：
   `S`(开市前) / `C`(09:15-09:25 开盘集合竞价) / `T`(09:25-14:57 连续交易) /
   `U`(14:57-15:00 收盘集合竞价) / `E`(15:00 后闭市)。第 2/3/4 位按“可正常交易/已上市/
   可接受新订单”填 `1`，其余补空格。

5. **`Symbol`、`MemberID`、`TraderCode` 缺失**
   源数据没有证券简称、会员代码、交易员代码。`Symbol(55)` 省略；
   `10211/10215` 省略（不发空值，否则 QuickFIX/simplefix 之类的严格解析器会报
   `EmptyValueError`）。需要的话可以在 `build.py` 的扩展字段里补。

6. **切片布局**
   真实 `W` 消息没有 `ChannelNO`。默认按通道输出 `step_snapshot_1001..1008.bin`，
   每个文件只含本通道证券；`--snapshot-layout global`（或 `both`）额外输出
   `step_snapshot_all.bin`，按时间归并全市场 2356 只证券。两者是同一批 W 消息的不同组织方式。

7. **会话消息**
   `build` 生成的 `.bin` 只包含行情消息（UB001/W），不含登录、心跳、频道心跳、重传应答；
   每条文件被视为一条已经登录成功的独立行情会话，`MsgSeqNum` 从 1 开始。
   需要会话行为时用 `serve`，它会在同一 TCP 会话里加入 Logon、心跳、`UA001`、`UA002` 应答，
   并把数据消息重新编号到该会话连续的 `MsgSeqNum`。

8. **数据口径**
   行情价格按 `×1/10000` 处理（例如 `96500 -> 9.65000`）；成交量为股；成交金额为元。
   B 股（900xxx）价格单位为美元，程序不做币种换算。
   每股收益、IOPV、指数等字段不在本数据集中，不输出。

---

## 7. 校验

`verify` 会逐条检查：

* `8=FIXT.1.1`、`BodyLength`、`CheckSum(10)`；
* `MsgSeqNum(34)` 从 1 连续；
* UB001 的 `ApplSeqNum(1181)` 从 1 连续、`ChannelNO(10201)` 与文件名一致；
* W 的 `NoMDEntries(268)` 与实际条目数一致。

测试结果示例（600000 全天）：

```
tick : 137,897 条 = 56,514 新增 + 16,653 删除 + 4 状态 + 64,726 成交
snap : 4,954 条
成交汇总 = 68,574,299 股 / 658,627,633.19 元
收盘快照 = 68,574,299 股 / 64,726 笔 / 658,627,633.00 元   ← 对得上
```

独立解析器：生成的 `.bin` 已用纯 Python 的 `simplefix`（第三方 FIX 解析器）完整解析，
所有消息的 `BodyLength`/`CheckSum` 均通过；pcap 已用 `scapy` 重组，载荷与 `.bin`
逐字节一致（测试中逐笔/切片/全市场三种 pcap 都做了逐字节比较）。

模拟器测试（`tests/test_serve.py`，`python -m unittest discover -s tests`）：

* 客户端 Logon → 服务端 Logon（MsgSeqNum=1）→ 5 条 UB001 → UA001 EndOfChannel；
* 全会话 `MsgSeqNum` 连续；
* UA002 2..4 重发 3 条、`10076=1`；`43/97` 默认不置，`--mark-possdup` 时置 Y；
* `1183=0` 时重发到文件末尾并回 `1183=最后一条`；
* Logout 应答。

另外用 `samples/` 手工验证过：100 倍速时约 318 秒的切片时间被压缩到 3.18 秒；
逐笔空闲时 UA001 每 `--channel-heartbeat` 秒发送；全市场快照端口只发 W、UA002 回状态 4。

QuickFIX 兼容性：消息是标准 FIXT.1.1 帧。QuickFIX/J、QuickFIX/C++ 使用自定义
DataDictionary 即可解析；本仓库未附带 XML 字典（不同 QuickFIX 版本对 FIXT 传输字典/
应用字典的加载方式不同），字段号与含义见上面的表和
`docs/step_field_map.csv`。需要的话可按表生成
`TransportDataDictionary` + `AppDataDictionary`（`DefaultApplVerID=9`）。

---

## 8. 参数速查

| 参数 | 默认 | 说明 |
|---|---|---|
| `--data` | 必填 | 输入根目录（下面全是 `<CODE>.SH/`） |
| `--out` | 必填 | 输出目录 |
| `--codes` | 全部 | 逗号分隔，如 `600000,600004`（可省略 `.SH`） |
| `--codes-file` | 无 | 代码清单文件（每行一个或逗号分隔） |
| `--limit` | 全部 | 只取排序后的前 N 只 |
| `--emit` | `stream` | `stream` / `pcap` / `both` |
| `--mss` | `1460` | pcap TCP 载荷上限（1500 MTU - 40） |
| `--tick-batch-ms` | `10` | 逐笔 pcap 批次空闲阈值 |
| `--pcap-acks` | 关 | pcap 中每两个数据段回一个 ACK |
| `--snapshot-layout` | `channel` | `channel` / `global` / `both` |
| `--status-mode` | `ub001` | `ub001` / `drop` |
| `--start-time` / `--end-time` | 全天 | 如 `093000000`、`93000000`、`09:30:00.000` |
| `--max-events` / `--max-snapshots` | 不限 | 每通道/每流条数上限，便于抽样 |
| `--no-tick` / `--no-snapshot` | 关 | 只生成另一类 |
| `--jobs` | `8` | 并行通道进程数 |
| `--channel-first` / `--channel-count` | `1001` / `8` | 通道编号规则 |
| `--sender` / `--target` | `MDGW` / `VSS` | 49/56 的值 |

`serve` 的参数见第 4.3、4.4 节；常用：`--channels`、`--base-port`、`--speed` /
`--topspeed` / `--pps` / `--mbps`、`--max-gap-ms`、`--no-snapshots`、
`--snapshot-all-port`、`--max-resend`、`--mark-possdup`。

---

## 9. 性能

实测（本机，D: 盘同盘读写）：

* **整通道 1001（296 只证券）**：逐笔 22,284,565 条 / 4.73 GB + 切片 1,196,877 条 /
  1.16 GB，单进程约 **6.5 分钟**（含读 CSV、写 .bin）；
* `verify` 同一通道：逐笔 67 秒（约 33 万条/秒），切片 5.8 秒；
* 50 只证券逐笔 4,960,898 条 / 1.05 GB，单进程 57 秒 ≈ 87k 条/秒；
* pcap：10 只证券 1,021,279 条 / 238 MB，28 秒 ≈ 36k 条/秒（含 TCP 头与校验和）。

全量粗估：逐笔约 170M 条、~38 GB；切片约 9M 条、~9 GB；合计 ~47 GB。
8 通道并行约 **15–30 分钟**（pcap 另计，体积约 +15%）。实际以 `manifest.json` 为准。

模拟器：

* 首次启动建 `.idx`：`samples/` 500 条瞬时；整通道 22M 条约几十秒（之后走缓存）；
* 100 倍速播放 `samples/`（含 100 条切片，交易所时间约 318 秒）实测 **3.18 秒**；
* `--topspeed` 下 500 条逐笔 + 100 条切片 + UA001 在毫秒级发完；
* UA002 重发 3 条 / 200 条均在毫秒级；重传期间直播暂停、完成后恢复。

---

## 10. 代码结构

```
sse_step_rebuild.py      入口
sse_step/
  step.py                STEP/FIX 编码器、消息重封装、解析/校验器
  source.py              Wind CSV 读取、通道映射、时间/序号检查
  build.py               通道归并、W/UB001 编码、pcap/MSS 打包
  serve.py               TCP 网关模拟器、索引、播放节流、UA001/UA002、快照归并
  cli.py                 build / verify / inspect / channels / serve 命令
tests/test_step.py       编码/解析/校验/重封装单元测试
tests/test_serve.py      网关模拟器集成测试（登录、播放、UA002、退出）
samples/                 截断示例（600000.SH，09:30 起 500 条逐笔 + 100 条切片）
docs/step_field_map.csv  STEP 字段号/类型/来源对照表
```
