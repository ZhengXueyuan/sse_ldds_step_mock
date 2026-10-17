# 示例输出（截断）

用以下命令生成，只取 600000.SH 从 09:30:00 起的前 500 条逐笔、前 100 条切片：

```bat
python sse_step_rebuild.py build --data D:\data\20261015 --out samples --codes 600000 --start-time 093000000 --max-events 500 --max-snapshots 100 --emit both --snapshot-layout both
```

内容：

* `tick/step_tick_1001.bin` / `.pcap`：500 条 UB001（含新增/删除/成交）；
* `snapshot/step_snapshot_1001.bin` / `.pcap`：100 条 W（通道 1001）；
* `snapshot/step_snapshot_all.bin` / `.pcap`：100 条 W（全市场布局；只有一只证券时与上面内容等价，但 MsgSeqNum 重新编号）；
* `manifest.json`、`channels.json`。

查看：

```bat
python sse_step_rebuild.py inspect --file samples\tick\step_tick_1001.bin --limit 8
python sse_step_rebuild.py verify  --out samples
```

起一个本地网关模拟器（监听 9001，全速播放，不带快照）：

```bat
python sse_step_rebuild.py serve --data samples --channels 1001 --topspeed --no-snapshots
```

注意：`--start-time`/`--max-events` 截断后 `ApplSeqNum` 从 1 重新连续编号，`MsgSeqNum` 1..500 连续；这只是抽样，不是完整交易日。
