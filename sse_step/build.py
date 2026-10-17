# -*- coding: utf-8 -*-
"""Channel builders: merge per-security sources into STEP streams / pcaps."""

from __future__ import annotations

import datetime as _dt
import heapq
import os
import struct
import time
from typing import Callable, Dict, List, Optional, Tuple

from . import step
from .source import (
    SnapshotEventStream,
    TickEventStream,
)

Progress = Optional[Callable[[dict], None]]


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

class StreamWriter:
    def __init__(self, path: str, buffer_size: int = 4 * 1024 * 1024):
        self.path = path
        self.fh = open(path, "wb")
        self.buf = bytearray()
        self.buffer_size = buffer_size
        self.bytes_written = 0

    def write(self, data: bytes) -> None:
        self.buf += data
        if len(self.buf) >= self.buffer_size:
            self.flush()

    def flush(self) -> None:
        if self.buf:
            self.fh.write(self.buf)
            self.bytes_written += len(self.buf)
            self.buf.clear()

    def close(self) -> None:
        self.flush()
        self.fh.close()


def _ip_checksum(data: bytes) -> int:
    if len(data) % 2:
        data += b"\x00"
    s = 0
    for i in range(0, len(data), 2):
        s += (data[i] << 8) | data[i + 1]
        s = (s & 0xFFFF) + (s >> 16)
    return (~s) & 0xFFFF


def _mac(text: str) -> bytes:
    return bytes(int(x, 16) for x in text.split(":"))


def _ip(text: str) -> bytes:
    return bytes(int(x) for x in text.split("."))


class PcapWriter:
    """One TCP connection per channel, server (MDGW) -> client (VSS).

    The application stream is independent of TCP segmentation.  This writer
    reproduces a plausible packetization: payload chunks are at most MSS bytes
    and a partial chunk is held until the batching rule fires.
    """

    ETH_TYPE_IP = 0x0800
    TCP_SYN = 0x02
    TCP_ACK = 0x10
    TCP_PSH = 0x08
    TCP_FIN = 0x01

    def __init__(
        self,
        path: str,
        channel: int,
        trade_date: str,
        server_mac: str = "02:00:00:00:00:01",
        client_mac: str = "02:00:00:00:00:02",
        server_ip: str = "10.0.0.1",
        client_ip: str = "10.0.0.2",
        server_port: Optional[int] = None,
        client_port: Optional[int] = None,
        send_acks: bool = False,
    ):
        self.path = path
        self.fh = open(path, "wb")
        self.fh.write(struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1))
        self.channel = channel
        self.date = trade_date
        self.server_mac = _mac(server_mac)
        self.client_mac = _mac(client_mac)
        self.server_ip = _ip(server_ip)
        self.client_ip = _ip(client_ip)
        self.server_port = server_port if server_port is not None else 9000 + (channel - 1000)
        self.client_port = client_port if client_port is not None else 50000 + (channel - 1001)
        self.send_acks = send_acks

        self.cli_isn = 0x12340000 + (channel - 1000) * 0x100
        self.srv_isn = 0x56780000 + (channel - 1000) * 0x100
        self.cli_seq = (self.cli_isn + 1) & 0xFFFFFFFF
        self.srv_seq = (self.srv_isn + 1) & 0xFFFFFFFF
        self.ip_id = 0x4000 + (channel - 1000) * 0x100
        self.started = False
        self.closed = False
        self.data_packets = 0
        self.bytes_written = 0
        self.packet_count = 0
        self._epoch_cache: Dict[str, int] = {}

    # -- time ------------------------------------------------------------
    def _epoch(self, t) -> Tuple[int, int]:
        s = step.format_time9(t)
        date = self.date
        base = self._epoch_cache.get(date)
        if base is None:
            d = _dt.datetime(
                int(date[0:4]), int(date[4:6]), int(date[6:8]),
                tzinfo=_dt.timezone(_dt.timedelta(hours=8)),
            )
            base = int(d.timestamp())
            self._epoch_cache[date] = base
        sec = base + int(s[0:2]) * 3600 + int(s[2:4]) * 60 + int(s[4:6])
        usec = int(s[6:9]) * 1000
        return sec, usec

    # -- frame writing ---------------------------------------------------
    def _frame(self, src_mac, dst_mac, src_ip, dst_ip, sport, dport,
               seq, ack, flags, payload: bytes) -> bytes:
        self.ip_id = (self.ip_id + 1) & 0xFFFF
        tcp_len = 20 + len(payload)
        ip_len = 20 + tcp_len
        eth = dst_mac + src_mac + struct.pack("!H", self.ETH_TYPE_IP)
        ip = struct.pack(
            "!BBHHHBBH4s4s",
            0x45, 0, ip_len, self.ip_id, 0x4000, 64, 6, 0, src_ip, dst_ip,
        )
        ip = ip[:10] + struct.pack("!H", _ip_checksum(ip)) + ip[12:]
        tcp = struct.pack(
            "!HHIIBBHHH", sport, dport, seq & 0xFFFFFFFF, ack & 0xFFFFFFFF,
            5 << 4, flags, 65535, 0, 0,
        )
        pseudo = src_ip + dst_ip + struct.pack("!BBH", 0, 6, tcp_len)
        csum = _ip_checksum(pseudo + tcp + payload)
        tcp = tcp[:16] + struct.pack("!H", csum) + tcp[18:]
        return eth + ip + tcp + payload

    def _record(self, frame: bytes, t) -> None:
        sec, usec = self._epoch(t)
        self.fh.write(struct.pack("<IIII", sec, usec, len(frame), len(frame)))
        self.fh.write(frame)
        self.bytes_written += 16 + len(frame)
        self.packet_count += 1

    def _ensure_started(self, t) -> None:
        if self.started:
            return
        self.started = True
        cli, srv = self.client_mac, self.server_mac
        cip, sip = self.client_ip, self.server_ip
        cp, sp = self.client_port, self.server_port
        # SYN
        self._record(self._frame(cli, srv, cip, sip, cp, sp,
                                 self.cli_isn, 0, self.TCP_SYN, b""), t)
        # SYN-ACK
        self._record(self._frame(srv, cli, sip, cip, sp, cp,
                                 self.srv_isn, self.cli_isn + 1,
                                 self.TCP_SYN | self.TCP_ACK, b""), t)
        # ACK
        self._record(self._frame(cli, srv, cip, sip, cp, sp,
                                 self.cli_seq, self.srv_seq,
                                 self.TCP_ACK, b""), t)

    def write_data(self, payload: bytes, t) -> None:
        if not payload:
            return
        self._ensure_started(t)
        self._record(
            self._frame(self.server_mac, self.client_mac,
                        self.server_ip, self.client_ip,
                        self.server_port, self.client_port,
                        self.srv_seq, self.cli_seq,
                        self.TCP_PSH | self.TCP_ACK, payload),
            t,
        )
        self.srv_seq = (self.srv_seq + len(payload)) & 0xFFFFFFFF
        self.data_packets += 1
        if self.send_acks and (self.data_packets & 1) == 0:
            self._record(
                self._frame(self.client_mac, self.server_mac,
                            self.client_ip, self.server_ip,
                            self.client_port, self.server_port,
                            self.cli_seq, self.srv_seq,
                            self.TCP_ACK, b""),
                t,
            )

    def close(self, t=None) -> None:
        if self.closed:
            return
        self.closed = True
        if self.started and t is not None:
            # FIN from server, FIN from client, final ACK.
            self._record(self._frame(
                self.server_mac, self.client_mac, self.server_ip, self.client_ip,
                self.server_port, self.client_port, self.srv_seq, self.cli_seq,
                self.TCP_FIN | self.TCP_ACK, b""), t)
            self.srv_seq = (self.srv_seq + 1) & 0xFFFFFFFF
            self._record(self._frame(
                self.client_mac, self.server_mac, self.client_ip, self.server_ip,
                self.client_port, self.server_port, self.cli_seq, self.srv_seq,
                self.TCP_FIN | self.TCP_ACK, b""), t)
            self.cli_seq = (self.cli_seq + 1) & 0xFFFFFFFF
            self._record(self._frame(
                self.server_mac, self.client_mac, self.server_ip, self.client_ip,
                self.server_port, self.client_port, self.srv_seq, self.cli_seq,
                self.TCP_ACK, b""), t)
        self.fh.close()


class PacketSink:
    """Split a message stream into MSS-sized TCP payload chunks."""

    def __init__(self, pcap: PcapWriter, mss: int = 1460,
                 tick_batch_ms: int = 10, slice_mode: bool = False):
        self.pcap = pcap
        self.mss = max(1, mss)
        self.tick_batch_ms = max(0, tick_batch_ms)
        self.slice_mode = slice_mode
        self.buf = bytearray()
        self.chunk_time = None
        self.last_time = None
        self.last_batch_time = None
        self.current_slice = None

    def add(self, msg: bytes, t, slice_key: Optional[int] = None,
            batch_time: Optional[int] = None) -> None:
        bt = t if batch_time is None else batch_time
        if self.slice_mode:
            if self.current_slice is None:
                self.current_slice = slice_key
            elif slice_key != self.current_slice:
                self.flush_all()
                self.current_slice = slice_key
        else:
            # bt must be milliseconds since midnight, not HHMMSSsss: raw
            # HHMMSSsss values are not linear across second/minute boundaries.
            if (self.buf and self.last_batch_time is not None
                    and bt - self.last_batch_time > self.tick_batch_ms):
                self.flush_all()
        if not self.buf:
            self.chunk_time = t
        self.buf += msg
        self.last_time = t
        self.last_batch_time = bt
        while len(self.buf) >= self.mss:
            chunk = bytes(self.buf[:self.mss])
            del self.buf[:self.mss]
            self.pcap.write_data(chunk, self.chunk_time if self.chunk_time is not None else t)
            self.chunk_time = t

    def flush_all(self) -> None:
        if self.buf:
            self.pcap.write_data(bytes(self.buf), self.chunk_time if self.chunk_time is not None else self.last_time)
            self.buf.clear()
        self.chunk_time = None

    def close(self, t=None) -> None:
        self.flush_all()
        self.pcap.close(t)


# ---------------------------------------------------------------------------
# Trading phase and snapshot entries
# ---------------------------------------------------------------------------

def trading_phase_code(t: int) -> str:
    """Best-effort TradingPhaseCode (8 chars) derived from exchange time.

    Source data has no product-status field.  Product status records in the
    tick feed (E/I/O/J/C) confirm the same boundaries.
    """
    if t < 91500000:
        return "S01     "
    if t < 92500000:
        return "C111    "
    if t < 145700000:
        return "T111    "
    if t < 150000000:
        return "U111    "
    return "E11     "


def snapshot_slice_key(t: int) -> int:
    s = step.format_time9(t)
    secs = int(s[0:2]) * 3600 + int(s[2:4]) * 60 + int(s[4:6])
    return max(0, (secs - (9 * 3600 + 15 * 60)) // 3)


def _int_field(row: List[str], i: int, code: str, line: int, name: str) -> int:
    v = row[i]
    if v == "" or v == " ":
        return 0
    try:
        return int(v)
    except ValueError:
        raise step.StepError("%s line %d: bad %s %r" % (code, line, name, v))


def build_snapshot_entries(row: List[str], code: str, line: int, t: int):
    """Return (entries, prev_close, cum_vol, num_trades, cum_amt)."""
    last = _int_field(row, 4, code, line, "成交价")
    open_px = _int_field(row, 15, code, line, "开盘价")
    high = _int_field(row, 13, code, line, "最高价")
    low = _int_field(row, 14, code, line, "最低价")
    prev = _int_field(row, 16, code, line, "前收盘")
    cum_vol = _int_field(row, 11, code, line, "当日累计成交量")
    cum_amt = _int_field(row, 12, code, line, "当日成交额")
    num_trades = _int_field(row, 7, code, line, "成交笔数")

    entries: List[Tuple[str, int, Optional[int], Optional[int]]] = []
    if last:
        entries.append(("2", last, None, None))
    if open_px:
        entries.append(("4", open_px, None, None))
    if high:
        entries.append(("7", high, None, None))
    if low:
        entries.append(("8", low, None, None))
    if cum_vol > 0 and cum_amt > 0:
        vwap = (cum_amt * 10000 + cum_vol // 2) // cum_vol
        entries.append(("9", vwap, None, None))
    if t >= 150000000 and last:
        entries.append(("5", last, None, None))

    # Bids: columns 37..46 price, 47..56 size
    for i in range(10):
        px = _int_field(row, 37 + i, code, line, "申买价%d" % (i + 1))
        sz = _int_field(row, 47 + i, code, line, "申买量%d" % (i + 1))
        if px or sz:
            entries.append(("0", px, sz, i))
    # Asks: columns 17..26 price, 27..36 size
    for i in range(10):
        px = _int_field(row, 17 + i, code, line, "申卖价%d" % (i + 1))
        sz = _int_field(row, 27 + i, code, line, "申卖量%d" % (i + 1))
        if px or sz:
            entries.append(("1", px, sz, i))
    return entries, prev, cum_vol, num_trades, cum_amt


# ---------------------------------------------------------------------------
# Channel builders
# ---------------------------------------------------------------------------

def _new_tick_stats() -> dict:
    return {
        "messages": 0, "orders_add": 0, "orders_delete": 0,
        "status": 0, "status_dropped": 0, "trades": 0,
        "bytes": 0, "first_time": None, "last_time": None,
        "appl_seq_first": None, "appl_seq_last": None,
    }


def _new_snapshot_stats() -> dict:
    return {
        "messages": 0, "entries": 0, "bytes": 0,
        "first_time": None, "last_time": None,
    }


def _note_time(stats: dict, t: int) -> None:
    if stats["first_time"] is None:
        stats["first_time"] = t
    stats["last_time"] = t


def build_tick_channel(
    channel: int,
    codes: List[str],
    data_dir: str,
    out_dir: str,
    cfg: dict,
    progress: Progress = None,
) -> dict:
    tick_dir = os.path.join(out_dir, "tick")
    os.makedirs(tick_dir, exist_ok=True)
    base = os.path.join(tick_dir, "step_tick_%d" % channel)
    writer = StreamWriter(base + ".bin") if cfg["emit_stream"] else None
    pcap = None
    sink = None
    if cfg["emit_pcap"]:
        pcap = PcapWriter(
            base + ".pcap", channel, cfg["trade_date"],
            send_acks=cfg.get("pcap_acks", False),
        )
        sink = PacketSink(pcap, mss=cfg["mss"], tick_batch_ms=cfg["tick_batch_ms"])

    stats = _new_tick_stats()
    streams: List[TickEventStream] = []
    heap: List[Tuple[int, str, int, tuple, TickEventStream]] = []
    serial = 0
    start = cfg.get("start_time")
    end = cfg.get("end_time")
    status_mode = cfg.get("status_mode", "ub001")
    max_events = cfg.get("max_events")
    appl_seq = 0
    msg_seq = 0
    last_progress = time.time()
    stopped = False

    try:
        for code in codes:
            sec_dir = os.path.join(data_dir, code)
            try:
                st = TickEventStream(sec_dir, code)
                ev = next(st)
            except StopIteration:
                st.close()
                continue
            if st.date is not None and cfg.get("trade_date") and st.date != cfg["trade_date"]:
                raise step.StepError(
                    "trade date in %s is %s, expected %s"
                    % (code, st.date, cfg["trade_date"])
                )
            serial += 1
            heapq.heappush(heap, (ev[0], code, serial, ev, st))
            streams.append(st)

        while heap:
            t, code, ser, ev, st = heapq.heappop(heap)
            if end is not None and t > end:
                st.close()
                stopped = True
                break

            emit = True
            if start is not None and t < start:
                emit = False
            if emit and ev[2] == "S" and status_mode == "drop":
                stats["status_dropped"] += 1
                emit = False
            if emit and max_events is not None and stats["messages"] >= max_events:
                st.close()
                stopped = True
                break

            if emit:
                _, _, kind, sec_id, qid, side, price, qty, bid_ref, offer_ref = ev
                send_time = step.sending_time(cfg["trade_date"], t)
                msg_seq += 1
                appl_seq += 1
                if kind in ("A", "D"):
                    exec_type = "0" if kind == "A" else "4"
                    use_price = price if kind == "A" else 0
                    use_qty = qty if kind == "A" else 0
                    if side not in ("B", "S"):
                        raise step.StepError(
                            "%s order seq %d: bad side %r" % (code, ev[1], side)
                        )
                    # MemberID/TraderCode are not present in the Wind files.
                    # Leave them out rather than emitting empty tag values,
                    # which strict FIX parsers reject.
                    ext = [
                        (54, 1 if side == "B" else 2),
                        (117, qid),
                    ]
                    stats["orders_add" if kind == "A" else "orders_delete"] += 1
                elif kind == "S":
                    exec_type = "S"
                    use_price = 0
                    use_qty = 0
                    ext = [(58, side)]
                    stats["status"] += 1
                elif kind == "F":
                    exec_type = "F"
                    use_price = price
                    use_qty = qty
                    ext = []
                    if bid_ref:
                        ext.append((10116, bid_ref))
                    if offer_ref:
                        ext.append((10117, offer_ref))
                    ext.append((8504, step.format_trade_amount_2(price, qty)))
                    ext.append((10333, 0))
                    stats["trades"] += 1
                else:
                    raise step.StepError(
                        "%s seq %d: unknown event kind %r" % (code, ev[1], kind)
                    )

                msg = step.encode_ub001(
                    seq=msg_seq,
                    send_time=send_time,
                    security_type="01",
                    channel=channel,
                    appl_seq=appl_seq,
                    md_stream_id="MD002",
                    security_id=sec_id,
                    transact_time=t,
                    exec_type=exec_type,
                    price_int=use_price,
                    qty=use_qty,
                    ext_fields=ext,
                    sender=cfg.get("sender", "MDGW"),
                    target=cfg.get("target", "VSS"),
                )
                stats["messages"] += 1
                stats["bytes"] += len(msg)
                if stats["appl_seq_first"] is None:
                    stats["appl_seq_first"] = appl_seq
                stats["appl_seq_last"] = appl_seq
                _note_time(stats, t)
                if writer is not None:
                    writer.write(msg)
                if sink is not None:
                    sink.add(msg, t, batch_time=step.time_to_ms(t))
                if progress is not None and (stats["messages"] & 0x3FFFF) == 0:
                    now = time.time()
                    if now - last_progress >= 2.0:
                        last_progress = now
                        p = dict(stats)
                        p["stage"] = "tick"
                        p["channel"] = channel
                        progress(p)

            try:
                nxt = next(st)
            except StopIteration:
                st.close()
            else:
                serial += 1
                heapq.heappush(heap, (nxt[0], code, serial, nxt, st))
    finally:
        for st in streams:
            try:
                st.close()
            except Exception:
                pass
        if writer is not None:
            writer.close()
        if sink is not None:
            sink.close(t=stats["last_time"])
        elif pcap is not None:
            pcap.close(t=None)

    stats["stopped_early"] = stopped
    stats["securities"] = len(streams)
    stats["appl_seq_count"] = appl_seq
    stats["msg_seq_count"] = msg_seq
    if writer is not None:
        stats["file"] = os.path.basename(writer.path)
        stats["file_bytes"] = writer.bytes_written
    if pcap is not None:
        stats["pcap"] = os.path.basename(pcap.path)
        stats["pcap_bytes"] = pcap.bytes_written
        stats["pcap_packets"] = pcap.packet_count
    return stats


def _build_snapshot_stream(
    label: str,
    pcap_channel: int,
    codes: List[str],
    data_dir: str,
    out_dir: str,
    cfg: dict,
    progress: Progress = None,
) -> dict:
    snap_dir = os.path.join(out_dir, "snapshot")
    os.makedirs(snap_dir, exist_ok=True)
    base = os.path.join(snap_dir, "step_snapshot_%s" % label)
    writer = StreamWriter(base + ".bin") if cfg["emit_stream"] else None
    pcap = None
    sink = None
    if cfg["emit_pcap"]:
        pcap = PcapWriter(
            base + ".pcap", pcap_channel, cfg["trade_date"],
            send_acks=cfg.get("pcap_acks", False),
        )
        sink = PacketSink(pcap, mss=cfg["mss"], slice_mode=True)

    stats = _new_snapshot_stats()
    streams: List[SnapshotEventStream] = []
    heap: List[Tuple[int, str, int, tuple, SnapshotEventStream]] = []
    serial = 0
    start = cfg.get("start_time")
    end = cfg.get("end_time")
    max_snaps = cfg.get("max_snapshots")
    msg_seq = 0
    last_progress = time.time()
    stopped = False

    try:
        for code in codes:
            sec_dir = os.path.join(data_dir, code)
            try:
                st = SnapshotEventStream(sec_dir, code)
                ev = next(st)
            except StopIteration:
                st.close()
                continue
            if st.date is not None and cfg.get("trade_date") and st.date != cfg["trade_date"]:
                raise step.StepError(
                    "trade date in %s is %s, expected %s"
                    % (code, st.date, cfg["trade_date"])
                )
            serial += 1
            heapq.heappush(heap, (ev[0], code, serial, ev, st))
            streams.append(st)

        while heap:
            t, code, ser, ev, st = heapq.heappop(heap)
            if end is not None and t > end:
                st.close()
                stopped = True
                break
            if start is not None and t < start:
                pass
            elif max_snaps is not None and stats["messages"] >= max_snaps:
                st.close()
                stopped = True
                break
            else:
                row = ev[1]
                entries, prev, cum_vol, num_trades, cum_amt = build_snapshot_entries(
                    row, code, st.line, t
                )
                msg_seq += 1
                msg = step.encode_snapshot(
                    seq=msg_seq,
                    send_time=step.sending_time(cfg["trade_date"], t),
                    security_type="01",
                    trade_date=cfg["trade_date"],
                    last_update_time=t,
                    md_stream_id="MD002",
                    security_id=row[1].strip(),
                    prev_close_px=prev,
                    total_volume=cum_vol,
                    num_trades=num_trades,
                    total_value_yuan=cum_amt,
                    entries=entries,
                    trading_phase_code=trading_phase_code(t),
                    sender=cfg.get("sender", "MDGW"),
                    target=cfg.get("target", "VSS"),
                )
                stats["messages"] += 1
                stats["entries"] += len(entries)
                stats["bytes"] += len(msg)
                _note_time(stats, t)
                if writer is not None:
                    writer.write(msg)
                if sink is not None:
                    sink.add(msg, t, slice_key=snapshot_slice_key(t))
                if progress is not None and (stats["messages"] & 0x3FFFF) == 0:
                    now = time.time()
                    if now - last_progress >= 2.0:
                        last_progress = now
                        p = dict(stats)
                        p["stage"] = "snapshot"
                        p["channel"] = pcap_channel
                        p["label"] = label
                        progress(p)

            try:
                nxt = next(st)
            except StopIteration:
                st.close()
            else:
                serial += 1
                heapq.heappush(heap, (nxt[0], code, serial, nxt, st))
    finally:
        for st in streams:
            try:
                st.close()
            except Exception:
                pass
        if writer is not None:
            writer.close()
        if sink is not None:
            sink.close(t=stats["last_time"])
        elif pcap is not None:
            pcap.close(t=None)

    stats["stopped_early"] = stopped
    stats["securities"] = len(streams)
    stats["msg_seq_count"] = msg_seq
    if writer is not None:
        stats["file"] = os.path.basename(writer.path)
        stats["file_bytes"] = writer.bytes_written
    if pcap is not None:
        stats["pcap"] = os.path.basename(pcap.path)
        stats["pcap_bytes"] = pcap.bytes_written
        stats["pcap_packets"] = pcap.packet_count
    return stats


def build_snapshot_channel(channel: int, codes: List[str], data_dir: str,
                           out_dir: str, cfg: dict, progress: Progress = None) -> dict:
    return _build_snapshot_stream(str(channel), channel, codes, data_dir, out_dir,
                                  cfg, progress)


def build_snapshot_global(codes: List[str], data_dir: str, out_dir: str,
                          cfg: dict, progress: Progress = None) -> dict:
    """All securities in one stream.  W carries no ChannelNO; this reproduces
    the 'one snapshot slice contains every contract' view."""
    return _build_snapshot_stream("all", 1000, codes, data_dir, out_dir,
                                  cfg, progress)


def build_channel(channel: int, codes: List[str], data_dir: str, out_dir: str,
                  cfg: dict, progress: Progress = None) -> dict:
    result = {
        "channel": channel,
        "securities": len(codes),
        "codes": codes,
    }
    if not cfg.get("no_tick"):
        result["tick"] = build_tick_channel(channel, codes, data_dir, out_dir, cfg, progress)
    if not cfg.get("no_snapshot") and not cfg.get("no_snapshot_channel"):
        result["snapshot"] = build_snapshot_channel(channel, codes, data_dir, out_dir, cfg, progress)
    return result
