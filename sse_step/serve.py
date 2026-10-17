# -*- coding: utf-8 -*-
"""SSE STEP market-data gateway simulator.

Features
--------
* one TCP listener per channel (default 9001..9008);
* STEP/FIXT.1.1 logon/logout/heartbeat/TestRequest handling;
* playback starts after logon and starts from the beginning of the channel
  file by default;
* exchange-time pacing with a speed multiplier, topspeed, pps or mbps cap;
* long idle gaps (pre-market, lunch break) can be compressed;
* UA001 channel heartbeat with ApplLastSeqNum / EndOfChannel;
* UA002 request/response retransmission for UB001 messages, with
  PossDupFlag/PossResend on resent messages;
* optional per-channel W snapshots interleaved with UB001 by exchange time;
* optional all-market snapshot stream on a separate port.

The simulator intentionally serves a whole pre-generated day.  A UA002 request
can address any ApplSeqNum present in the file, including data that has not yet
been reached by the live playback.  See README for the rationale.
"""

from __future__ import annotations

import array
import heapq
import mmap
import os
import socket
import struct
import sys
import threading
import time
import traceback
from datetime import datetime

from . import step

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_LEVELS = {"debug": 0, "info": 1, "warn": 2, "error": 3}
_LOG_LEVEL = 1
_LOG_LOCK = threading.Lock()


def set_log_level(level: str) -> None:
    global _LOG_LEVEL
    _LOG_LEVEL = _LEVELS.get(level, 1)


def log(channel, message: str, level: str = "info") -> None:
    if _LEVELS.get(level, 1) < _LOG_LEVEL:
        return
    with _LOG_LOCK:
        print("%s [%s] %s" % (time.strftime("%H:%M:%S"), channel, message), flush=True)


# ---------------------------------------------------------------------------
# Raw message access
# ---------------------------------------------------------------------------

class RawMsg:
    __slots__ = ("start", "end", "msg_type", "sending_time", "time_ms",
                 "appl_seq", "raw")

    def __init__(self, start, end, msg_type, sending_time, time_ms, appl_seq, raw):
        self.start = start
        self.end = end
        self.msg_type = msg_type
        self.sending_time = sending_time
        self.time_ms = time_ms
        self.appl_seq = appl_seq
        self.raw = raw


def _find_value(mm, start: int, end: int, tag: bytes):
    needle = b"\x01" + tag + b"="
    i = mm.find(needle, start, end)
    if i < 0:
        return None
    i += len(needle)
    j = mm.find(step.SOH, i, end)
    if j < 0:
        return None
    return mm[i:j]


def parse_raw(mm, pos: int, size: int) -> RawMsg:
    if mm[pos:pos + len(step.BEGIN_FIELD)] != step.BEGIN_FIELD:
        raise step.StepError("message does not start at %d" % pos)
    p = pos + len(step.BEGIN_FIELD)
    if mm[p:p + 2] != b"9=":
        raise step.StepError("BodyLength not second at %d" % pos)
    e = mm.find(step.SOH, p)
    if e < 0:
        raise step.StepError("unterminated BodyLength at %d" % pos)
    body_len = int(mm[p + 2:e])
    body_start = e + 1
    body_end = body_start + body_len
    if body_end > size or mm[body_end:body_end + 3] != b"10=":
        raise step.StepError("bad body boundary at %d" % pos)
    cs_end = mm.find(step.SOH, body_end)
    if cs_end < 0:
        raise step.StepError("unterminated CheckSum at %d" % pos)
    end = cs_end + 1

    if mm[body_start:body_start + 3] != b"35=":
        raise step.StepError("MsgType not first at %d" % pos)
    te = mm.find(step.SOH, body_start)
    msg_type = mm[body_start + 3:te].decode("ascii")

    st_b = _find_value(mm, body_start, body_end, b"52")
    sending_time = st_b.decode("ascii") if st_b else None
    time_ms = None
    if sending_time:
        try:
            _, t9 = step.parse_sending_time(sending_time)
            time_ms = step.time_to_ms(t9)
        except step.StepError:
            time_ms = None
    if time_ms is None:
        fallback = b"60" if msg_type == "UB001" else b"779"
        tb = _find_value(mm, body_start, body_end, fallback)
        if tb:
            time_ms = step.time_to_ms(int(tb))
    if time_ms is None:
        raise step.StepError("message at %d has no usable time" % pos)

    appl = None
    if msg_type == "UB001":
        ab = _find_value(mm, body_start, body_end, b"1181")
        if ab is None:
            raise step.StepError("UB001 at %d has no ApplSeqNum" % pos)
        appl = int(ab)

    return RawMsg(pos, end, msg_type, sending_time, time_ms, appl,
                  bytes(mm[pos:end]))


# ---------------------------------------------------------------------------
# ApplSeqNum -> file offset index
# ---------------------------------------------------------------------------

INDEX_MAGIC = b"SSEIDX01"
INDEX_HEADER = struct.Struct("<8sQQQQ")  # magic, count, first_appl, size, mtime_ns
INDEX_HEADER_SIZE = INDEX_HEADER.size


def _build_index_offsets(bin_path: str):
    st = os.stat(bin_path)
    size = st.st_size
    offsets = array.array("Q")
    first = None
    expected = None
    with open(bin_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            pos = 0
            while pos < size:
                if mm[pos:pos + len(step.BEGIN_FIELD)] != step.BEGIN_FIELD:
                    nxt = mm.find(step.BEGIN_FIELD, pos)
                    if nxt < 0:
                        break
                    gap = mm[pos:nxt]
                    if gap.strip(b"\x00\r\n \t"):
                        raise step.StepError(
                            "%s: garbage at %d before next message" % (bin_path, pos)
                        )
                    pos = nxt
                start = pos
                p = start + len(step.BEGIN_FIELD)
                if mm[p:p + 2] != b"9=":
                    raise step.StepError("%s: BodyLength not second at %d"
                                         % (bin_path, start))
                e = mm.find(step.SOH, p)
                if e < 0:
                    raise step.StepError("%s: unterminated BodyLength at %d"
                                         % (bin_path, start))
                body_len = int(mm[p + 2:e])
                body_start = e + 1
                body_end = body_start + body_len
                if body_end > size or mm[body_end:body_end + 3] != b"10=":
                    raise step.StepError("%s: bad message at %d" % (bin_path, start))
                cs_end = mm.find(step.SOH, body_end)
                if cs_end < 0:
                    raise step.StepError("%s: unterminated CheckSum at %d"
                                         % (bin_path, start))
                ab = _find_value(mm, body_start, body_end, b"1181")
                if ab is None:
                    raise step.StepError("%s: no ApplSeqNum at %d"
                                         % (bin_path, start))
                appl = int(ab)
                if first is None:
                    first = appl
                    expected = appl
                elif appl != expected:
                    raise step.StepError(
                        "%s: ApplSeqNum not continuous at %d: %d != %d"
                        % (bin_path, start, appl, expected)
                    )
                offsets.append(start)
                expected += 1
                pos = cs_end + 1
        finally:
            mm.close()
    if first is None:
        raise step.StepError("%s: no messages" % bin_path)
    offsets.append(size)
    return offsets, first, st


class MessageIndex:
    def __init__(self, offsets=None, mm=None, idx_fh=None, first_appl=0,
                 count=0, bin_size=0):
        self._offsets = offsets
        self._mm = mm
        self._idx_fh = idx_fh
        self.first_appl = first_appl
        self.count = count
        self.last_appl = first_appl + count - 1
        self.bin_size = bin_size

    @classmethod
    def from_file(cls, idx_path: str, bin_path: str):
        st = os.stat(bin_path)
        fh = open(idx_path, "rb")
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            magic, count, first, bin_size, mtime_ns = INDEX_HEADER.unpack(
                mm[:INDEX_HEADER_SIZE])
            if magic != INDEX_MAGIC:
                raise step.StepError("bad index magic in %s" % idx_path)
            if bin_size != st.st_size or mtime_ns != st.st_mtime_ns:
                raise step.StepError("index out of date for %s" % bin_path)
            need = INDEX_HEADER_SIZE + (count + 1) * 8
            if len(mm) != need:
                raise step.StepError("bad index size for %s" % idx_path)
            if count <= 0:
                raise step.StepError("empty index for %s" % bin_path)
        except Exception:
            mm.close()
            fh.close()
            raise
        return cls(mm=mm, idx_fh=fh, first_appl=first, count=count,
                   bin_size=bin_size)

    @classmethod
    def from_offsets(cls, offsets, first_appl, bin_size):
        return cls(offsets=offsets, first_appl=first_appl,
                   count=len(offsets) - 1, bin_size=bin_size)

    @classmethod
    def load_or_build(cls, bin_path: str, rebuild: bool = False):
        idx_path = bin_path + ".idx"
        if not rebuild and os.path.exists(idx_path):
            try:
                return cls.from_file(idx_path, bin_path)
            except Exception as exc:
                log("index", "rebuilding %s: %s" % (idx_path, exc), "warn")
        t0 = time.time()
        offsets, first, st = _build_index_offsets(bin_path)
        count = len(offsets) - 1
        try:
            tmp = idx_path + ".tmp"
            with open(tmp, "wb") as f:
                f.write(INDEX_HEADER.pack(INDEX_MAGIC, count, first,
                                          st.st_size, st.st_mtime_ns))
                if sys.byteorder != "little":
                    offsets.byteswap()
                offsets.tofile(f)
            os.replace(tmp, idx_path)
            log("index", "%s: built %d entries (ApplSeqNum %d..%d) in %.1fs"
                % (os.path.basename(bin_path), count, first, first + count - 1,
                   time.time() - t0))
            return cls.from_file(idx_path, bin_path)
        except OSError as exc:
            log("index", "cannot cache index for %s (%s); using memory index"
                % (bin_path, exc), "warn")
            return cls.from_offsets(offsets, first, st.st_size)

    def _off(self, i: int) -> int:
        if self._mm is not None:
            return struct.unpack_from("<Q", self._mm, INDEX_HEADER_SIZE + 8 * i)[0]
        return self._offsets[i]

    def get(self, seq: int):
        if seq < self.first_appl or seq > self.last_appl:
            return None
        i = seq - self.first_appl
        start = self._off(i)
        end = self._off(i + 1)
        return start, end - start

    def close(self):
        if self._mm is not None:
            try:
                self._mm.close()
            except Exception:
                pass
            self._mm = None
        if self._idx_fh is not None:
            try:
                self._idx_fh.close()
            except Exception:
                pass
            self._idx_fh = None


# ---------------------------------------------------------------------------
# Channel data
# ---------------------------------------------------------------------------

def _mmap_ro(path: str):
    fh = open(path, "rb")
    mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
    return fh, mm


class ChannelData:
    def __init__(self, channel, tick_path=None, snapshot_path=None,
                 rebuild_index=False):
        self.channel = channel
        self.tick_path = tick_path
        self.snapshot_path = snapshot_path
        self.tick_fh = self.snapshot_fh = None
        self.tick_mm = self.snapshot_mm = None
        self.index = None
        if tick_path and os.path.exists(tick_path):
            self.tick_fh, self.tick_mm = _mmap_ro(tick_path)
        if snapshot_path and os.path.exists(snapshot_path):
            self.snapshot_fh, self.snapshot_mm = _mmap_ro(snapshot_path)
        if self.tick_mm is not None:
            self.index = MessageIndex.load_or_build(
                tick_path, rebuild=rebuild_index)

    def read_tick(self, seq: int):
        if self.index is None or self.tick_mm is None:
            return None
        r = self.index.get(seq)
        if r is None:
            return None
        off, length = r
        return bytes(self.tick_mm[off:off + length])

    def close(self):
        if self.index is not None:
            self.index.close()
            self.index = None
        for name in ("tick_mm", "snapshot_mm"):
            mm = getattr(self, name)
            if mm is not None:
                try:
                    mm.close()
                except Exception:
                    pass
                setattr(self, name, None)
        for name in ("tick_fh", "snapshot_fh"):
            fh = getattr(self, name)
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    pass
                setattr(self, name, None)


class FileMessageStream:
    def __init__(self, mm, start_ms=None, end_ms=None):
        self.mm = mm
        self.size = len(mm)
        self.start_ms = start_ms
        self.end_ms = end_ms

    def __iter__(self):
        pos = 0
        while pos < self.size:
            msg = parse_raw(self.mm, pos, self.size)
            pos = msg.end
            if self.start_ms is not None and msg.time_ms < self.start_ms:
                continue
            if self.end_ms is not None and msg.time_ms > self.end_ms:
                break
            yield msg


# ---------------------------------------------------------------------------
# Pacing
# ---------------------------------------------------------------------------

class Pacer:
    def __init__(self, speed=1.0, topspeed=False, pps=None, mbps=None,
                 max_gap_ms=5000):
        self.speed = speed if speed and speed > 0 else 1.0
        self.topspeed = topspeed
        self.pps = pps
        self.mbps = mbps
        self.max_gap_ms = max_gap_ms
        self.start = 0.0
        self.prev_ms = None
        self.play_ms = 0.0
        self.msg_count = 0
        self.bytes = 0

    def reset(self, first_ms: int):
        self.start = time.monotonic()
        self.prev_ms = first_ms
        self.play_ms = 0.0
        self.msg_count = 0
        self.bytes = 0

    def wait(self, msg_ms: int, nbytes: int, stop_event) -> bool:
        if self.topspeed:
            return not stop_event.is_set()
        if self.pps:
            if self.msg_count == 0:
                self.msg_count = 1
                return not stop_event.is_set()
            target = self.start + self.msg_count / float(self.pps)
            self.msg_count += 1
        elif self.mbps:
            if self.bytes == 0:
                self.bytes = nbytes
                return not stop_event.is_set()
            target = self.start + self.bytes / (self.mbps * 125000.0)
            self.bytes += nbytes
        else:
            if self.prev_ms is None:
                self.prev_ms = msg_ms
            delta = msg_ms - self.prev_ms
            if delta < 0:
                delta = 0
            if self.max_gap_ms and delta > self.max_gap_ms:
                delta = self.max_gap_ms
            self.play_ms += delta
            self.prev_ms = msg_ms
            target = self.start + (self.play_ms / 1000.0) / self.speed
        while True:
            now = time.monotonic()
            if now >= target:
                return not stop_event.is_set()
            if stop_event.wait(min(target - now, 0.5)):
                return False


# ---------------------------------------------------------------------------
# Incoming FIX stream reader
# ---------------------------------------------------------------------------

class FixSocketReader:
    def __init__(self):
        self.buf = bytearray()

    def feed(self, data: bytes):
        self.buf += data

    def pop(self):
        if not self.buf:
            return None
        start = self.buf.find(step.BEGIN_FIELD)
        if start < 0:
            # Discard junk, keep a short tail in case a BeginString is split.
            if len(self.buf) > 64:
                del self.buf[:-32]
            return None
        if start:
            del self.buf[:start]
        p = len(step.BEGIN_FIELD)
        if len(self.buf) < p + 2:
            return None
        if self.buf[p:p + 2] != b"9=":
            del self.buf[:1]
            return None
        e = self.buf.find(step.SOH, p)
        if e < 0:
            return None
        try:
            body_len = int(self.buf[p + 2:e])
        except ValueError:
            del self.buf[:1]
            return None
        body_start = e + 1
        body_end = body_start + body_len
        # "10=" + 3 digits + SOH
        total = body_end + 7
        if len(self.buf) < total:
            return None
        if self.buf[body_end:body_end + 3] != b"10=":
            del self.buf[:1]
            return None
        raw = bytes(self.buf[:total])
        del self.buf[:total]
        return raw


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def now_sending_time() -> str:
    now = datetime.now()
    return now.strftime("%Y%m%d-%H:%M:%S.") + "%03d" % (now.microsecond // 1000)


class Session:
    def __init__(self, server, conn: socket.socket, addr, data: ChannelData,
                 cfg: dict):
        self.server = server
        self.conn = conn
        self.addr = addr
        self.data = data
        self.cfg = cfg
        self.channel = data.channel
        self.sender = cfg.get("sender", "MDGW")
        self.target = cfg.get("target", "VSS")

        self.send_lock = threading.Lock()
        self.stream_lock = threading.Lock()
        self.retransmit_lock = threading.Lock()
        # FIX MsgSeqNum starts at 1.  All outgoing messages (logon, data,
        # heartbeats, UA001/UA002) share this counter.
        self.out_seq = 1
        self.last_send = time.monotonic()
        self.last_recv = time.monotonic()
        self.last_tick_send = time.monotonic()
        self.last_ua001 = 0.0
        self.last_appl_sent = 0
        self.client_seq = None
        self.heartbtint = int(cfg.get("heartbeat", 30))
        self.logged_in = False
        self.playback_done = False
        self.stop_event = threading.Event()
        self.playback_go = threading.Event()
        self.reader = FixSocketReader()
        self.playback_thread = None
        self.heartbeat_thread = None
        self.count_sent = 0

    # -- lifecycle -------------------------------------------------------
    def start(self):
        try:
            self.conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        self.playback_thread = threading.Thread(
            target=self._playback_loop, name="playback-%d" % self.channel,
            daemon=True)
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="heartbeat-%d" % self.channel,
            daemon=True)
        self.playback_thread.start()
        self.heartbeat_thread.start()
        if self.cfg.get("start_on_connect") or not self.cfg.get("require_logon", True):
            self.playback_go.set()

    def run(self):
        try:
            self.start()
            self._reader_loop()
        finally:
            self.close()

    def close(self):
        self.stop_event.set()
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass
        cur = threading.current_thread()
        for t in (self.playback_thread, self.heartbeat_thread):
            if t is not None and t is not cur:
                t.join(timeout=1.0)
        log(self.channel, "session closed %s:%d" % (self.addr[0], self.addr[1]))

    # -- socket I/O ------------------------------------------------------
    def _sendall_locked(self, data: bytes) -> bool:
        try:
            self.conn.sendall(data)
            self.last_send = time.monotonic()
            return True
        except OSError:
            self.stop_event.set()
            return False

    def _send_encoded(self, msg_type: str, body) -> bool:
        with self.send_lock:
            seq = self.out_seq
            self.out_seq += 1
            try:
                out = step.encode_message(
                    msg_type, seq, now_sending_time(), body,
                    sender=self.sender, target=self.target)
            except Exception:
                log(self.channel, "encode failed for %s" % msg_type, "error")
                return False
            return self._sendall_locked(out)

    def _send_rewrapped_raw(self, raw: bytes, poss_dup: bool = False) -> bool:
        with self.send_lock:
            seq = self.out_seq
            self.out_seq += 1
            try:
                out = step.rewrap_message(
                    raw, seq, sending_time=None,
                    poss_dup=poss_dup, poss_resend=poss_dup,
                    sender=self.sender, target=self.target)
            except Exception:
                log(self.channel, "rewrap failed", "error")
                return False
            return self._sendall_locked(out)

    # -- session messages ------------------------------------------------
    def send_logon(self):
        self._send_encoded("A", [
            (98, 0),
            (108, self.heartbtint),
            (1137, 9),
            (1407, 124),
            (1408, "STEP1.20_SH_0.62"),
        ])

    def send_logout(self, text: str = "", status: int = 0):
        body = [(1409, status)]
        if text:
            body.append((58, text))
        self._send_encoded("5", body)

    def send_heartbeat(self, test_req_id=None):
        body = [(112, test_req_id)] if test_req_id else []
        self._send_encoded("0", body)

    def send_ua001(self, end_of_channel: bool = False):
        if self.data.index is None:
            return
        body = [
            (167, "01"),
            (10201, self.channel),
            (1350, self.last_appl_sent or 0),
            (10205, "Y" if end_of_channel else "N"),
        ]
        self._send_encoded("UA001", body)
        self.last_ua001 = time.monotonic()

    def send_ua002_response(self, beg: int, end: int, status: int,
                            text: str = ""):
        body = [
            (10201, self.channel),
            (10077, 1),
            (1182, beg),
            (1183, end),
            (10076, status),
        ]
        if text:
            body.append((58, text))
        self._send_encoded("UA002", body)

    def send_sequence_reset(self):
        with self.send_lock:
            seq = self.out_seq
            self.out_seq += 1
            body = [(123, "N"), (36, seq + 1)]
            out = step.encode_message("4", seq, now_sending_time(), body,
                                      sender=self.sender, target=self.target)
            self._sendall_locked(out)

    # -- reader ----------------------------------------------------------
    def _reader_loop(self):
        self.conn.settimeout(0.5)
        login_deadline = time.monotonic() + float(self.cfg.get("login_timeout", 30))
        while not self.stop_event.is_set():
            try:
                chunk = self.conn.recv(65536)
            except socket.timeout:
                if (not self.logged_in and self.cfg.get("require_logon", True)
                        and time.monotonic() > login_deadline):
                    log(self.channel, "login timeout", "warn")
                    break
                continue
            except OSError:
                break
            if not chunk:
                break
            self.last_recv = time.monotonic()
            self.reader.feed(chunk)
            while True:
                raw = self.reader.pop()
                if raw is None:
                    break
                self._handle(raw)
                if self.stop_event.is_set():
                    return

    def _handle(self, raw: bytes):
        try:
            msgs = list(step.iter_messages(raw))
        except Exception as exc:
            log(self.channel, "bad client message: %s" % exc, "warn")
            return
        if not msgs:
            return
        msg = msgs[0]
        msg_type = msg["msg_type"]
        seq = msg["seq"]
        if seq is not None:
            if self.client_seq is not None and seq != self.client_seq + 1:
                log(self.channel, "client MsgSeqNum %d after %d"
                    % (seq, self.client_seq), "debug")
            self.client_seq = seq
        if msg_type == "A":
            hb = step.get_field(msg, 108)
            if hb:
                try:
                    self.heartbtint = max(1, int(hb))
                except ValueError:
                    pass
            if self.logged_in:
                log(self.channel, "duplicate logon", "warn")
                return
            self.send_logon()
            self.logged_in = True
            self.last_tick_send = time.monotonic()
            self.playback_go.set()
            log(self.channel, "logon from %s:%d (HeartBtInt=%d)"
                % (self.addr[0], self.addr[1], self.heartbtint))
        elif msg_type == "0":
            return
        elif msg_type == "1":
            self.send_heartbeat(step.get_field(msg, 112))
        elif msg_type == "5":
            self.send_logout("logout acknowledged", 0)
            self.stop_event.set()
        elif msg_type == "2":
            self.send_sequence_reset()
        elif msg_type == "4":
            return
        elif msg_type == "UA002":
            self._handle_ua002(msg)
        elif msg_type == "UA001":
            return
        else:
            log(self.channel, "ignoring client MsgType=%s" % msg_type, "debug")

    # -- UA002 retransmission -------------------------------------------
    def _handle_ua002(self, msg):
        if not self.logged_in:
            log(self.channel, "UA002 before logon", "warn")
            return
        index = self.data.index
        if index is None or self.data.tick_mm is None:
            self.send_ua002_response(0, 0, 4, "no tick data on this channel")
            return
        try:
            ch = int(step.get_field(msg, 10201) or 0)
            rtype = int(step.get_field(msg, 10077) or 0)
            beg = int(step.get_field(msg, 1182) or 0)
            end_raw = step.get_field(msg, 1183)
            end = int(end_raw) if end_raw not in (None, "") else 0
        except (TypeError, ValueError):
            self.send_ua002_response(0, 0, 99, "bad UA002 fields")
            return
        if ch != self.channel:
            self.send_ua002_response(beg, end, 99, "channel mismatch")
            return
        if rtype != 1:
            self.send_ua002_response(beg, end, 99, "unsupported ResendType")
            return
        if beg <= 0:
            self.send_ua002_response(beg, end, 99, "bad ApplBegSeqNum")
            return
        if end == 0:
            end = index.last_appl
        if end < beg:
            self.send_ua002_response(beg, end, 99, "end before begin")
            return
        if not self.retransmit_lock.acquire(blocking=False):
            self.send_ua002_response(beg, end, 5, "retransmission in progress")
            return
        try:
            first, last = index.first_appl, index.last_appl
            sb = max(beg, first)
            se = min(end, last)
            status = 1
            if sb > se:
                status = 4
                sb = se = 0
            else:
                if beg < first or end > last:
                    status = 2
                max_resend = int(self.cfg.get("max_resend", 200))
                if se - sb + 1 > max_resend:
                    se = sb + max_resend - 1
                    status = 2
            sent = 0
            with self.stream_lock:
                if status != 4:
                    for seq in range(sb, se + 1):
                        raw = self.data.read_tick(seq)
                        if raw is None:
                            status = 2
                            break
                        if not self._send_rewrapped_raw(
                                raw, poss_dup=bool(self.cfg.get("mark_possdup", False))):
                            return
                        sent += 1
                self.send_ua002_response(
                    sb if sent else beg,
                    se if sent else 0,
                    status,
                    "" if status == 1 else "partial" if status == 2 else "unavailable",
                )
            log(self.channel, "UA002 %d..%d -> sent %d status=%d"
                % (beg, end, sent, status))
        finally:
            self.retransmit_lock.release()

    # -- playback --------------------------------------------------------
    def _playback_loop(self):
        try:
            while not self.playback_go.is_set():
                if self.stop_event.wait(0.2):
                    return
            if self.stop_event.is_set():
                return
            start_ms = self.cfg.get("start_ms")
            end_ms = self.cfg.get("end_ms")
            streams = []
            if self.data.tick_mm is not None:
                streams.append(iter(FileMessageStream(self.data.tick_mm,
                                                      start_ms, end_ms)))
            # A snapshot-only session (the global snapshot port) must always
            # play its file, even when per-channel snapshot interleaving is
            # disabled with --no-snapshots.
            if (self.data.snapshot_mm is not None
                    and (self.cfg.get("with_snapshots", True)
                         or self.data.tick_mm is None)):
                streams.append(iter(FileMessageStream(self.data.snapshot_mm,
                                                      start_ms, end_ms)))
            if not streams:
                log(self.channel, "no playback data", "warn")
                return
            if len(streams) == 2:
                it = heapq.merge(streams[0], streams[1],
                                 key=lambda m: m.time_ms)
            else:
                it = streams[0]
            pacer = Pacer(
                speed=self.cfg.get("speed", 1.0),
                topspeed=self.cfg.get("topspeed", False),
                pps=self.cfg.get("pps"),
                mbps=self.cfg.get("mbps"),
                max_gap_ms=self.cfg.get("max_gap_ms", 5000),
            )
            first = True
            t0 = time.monotonic()
            n = 0
            for msg in it:
                if self.stop_event.is_set():
                    return
                if first:
                    pacer.reset(msg.time_ms)
                    first = False
                    self.last_tick_send = time.monotonic()
                    log(self.channel, "playback started at %s"
                        % (msg.sending_time or msg.time_ms))
                if not pacer.wait(msg.time_ms, len(msg.raw), self.stop_event):
                    return
                with self.stream_lock:
                    if not self._send_rewrapped_raw(msg.raw, poss_dup=False):
                        return
                if msg.msg_type == "UB001":
                    self.last_tick_send = time.monotonic()
                    if msg.appl_seq is not None:
                        self.last_appl_sent = msg.appl_seq
                n += 1
                self.count_sent = n
                if n % 100000 == 0:
                    log(self.channel, "played %d messages (%.1fs)"
                        % (n, time.monotonic() - t0))
            self.playback_done = True
            log(self.channel, "playback finished: %d messages in %.1fs"
                % (n, time.monotonic() - t0))
            if self.data.index is not None:
                self.send_ua001(end_of_channel=True)
            if self.cfg.get("close_at_end"):
                time.sleep(0.2)
                self.stop_event.set()
        except Exception:
            log(self.channel, "playback error:\n%s" % traceback.format_exc(),
                "error")
            self.stop_event.set()

    # -- heartbeats ------------------------------------------------------
    def _heartbeat_loop(self):
        while not self.stop_event.wait(0.5):
            if not self.logged_in:
                continue
            now = time.monotonic()
            if now - self.last_send >= self.heartbtint:
                self.send_heartbeat()
            interval = float(self.cfg.get("channel_heartbeat", 3.0) or 0)
            if (interval > 0 and self.data.index is not None
                    and not self.playback_done):
                idle_since = max(self.last_tick_send, self.last_ua001)
                if now - idle_since >= interval:
                    self.send_ua001(end_of_channel=False)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

class GatewayServer:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.channels = {}          # channel -> ChannelData
        self.listeners = []         # (socket, channel, data)
        self.sessions = set()
        self.sessions_lock = threading.Lock()
        self.threads = []
        self.snapshot_all = None

    def _prepare(self):
        data_dir = self.cfg["data_dir"]
        tick_dir = os.path.join(data_dir, "tick")
        snap_dir = os.path.join(data_dir, "snapshot")
        for ch in self.cfg["channels"]:
            tick_path = os.path.join(tick_dir, "step_tick_%d.bin" % ch)
            snap_path = os.path.join(snap_dir, "step_snapshot_%d.bin" % ch)
            if not os.path.exists(tick_path) and not os.path.exists(snap_path):
                log(ch, "no tick/snapshot file, channel skipped", "warn")
                continue
            log(ch, "loading tick=%s snapshot=%s"
                % (os.path.basename(tick_path) if os.path.exists(tick_path) else "-",
                   os.path.basename(snap_path) if os.path.exists(snap_path) else "-"))
            data = ChannelData(ch, tick_path, snap_path,
                               rebuild_index=self.cfg.get("rebuild_index", False))
            self.channels[ch] = data
            if data.index is not None:
                log(ch, "tick messages=%d ApplSeqNum=%d..%d"
                    % (data.index.count, data.index.first_appl,
                       data.index.last_appl))
            if data.snapshot_mm is not None:
                log(ch, "snapshot bytes=%d" % len(data.snapshot_mm))

        all_path = os.path.join(snap_dir, "step_snapshot_all.bin")
        if self.cfg.get("snapshot_all_port") and os.path.exists(all_path):
            self.snapshot_all = ChannelData(1000, None, all_path)
            log("ALL", "global snapshot file loaded: %s" % all_path)
        elif self.cfg.get("snapshot_all_port"):
            log("ALL", "snapshot_all_port requested but %s not found"
                % all_path, "warn")

    def _make_listener(self, port: int):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.cfg.get("host", "0.0.0.0"), port))
        s.listen(16)
        s.settimeout(1.0)
        return s

    def _accept_loop(self, listener, data: ChannelData):
        while not self.stop_event.is_set():
            try:
                conn, addr = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.sessions_lock:
                if len(self.sessions) >= int(self.cfg.get("max_clients", 16)):
                    log(data.channel, "rejecting %s:%d: max clients"
                        % (addr[0], addr[1]), "warn")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    continue
            log(data.channel, "client connected %s:%d" % (addr[0], addr[1]))
            sess = Session(self, conn, addr, data, self.cfg)
            with self.sessions_lock:
                self.sessions.add(sess)
            t = threading.Thread(target=self._run_session, args=(sess,),
                                 name="session-%d" % data.channel, daemon=True)
            t.start()
            self.threads.append(t)

    def _run_session(self, sess: Session):
        try:
            sess.run()
        finally:
            with self.sessions_lock:
                self.sessions.discard(sess)

    def start(self):
        self._prepare()
        if not self.channels and self.snapshot_all is None:
            raise RuntimeError("no channels with data to serve")
        base = int(self.cfg.get("base_port", 9001))
        for ch, data in sorted(self.channels.items()):
            port = base + (ch - 1001)
            listener = self._make_listener(port)
            self.listeners.append((listener, data))
            log(ch, "listening on %s:%d" % (self.cfg.get("host", "0.0.0.0"), port))
        if self.snapshot_all is not None:
            port = int(self.cfg["snapshot_all_port"])
            listener = self._make_listener(port)
            self.listeners.append((listener, self.snapshot_all))
            log("ALL", "snapshot stream listening on %s:%d"
                % (self.cfg.get("host", "0.0.0.0"), port))
        for listener, data in self.listeners:
            t = threading.Thread(target=self._accept_loop, args=(listener, data),
                                 name="accept-%d" % data.channel, daemon=True)
            t.start()
            self.threads.append(t)
        log("GW", "ready; Ctrl+C to stop")

    def run_forever(self):
        try:
            self.start()
            while not self.stop_event.wait(0.5):
                pass
        except KeyboardInterrupt:
            log("GW", "interrupted")
        finally:
            self.stop()

    def stop(self):
        self.stop_event.set()
        for listener, _ in self.listeners:
            try:
                listener.close()
            except Exception:
                pass
        with self.sessions_lock:
            sessions = list(self.sessions)
        for sess in sessions:
            try:
                sess.close()
            except Exception:
                pass
        cur = threading.current_thread()
        for t in list(self.threads):
            if t is not cur and t.is_alive():
                t.join(timeout=1.0)
        for data in list(self.channels.values()) + (
                [self.snapshot_all] if self.snapshot_all else []):
            try:
                data.close()
            except Exception:
                pass
        log("GW", "stopped")

    def plan(self):
        self._prepare()
        for ch, data in sorted(self.channels.items()):
            port = int(self.cfg.get("base_port", 9001)) + (ch - 1001)
            tick = ("%d msgs (ApplSeqNum %d..%d)"
                    % (data.index.count, data.index.first_appl,
                       data.index.last_appl)) if data.index else "-"
            snap = ("%d bytes" % len(data.snapshot_mm)) if data.snapshot_mm else "-"
            print("channel %d: port %d, tick %s, snapshot %s"
                  % (ch, port, tick, snap))
        if self.snapshot_all is not None:
            print("global snapshot: port %d, %d bytes"
                  % (int(self.cfg["snapshot_all_port"]),
                     len(self.snapshot_all.snapshot_mm)))
        if self.cfg.get("with_snapshots", True):
            print("snapshots interleaved with ticks by exchange time")
        print("playback: speed=%s topspeed=%s pps=%s mbps=%s max_gap_ms=%s"
              % (self.cfg.get("speed"), self.cfg.get("topspeed"),
                 self.cfg.get("pps"), self.cfg.get("mbps"),
                 self.cfg.get("max_gap_ms")))
        print("start: %s" % ("on TCP connect" if self.cfg.get("start_on_connect")
                             else "after logon"))
        self.stop()
