# -*- coding: utf-8 -*-
"""Minimal STEP / FIXT.1.1 encoder and parser for the SSE market-data gateway.

Reference:
  IS120 上海证券交易所行情网关 STEP 数据接口规范 0.62 (2026-07-03)

The encoder emits standard FIX framing:

      8=FIXT.1.1<SOH>9=<BodyLength><SOH>35=<MsgType><SOH>...<SOH>10=<CheckSum><SOH>

BodyLength counts every byte after the SOH that terminates the 9= field up to
and including the SOH immediately before "10=".  CheckSum is the low 8 bits of
the sum of every byte from the '8' of "8=" up to (not including) "10=".

The implementation is deliberately dependency-free so that the rebuild tool can
run on a bare Windows/Python installation.  Messages produced here can also be
parsed by QuickFIX-style engines with an appropriate data dictionary.
"""

from __future__ import annotations

import re
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

SOH = b"\x01"
BEGIN_STRING = b"FIXT.1.1"
BEGIN_FIELD = b"8=" + BEGIN_STRING + SOH


class StepError(ValueError):
    pass


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def field(tag: int, value) -> bytes:
    """Encode one FIX field.  Values are ASCII unless bytes are supplied."""
    if isinstance(value, bytes):
        raw = value
    else:
        raw = str(value).encode("ascii")
    return b"%d=%s\x01" % (tag, raw)


def fields(items: Iterable[Tuple[int, object]]) -> bytes:
    out = bytearray()
    for tag, value in items:
        out += field(tag, value)
    return bytes(out)


def format_price_5(px_int: int) -> str:
    """SSE STEP N14(5) price.  Source prices are integer 1/10000 units."""
    if px_int < 0:
        raise StepError("negative price")
    return "%d.%05d" % (px_int // 10000, (px_int % 10000) * 10)


def format_amount_2(amount_yuan: int) -> str:
    """SSE STEP N17(2) amount from an integer number of yuan."""
    if amount_yuan < 0:
        raise StepError("negative amount")
    return "%d.00" % amount_yuan


def format_trade_amount_2(px_int: int, qty: int) -> str:
    """Trade amount = price * qty, rounded to cents (half up)."""
    if px_int < 0 or qty < 0:
        raise StepError("negative trade amount input")
    cents = (px_int * qty + 50) // 100  # px_int is 1e-4 yuan
    return "%d.%02d" % (cents // 100, cents % 100)


def format_time9(t) -> str:
    """HHMMSSsss, zero padded to 9 digits.  Source files omit leading zeros."""
    s = str(t).strip()
    if not s.isdigit():
        raise StepError("bad exchange time %r" % (t,))
    if len(s) > 9:
        raise StepError("exchange time too long: %r" % (t,))
    s = s.zfill(9)
    hh, mm, ss = int(s[0:2]), int(s[2:4]), int(s[4:6])
    if hh > 23 or mm > 59 or ss > 59:
        raise StepError("invalid exchange time %r" % (t,))
    return s


def time_hhmmss(t) -> str:
    s = format_time9(t)
    return "%s:%s:%s.%s" % (s[0:2], s[2:4], s[4:6], s[6:9])


def time_to_ms(t) -> int:
    """Milliseconds since midnight.  Correct across second/minute boundaries.

    HHMMSSsss is a decimal encoding, so raw numeric differences are wrong at
    :00 / minute boundaries (e.g. 093059999 -> 093100000 differs by 40001).
    """
    t = int(t)
    ms = t % 1000
    sec = (t // 1000) % 100
    minute = (t // 100000) % 100
    hour = t // 10000000
    return ((hour * 60 + minute) * 60 + sec) * 1000 + ms


def sending_time(trade_date: str, t) -> str:
    return "%s-%s" % (trade_date, time_hhmmss(t))


# ---------------------------------------------------------------------------
# Message builder
# ---------------------------------------------------------------------------

def encode_message(
    msg_type: str,
    seq: int,
    send_time: str,
    body: Sequence[Tuple[int, object]],
    sender: str = "MDGW",
    target: str = "VSS",
    message_encoding: Optional[str] = "GBK",
) -> bytes:
    """Build one complete STEP message.

    msg_type   : e.g. "W", "UB001", "h", "A"
    seq        : session MsgSeqNum (tag 34)
    send_time  : exchange time, YYYYMMDD-HH:mm:ss.sss
    body       : application fields *after* the message header
    """
    out = bytearray()
    out += b"35=" + msg_type.encode("ascii") + SOH
    out += field(49, sender)
    out += field(56, target)
    out += field(34, seq)
    out += field(52, send_time)
    if message_encoding:
        out += field(347, message_encoding)
    out += fields(body)

    msg = bytearray()
    msg += BEGIN_FIELD
    msg += b"9=" + str(len(out)).encode("ascii") + SOH
    msg += out
    checksum = sum(msg) & 0xFF
    msg += b"10=%03d" % checksum + SOH
    return bytes(msg)


# ---------------------------------------------------------------------------
# Application-message helpers
# ---------------------------------------------------------------------------

def encode_ub001(
    seq: int,
    send_time: str,
    security_type: str,
    channel: int,
    appl_seq: int,
    md_stream_id: str,
    security_id: str,
    transact_time,
    exec_type: str,
    price_int: int,
    qty: int,
    ext_fields: Sequence[Tuple[int, object]] = (),
    sender: str = "MDGW",
    target: str = "VSS",
) -> bytes:
    body = [
        (167, security_type),
        (10201, channel),
        (1181, appl_seq),
        (1500, md_stream_id),
        (48, security_id),
        (60, format_time9(transact_time)),
        (150, exec_type),
        (44, format_price_5(price_int)),
        (38, qty),
    ]
    body.extend(ext_fields)
    return encode_message("UB001", seq, send_time, body, sender=sender, target=target)


def encode_snapshot(
    seq: int,
    send_time: str,
    security_type: str,
    trade_date: str,
    last_update_time,
    md_stream_id: str,
    security_id: str,
    prev_close_px: int,
    total_volume: int,
    num_trades: int,
    total_value_yuan: int,
    entries: Sequence[Tuple[str, int, Optional[int], Optional[int]]],
    trading_phase_code: str,
    symbol: Optional[str] = None,
    sender: str = "MDGW",
    target: str = "VSS",
) -> bytes:
    """entries: (MDEntryType, price_int, size_or_None, position_or_None)."""
    body: List[Tuple[int, object]] = [
        (167, security_type),
        (339, 3),                      # 3 = 产品（正常交易）
        (75, trade_date),
        (779, format_time9(last_update_time)),
        (1500, md_stream_id),
        (48, security_id),
    ]
    if symbol:
        body.append((55, symbol))
    body.extend(
        [
            (140, format_price_5(prev_close_px)),
            (387, total_volume),
            (8503, num_trades),
            (8504, format_amount_2(total_value_yuan)),
            (268, len(entries)),
        ]
    )
    for entry_type, px, size, position in entries:
        body.append((269, entry_type))
        body.append((270, format_price_5(px)))
        if size is not None:
            body.append((271, size))
        if position is not None:
            body.append((290, position))
    body.append((8538, trading_phase_code))
    return encode_message("W", seq, send_time, body, sender=sender, target=target)


def encode_market_status(
    seq: int,
    send_time: str,
    security_type: str,
    trad_ses_mode: int,
    trading_session_id: str,
    total_related_sym: int,
    sender: str = "MDGW",
    target: str = "VSS",
) -> bytes:
    """MsgType=h (market status).  Kept for completeness; not emitted by default."""
    body = [
        (167, security_type),
        (339, trad_ses_mode),
        (336, trading_session_id),
        (393, total_related_sym),
    ]
    return encode_message("h", seq, send_time, body, sender=sender, target=target)


# ---------------------------------------------------------------------------
# Parser / validator
# ---------------------------------------------------------------------------

def _parse_field_block(block: bytes) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for part in block.split(SOH):
        if not part:
            continue
        if b"=" not in part:
            raise StepError("field without '=': %r" % (part[:40],))
        tag_s, value_b = part.split(b"=", 1)
        try:
            tag = int(tag_s)
        except ValueError:
            raise StepError("bad tag %r" % (tag_s[:20],))
        out.append((tag, value_b.decode("gbk", errors="replace")))
    return out


def iter_messages(data: bytes) -> Iterator[dict]:
    """Yield parsed messages from a concatenated STEP byte stream.

    Each result: {msg_type, seq, body_len, checksum, start, end,
                  header: [(tag, value)...], body: [(tag, value)...]}
    The parser is deliberately forgiving about field order and unknown tags.
    """
    pos = 0
    n = len(data)
    while pos < n:
        start = data.find(BEGIN_FIELD, pos)
        if start < 0:
            # Do not copy the whole tail when data is an mmap of a multi-GB file.
            tail = data[pos:min(n, pos + 64)]
            if tail.strip(b"\x00\r\n \t"):
                raise StepError("trailing garbage at offset %d" % pos)
            return
        p = start + len(BEGIN_FIELD)
        if data[p:p + 2] != b"9=":
            raise StepError("BodyLength is not the second field at %d" % start)
        e = data.find(SOH, p)
        if e < 0:
            raise StepError("unterminated BodyLength at %d" % start)
        body_len = int(data[p + 2:e])
        body_start = e + 1
        body_end = body_start + body_len
        if body_end > n:
            raise StepError("truncated message at %d" % start)
        if data[body_end:body_end + 3] != b"10=":
            raise StepError("missing CheckSum at %d" % body_end)
        cs_end = data.find(SOH, body_end)
        if cs_end < 0:
            raise StepError("unterminated CheckSum at %d" % body_end)
        cs_text = data[body_end + 3:cs_end]
        if len(cs_text) != 3 or not cs_text.isdigit():
            raise StepError("bad CheckSum field at %d" % body_end)
        checksum = int(cs_text)
        calc = sum(data[start:body_end]) & 0xFF
        if calc != checksum:
            raise StepError(
                "checksum mismatch at %d: field=%d calc=%d" % (start, checksum, calc)
            )
        block = data[body_start:body_end]
        all_fields = _parse_field_block(block)
        if not all_fields or all_fields[0][0] != 35:
            raise StepError("MsgType is not the first body field at %d" % start)
        msg_type = all_fields[0][1]
        seq = None
        header = []
        body = []
        in_header = True
        for tag, value in all_fields[1:]:
            if in_header and tag in (49, 56, 34, 43, 97, 52, 347):
                header.append((tag, value))
                if tag == 34:
                    seq = int(value)
            else:
                in_header = False
                body.append((tag, value))
        yield {
            "msg_type": msg_type,
            "seq": seq,
            "body_len": body_len,
            "checksum": checksum,
            "start": start,
            "end": cs_end + 1,
            "header": header,
            "body": body,
        }
        pos = cs_end + 1


def parse_stream(data: bytes) -> List[dict]:
    return list(iter_messages(data))


def get_field(msg: dict, tag: int, default=None):
    for t, v in msg["body"]:
        if t == tag:
            return v
    return default


def get_all(msg: dict, tag: int) -> List[str]:
    return [v for t, v in msg["body"] if t == tag]


def parse_md_entries(msg: dict) -> List[dict]:
    """Decode the NoMDEntries group of a W message."""
    count = int(get_field(msg, 268, "0") or 0)
    entries: List[dict] = []
    current = None
    for tag, value in msg["body"]:
        if tag == 269:
            if current is not None:
                entries.append(current)
            current = {"type": value}
        elif current is not None and tag in (270, 271, 290):
            current[tag] = value
    if current is not None:
        entries.append(current)
    if count != len(entries):
        raise StepError(
            "NoMDEntries=%d but decoded %d entries" % (count, len(entries))
        )
    return entries


_TIME_RE = re.compile(r"^(\d{8})-(\d{2}):(\d{2}):(\d{2})\.(\d{3})$")


def parse_sending_time(value: str) -> Tuple[str, str]:
    m = _TIME_RE.match(value)
    if not m:
        raise StepError("bad SendingTime %r" % value)
    date = m.group(1)
    hhmmsssss = m.group(2) + m.group(3) + m.group(4) + m.group(5)
    return date, hhmmsssss
