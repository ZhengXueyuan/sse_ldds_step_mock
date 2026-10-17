# -*- coding: utf-8 -*-
"""Readers for the Wind per-security CSV files.

Directory layout (as delivered in D:\\data\\20261015):

    <CODE>.SH/
        行情.csv       3-second snapshots, 10-level book
        逐笔委托.csv   order stream (add/delete plus product-status records)
        逐笔成交.csv   trade stream

The tick files use the exchange's original per-channel ApplSeqNum.  Because a
per-security file only contains that security's subset of the channel stream,
the sequence numbers are sparse and are *not* comparable across securities from
different original channels.  We therefore renumber continuously per output
channel while preserving each security's internal order.
"""

from __future__ import annotations

import csv
import io
import os
from typing import List, Optional, Tuple

from .step import StepError


# Event tuple layout used by the tick pipeline:
#   (time, app_seq, kind, security_id, quote_id, side, price, qty, bid_ref, offer_ref)
# kind: 'A' add, 'D' delete, 'S' product status, 'F' trade
TickEvent = Tuple[int, int, str, str, str, str, int, int, str, str]

ORDER_HEADER = ["万得代码", "交易所代码", "自然日", "时间", "委托编号", "交易所委托号",
                "委托类型", "委托代码", "委托价格", "委托数量"]
TRADE_HEADER = ["万得代码", "交易所代码", "自然日", "时间", "成交编号", "成交代码",
                "委托代码", "BS标志", "成交价格", "成交数量", "叫卖序号", "叫买序号"]
SNAPSHOT_HEADER = [
    "万得代码", "交易所代码", "自然日", "时间", "成交价", "成交量", "成交额", "成交笔数",
    "IOPV", "成交标志", "BS标志", "当日累计成交量", "当日成交额", "最高价", "最低价",
    "开盘价", "前收盘",
    "申卖价1", "申卖价2", "申卖价3", "申卖价4", "申卖价5",
    "申卖价6", "申卖价7", "申卖价8", "申卖价9", "申卖价10",
    "申卖量1", "申卖量2", "申卖量3", "申卖量4", "申卖量5",
    "申卖量6", "申卖量7", "申卖量8", "申卖量9", "申卖量10",
    "申买价1", "申买价2", "申买价3", "申买价4", "申买价5",
    "申买价6", "申买价7", "申买价8", "申买价9", "申买价10",
    "申买量1", "申买量2", "申买量3", "申买量4", "申买量5",
    "申买量6", "申买量7", "申买量8", "申买量9", "申买量10",
    "加权平均叫卖价", "加权平均叫买价", "叫卖总量", "叫买总量",
]


def discover_codes(data_dir: str) -> List[str]:
    """Return sorted directory names such as '600000.SH'."""
    codes = []
    with os.scandir(data_dir) as it:
        for entry in it:
            if entry.is_dir() and entry.name.upper().endswith(".SH"):
                codes.append(entry.name)
    codes.sort(key=_code_sort_key)
    return codes


def _code_sort_key(code: str):
    stem = code.split(".", 1)[0]
    try:
        return (0, int(stem), code)
    except ValueError:
        return (1, 0, code)


def code_stem(code: str) -> str:
    return code.split(".", 1)[0]


def channel_of(code: str, first: int = 1001, count: int = 8) -> int:
    """channel = 1001 + (numeric security code mod 8)."""
    return first + (int(code_stem(code)) % count)


def _open_csv(path: str):
    # errors='replace' keeps a single malformed byte from aborting a 15 GB run;
    # the affected field will surface later as a parse error with context.
    return io.open(path, "r", encoding="gbk", errors="replace", newline="")


def _check_header(got: List[str], expected: List[str], path: str) -> None:
    # The files carry a trailing comma, hence an extra empty header cell.
    got = [x for x in got if x != ""]
    if got[:len(expected)] != expected:
        raise StepError(
            "unexpected header in %s\nexpected=%r\ngot     =%r"
            % (path, expected, got)
        )


class TickEventStream:
    """Merge one security's order and trade CSVs by (time, ApplSeqNum)."""

    __slots__ = ("code", "orders_fh", "trades_fh", "o_reader", "t_reader",
                 "o_next", "t_next", "o_line", "t_line", "o_last", "t_last",
                 "closed", "date")

    def __init__(self, security_dir: str, code: str):
        self.code = code
        op = os.path.join(security_dir, "逐笔委托.csv")
        tp = os.path.join(security_dir, "逐笔成交.csv")
        self.orders_fh = _open_csv(op)
        self.trades_fh = _open_csv(tp)
        self.o_reader = csv.reader(self.orders_fh)
        self.t_reader = csv.reader(self.trades_fh)
        try:
            oh = next(self.o_reader)
            th = next(self.t_reader)
        except StopIteration:
            raise StepError("empty tick CSV in %s" % security_dir)
        _check_header(oh, ORDER_HEADER, op)
        _check_header(th, TRADE_HEADER, tp)
        self.o_line = 1
        self.t_line = 1
        self.o_last = (-1, -1)
        self.t_last = (-1, -1)
        self.closed = False
        self.date = None
        self.o_next = self._read_order()
        self.t_next = self._read_trade()

    def _read_order(self) -> Optional[TickEvent]:
        for row in self.o_reader:
            self.o_line += 1
            if not row or not any(row):
                continue
            if len(row) < 10:
                raise StepError("%s 逐笔委托.csv:%d: short row" % (self.code, self.o_line))
            try:
                t = int(row[3])
                seq = int(row[4])
                kind = row[6].strip()
                side = row[7].strip()
                price = int(row[8])
                qty = int(row[9])
            except ValueError as exc:
                raise StepError(
                    "%s 逐笔委托.csv:%d: %s (%r)"
                    % (self.code, self.o_line, exc, row[:10])
                )
            self._note_date(row[2], "逐笔委托.csv")
            key = (t, seq)
            if key < self.o_last:
                raise StepError(
                    "%s 逐笔委托.csv:%d: not sorted by (time, ApplSeqNum): %r after %r"
                    % (self.code, self.o_line, key, self.o_last)
                )
            self.o_last = key
            return (t, seq, kind, row[1].strip(), row[5].strip(), side,
                    price, qty, "", "")
        return None

    def _read_trade(self) -> Optional[TickEvent]:
        for row in self.t_reader:
            self.t_line += 1
            if not row or not any(row):
                continue
            if len(row) < 12:
                raise StepError("%s 逐笔成交.csv:%d: short row" % (self.code, self.t_line))
            try:
                t = int(row[3])
                seq = int(row[4])
                price = int(row[8])
                qty = int(row[9])
            except ValueError as exc:
                raise StepError(
                    "%s 逐笔成交.csv:%d: %s (%r)"
                    % (self.code, self.t_line, exc, row[:12])
                )
            self._note_date(row[2], "逐笔成交.csv")
            key = (t, seq)
            if key < self.t_last:
                raise StepError(
                    "%s 逐笔成交.csv:%d: not sorted by (time, ApplSeqNum): %r after %r"
                    % (self.code, self.t_line, key, self.t_last)
                )
            self.t_last = key
            # row[7] is the aggressor side; row[10]/row[11] are sell/buy order ids
            return (t, seq, "F", row[1].strip(), "", row[7].strip(), price, qty,
                    row[11].strip(), row[10].strip())
        return None

    def _note_date(self, date: str, source: str) -> None:
        if self.date is None:
            self.date = date
        elif date != self.date:
            raise StepError(
                "%s %s: mixed trade dates %r and %r"
                % (self.code, source, self.date, date)
            )

    def __iter__(self):
        return self

    def __next__(self) -> TickEvent:
        o, t = self.o_next, self.t_next
        if o is None and t is None:
            self.close()
            raise StopIteration
        if o is None:
            self.t_next = self._read_trade()
            return t  # type: ignore[return-value]
        if t is None:
            self.o_next = self._read_order()
            return o
        if (o[0], o[1]) <= (t[0], t[1]):
            self.o_next = self._read_order()
            return o
        self.t_next = self._read_trade()
        return t

    def close(self):
        if not self.closed:
            self.closed = True
            self.orders_fh.close()
            self.trades_fh.close()


class SnapshotEventStream:
    """Iterate one security's 行情.csv rows in time order."""

    __slots__ = ("code", "fh", "reader", "line", "last_time", "next_row", "closed", "date")

    def __init__(self, security_dir: str, code: str):
        self.code = code
        path = os.path.join(security_dir, "行情.csv")
        self.fh = _open_csv(path)
        self.reader = csv.reader(self.fh)
        try:
            header = next(self.reader)
        except StopIteration:
            raise StepError("empty snapshot CSV in %s" % security_dir)
        _check_header(header, SNAPSHOT_HEADER, path)
        self.line = 1
        self.last_time = -1
        self.closed = False
        self.date = None
        self.next_row = self._read()

    def _read(self):
        for row in self.reader:
            self.line += 1
            if not row or not any(row):
                continue
            if len(row) < 57:
                raise StepError(
                    "%s 行情.csv:%d: short row (%d columns)"
                    % (self.code, self.line, len(row))
                )
            try:
                t = int(row[3])
            except ValueError as exc:
                raise StepError(
                    "%s 行情.csv:%d: %s" % (self.code, self.line, exc)
                )
            if t < self.last_time:
                raise StepError(
                    "%s 行情.csv:%d: not time sorted: %d after %d"
                    % (self.code, self.line, t, self.last_time)
                )
            self.last_time = t
            date = row[2].strip()
            if self.date is None:
                self.date = date
            elif date != self.date:
                raise StepError(
                    "%s 行情.csv:%d: mixed trade dates %r and %r"
                    % (self.code, self.line, self.date, date)
                )
            return (t, row)
        return None

    def __iter__(self):
        return self

    def __next__(self):
        row = self.next_row
        if row is None:
            self.close()
            raise StopIteration
        self.next_row = self._read()
        return row

    def close(self):
        if not self.closed:
            self.closed = True
            self.fh.close()


def detect_trade_date(data_dir: str, codes: List[str]) -> str:
    """Read the 自然日 field from the first usable row of the first security."""
    for code in codes:
        for fname, col, has_header in (
            ("行情.csv", 2, True),
            ("逐笔委托.csv", 2, True),
            ("逐笔成交.csv", 2, True),
        ):
            path = os.path.join(data_dir, code, fname)
            if not os.path.exists(path):
                continue
            with _open_csv(path) as fh:
                reader = csv.reader(fh)
                try:
                    if has_header:
                        next(reader)
                    for row in reader:
                        if row and len(row) > col and row[col].strip():
                            return row[col].strip()
                except StopIteration:
                    pass
    raise StepError("cannot determine trade date under %s" % data_dir)


def parse_int(value: str, field_name: str, code: str, line: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise StepError("%s line %d: bad %s %r" % (code, line, field_name, value))
