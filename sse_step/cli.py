# -*- coding: utf-8 -*-
"""Command line interface for the SSE STEP market-data rebuild tool."""

from __future__ import annotations

import argparse
import json
import mmap
import multiprocessing
import os
import queue as queue_mod
import sys
import time
import traceback
from collections import OrderedDict
from typing import Dict, List

from . import build, step
from .source import (
    channel_of,
    code_stem,
    detect_trade_date,
    discover_codes,
)


def _reconfigure_stdout() -> None:
    try:
        if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Worker (must be importable by multiprocessing 'spawn')
# ---------------------------------------------------------------------------

def worker_entry(channel: int, codes: List[str], data_dir: str, out_dir: str,
                 cfg: dict, q) -> None:
    def progress(p: dict) -> None:
        try:
            q.put(("progress", channel, p))
        except Exception:
            pass

    try:
        result = build.build_channel(channel, codes, data_dir, out_dir, cfg, progress)
        q.put(("done", channel, result))
    except Exception:
        q.put(("error", channel, traceback.format_exc()))


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

def _parse_codes_option(value: str) -> List[str]:
    out = []
    for part in value.replace("\n", ",").replace("\r", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if not part.upper().endswith(".SH"):
            part = part + ".SH"
        out.append(part)
    return out


def _selected_codes(args, all_codes: List[str]) -> List[str]:
    wanted: List[str] = []
    if getattr(args, "codes", None):
        wanted.extend(_parse_codes_option(args.codes))
    if getattr(args, "codes_file", None):
        with open(args.codes_file, "r", encoding="utf-8-sig") as f:
            wanted.extend(_parse_codes_option(f.read()))
    if not wanted:
        return all_codes
    seen = set()
    out = []
    for code in wanted:
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def cmd_build(args) -> int:
    data_dir = os.path.abspath(args.data)
    out_dir = os.path.abspath(args.out)
    if not os.path.isdir(data_dir):
        print("error: data directory not found: %s" % data_dir, file=sys.stderr)
        return 2
    codes = discover_codes(data_dir)
    selected = _selected_codes(args, codes)
    missing = [c for c in selected if c not in codes]
    if missing:
        print("error: unknown/missing security directories: %s" % ", ".join(missing),
              file=sys.stderr)
        return 2
    codes = selected
    if args.limit:
        codes = codes[:args.limit]
    if not codes:
        print("error: no .SH securities selected", file=sys.stderr)
        return 2

    trade_date = args.date or detect_trade_date(data_dir, codes)
    if len(trade_date) != 8 or not trade_date.isdigit():
        print("error: bad trade date %r" % trade_date, file=sys.stderr)
        return 2

    grouped: "OrderedDict[int, List[str]]" = OrderedDict()
    for ch in range(args.channel_first, args.channel_first + args.channel_count):
        grouped[ch] = []
    for code in codes:
        ch = channel_of(code, args.channel_first, args.channel_count)
        grouped.setdefault(ch, []).append(code)
    grouped = OrderedDict((k, v) for k, v in grouped.items() if v)
    for ch in grouped:
        grouped[ch].sort(key=lambda c: int(code_stem(c)))

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.join(out_dir, "tick"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "snapshot"), exist_ok=True)

    with open(os.path.join(out_dir, "channels.json"), "w", encoding="utf-8") as f:
        json.dump({str(k): v for k, v in grouped.items()}, f, ensure_ascii=False, indent=1)

    if args.no_snapshot:
        snapshot_layout = "none"
    else:
        snapshot_layout = args.snapshot_layout
    cfg = {
        "trade_date": trade_date,
        "emit_stream": args.emit in ("stream", "both"),
        "emit_pcap": args.emit in ("pcap", "both"),
        "mss": args.mss,
        "tick_batch_ms": args.tick_batch_ms,
        "pcap_acks": args.pcap_acks,
        "status_mode": args.status_mode,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "max_events": args.max_events,
        "max_snapshots": args.max_snapshots,
        "no_tick": args.no_tick,
        "no_snapshot": snapshot_layout == "none",
        # channel workers only build their own snapshot files when requested
        "no_snapshot_channel": snapshot_layout not in ("channel", "both"),
        "snapshot_layout": snapshot_layout,
        "sender": args.sender,
        "target": args.target,
    }

    print("source      : %s" % data_dir)
    print("output      : %s" % out_dir)
    print("trade date  : %s" % trade_date)
    print("securities  : %d in %d channel(s)" % (len(codes), len(grouped)))
    for ch, cs in grouped.items():
        print("  %d: %d securities" % (ch, len(cs)))
    print("emit        : %s" % args.emit)
    if args.emit != "stream":
        print("mss         : %d bytes, tick batch window %d ms"
              % (args.mss, args.tick_batch_ms))

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    channels = list(grouped.keys())
    jobs = max(1, min(args.jobs, len(channels)))
    pending = list(channels)
    running: Dict[int, object] = {}
    results: Dict[int, dict] = {}
    errors: Dict[int, str] = {}
    dead_since: Dict[int, float] = {}

    def start_next():
        if not pending:
            return
        ch = pending.pop(0)
        p = ctx.Process(
            target=worker_entry,
            args=(ch, grouped[ch], data_dir, out_dir, cfg, q),
            name="channel-%d" % ch,
        )
        p.start()
        running[ch] = p
        dead_since.pop(ch, None)

    def handle(item):
        kind = item[0]
        if kind == "progress":
            _, ch, p = item
            stage = p.get("stage")
            if stage == "tick":
                print("  [%d tick    ] messages=%-10d add=%-9d del=%-8d trade=%-9d status=%-4d"
                      % (ch, p.get("messages", 0), p.get("orders_add", 0),
                         p.get("orders_delete", 0), p.get("trades", 0),
                         p.get("status", 0)))
            else:
                print("  [%d snapshot] messages=%-10d entries=%-10d"
                      % (ch, p.get("messages", 0), p.get("entries", 0)))
            return
        if kind == "done":
            _, ch, result = item
            results[ch] = result
            proc = running.pop(ch, None)
            if proc is not None:
                proc.join()
            dead_since.pop(ch, None)
            tick = result.get("tick") or {}
            snap = result.get("snapshot") or {}
            print("  [%d done    ] tick_msgs=%d tick_bytes=%d snap_msgs=%d snap_bytes=%d"
                  % (ch, tick.get("messages", 0), tick.get("bytes", 0),
                     snap.get("messages", 0), snap.get("bytes", 0)))
            start_next()
            return
        if kind == "error":
            _, ch, tb = item
            errors[ch] = tb
            proc = running.pop(ch, None)
            if proc is not None:
                proc.join()
            dead_since.pop(ch, None)
            print("  [%d FAILED]\n%s" % (ch, tb), file=sys.stderr)
            start_next()
            return

    for _ in range(jobs):
        start_next()

    try:
        while running:
            try:
                item = q.get(timeout=0.5)
            except queue_mod.Empty:
                pass
            else:
                handle(item)
                # Drain anything else that is already queued.
                while True:
                    try:
                        handle(q.get_nowait())
                    except queue_mod.Empty:
                        break
            # Reap processes that died without reporting.  Give the queue a
            # grace period first: a worker sends 'done' immediately before it
            # exits, so the message can still be in flight.
            now = time.time()
            for ch in list(running):
                p = running[ch]
                if p.is_alive():
                    dead_since.pop(ch, None)
                    continue
                first = dead_since.get(ch)
                if first is None:
                    dead_since[ch] = now
                elif now - first > 1.0:
                    p.join()
                    running.pop(ch, None)
                    dead_since.pop(ch, None)
                    if ch not in results and ch not in errors:
                        errors[ch] = "process exited with code %s" % p.exitcode
                    start_next()
    finally:
        for ch, p in list(running.items()):
            if p.is_alive():
                p.terminate()
            p.join()
        q.close()

    if errors:
        print("error: %d channel(s) failed" % len(errors), file=sys.stderr)
        for ch, tb in errors.items():
            print("--- channel %d ---\n%s" % (ch, tb), file=sys.stderr)
        return 1

    global_result = None
    if snapshot_layout in ("global", "both") and not args.no_snapshot:
        print("building global snapshot stream (all %d securities)..." % len(codes))
        last_report = [0.0]

        def gprogress(p):
            now = time.time()
            if now - last_report[0] >= 2.0:
                last_report[0] = now
                print("  [global snapshot] messages=%-10d entries=%-10d"
                      % (p.get("messages", 0), p.get("entries", 0)),
                      end="\r", flush=True)

        try:
            global_result = build.build_snapshot_global(
                codes, data_dir, out_dir, cfg, gprogress)
            print("  [global snapshot] messages=%-10d entries=%-10d  done"
                  % (global_result.get("messages", 0), global_result.get("entries", 0)))
        except Exception:
            print("\nerror: global snapshot build failed", file=sys.stderr)
            traceback.print_exc()
            return 1

    manifest = _make_manifest(data_dir, out_dir, trade_date, cfg, args, grouped,
                              results, global_result)
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    t = manifest["totals"]
    print("total tick messages     : %d" % t["tick_messages"])
    print("total snapshot messages : %d (global: %d)"
          % (t["snapshot_messages"], t["global_snapshot_messages"]))
    print("total stream bytes      : %d" % t["stream_bytes"])
    print("manifest                : %s" % os.path.join(out_dir, "manifest.json"))
    return 0


def _make_manifest(data_dir, out_dir, trade_date, cfg, args, grouped, results,
                   global_result=None) -> dict:
    tick_messages = sum((r.get("tick") or {}).get("messages", 0) for r in results.values())
    snap_messages = sum((r.get("snapshot") or {}).get("messages", 0) for r in results.values())
    tick_bytes = sum((r.get("tick") or {}).get("bytes", 0) for r in results.values())
    snap_bytes = sum((r.get("snapshot") or {}).get("bytes", 0) for r in results.values())
    tick_orders_add = sum((r.get("tick") or {}).get("orders_add", 0) for r in results.values())
    tick_orders_delete = sum((r.get("tick") or {}).get("orders_delete", 0) for r in results.values())
    tick_trades = sum((r.get("tick") or {}).get("trades", 0) for r in results.values())
    tick_status = sum((r.get("tick") or {}).get("status", 0) for r in results.values())
    tick_status_dropped = sum((r.get("tick") or {}).get("status_dropped", 0) for r in results.values())
    snap_entries = sum((r.get("snapshot") or {}).get("entries", 0) for r in results.values())

    return {
        "tool": "sse_step_rebuild",
        "format_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "source_dir": data_dir,
        "output_dir": out_dir,
        "trade_date": trade_date,
        "config": cfg,
        "channel_mapping": "channel = %d + (numeric security code %% %d)"
                           % (args.channel_first, args.channel_count),
        "channels": OrderedDict(
            (str(ch), results[ch]) for ch in sorted(results)
        ),
        "global_snapshot": global_result,
        "totals": {
            "securities": sum(len(v) for v in grouped.values()),
            "channels": len(grouped),
            "tick_messages": tick_messages,
            "tick_orders_add": tick_orders_add,
            "tick_orders_delete": tick_orders_delete,
            "tick_trades": tick_trades,
            "tick_status": tick_status,
            "tick_status_dropped": tick_status_dropped,
            "snapshot_messages": snap_messages,
            "snapshot_entries": snap_entries,
            "global_snapshot_messages": (global_result or {}).get("messages", 0),
            "global_snapshot_entries": (global_result or {}).get("entries", 0),
            "stream_bytes": tick_bytes + snap_bytes
                            + (global_result or {}).get("bytes", 0),
            "pcap_bytes": sum((r.get("tick") or {}).get("pcap_bytes", 0) for r in results.values())
                          + sum((r.get("snapshot") or {}).get("pcap_bytes", 0) for r in results.values())
                          + (global_result or {}).get("pcap_bytes", 0),
        },
        "notes": [
            "Each output file is an independent STEP session: MsgSeqNum starts at 1.",
            "ApplSeqNum is renumbered continuously per output channel; the source "
            "files carry sparse per-security subsets of the original exchange "
            "channel sequence, so cross-security order is reconstructed by "
            "TransactTime (ties broken deterministically by security code).",
            "Source product-status records (OrdType=S, code E/I/O/J/C) are emitted "
            "as UB001 with ExecType=S and Text(58)=<code>. This is an extension: "
            "IS120 0.62 defines ExecType 0/4/F only and documents UB001 for "
            "SecurityType=13 (bond non-matching).",
            "TradingPhaseCode is derived from exchange time; the source snapshot "
            "files do not carry product status.",
            "MDEntryType=9 (daily VWAP) is computed as cumulative amount / "
            "cumulative volume; it is not present in the source snapshot columns.",
            "Source order files omit some marketable orders (trades may reference "
            "order IDs with no corresponding add record). Trades are emitted with "
            "the references exactly as supplied.",
            "Symbol(55), MemberID(10211) and TraderCode(10215) are not present in "
            "the source; they are omitted rather than emitted empty, because strict "
            "FIX parsers reject empty field values.",
        ],
    }


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

def _body_value(body: bytes, tag: bytes):
    """Extract a tag value from a raw FIX body without decoding every field."""
    needle = b"\x01" + tag + b"="
    i = body.find(needle)
    if i < 0:
        return None
    i += len(needle)
    j = body.find(b"\x01", i)
    if j < 0:
        j = len(body)
    return body[i:j]


def _verify_file(path: str, expect_type: str, channel, limit: int = 0) -> dict:
    """Fast structural verification of a generated stream.

    Checks FIX framing, BodyLength, CheckSum, MsgSeqNum continuity, UB001
    ApplSeqNum/ChannelNO continuity, and the W NoMDEntries count.  It avoids
    the generic field parser, which is far too slow for multi-GB files.
    """
    last_seq = 0
    last_appl = 0
    counts = {"messages": 0, "orders_add": 0, "orders_delete": 0, "status": 0,
              "trades": 0, "snapshots": 0, "entries": 0}
    first_appl = None
    last_time = None
    size = os.path.getsize(path)
    if size == 0:
        raise step.StepError("%s: empty file" % path)
    with open(path, "rb") as f:
        data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            pos = 0
            n = len(data)
            while pos < n:
                start = data.find(step.BEGIN_FIELD, pos)
                if start < 0:
                    tail = data[pos:min(n, pos + 64)]
                    if tail.strip(b"\x00\r\n \t"):
                        raise step.StepError("%s: trailing garbage at %d" % (path, pos))
                    break
                p = start + len(step.BEGIN_FIELD)
                if data[p:p + 2] != b"9=":
                    raise step.StepError("%s: BodyLength not second at %d" % (path, start))
                e = data.find(step.SOH, p)
                if e < 0:
                    raise step.StepError("%s: unterminated BodyLength at %d" % (path, start))
                body_len = int(data[p + 2:e])
                body_start = e + 1
                body_end = body_start + body_len
                if body_end > n:
                    raise step.StepError("%s: truncated message at %d" % (path, start))
                if data[body_end:body_end + 3] != b"10=":
                    raise step.StepError("%s: missing CheckSum at %d" % (path, body_end))
                cs_end = data.find(step.SOH, body_end)
                if cs_end < 0:
                    raise step.StepError("%s: unterminated CheckSum at %d" % (path, body_end))
                if (sum(data[start:body_end]) & 0xFF) != int(data[body_end + 3:cs_end]):
                    raise step.StepError("%s: checksum mismatch at %d" % (path, start))

                body = data[body_start:body_end]
                first_soh = body.find(step.SOH)
                if first_soh < 0 or body[:3] != b"35=":
                    raise step.StepError("%s: bad MsgType field at %d" % (path, start))
                msg_type = body[3:first_soh].decode("ascii")
                if msg_type != expect_type:
                    raise step.StepError(
                        "%s: message %d type %r != %r"
                        % (path, counts["messages"] + 1, msg_type, expect_type)
                    )
                seq_raw = _body_value(body, b"34")
                if seq_raw is None:
                    raise step.StepError("%s: missing MsgSeqNum at %d" % (path, start))
                seq = int(seq_raw)
                if seq != last_seq + 1:
                    raise step.StepError(
                        "%s: MsgSeqNum jumped from %d to %d" % (path, last_seq, seq)
                    )
                last_seq = seq

                counts["messages"] += 1
                if expect_type == "UB001":
                    ch_raw = _body_value(body, b"10201")
                    if ch_raw is None:
                        raise step.StepError("%s: missing ChannelNO" % path)
                    ch = int(ch_raw)
                    if channel is not None and ch != channel:
                        raise step.StepError(
                            "%s: ChannelNO %d != %d" % (path, ch, channel)
                        )
                    appl_raw = _body_value(body, b"1181")
                    if appl_raw is None:
                        raise step.StepError("%s: missing ApplSeqNum" % path)
                    appl = int(appl_raw)
                    if appl != last_appl + 1:
                        raise step.StepError(
                            "%s: ApplSeqNum jumped from %d to %d" % (path, last_appl, appl)
                        )
                    if first_appl is None:
                        first_appl = appl
                    last_appl = appl
                    et = _body_value(body, b"150")
                    if et == b"0":
                        counts["orders_add"] += 1
                    elif et == b"4":
                        counts["orders_delete"] += 1
                    elif et == b"F":
                        counts["trades"] += 1
                    elif et == b"S":
                        counts["status"] += 1
                    else:
                        raise step.StepError(
                            "%s: unknown ExecType %r" % (path, et)
                        )
                    t = _body_value(body, b"60")
                else:
                    no_md_raw = _body_value(body, b"268")
                    if no_md_raw is None:
                        raise step.StepError("%s: missing NoMDEntries" % path)
                    no_md = int(no_md_raw)
                    actual = body.count(b"\x01269=")
                    if no_md != actual:
                        raise step.StepError(
                            "%s: NoMDEntries=%d but %d entries"
                            % (path, no_md, actual)
                        )
                    counts["snapshots"] += 1
                    counts["entries"] += actual
                    t = _body_value(body, b"779")

                if t is not None:
                    last_time = t.decode("ascii")
                pos = cs_end + 1
                if limit and counts["messages"] >= limit:
                    break
        finally:
            data.close()
    counts["first_appl"] = first_appl
    counts["last_appl"] = last_appl
    counts["last_time"] = last_time
    return counts


def cmd_verify(args) -> int:
    out_dir = os.path.abspath(args.out)
    targets = []
    for sub, pattern, mtype in (
        ("tick", "step_tick_", "UB001"),
        ("snapshot", "step_snapshot_", "W"),
    ):
        d = os.path.join(out_dir, sub)
        if not os.path.isdir(d):
            continue
        for name in sorted(os.listdir(d)):
            if not name.startswith(pattern) or not name.endswith(".bin"):
                continue
            if args.channel and ("_%d." % args.channel) not in name:
                continue
            label = name[len(pattern):-4]
            ch = None if label == "all" else int(label)
            targets.append((os.path.join(d, name), mtype, ch))
    if not targets:
        print("error: no .bin files found under %s" % out_dir, file=sys.stderr)
        return 2
    ok = True
    for path, mtype, ch in targets:
        t0 = time.time()
        try:
            c = _verify_file(path, mtype, ch, args.limit)
        except Exception as exc:
            ok = False
            print("FAIL %s: %s" % (path, exc), file=sys.stderr)
            continue
        if mtype == "UB001":
            print("OK   %s  msgs=%d add=%d del=%d trade=%d status=%d appl=%s..%s last_time=%s (%.1fs)"
                  % (path, c["messages"], c["orders_add"], c["orders_delete"],
                     c["trades"], c["status"], c["first_appl"], c["last_appl"],
                     c["last_time"], time.time() - t0))
        else:
            print("OK   %s  snapshots=%d entries=%d last_time=%s (%.1fs)"
                  % (path, c["snapshots"], c["entries"], c["last_time"], time.time() - t0))
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

def _fmt_msg(msg: dict, max_entries: int = 6) -> str:
    out = []
    out.append("type=%s seq=%s bodylen=%d checksum=%03d" % (
        msg["msg_type"], msg["seq"], msg["body_len"], msg["checksum"]))
    hdr = {t: v for t, v in msg["header"]}
    out.append("  sending_time=%s sender=%s target=%s" % (
        hdr.get(52), hdr.get(49), hdr.get(56)))
    body = msg["body"]
    if msg["msg_type"] == "W":
        base = {t: v for t, v in body if t in (167, 339, 75, 779, 1500, 48, 55, 140, 387, 8503, 8504, 268, 8538)}
        out.append("  " + " ".join("%d=%s" % (k, base[k]) for k in sorted(base)))
        entries = step.parse_md_entries(msg)
        for e in entries[:max_entries]:
            out.append("    entry type=%s px=%s size=%s pos=%s" % (
                e.get("type"), e.get(270), e.get(271), e.get(290)))
        if len(entries) > max_entries:
            out.append("    ... %d more entries" % (len(entries) - max_entries))
    elif msg["msg_type"] == "UB001":
        wanted = (167, 10201, 1181, 1500, 48, 60, 150, 44, 38,
                  54, 117, 58, 10116, 10117, 8504, 10333, 10211, 10215)
        d = {}
        for t, v in body:
            if t in wanted:
                d[t] = v
        out.append("  " + " ".join("%d=%s" % (k, d[k]) for k in sorted(d)))
    else:
        out.append("  " + " ".join("%d=%s" % (t, v) for t, v in body[:20]))
    return "\n".join(out)


def cmd_inspect(args) -> int:
    path = os.path.abspath(args.file)
    if not os.path.isfile(path):
        print("error: file not found: %s" % path, file=sys.stderr)
        return 2
    with open(path, "rb") as f:
        data = f.read(args.bytes)
    if args.offset:
        # Parse from the requested byte offset only if it is a message boundary.
        data = data[args.offset:]
    n = 0
    try:
        for msg in step.iter_messages(data):
            print(_fmt_msg(msg, args.max_entries))
            n += 1
            if args.limit and n >= args.limit:
                break
    except step.StepError as exc:
        if not args.allow_truncated:
            print("error: %s" % exc, file=sys.stderr)
            if n == 0:
                return 1
        print("(stopped after %d messages: %s)" % (n, exc), file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# channels
# ---------------------------------------------------------------------------

def cmd_channels(args) -> int:
    data_dir = os.path.abspath(args.data)
    all_codes = discover_codes(data_dir)
    codes = _selected_codes(args, all_codes)
    missing = [c for c in codes if c not in all_codes]
    if missing:
        print("error: unknown/missing: %s" % ", ".join(missing), file=sys.stderr)
        return 2
    if args.limit:
        codes = codes[:args.limit]
    grouped: "OrderedDict[int, List[str]]" = OrderedDict()
    for code in codes:
        grouped.setdefault(channel_of(code, args.channel_first, args.channel_count), []).append(code)
    for ch in sorted(grouped):
        grouped[ch].sort(key=lambda c: int(code_stem(c)))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in grouped.items()}, f,
                      ensure_ascii=False, indent=1)
        print("wrote %s (%d channels, %d securities)" % (args.out, len(grouped), len(codes)))
    else:
        for ch, cs in grouped.items():
            print("%d (%d): %s" % (ch, len(cs), ",".join(cs)))
    return 0


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------

def _time_arg(value: str) -> int:
    s = value.replace(":", "").replace(".", "")
    if not s.isdigit() or len(s) > 9:
        raise argparse.ArgumentTypeError("time must look like 093000000, 93000000 or 09:30:00.000")
    return int(s.zfill(9))


def _add_common(parser):
    parser.add_argument("--channel-first", type=int, default=1001,
                        help="first channel number (default 1001)")
    parser.add_argument("--channel-count", type=int, default=8,
                        help="number of channels (default 8)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sse-step-rebuild",
        description="Rebuild SSE STEP tick (UB001) and snapshot (W) streams "
                    "from Wind per-security CSV files.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="build channel streams")
    b.add_argument("--data", required=True, help="input directory containing <CODE>.SH directories")
    b.add_argument("--out", required=True, help="output directory")
    b.add_argument("--codes", help="comma separated codes (e.g. 600000.SH,600004.SH)")
    b.add_argument("--codes-file", help="text file with codes, one per line or comma separated")
    b.add_argument("--limit", type=int, help="use only the first N securities")
    b.add_argument("--date", help="trade date YYYYMMDD (default: detect from data)")
    b.add_argument("--emit", choices=("stream", "pcap", "both"), default="stream",
                   help="stream: concatenated STEP messages; pcap: TCP capture; both (default stream)")
    b.add_argument("--mss", type=int, default=1460,
                   help="TCP payload size for pcap output (default 1460 = 1500 MTU)")
    b.add_argument("--tick-batch-ms", type=int, default=10,
                   help="flush a partial tick packet after this idle gap (default 10 ms)")
    b.add_argument("--pcap-acks", action="store_true",
                   help="include client ACKs every second data packet in pcap output")
    b.add_argument("--status-mode", choices=("ub001", "drop"), default="ub001",
                   help="emit source OrdType=S records as UB001/ExecType=S, or drop them")
    b.add_argument("--start-time", type=_time_arg, help="drop events before this exchange time")
    b.add_argument("--end-time", type=_time_arg, help="stop after this exchange time")
    b.add_argument("--max-events", type=int, help="per channel tick message cap (testing)")
    b.add_argument("--max-snapshots", type=int, help="per output snapshot stream message cap (testing)")
    b.add_argument("--no-tick", action="store_true", help="skip the tick stream")
    b.add_argument("--no-snapshot", action="store_true", help="skip the snapshot stream")
    b.add_argument("--snapshot-layout", choices=("channel", "global", "both"),
                   default="channel",
                   help="channel: one snapshot file per channel (default); "
                        "global: one stream containing all contracts; both")
    b.add_argument("--jobs", type=int, default=8, help="parallel channel workers (default 8)")
    b.add_argument("--sender", default="MDGW", help="SenderCompID (default MDGW)")
    b.add_argument("--target", default="VSS", help="TargetCompID (default VSS)")
    _add_common(b)
    b.set_defaults(func=cmd_build)

    v = sub.add_parser("verify", help="validate generated STEP streams")
    v.add_argument("--out", required=True, help="output directory")
    v.add_argument("--channel", type=int, help="only verify one channel")
    v.add_argument("--limit", type=int, default=0, help="verify at most N messages per file")
    v.set_defaults(func=cmd_verify)

    i = sub.add_parser("inspect", help="print decoded STEP messages")
    i.add_argument("--file", required=True)
    i.add_argument("--limit", type=int, default=5)
    i.add_argument("--bytes", type=int, default=2 * 1024 * 1024,
                   help="maximum bytes to read (default 2 MiB)")
    i.add_argument("--offset", type=int, default=0)
    i.add_argument("--max-entries", type=int, default=6)
    i.add_argument("--allow-truncated", action="store_true")
    i.set_defaults(func=cmd_inspect)

    c = sub.add_parser("channels", help="show/export the channel mapping")
    c.add_argument("--data", required=True)
    c.add_argument("--codes")
    c.add_argument("--codes-file")
    c.add_argument("--limit", type=int)
    c.add_argument("--out")
    _add_common(c)
    c.set_defaults(func=cmd_channels)
    return p


def main(argv=None) -> int:
    _reconfigure_stdout()
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
