# -*- coding: utf-8 -*-
import io
import os
import tempfile
import unittest

from sse_step import build, source, step


class TestFormatting(unittest.TestCase):
    def test_price(self):
        self.assertEqual(step.format_price_5(96500), "9.65000")
        self.assertEqual(step.format_price_5(12860000), "1286.00000")
        self.assertEqual(step.format_price_5(0), "0.00000")
        self.assertEqual(step.format_price_5(37450), "3.74500")

    def test_time(self):
        self.assertEqual(step.format_time9(93000000), "093000000")
        self.assertEqual(step.format_time9(2316960), "002316960")
        self.assertEqual(step.time_hhmmss(93000020), "09:30:00.020")
        self.assertEqual(step.sending_time("20261015", 93000020),
                         "20261015-09:30:00.020")

    def test_time_to_ms_boundaries(self):
        self.assertEqual(step.time_to_ms(93100000) - step.time_to_ms(93059999), 1)
        self.assertEqual(step.time_to_ms(100000000) - step.time_to_ms(95959999), 1)

    def test_trade_amount(self):
        self.assertEqual(step.format_trade_amount_2(96500, 100), "965.00")
        self.assertEqual(step.format_trade_amount_2(96100, 5500), "52855.00")


class TestRoundTrip(unittest.TestCase):
    def test_ub001_order(self):
        raw = step.encode_ub001(
            seq=7, send_time="20261015-09:30:00.020",
            security_type="01", channel=1003, appl_seq=42,
            md_stream_id="MD002", security_id="600010",
            transact_time=93000020, exec_type="0",
            price_int=96500, qty=1000,
            ext_fields=[(54, 2), (117, "275966")],
        )
        msg = list(step.iter_messages(raw))[0]
        self.assertEqual(msg["msg_type"], "UB001")
        self.assertEqual(msg["seq"], 7)
        self.assertEqual(step.get_field(msg, 10201), "1003")
        self.assertEqual(step.get_field(msg, 1181), "42")
        self.assertEqual(step.get_field(msg, 150), "0")
        self.assertEqual(step.get_field(msg, 44), "9.65000")
        self.assertEqual(step.get_field(msg, 38), "1000")
        self.assertEqual(step.get_field(msg, 54), "2")
        self.assertEqual(step.get_field(msg, 117), "275966")

    def test_ub001_trade_and_status(self):
        trade = step.encode_ub001(
            seq=1, send_time="20261015-09:25:00.000",
            security_type="01", channel=1001, appl_seq=3,
            md_stream_id="MD002", security_id="600000",
            transact_time=92500000, exec_type="F",
            price_int=96500, qty=100,
            ext_fields=[(10116, "188379"), (10117, "86278"),
                        (8504, step.format_trade_amount_2(96500, 100)), (10333, 0)],
        )
        status = step.encode_ub001(
            seq=2, send_time="20261015-09:15:00.000",
            security_type="01", channel=1001, appl_seq=4,
            md_stream_id="MD002", security_id="600000",
            transact_time=91500000, exec_type="S",
            price_int=0, qty=0, ext_fields=[(58, "I")],
        )
        msgs = list(step.iter_messages(trade + status))
        self.assertEqual(step.get_field(msgs[0], 150), "F")
        self.assertEqual(step.get_field(msgs[0], 8504), "965.00")
        self.assertEqual(step.get_field(msgs[1], 150), "S")
        self.assertEqual(step.get_field(msgs[1], 58), "I")

    def test_snapshot(self):
        raw = step.encode_snapshot(
            seq=1, send_time="20261015-10:00:00.000",
            security_type="01", trade_date="20261015",
            last_update_time=100000000, md_stream_id="MD002",
            security_id="600000", prev_close_px=96500,
            total_volume=13882240, num_trades=12166,
            total_value_yuan=133467487,
            entries=[("2", 96100, None, None),
                     ("9", 96143, None, None),
                     ("0", 96100, 13800, 0),
                     ("0", 0, 6600, 1),
                     ("1", 96200, 65972, 0)],
            trading_phase_code="T111    ",
        )
        msg = list(step.iter_messages(raw))[0]
        self.assertEqual(msg["msg_type"], "W")
        self.assertEqual(step.get_field(msg, 268), "5")
        entries = step.parse_md_entries(msg)
        self.assertEqual(len(entries), 5)
        self.assertEqual(entries[3][270], "0.00000")
        self.assertEqual(entries[3][271], "6600")
        self.assertEqual(entries[3][290], "1")

    def test_bad_checksum_detected(self):
        raw = bytearray(step.encode_ub001(
            seq=1, send_time="20261015-09:30:00.000",
            security_type="01", channel=1001, appl_seq=1,
            md_stream_id="MD002", security_id="600000",
            transact_time=93000000, exec_type="0",
            price_int=96500, qty=100, ext_fields=[(54, 1), (117, "1")],
        ))
        # Flip a body byte (not the checksum field).
        raw[60] ^= 0x01
        with self.assertRaises(step.StepError):
            list(step.iter_messages(bytes(raw)))


class TestSource(unittest.TestCase):
    def test_channel_mapping(self):
        self.assertEqual(source.channel_of("600000.SH"), 1001)
        self.assertEqual(source.channel_of("600004.SH"), 1005)
        self.assertEqual(source.channel_of("900948.SH"), 1005)
        self.assertEqual(source.channel_of("688001.SH"), 1002)

    def test_tick_merge_order(self):
        d = tempfile.mkdtemp()
        try:
            os.makedirs(os.path.join(d, "600000.SH"))
            p = os.path.join(d, "600000.SH")
            with io.open(os.path.join(p, "逐笔委托.csv"), "w", encoding="gbk", newline="") as f:
                f.write(",".join(source.ORDER_HEADER) + ",\n")
                f.write("600000.SH,600000,20261015,93000000,2,100,A,B,96500,100,\n")
                f.write("600000.SH,600000,20261015,93000001,4,0,S,I,0,0,\n")
            with io.open(os.path.join(p, "逐笔成交.csv"), "w", encoding="gbk", newline="") as f:
                f.write(",".join(source.TRADE_HEADER) + ",\n")
                f.write("600000.SH,600000,20261015,93000000,1, , ,S,96500,100,100,200,\n")
            st = source.TickEventStream(p, "600000.SH")
            events = list(st)
            self.assertEqual([e[1] for e in events], [1, 2, 4])
            self.assertEqual([e[2] for e in events], ["F", "A", "S"])
            self.assertEqual(events[0][8], "200")  # bid ref
            self.assertEqual(events[0][9], "100")  # offer ref
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_snapshot_entries(self):
        row = [""] * 66
        row[1] = "600000"
        row[3] = "100000000"
        row[4] = "96100"
        row[7] = "12166"
        row[11] = "13882240"
        row[12] = "133467487"
        row[13] = "96900"
        row[14] = "95600"
        row[15] = "96500"
        row[16] = "96500"
        row[17] = "96200"   # ask1 price
        row[27] = "65972"   # ask1 size
        row[37] = "96100"   # bid1 price
        row[47] = "13800"   # bid1 size
        entries, prev, vol, ntr, amt = build.build_snapshot_entries(row, "600000", 2, 100000000)
        types = [e[0] for e in entries]
        self.assertIn("2", types)
        self.assertIn("9", types)
        self.assertIn("0", types)
        self.assertIn("1", types)
        self.assertEqual(prev, 96500)
        self.assertEqual(vol, 13882240)
        self.assertEqual(ntr, 12166)
        self.assertEqual(amt, 133467487)


class TestVerify(unittest.TestCase):
    def test_verify_ub001(self):
        from sse_step.cli import _verify_file
        m1 = step.encode_ub001(
            seq=1, send_time="20261015-09:30:00.000", security_type="01",
            channel=1001, appl_seq=1, md_stream_id="MD002", security_id="600000",
            transact_time=93000000, exec_type="0", price_int=96500, qty=100,
            ext_fields=[(54, 1), (117, "1")])
        m2 = step.encode_ub001(
            seq=2, send_time="20261015-09:30:00.001", security_type="01",
            channel=1001, appl_seq=2, md_stream_id="MD002", security_id="600000",
            transact_time=93000001, exec_type="F", price_int=96500, qty=100,
            ext_fields=[(10116, "1"), (10117, "2"), (8504, "965.00"), (10333, 0)])
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            path = f.name
            f.write(m1 + m2)
        try:
            c = _verify_file(path, "UB001", 1001)
            self.assertEqual(c["messages"], 2)
            self.assertEqual(c["orders_add"], 1)
            self.assertEqual(c["trades"], 1)
            self.assertEqual(c["first_appl"], 1)
            self.assertEqual(c["last_appl"], 2)
            with open(path, "r+b") as f:
                f.seek(60)
                b = f.read(1)
                f.seek(60)
                f.write(bytes([b[0] ^ 0x01]))
            with self.assertRaises(step.StepError):
                _verify_file(path, "UB001", 1001)
        finally:
            os.unlink(path)

    def test_verify_snapshot(self):
        from sse_step.cli import _verify_file
        raw = step.encode_snapshot(
            seq=1, send_time="20261015-10:00:00.000", security_type="01",
            trade_date="20261015", last_update_time=100000000,
            md_stream_id="MD002", security_id="600000", prev_close_px=96500,
            total_volume=100, num_trades=1, total_value_yuan=965,
            entries=[("2", 96100, None, None), ("0", 96100, 100, 0)],
            trading_phase_code="T111    ")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            path = f.name
            f.write(raw)
        try:
            c = _verify_file(path, "W", None)
            self.assertEqual(c["snapshots"], 1)
            self.assertEqual(c["entries"], 2)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
