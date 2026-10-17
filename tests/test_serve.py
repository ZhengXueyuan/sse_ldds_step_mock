# -*- coding: utf-8 -*-
import os
import shutil
import socket
import tempfile
import time
import unittest

from sse_step import serve, step


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestGatewayServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        serve.set_log_level("error")
        cls.dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(cls.dir, "tick"))
        cls.tick_path = os.path.join(cls.dir, "tick", "step_tick_1001.bin")
        messages = []
        for i in range(1, 6):
            messages.append(step.encode_ub001(
                seq=i,
                send_time="20261015-09:30:00.%03d" % i,
                security_type="01",
                channel=1001,
                appl_seq=i,
                md_stream_id="MD002",
                security_id="600000",
                transact_time=93000000 + i,
                exec_type="0",
                price_int=96500,
                qty=100 * i,
                ext_fields=[(54, 1), (117, str(1000 + i))],
            ))
        with open(cls.tick_path, "wb") as f:
            f.write(b"".join(messages))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def _start(self, mark_possdup=False):
        port = _free_port()
        cfg = {
            "data_dir": self.dir,
            "host": "127.0.0.1",
            "base_port": port,
            "channels": [1001],
            "speed": 1.0,
            "topspeed": True,
            "pps": None,
            "mbps": None,
            "max_gap_ms": 0,
            "start_ms": None,
            "end_ms": None,
            "with_snapshots": False,
            "snapshot_all_port": 0,
            "heartbeat": 30,
            "channel_heartbeat": 3.0,
            "require_logon": True,
            "start_on_connect": False,
            "login_timeout": 5.0,
            "max_clients": 4,
            "max_resend": 200,
            "close_at_end": False,
            "rebuild_index": False,
            "mark_possdup": mark_possdup,
            "sender": "MDGW",
            "target": "VSS",
        }
        srv = serve.GatewayServer(cfg)
        srv.start()
        return srv, port

    @staticmethod
    def _read_msgs(sock, reader, timeout=2.0):
        end = time.time() + timeout
        out = []
        while time.time() < end:
            try:
                data = sock.recv(65536)
            except socket.timeout:
                break
            if not data:
                break
            reader.feed(data)
            while True:
                raw = reader.pop()
                if raw is None:
                    break
                out.append(list(step.iter_messages(raw))[0])
        return out

    def _client(self, port):
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        s.settimeout(0.5)
        reader = serve.FixSocketReader()
        s.sendall(step.encode_message(
            "A", 1, "20261017-15:00:00.000",
            [(98, 0), (108, 30), (1137, 9), (1407, 124),
             (1408, "STEP1.20_SH_0.62")],
            sender="VSS", target="MDGW"))
        return s, reader

    def _play_to_end(self, s, reader):
        msgs = []
        deadline = time.time() + 5
        end_msg = None
        while time.time() < deadline and end_msg is None:
            for m in self._read_msgs(s, reader, 1.0):
                msgs.append(m)
                if m["msg_type"] == "UA001" and step.get_field(m, 10205) == "Y":
                    end_msg = m
        return msgs, end_msg

    def test_logon_playback_and_resend(self):
        srv, port = self._start()
        try:
            s, reader = self._client(port)
            msgs, end_msg = self._play_to_end(s, reader)
            self.assertIsNotNone(end_msg)
            self.assertEqual(msgs[0]["msg_type"], "A")
            self.assertEqual(msgs[0]["seq"], 1)
            seqs = [m["seq"] for m in msgs]
            self.assertEqual(seqs, list(range(1, len(seqs) + 1)))
            self.assertEqual(sum(1 for m in msgs if m["msg_type"] == "UB001"), 5)
            self.assertEqual(step.get_field(end_msg, 1350), "5")
            self.assertEqual(step.get_field(end_msg, 10201), "1001")

            s.sendall(step.encode_message(
                "UA002", 2, "20261017-15:00:00.000",
                [(10201, 1001), (10077, 1), (1182, 2), (1183, 4)],
                sender="VSS", target="MDGW"))
            resent = []
            resp = None
            deadline = time.time() + 5
            while time.time() < deadline and resp is None:
                for m in self._read_msgs(s, reader, 1.0):
                    if m["msg_type"] == "UB001":
                        resent.append(m)
                    elif m["msg_type"] == "UA002":
                        resp = m
            self.assertEqual([step.get_field(m, 1181) for m in resent],
                             ["2", "3", "4"])
            for m in resent:
                self.assertIsNone(dict(m["header"]).get(43))
            self.assertEqual(step.get_field(resp, 10076), "1")
            self.assertEqual(step.get_field(resp, 1182), "2")
            self.assertEqual(step.get_field(resp, 1183), "4")

            # End=0 means "to the last record in the file".
            s.sendall(step.encode_message(
                "UA002", 3, "20261017-15:00:00.000",
                [(10201, 1001), (10077, 1), (1182, 1), (1183, 0)],
                sender="VSS", target="MDGW"))
            resent2 = []
            resp2 = None
            deadline = time.time() + 5
            while time.time() < deadline and resp2 is None:
                for m in self._read_msgs(s, reader, 1.0):
                    if m["msg_type"] == "UB001":
                        resent2.append(m)
                    elif m["msg_type"] == "UA002":
                        resp2 = m
            self.assertEqual(len(resent2), 5)
            self.assertEqual(step.get_field(resp2, 10076), "1")
            self.assertEqual(step.get_field(resp2, 1183), "5")

            s.sendall(step.encode_message(
                "5", 4, "20261017-15:00:00.000",
                [(1409, 0), (58, "bye")], sender="VSS", target="MDGW"))
            logout = [m for m in self._read_msgs(s, reader, 2.0)
                      if m["msg_type"] == "5"]
            self.assertTrue(logout)
            s.close()
        finally:
            srv.stop()

    def test_mark_possdup(self):
        srv, port = self._start(mark_possdup=True)
        try:
            s, reader = self._client(port)
            self._play_to_end(s, reader)
            s.sendall(step.encode_message(
                "UA002", 2, "20261017-15:00:00.000",
                [(10201, 1001), (10077, 1), (1182, 1), (1183, 2)],
                sender="VSS", target="MDGW"))
            resent = []
            resp = None
            deadline = time.time() + 5
            while time.time() < deadline and resp is None:
                for m in self._read_msgs(s, reader, 1.0):
                    if m["msg_type"] == "UB001":
                        resent.append(m)
                    elif m["msg_type"] == "UA002":
                        resp = m
            self.assertEqual(len(resent), 2)
            for m in resent:
                hdr = dict(m["header"])
                self.assertEqual(hdr.get(43), "Y")
                self.assertEqual(hdr.get(97), "Y")
            self.assertEqual(step.get_field(resp, 10076), "1")
            s.close()
        finally:
            srv.stop()


if __name__ == "__main__":
    unittest.main()
