"""
Test Biosignal Mock Server.

Implements the biosignal-mock TCP NDJSON protocol for testing fault handling:
- Mode signal / ramp
- CRC-32 (zlib) calculation on data text
- Injected faults: corrupt CRC, duplicate replies, simulated losses, mock restarts
"""

from __future__ import annotations
import json
import socket
import threading
import time
import zlib
from typing import Any, Dict, List, Optional


class MockServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        session_id: str = "test-session-1",
        batch_count: int = 10,
    ) -> None:
        self.host = host
        self.port = port
        self.session_id = session_id
        self.batch_count = batch_count
        self._server_sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self.running = False

        # Fault injection flags
        self.inject_corrupt_crc: bool = False
        self.corrupt_crc_counter: int = 0
        self.inject_duplicate_replies: bool = False
        self.inject_loss_event: Optional[Dict[str, Any]] = None
        self.hang_server: bool = False

        # Sensor positions
        self.sensor_index = {1: 0, 2: 0}

    def start(self) -> int:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self.port = self._server_sock.getsockname()[1]
        self._server_sock.listen(1)
        self.running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self.port

    def stop(self) -> None:
        self.running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=1.0)

    def _generate_samples(self, sensor: int, start_idx: int, count: int) -> List[List[int]]:
        """Generate ramp samples as per Section 6.1."""
        samples = []
        for i in range(start_idx, start_idx + count):
            if sensor == 1:
                # 3 channels: (3i + c) mod 8388608
                samples.append([(3 * i + c) % 8388608 for c in range(3)])
            else:
                # 2 channels: (2i + c) mod 8388608
                samples.append([(2 * i + c) % 8388608 for c in range(2)])
        return samples

    def _build_reply(self, sensor: int, first_idx: int, count: int, lost: List[Dict[str, Any]], corrupt_crc: bool = False) -> bytes:
        data = self._generate_samples(sensor, first_idx, count)
        data_json = json.dumps(data)
        data_bytes = data_json.encode("utf-8")
        calc_crc = zlib.crc32(data_bytes) & 0xFFFFFFFF
        crc_str = f"{calc_crc:08x}"
        if corrupt_crc:
            crc_str = "deadbeef"

        reply_dict = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": first_idx,
            "count": count,
            "lost": lost,
            "session_end": False,
            "crc32": crc_str,
            "data": data,
        }
        return json.dumps(reply_dict).encode("utf-8") + b"\n"

    def _run(self) -> None:
        while self.running:
            try:
                conn, _ = self._server_sock.accept()
            except Exception:
                break

            with conn:
                reader = conn.makefile("rb")
                while self.running:
                    try:
                        line = reader.readline()
                        if not line:
                            break
                    except (ConnectionResetError, BrokenPipeError, OSError):
                        break

                    if self.hang_server:
                        time.sleep(10.0)
                        continue

                    try:
                        req = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue

                    cmd = req.get("cmd")
                    if cmd == "read":
                        sensor = req.get("sensor", 1)
                        req_from = req.get("from_index")
                        if req_from is not None:
                            first_idx = req_from
                        else:
                            first_idx = self.sensor_index[sensor]

                        count = self.batch_count
                        self.sensor_index[sensor] = first_idx + count

                        lost = []
                        if self.inject_loss_event:
                            lost.append(self.inject_loss_event)
                            self.inject_loss_event = None

                        corrupt = False
                        if self.inject_corrupt_crc and self.corrupt_crc_counter > 0:
                            corrupt = True
                            self.corrupt_crc_counter -= 1

                        reply_bytes = self._build_reply(sensor, first_idx, count, lost, corrupt_crc=corrupt)
                        conn.sendall(reply_bytes)

                        if self.inject_duplicate_replies:
                            # Send the exact same reply again
                            conn.sendall(reply_bytes)
