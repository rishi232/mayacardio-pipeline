"""
MayaCardio Biosignal Mock TCP Client.

Reads Sensor 1 and Sensor 2 continuously over TCP NDJSON from biosignal-mock.
CRITICAL INVARIANTS:
- Protocol: newline-delimited JSON over TCP, ONE client at a time.
- Request: {"cmd": "read", "sensor": 1}
- Re-read: {"cmd": "read", "sensor": 1, "from_index": N}
- Verifies CRC-32 (zlib) of raw data text from '[' to matching ']'.
- On CRC mismatch: rejects corrupt payload, re-reads with from_index.
- Records all reported losses to LossLedger BEFORE data processing.
- Detects mock restarts (new session_id); records unread samples of old session
  as lost with to_index=None.
- Handles mock disconnects/reconnects gracefully.
- Disk full: stops WAL writes, records loss with reason 'disk_full', keeps reading.
"""

from __future__ import annotations
import json
import logging
import os
import shutil
import socket
import time
import zlib
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.dedupe import DedupeManager
from src.ledger import LossLedger
from src.wal import SegmentedWAL

logger = logging.getLogger("MockClient")
MIN_FREE_DISK_BYTES = 5 * 1024 * 1024  # 5 MB safety margin


def extract_data_and_verify_crc(raw_line: bytes) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Extracts the JSON payload and validates the CRC-32 of the data array.
    Spec: CRC-32 (zlib) of the data text EXACTLY as sent, from '[' to the matching ']'.
    Wrong crc32 => reply is corrupt, MUST NOT be used.
    """
    try:
        reply = json.loads(raw_line.decode("utf-8"))
    except Exception:
        return False, None

    expected_crc_str = reply.get("crc32")
    if expected_crc_str is None:
        return False, None

    # Find the data text in raw_line from '[' to matching ']'
    data_marker = b'"data"'
    data_pos = raw_line.find(data_marker)
    if data_pos == -1:
        return False, None

    # Find first '[' after "data"
    first_bracket = raw_line.find(b"[", data_pos)
    if first_bracket == -1:
        return False, None

    # Count brackets to find matching closing ']'
    depth = 0
    match_end = -1
    in_string = False
    escape = False
    for i in range(first_bracket, len(raw_line)):
        b = raw_line[i : i + 1]
        if escape:
            escape = False
            continue
        if b == b"\\":
            escape = True
            continue
        if b == b'"':
            in_string = not in_string
            continue
        if not in_string:
            if b == b"[":
                depth += 1
            elif b == b"]":
                depth -= 1
                if depth == 0:
                    match_end = i + 1
                    break

    if match_end == -1:
        return False, None

    data_bytes = raw_line[first_bracket:match_end]
    calc_crc = zlib.crc32(data_bytes) & 0xFFFFFFFF
    try:
        if isinstance(expected_crc_str, int):
            expected_crc = expected_crc_str & 0xFFFFFFFF
        else:
            expected_crc = int(expected_crc_str, 16) & 0xFFFFFFFF
    except Exception:
        return False, None

    if calc_crc != expected_crc:
        return False, None

    return True, reply


class MockClient:
    """
    Client for biosignal-mock service.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7100,
        wal: Optional[SegmentedWAL] = None,
        loss_ledger: Optional[LossLedger] = None,
        on_sample_callback: Optional[Callable[[str, int, int, List[int]], None]] = None,
        connect_timeout: float = 3.0,
        read_timeout: float = 3.0,
    ) -> None:
        self.host = host
        self.port = port
        self.wal = wal
        self.loss_ledger = loss_ledger
        self.on_sample_callback = on_sample_callback
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        self._sock: Optional[socket.socket] = None
        self._reader = None
        self._running = False

        # Session tracking per sensor: sensor -> (session_id, last_accepted_index)
        self.last_seen: Dict[int, Tuple[str, int]] = {}
        self.current_session_id: Optional[str] = None

    def connect(self) -> bool:
        """Establish TCP connection to the mock."""
        self.close()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.connect_timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(self.read_timeout)
            self._sock = sock
            self._reader = sock.makefile("rb")
            return True
        except Exception as e:
            self.close()
            return False

    def close(self) -> None:
        """Close TCP connection."""
        if self._reader:
            try:
                self._reader.close()
            except Exception:
                pass
            self._reader = None
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def is_connected(self) -> bool:
        return self._sock is not None

    def read_sensor_batch(
        self, sensor: int, from_index: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Sends read request for sensor (optionally with from_index) and reads the reply.
        Validates CRC32. If CRC32 is wrong, retries with from_index up to 3 times.
        Returns parsed reply dict or None on connection error.
        """
        req_from = from_index
        if req_from is None and sensor in self.last_seen:
            # Request next expected index
            _, last_idx = self.last_seen[sensor]
            req_from = last_idx + 1

        attempts = 0
        max_crc_retries = 3

        while attempts <= max_crc_retries:
            attempts += 1
            if not self._sock or not self._reader:
                if not self.connect():
                    return None

            req: Dict[str, Any] = {"cmd": "read", "sensor": sensor}
            if req_from is not None:
                req["from_index"] = req_from

            req_bytes = json.dumps(req).encode("utf-8") + b"\n"
            try:
                self._sock.sendall(req_bytes)
                raw_line = self._reader.readline()
                if not raw_line:
                    self.close()
                    return None
            except (socket.timeout, socket.error, OSError):
                self.close()
                return None

            # Verify CRC32
            is_valid, reply = extract_data_and_verify_crc(raw_line)
            if not is_valid:
                logger.warning(
                    f"Corrupt CRC received for sensor {sensor} (attempt {attempts}), requesting re-read"
                )
                # Re-read with from_index; do NOT persist corrupt payload
                if req_from is None:
                    req_from = 0
                continue

            # Valid reply received!
            self._handle_valid_reply(sensor, reply)
            return reply

        return None

    def _handle_valid_reply(self, sensor: int, reply: Dict[str, Any]) -> None:
        """Process a valid reply: record losses, check session restart, persist data."""
        sess_id = reply["session_id"]
        first_idx = reply.get("first_index", 0)
        data = reply.get("data", [])
        lost_ranges = reply.get("lost", [])
        session_end = reply.get("session_end", False)

        # 1. Detect mock restart (new session_id)
        if self.current_session_id is not None and self.current_session_id != sess_id:
            # Mock restarted! Unread data of old session is lost and must be listed with to_index=None
            for prev_sensor, (prev_sess, prev_idx) in list(self.last_seen.items()):
                if prev_sess == self.current_session_id and self.loss_ledger:
                    self.loss_ledger.record_loss(
                        session_id=prev_sess,
                        sensor=prev_sensor,
                        from_index=prev_idx + 1,
                        to_index=None,
                        reason="mock_restart",
                    )
            # Reset session tracking
            self.last_seen.clear()

        self.current_session_id = sess_id

        # 2. Record any losses reported in the mock reply BEFORE processing data
        if self.loss_ledger and lost_ranges:
            for l in lost_ranges:
                self.loss_ledger.record_loss(
                    session_id=sess_id,
                    sensor=sensor,
                    from_index=l["from_index"],
                    to_index=l["to_index"],
                    reason=l.get("reason", "mock_reported_loss"),
                )

        # 3. Check disk space before saving to WAL
        disk_full = False
        if self.wal:
            try:
                stat = shutil.disk_usage(self.wal.data_dir)
                if stat.free < MIN_FREE_DISK_BYTES:
                    disk_full = True
            except Exception:
                pass

        if disk_full:
            # Stop accepting new samples into WAL, record loss entry with reason 'disk_full'
            if self.loss_ledger and data:
                last_idx = first_idx + len(data) - 1
                self.loss_ledger.record_loss(
                    session_id=sess_id,
                    sensor=sensor,
                    from_index=first_idx,
                    to_index=last_idx,
                    reason="disk_full",
                )
            return

        # 4. Save accepted samples to WAL
        if self.wal and data:
            samples_to_write = [
                (first_idx + i, val) for i, val in enumerate(data)
            ]
            self.wal.write_batch(sess_id, sensor, samples_to_write)

        # 5. Notify callback and update last_seen
        if data:
            last_idx = first_idx + len(data) - 1
            self.last_seen[sensor] = (sess_id, last_idx)
            if self.on_sample_callback:
                for i, val in enumerate(data):
                    self.on_sample_callback(sess_id, sensor, first_idx + i, val)
