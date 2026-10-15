"""
MayaCardio Cloud Service.

Implements the cloud ingestion and processing service (Section 5).
Can be deployed to any free cloud provider (Fly.io free allowance, Render free, etc.).
Uses SQLite for idempotent upserts and durable storage.
CRITICAL INVARIANTS:
- POST /ingest: Idempotent upsert of samples keyed by (session_id, sensor, index). Returns acked_up_to.
- POST /result: Idempotent upsert of window results keyed by (session_id, window, mode).
- GET  /window: Cloud-side window computation using the EXACT SAME pure function as device (src.window).
- GET  /export: JSONL export for judges: one line per sample, followed by one line per loss.
- GET  /probe or /health: Lightweight heartbeat for device network probe.
"""

from __future__ import annotations
import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from src.window import compute_window


class CloudStorage:
    def __init__(self, db_path: str = "cloud_store.db") -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._persistent_conn: Optional[sqlite3.Connection] = None
        if self.db_path == ":memory:":
            self._persistent_conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self._persistent_conn is not None:
            return self._persistent_conn
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _close_connection(self, conn: sqlite3.Connection) -> None:
        if conn is not self._persistent_conn:
            conn.close()

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_connection()
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS samples (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL,
                        sample_index INTEGER NOT NULL,
                        values_json TEXT NOT NULL,
                        PRIMARY KEY (session_id, sensor, sample_index)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS results (
                        session_id TEXT NOT NULL,
                        window INTEGER NOT NULL,
                        mode TEXT NOT NULL,
                        result_json TEXT NOT NULL,
                        created_at REAL DEFAULT (strftime('%s', 'now')),
                        PRIMARY KEY (session_id, window, mode)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS losses (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL,
                        from_index INTEGER NOT NULL,
                        to_index INTEGER,
                        reason TEXT NOT NULL
                    )
                """)
            self._close_connection(conn)

    def upsert_samples(
        self, session_id: str, sensor: int, samples: List[Dict[str, Any]]
    ) -> int:
        """
        Idempotent upsert of samples keyed by (session_id, sensor, index).
        samples: [{"index": int, "values": List[int]}, ...]
        Returns the contiguous acked_up_to index.
        """
        rows = [
            (session_id, sensor, s["index"], json.dumps(s["values"]))
            for s in samples
        ]
        with self._lock:
            conn = self._get_connection()
            with conn:
                conn.executemany("""
                    INSERT OR REPLACE INTO samples (session_id, sensor, sample_index, values_json)
                    VALUES (?, ?, ?, ?)
                """, rows)

            # Determine contiguous acked_up_to for this (session_id, sensor)
            # Find all indexes for this session and sensor in ascending order
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sample_index FROM samples
                WHERE session_id = ? AND sensor = ?
                ORDER BY sample_index ASC
            """, (session_id, sensor))
            all_indices = [r[0] for r in cursor.fetchall()]
            self._close_connection(conn)

        acked_up_to = self._calculate_contiguous_ack(all_indices)
        return acked_up_to

    @staticmethod
    def _calculate_contiguous_ack(sorted_indices: List[int]) -> Optional[int]:
        if not sorted_indices:
            return None
        # If does not start with 0, highest contiguous from 0 is not yet 0
        if sorted_indices[0] != 0:
            return None
        acked = 0
        for i in range(1, len(sorted_indices)):
            if sorted_indices[i] == acked + 1:
                acked += 1
            elif sorted_indices[i] == acked:
                continue
            else:
                break
        return acked

    def upsert_result(self, result: Dict[str, Any]) -> None:
        """Idempotent upsert of window result keyed by (session_id, window, mode)."""
        session_id = result.get("session_id", "default")
        window = result["window"]
        mode = result["mode"]
        res_json = json.dumps(result)
        with self._lock:
            conn = self._get_connection()
            with conn:
                conn.execute("""
                    INSERT OR REPLACE INTO results (session_id, window, mode, result_json)
                    VALUES (?, ?, ?, ?)
                """, (session_id, window, mode, res_json))
            self._close_connection(conn)

    def record_loss(
        self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str
    ) -> None:
        with self._lock:
            conn = self._get_connection()
            with conn:
                conn.execute("""
                    INSERT INTO losses (session_id, sensor, from_index, to_index, reason)
                    VALUES (?, ?, ?, ?, ?)
                """, (session_id, sensor, from_index, to_index, reason))
            self._close_connection(conn)

    def get_samples_for_window(
        self, session_id: str, window: int
    ) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
        """
        Fetch samples needed for window w (samples 400w .. 400w+399) and prior 800 samples for sensor 1.
        """
        # For sensor 1, heart rate might need up to 400*(w-4) samples
        min_idx = max(0, 400 * (window - 4))
        max_idx = 400 * (window + 1)  # include buffer for i+1 peak check

        s1_data: Dict[int, List[int]] = {}
        s2_data: Dict[int, List[int]] = {}

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sensor, sample_index, values_json FROM samples
                WHERE session_id = ? AND sample_index >= ? AND sample_index <= ?
            """, (session_id, min_idx, max_idx))
            for sensor, s_idx, v_json in cursor.fetchall():
                vals = json.loads(v_json)
                if sensor == 1:
                    s1_data[s_idx] = vals
                elif sensor == 2:
                    s2_data[s_idx] = vals
            self._close_connection(conn)

        return s1_data, s2_data

    def get_results(self, session_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT result_json FROM results
                WHERE session_id = ?
                ORDER BY window ASC, mode ASC
            """, (session_id,))
            results = [json.loads(r[0]) for r in cursor.fetchall()]
            self._close_connection(conn)
        return results

    def get_export_lines(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Returns JSONL objects for export:
        one per sample, {"sensor":1,"index":120000,"values":[720,1200,1560]}
        followed by one per loss, {"sensor":2,"from_index":4000,"to_index":4399,"reason":"..."}
        """
        lines: List[Dict[str, Any]] = []
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sensor, sample_index, values_json FROM samples
                WHERE session_id = ?
                ORDER BY sensor ASC, sample_index ASC
            """, (session_id,))
            for sensor, s_idx, v_json in cursor.fetchall():
                lines.append({
                    "sensor": sensor,
                    "index": s_idx,
                    "values": json.loads(v_json),
                })

            cursor.execute("""
                SELECT sensor, from_index, to_index, reason FROM losses
                WHERE session_id = ?
                ORDER BY sensor ASC, from_index ASC
            """, (session_id,))
            for sensor, f_idx, t_idx, reason in cursor.fetchall():
                lines.append({
                    "sensor": sensor,
                    "from_index": f_idx,
                    "to_index": t_idx,
                    "reason": reason,
                })
            self._close_connection(conn)
        return lines


# Clean Python HTTP handler implementing the cloud spec
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse


def create_cloud_handler(storage: CloudStorage):
    class CloudRequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            # Suppress noisy standard HTTP logs in tests
            pass

        def _send_json(self, status_code: int, data: Any) -> None:
            body = json.dumps(data).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            params = urllib.parse.parse_qs(parsed.query)

            if path in ("/health", "/probe"):
                self._send_json(200, {"status": "healthy"})
                return

            if path == "/window":
                # GET /window?session_id=...&window=N
                session_id = params.get("session_id", ["default"])[0]
                window_str = params.get("window", ["0"])[0]
                try:
                    w = int(window_str)
                except ValueError:
                    self._send_json(400, {"error": "Invalid window parameter"})
                    return

                # Get samples and compute using exact same pure function
                s1_data, s2_data = storage.get_samples_for_window(session_id, w)
                result = compute_window(
                    window=w,
                    sensor_1_data=s1_data,
                    sensor_2_data=s2_data,
                    mode="online",
                    session_id=session_id,
                )
                # Store online result in cloud store
                storage.upsert_result(result)
                self._send_json(200, result)
                return

            if path == "/export":
                # GET /export?session_id=...
                session_id = params.get("session_id", [""])[0]
                lines = storage.get_export_lines(session_id)
                # Output as NDJSON / JSONL
                body_lines = [json.dumps(l) for l in lines]
                body = ("\n".join(body_lines) + ("\n" if body_lines else "")).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/results":
                session_id = params.get("session_id", [""])[0]
                results = storage.get_results(session_id)
                self._send_json(200, results)
                return

            self._send_json(404, {"error": "Not found"})

        def do_POST(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path

            content_len = int(self.headers.get("Content-Length", 0))
            body_bytes = self.rfile.read(content_len)
            try:
                payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
            except Exception:
                self._send_json(400, {"error": "Invalid JSON body"})
                return

            if path == "/ingest":
                # POST /ingest: idempotent upsert of samples keyed by (session_id, sensor, index)
                session_id = payload.get("session_id", "default")
                sensor = payload.get("sensor", 1)
                samples = payload.get("samples", [])
                acked_up_to = storage.upsert_samples(session_id, sensor, samples)
                self._send_json(200, {
                    "status": "ok",
                    "session_id": session_id,
                    "sensor": sensor,
                    "acked_up_to": acked_up_to,
                })
                return

            if path == "/result":
                # POST /result: idempotent upsert of window results keyed by (session_id, window, mode)
                storage.upsert_result(payload)
                self._send_json(200, {"status": "ok"})
                return

            if path == "/loss":
                session_id = payload.get("session_id", "default")
                sensor = payload.get("sensor", 1)
                f_idx = payload.get("from_index", 0)
                t_idx = payload.get("to_index")
                reason = payload.get("reason", "unknown")
                storage.record_loss(session_id, sensor, f_idx, t_idx, reason)
                self._send_json(200, {"status": "ok"})
                return

            self._send_json(404, {"error": "Not found"})

    return CloudRequestHandler


class CloudServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, storage: Optional[CloudStorage] = None) -> None:
        self.host = host
        self.port = port
        self.storage = storage or CloudStorage(":memory:")
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.running = False

    def start(self) -> int:
        handler_cls = create_cloud_handler(self.storage)
        self._server = HTTPServer((self.host, self.port), handler_cls)
        self.port = self._server.server_address[1]
        self.running = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self.port

    def stop(self) -> None:
        self.running = False
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if self._thread:
            self._thread.join(timeout=1.0)
