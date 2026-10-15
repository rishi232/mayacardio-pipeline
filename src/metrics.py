"""
MayaCardio Metrics and Resource Monitoring.

Exposes /metrics on localhost with CPU %, RSS memory MB, disk bytes written,
and network bytes sent counters.
Also logs summaries for resource reporting.
"""

from __future__ import annotations
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional

# Try psutil if available, otherwise fallback to standard OS counters
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


class MetricsCollector:
    def __init__(self, data_dir: str = "./data") -> None:
        self.data_dir = data_dir
        self.start_time = time.time()
        self.disk_bytes_written = 0
        self.net_bytes_sent = 0
        self._lock = threading.Lock()
        self.pid = os.getpid()
        self._process = psutil.Process(self.pid) if HAS_PSUTIL else None

    def record_disk_write(self, byte_count: int) -> None:
        with self._lock:
            self.disk_bytes_written += byte_count

    def record_net_sent(self, byte_count: int) -> None:
        with self._lock:
            self.net_bytes_sent += byte_count

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            disk_mb = round(self.disk_bytes_written / (1024 * 1024), 2)
            net_mb = round(self.net_bytes_sent / (1024 * 1024), 2)
            uptime_sec = round(time.time() - self.start_time, 1)

        cpu_percent = 0.0
        rss_mb = 0.0
        if self._process:
            try:
                cpu_percent = self._process.cpu_percent(interval=None)
                rss_mb = round(self._process.memory_info().rss / (1024 * 1024), 2)
            except Exception:
                pass

        return {
            "uptime_seconds": uptime_sec,
            "cpu_percent": cpu_percent,
            "rss_mb": rss_mb,
            "disk_bytes_written": self.disk_bytes_written,
            "disk_mb_written": disk_mb,
            "net_bytes_sent": self.net_bytes_sent,
            "net_mb_sent": net_mb,
        }


def start_metrics_server(collector: MetricsCollector, port: int = 9100) -> Optional[HTTPServer]:
    class MetricsHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            if self.path in ("/metrics", "/metrics/"):
                m = collector.get_metrics()
                # Expose both Prometheus style text and JSON
                output = (
                    f"# HELP process_cpu_percent Current CPU usage percent\n"
                    f"# TYPE process_cpu_percent gauge\n"
                    f"process_cpu_percent {m['cpu_percent']}\n"
                    f"# HELP process_rss_mb Resident memory in MB\n"
                    f"# TYPE process_rss_mb gauge\n"
                    f"process_rss_mb {m['rss_mb']}\n"
                    f"# HELP disk_bytes_written Total disk bytes written\n"
                    f"# TYPE disk_bytes_written counter\n"
                    f"disk_bytes_written {m['disk_bytes_written']}\n"
                    f"# HELP net_bytes_sent Total network bytes sent\n"
                    f"# TYPE net_bytes_sent counter\n"
                    f"net_bytes_sent {m['net_bytes_sent']}\n"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(output)))
                self.end_headers()
                self.wfile.write(output)
            elif self.path in ("/metrics.json", "/json"):
                data = json.dumps(collector.get_metrics(), indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_response(404)
                self.end_headers()

    try:
        server = HTTPServer(("127.0.0.1", port), MetricsHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        return server
    except Exception:
        return None
