"""
MayaCardio Mode FSM and Network Probe.

CRITICAL INVARIANTS:
- Lightweight heartbeat to the cloud endpoint every <= 3s.
- Two consecutive failures => OFFLINE.
- One success => ONLINE.
- Each transition must complete within 10 seconds.
- Thread-safe state tracking.
"""

from __future__ import annotations
import logging
import threading
import time
import urllib.request
from typing import Callable, List, Optional

logger = logging.getLogger("ModeFSM")


class NetworkProbeFSM:
    def __init__(
        self,
        cloud_url: str,
        probe_interval: float = 2.0,
        probe_timeout: float = 2.0,
        initial_mode: str = "offline",
        on_mode_change: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.cloud_url = cloud_url.rstrip("/")
        self.probe_interval = probe_interval
        self.probe_timeout = probe_timeout
        self.mode = initial_mode.lower()
        self.on_mode_change = on_mode_change

        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._probe_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)

    def check_connection(self) -> bool:
        """Executes a single lightweight probe to GET /probe or GET /health."""
        try:
            url = f"{self.cloud_url}/probe"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=self.probe_timeout) as resp:
                return resp.status == 200
        except Exception:
            # Fallback to /health
            try:
                url_health = f"{self.cloud_url}/health"
                req_h = urllib.request.Request(url_health, method="GET")
                with urllib.request.urlopen(req_h, timeout=self.probe_timeout) as resp_h:
                    return resp_h.status == 200
            except Exception:
                return False

    def update_with_probe_result(self, success: bool) -> Optional[str]:
        """
        Updates consecutive success/failure counters and triggers transitions.
        Rule:
          Two consecutive failures => OFFLINE.
          One success => ONLINE.
        Returns the new mode if changed, or None.
        """
        old_mode = self.mode
        new_mode: Optional[str] = None

        with self._lock:
            if success:
                self._consecutive_failures = 0
                self._consecutive_successes += 1
                if self.mode == "offline":
                    # One success => ONLINE
                    self.mode = "online"
                    new_mode = "online"
            else:
                self._consecutive_successes = 0
                self._consecutive_failures += 1
                if self.mode == "online" and self._consecutive_failures >= 2:
                    # Two consecutive failures => OFFLINE
                    self.mode = "offline"
                    new_mode = "offline"

        if new_mode is not None:
            logger.info(f"Mode transition: {old_mode.upper()} -> {new_mode.upper()}")
            if self.on_mode_change:
                try:
                    self.on_mode_change(old_mode, new_mode)
                except Exception as e:
                    logger.error(f"Error in on_mode_change callback: {e}")

        return new_mode

    def _probe_loop(self) -> None:
        while self._running:
            success = self.check_connection()
            self.update_with_probe_result(success)
            time.sleep(self.probe_interval)

    def get_mode(self) -> str:
        with self._lock:
            return self.mode
