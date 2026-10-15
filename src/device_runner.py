"""
MayaCardio Device Runner.

Coordinates the device ingestion, window processing, WAL storage, and cloud upload.
Runs as a single process under `pipeline run`.
"""

from __future__ import annotations
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from src.dedupe import DedupeManager
from src.fsm import NetworkProbeFSM
from src.ledger import LossLedger
from src.metrics import MetricsCollector, start_metrics_server
from src.mock_client import MockClient
from src.uploader import CloudUploader
from src.wal import SegmentedWAL
from src.window import compute_window

logger = logging.getLogger("DeviceRunner")


class DevicePipeline:
    def __init__(
        self,
        data_dir: str = "./data",
        mock_host: str = "127.0.0.1",
        mock_port: int = 7100,
        cloud_url: str = "http://127.0.0.1:8000",
        metrics_port: int = 9100,
    ) -> None:
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.mock_host = mock_host
        self.mock_port = mock_port
        self.cloud_url = cloud_url
        self.metrics_port = metrics_port

        self.dedupe_mgr = DedupeManager(os.path.join(self.data_dir, "index"))
        self.wal = SegmentedWAL(self.data_dir, dedupe_manager=self.dedupe_mgr)
        self.loss_ledger = LossLedger(self.data_dir)
        self.metrics = MetricsCollector(self.data_dir)

        # FSM and Uploader
        self.fsm = NetworkProbeFSM(
            cloud_url=self.cloud_url,
            probe_interval=2.0,
            probe_timeout=2.0,
            initial_mode="offline",
            on_mode_change=self._on_mode_change,
        )
        self.uploader = CloudUploader(
            cloud_url=self.cloud_url,
            fsm=self.fsm,
            wal=self.wal,
            loss_ledger=self.loss_ledger,
        )

        # Mock client
        self.mock_client = MockClient(
            host=self.mock_host,
            port=self.mock_port,
            wal=self.wal,
            loss_ledger=self.loss_ledger,
            on_sample_callback=self._on_sample_received,
        )

        # In-memory buffer for window processing: session_id -> {1: {idx: vals}, 2: {idx: vals}}
        self._session_buffers: Dict[str, Dict[int, Dict[int, List[int]]]] = {}
        # Highest processed window per session: session_id -> window_index
        self._processed_windows: Dict[str, int] = {}
        self._results_path = os.path.join(self.data_dir, "results.jsonl")
        self._status_path = os.path.join(self.data_dir, "status.json")

        self.running = False
        self._results_lock = threading.Lock()

    def _on_mode_change(self, old_mode: str, new_mode: str) -> None:
        self._write_status()

    def _on_sample_received(self, session_id: str, sensor: int, index: int, values: List[int]) -> None:
        if session_id not in self._session_buffers:
            self._session_buffers[session_id] = {1: {}, 2: {}}
            self._processed_windows[session_id] = -1

        self._session_buffers[session_id][sensor][index] = values

    def _save_result(self, result: Dict[str, Any]) -> None:
        """Appends result to results.jsonl and syncs."""
        line = json.dumps(result) + "\n"
        with self._results_lock:
            with open(self._results_path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())

    def _evaluate_ready_windows(self, session_id: str) -> None:
        """
        Check if samples are ready for window w (samples 400w .. 400w+399).
        Section 3.2: Every second's result must appear in the cloud within 5 seconds.
        Section 3.3: In offline mode, run SAME window function on device, mode="offline".
        """
        buf = self._session_buffers.get(session_id)
        if not buf:
            return

        last_proc = self._processed_windows.get(session_id, -1)
        # Determine maximum index currently in buffer
        max_idx_1 = max(buf[1].keys()) if buf[1] else -1
        max_idx_2 = max(buf[2].keys()) if buf[2] else -1
        max_available = min(max_idx_1, max_idx_2)
        if max_available < 0:
            return

        target_window = (max_available + 1) // 400 - 1

        for w in range(last_proc + 1, target_window + 1):
            mode = self.fsm.get_mode()
            # Compute window result
            result = compute_window(
                window=w,
                sensor_1_data=buf[1],
                sensor_2_data=buf[2],
                mode=mode,
                session_id=session_id,
            )
            # Save locally
            self._save_result(result)

            # If online, also send to cloud /result or trigger cloud window
            if mode == "online":
                self.uploader.post_result(result)

            self._processed_windows[session_id] = w

            # Prune memory buffer: keep only last 5000 samples to keep memory <= 50 MB
            prune_cutoff = 400 * (w - 10)
            if prune_cutoff > 0:
                for s in (1, 2):
                    for k in list(buf[s].keys()):
                        if k < prune_cutoff:
                            del buf[s][k]

    def _write_status(self) -> None:
        """Atomic write of current status to status.json."""
        mode = self.fsm.get_mode()
        saved = self.wal.count_samples()
        in_cloud = self.uploader.get_cloud_sample_count()
        backlog = max(0, saved - in_cloud)

        status_dict = {
            "mode": mode,
            "samples_saved": saved,
            "samples_in_cloud": in_cloud,
            "backlog": backlog,
        }
        tmp = self._status_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(status_dict, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._status_path)
        except Exception:
            pass

    def run(self) -> None:
        """Main execution loop."""
        self.running = True
        logger.info(f"Starting pipeline in data_dir={self.data_dir} mock={self.mock_host}:{self.mock_port}")

        # Start metrics server
        start_metrics_server(self.metrics, port=self.metrics_port)

        # Start FSM and Uploader
        self.fsm.start()
        self.uploader.start()

        # Connect to mock
        self.mock_client.connect()

        last_status_time = 0.0
        try:
            while self.running:
                # Read both sensors continuously
                for sensor in (1, 2):
                    reply = self.mock_client.read_sensor_batch(sensor)
                    if reply and reply.get("data"):
                        sess_id = reply["session_id"]
                        first_idx = reply["first_index"]
                        data = reply["data"]
                        samples_list = [(first_idx + i, val) for i, val in enumerate(data)]
                        # Live upload enqueue
                        self.uploader.enqueue_live_batch(sess_id, sensor, samples_list)
                        # Evaluate any newly completed windows
                        self._evaluate_ready_windows(sess_id)

                now = time.time()
                if now - last_status_time >= 1.0:
                    self._write_status()
                    last_status_time = now

                # Realistic pace matching 400 Hz sensor generation
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Stopping pipeline...")
        finally:
            self.stop()

    def stop(self) -> None:
        self.running = False
        self.mock_client.close()
        self.uploader.stop()
        self.fsm.stop()
        self.wal.close()
        self._write_status()
