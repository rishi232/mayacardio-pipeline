"""
MayaCardio Cloud Uploader and Catch-up Engine.

CRITICAL INVARIANTS:
- Bounded queue with backpressure.
- Exponential backoff on transient errors.
- Cloud /ingest acked_up_to drives queue trimming and catch-up.
- Catch-up sync worker drains offline backlog from WAL in priority order.
- Idempotent upload keyed by (session_id, sensor, index).
"""

from __future__ import annotations
import json
import logging
import queue
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from src.fsm import NetworkProbeFSM
from src.ledger import LossLedger
from src.wal import SegmentedWAL

logger = logging.getLogger("Uploader")
BATCH_SIZE = 400


class CloudUploader:
    def __init__(
        self,
        cloud_url: str,
        fsm: NetworkProbeFSM,
        wal: SegmentedWAL,
        loss_ledger: Optional[LossLedger] = None,
        max_queue_size: int = 500,
        request_timeout: float = 4.0,
    ) -> None:
        self.cloud_url = cloud_url.rstrip("/")
        self.fsm = fsm
        self.wal = wal
        self.loss_ledger = loss_ledger
        self.request_timeout = request_timeout

        # In-memory live batch queue: bounded to prevent memory growth
        self.live_queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=max_queue_size)

        # acked_up_to per (session_id, sensor)
        self.acked_up_to: Dict[Tuple[str, int], int] = {}
        self._lock = threading.Lock()

        self._running = False
        self._live_thread: Optional[threading.Thread] = None
        self._catchup_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._running = True
        self._live_thread = threading.Thread(target=self._live_upload_loop, daemon=True)
        self._live_thread.start()
        self._catchup_thread = threading.Thread(target=self._catchup_loop, daemon=True)
        self._catchup_thread.start()

    def stop(self) -> None:
        self._running = False
        if self._live_thread:
            self._live_thread.join(timeout=1.0)
        if self._catchup_thread:
            self._catchup_thread.join(timeout=1.0)

    def enqueue_live_batch(
        self, session_id: str, sensor: int, samples: List[Tuple[int, List[int]]]
    ) -> bool:
        """
        Enqueues samples for upload. Batches them efficiently up to BATCH_SIZE.
        """
        if self.fsm.get_mode() != "online":
            return False

        item = {
            "session_id": session_id,
            "sensor": sensor,
            "samples": [{"index": s[0], "values": s[1]} for s in samples],
        }
        try:
            self.live_queue.put_nowait(item)
            return True
        except queue.Full:
            return False

    def post_ingest(self, session_id: str, sensor: int, samples: List[Dict[str, Any]]) -> Optional[int]:
        """
        Uploads a batch of samples to POST /ingest.
        Returns the new acked_up_to returned by the cloud, or None on failure.
        """
        if not samples:
            return None
        url = f"{self.cloud_url}/ingest"
        payload = {
            "session_id": session_id,
            "sensor": sensor,
            "samples": samples,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                if resp.status == 200:
                    res_json = json.loads(resp.read().decode("utf-8"))
                    new_ack = res_json.get("acked_up_to")
                    with self._lock:
                        if new_ack is not None:
                            curr_ack = self.acked_up_to.get((session_id, sensor), -1)
                            if new_ack > curr_ack:
                                self.acked_up_to[(session_id, sensor)] = new_ack
                    return new_ack
        except Exception:
            return None
        return None

    def post_result(self, result_dict: Dict[str, Any]) -> bool:
        """Uploads a computed window result to POST /result."""
        url = f"{self.cloud_url}/result"
        data = json.dumps(result_dict).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                return resp.status == 200
        except Exception:
            return False

    def post_loss(self, loss_entry: Dict[str, Any]) -> bool:
        """Uploads a loss record to POST /loss."""
        url = f"{self.cloud_url}/loss"
        data = json.dumps(loss_entry).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                return resp.status == 200
        except Exception:
            return False

    def trigger_cloud_window(self, session_id: str, window: int) -> Optional[Dict[str, Any]]:
        """Invokes GET /window on cloud to produce an online result in cloud."""
        url = f"{self.cloud_url}/window?session_id={session_id}&window={window}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None
        return None

    def _live_upload_loop(self) -> None:
        """Accumulates live items into larger batches and uploads."""
        pending: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        last_flush = time.time()

        while self._running:
            try:
                item = self.live_queue.get(timeout=0.05)
                key = (item["session_id"], item["sensor"])
                if key not in pending:
                    pending[key] = []
                pending[key].extend(item["samples"])
            except queue.Empty:
                pass

            now = time.time()
            # Flush if batch is large enough or every 0.1 seconds
            for key in list(pending.keys()):
                samples = pending[key]
                if len(samples) >= BATCH_SIZE or (samples and now - last_flush >= 0.1):
                    sess_id, sensor = key
                    batch_to_send = samples[:BATCH_SIZE]
                    pending[key] = samples[BATCH_SIZE:]
                    if not pending[key]:
                        del pending[key]

                    if self.fsm.get_mode() == "online":
                        ack = self.post_ingest(sess_id, sensor, batch_to_send)
                        if ack is None:
                            self.fsm.update_with_probe_result(False)

            last_flush = now

    def _catchup_loop(self) -> None:
        """Periodically scans WAL for any un-acked samples and syncs them in large batches."""
        backoff = 0.5
        while self._running:
            if self.fsm.get_mode() != "online":
                time.sleep(0.5)
                continue

            had_work, success = self.run_catchup_pass()
            if not success:
                time.sleep(min(backoff, 3.0))
                backoff = min(backoff * 1.5, 5.0)
            else:
                backoff = 0.5
                if had_work:
                    # Stream immediately with no sleep until caught up
                    continue
                else:
                    time.sleep(0.5)

    def run_catchup_pass(self) -> Tuple[bool, bool]:
        """
        Executes catchup from WAL.
        Collects up to 1000 samples per pass.
        Returns (had_work, success).
        """
        if self.loss_ledger:
            losses = self.loss_ledger.get_losses()
            for l in losses:
                self.post_loss(l)

        # Collect un-acked samples from WAL
        CATCHUP_BATCH = 1000
        batches_to_send: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        found_count = 0

        for sample in self.wal.iter_samples():
            sess = sample["session_id"]
            sensor = sample["sensor"]
            idx = sample["index"]
            with self._lock:
                curr_ack = self.acked_up_to.get((sess, sensor), -1)

            if idx > curr_ack:
                key = (sess, sensor)
                if key not in batches_to_send:
                    batches_to_send[key] = []
                batches_to_send[key].append({"index": idx, "values": sample["values"]})
                found_count += 1
                if found_count >= CATCHUP_BATCH:
                    break

        if not batches_to_send:
            return False, True

        had_work = True
        all_success = True
        for (sess, sensor), samples in batches_to_send.items():
            ack = self.post_ingest(sess, sensor, samples)
            if ack is None:
                all_success = False

        return had_work, all_success

    def get_cloud_sample_count(self) -> int:
        with self._lock:
            # Sum contiguous acked indices + 1 (since 0-indexed)
            total = 0
            for ack in self.acked_up_to.values():
                if ack >= 0:
                    total += (ack + 1)
            return total
