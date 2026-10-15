"""
MayaCardio Loss Ledger.

Separate, append-only, crash-safe ledger for recording all sample loss events.
CRITICAL INVARIANT: A loss record is ALWAYS written and fsync'd BEFORE any data
is dropped or before accepting any gap or session restart.
"""

from __future__ import annotations
import json
import os
import threading
from typing import Any, Dict, List, Optional


class LossLedger:
    """
    Append-only loss ledger stored at data_dir/loss_ledger.jsonl.
    Thread-safe and crash-safe via immediate fsync.
    """

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.ledger_path = os.path.join(self.data_dir, "loss_ledger.jsonl")
        self._lock = threading.Lock()
        self._losses: List[Dict[str, Any]] = []
        self._load_existing()

    def _load_existing(self) -> None:
        """Load existing loss entries on startup."""
        if not os.path.exists(self.ledger_path):
            return
        with open(self.ledger_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        self._losses.append(json.loads(line))
                    except Exception:
                        pass

    def record_loss(
        self,
        session_id: str,
        sensor: int,
        from_index: int,
        to_index: Optional[int],
        reason: str,
    ) -> Dict[str, Any]:
        """
        Record a loss event.
        Must be written and fsync'd BEFORE dropping data or moving on.
        """
        entry: Dict[str, Any] = {
            "session_id": session_id,
            "sensor": sensor,
            "from_index": from_index,
            "to_index": to_index,
            "reason": reason,
        }

        line = json.dumps(entry) + "\n"
        with self._lock:
            with open(self.ledger_path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            self._losses.append(entry)

        return entry

    def get_losses(self, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return all recorded loss entries, optionally filtered by session_id."""
        with self._lock:
            if session_id is None:
                return list(self._losses)
            return [l for l in self._losses if l.get("session_id") == session_id]
