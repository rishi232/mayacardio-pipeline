"""
MayaCardio Segmented Write-Ahead Log (WAL).

Crash-safe, append-only segmented store for sample data.
CRITICAL INVARIANTS:
- Every accepted sample is written in a framed record: [4-byte len][4-byte crc32][payload].
- fsync after every batch write. Max data loss on power cut is strictly bounded (<= 1.0s).
- Startup scans all segments, detects and truncates any torn tail records cleanly,
  and rebuilds the deduplication index without loss or duplication.
- Atomic rename on segment rollover.
"""

from __future__ import annotations
import glob
import json
import os
import struct
import threading
import zlib
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

from src.dedupe import DedupeManager

# Frame format: 4-byte uint32 length (little-endian), 4-byte uint32 CRC-32 (little-endian)
HEADER_FORMAT = "<II"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
DEFAULT_MAX_SEGMENT_BYTES = 5 * 1024 * 1024  # 5 MB per segment (~100,000 samples)


class WALRecord:
    @staticmethod
    def encode(payload_dict: Dict[str, Any]) -> bytes:
        data = json.dumps(payload_dict, separators=(",", ":")).encode("utf-8")
        crc = zlib.crc32(data) & 0xFFFFFFFF
        header = struct.pack(HEADER_FORMAT, len(data), crc)
        return header + data

    @staticmethod
    def decode_frame(buffer: bytes, offset: int = 0) -> Tuple[Optional[Dict[str, Any]], int]:
        """
        Attempts to decode a frame from buffer at offset.
        Returns (record_dict, new_offset).
        If incomplete/torn: returns (None, offset).
        If corrupt CRC: raises ValueError.
        """
        if len(buffer) - offset < HEADER_SIZE:
            return None, offset
        payload_len, expected_crc = struct.unpack_from(HEADER_FORMAT, buffer, offset)
        frame_end = offset + HEADER_SIZE + payload_len
        if len(buffer) < frame_end:
            # Incomplete / torn payload
            return None, offset
        payload = buffer[offset + HEADER_SIZE : frame_end]
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise ValueError(f"CRC mismatch at offset {offset}: expected {expected_crc:#x}, got {actual_crc:#x}")
        record = json.loads(payload.decode("utf-8"))
        return record, frame_end


class SegmentedWAL:
    """
    Manages segmented WAL files in data_dir/wal.
    Segments are named:
      wal_00000001.log (sealed)
      wal_00000002.wal (active append segment)
    """

    def __init__(
        self,
        data_dir: str,
        dedupe_manager: Optional[DedupeManager] = None,
        max_segment_bytes: int = DEFAULT_MAX_SEGMENT_BYTES,
    ) -> None:
        self.data_dir = data_dir
        self.wal_dir = os.path.join(self.data_dir, "wal")
        os.makedirs(self.wal_dir, exist_ok=True)
        self.max_segment_bytes = max_segment_bytes
        self.dedupe_manager = dedupe_manager or DedupeManager(os.path.join(self.data_dir, "index"))
        self._lock = threading.Lock()

        self.current_segment_id = 1
        self.current_file = None
        self.current_segment_bytes = 0

        # Scan existing segments, recover from any torn tail records, and rebuild state
        self._recover_and_open()

    def _segment_path(self, segment_id: int, sealed: bool = False) -> str:
        ext = "log" if sealed else "wal"
        return os.path.join(self.wal_dir, f"wal_{segment_id:08d}.{ext}")

    def _recover_and_open(self) -> None:
        """Scan all segment files, drop any torn tail records, and open active segment."""
        # Find all .log and .wal files
        log_files = sorted(glob.glob(os.path.join(self.wal_dir, "wal_*.log")))
        wal_files = sorted(glob.glob(os.path.join(self.wal_dir, "wal_*.wal")))
        tmp_files = sorted(glob.glob(os.path.join(self.wal_dir, "wal_*.tmp")))

        # Any leftover .tmp files from an interrupted rollover can be renamed to .wal
        for tmp_f in tmp_files:
            wal_target = tmp_f[:-4] + ".wal"
            if not os.path.exists(wal_target):
                os.replace(tmp_f, wal_target)
                wal_files.append(wal_target)
            else:
                os.remove(tmp_f)

        wal_files = sorted(glob.glob(os.path.join(self.wal_dir, "wal_*.wal")))
        all_segments = sorted(log_files + wal_files)

        max_seg_id = 0
        for seg_path in all_segments:
            seg_name = os.path.basename(seg_path)
            try:
                seg_id = int(seg_name.split("_")[1].split(".")[0])
                max_seg_id = max(max_seg_id, seg_id)
            except Exception:
                continue

            # Verify and clean any torn records at the end of each segment
            self._verify_and_clean_segment(seg_path)

        # Open next or existing active segment
        if wal_files:
            active_path = wal_files[-1]
            seg_id = int(os.path.basename(active_path).split("_")[1].split(".")[0])
            self.current_segment_id = seg_id
            self.current_segment_bytes = os.path.getsize(active_path)
            self.current_file = open(active_path, "a+b")
        else:
            self.current_segment_id = max(max_seg_id, 0) + 1
            new_path = self._segment_path(self.current_segment_id, sealed=False)
            self.current_file = open(new_path, "a+b")
            self.current_segment_bytes = 0

    def _verify_and_clean_segment(self, path: str) -> None:
        """Reads a segment file, validates all records, truncates at first corruption/torn tail."""
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return

        valid_offset = 0
        with open(path, "rb") as f:
            content = f.read()

        offset = 0
        content_len = len(content)
        while offset < content_len:
            if content_len - offset < HEADER_SIZE:
                # Incomplete header at end of file: torn tail record
                break
            payload_len, expected_crc = struct.unpack_from(HEADER_FORMAT, content, offset)
            frame_end = offset + HEADER_SIZE + payload_len
            if content_len < frame_end:
                # Incomplete payload at end of file: torn tail record
                break
            payload = content[offset + HEADER_SIZE : frame_end]
            actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if actual_crc != expected_crc:
                # CRC mismatch: corrupted tail record
                break

            # Valid record
            try:
                rec = json.loads(payload.decode("utf-8"))
                # Rebuild dedupe index from valid record
                self._index_record(rec)
            except Exception:
                break

            valid_offset = frame_end
            offset = frame_end

        # If file has trailing torn bytes, truncate to valid_offset
        if valid_offset < content_len:
            with open(path, "r+b") as f:
                f.seek(valid_offset)
                f.truncate()

    def _index_record(self, rec: Dict[str, Any]) -> None:
        """Add record samples to dedupe index."""
        rec_type = rec.get("type", "batch")
        session_id = rec.get("session_id", "")
        sensor = rec.get("sensor", 1)
        if rec_type == "batch":
            samples = rec.get("samples", [])
            for s in samples:
                idx = s[0]
                self.dedupe_manager.record_sample(session_id, sensor, idx)
        elif rec_type == "sample":
            idx = rec.get("index")
            if idx is not None:
                self.dedupe_manager.record_sample(session_id, sensor, idx)

    def _rotate_segment(self) -> None:
        """Atomically seal current segment and open a new one."""
        if self.current_file:
            self.current_file.flush()
            os.fsync(self.current_file.fileno())
            self.current_file.close()

        old_active_path = self._segment_path(self.current_segment_id, sealed=False)
        sealed_path = self._segment_path(self.current_segment_id, sealed=True)
        if os.path.exists(old_active_path):
            os.replace(old_active_path, sealed_path)

        self.current_segment_id += 1
        new_active_path = self._segment_path(self.current_segment_id, sealed=False)
        self.current_file = open(new_active_path, "a+b")
        self.current_segment_bytes = 0

    def write_batch(
        self,
        session_id: str,
        sensor: int,
        samples: List[Tuple[int, List[int]]],
    ) -> int:
        """
        Write a batch of samples to the WAL.
        Deduplicates before writing: only writes samples not yet present in dedupe index.
        fsync is executed immediately after writing.
        Returns the number of new samples persisted.
        """
        with self._lock:
            # Filter duplicates
            idx_manager = self.dedupe_manager.get_index(session_id, sensor)
            new_samples = [s for s in samples if not idx_manager.contains(s[0])]

            if not new_samples:
                return 0

            payload_dict = {
                "type": "batch",
                "session_id": session_id,
                "sensor": sensor,
                "samples": [[s[0], s[1]] for s in new_samples],
            }
            frame = WALRecord.encode(payload_dict)

            # Check for segment rotation
            if self.current_segment_bytes + len(frame) > self.max_segment_bytes:
                self._rotate_segment()

            self.current_file.write(frame)
            self.current_file.flush()
            os.fsync(self.current_file.fileno())
            self.current_segment_bytes += len(frame)

            # Update dedupe index
            for s in new_samples:
                idx_manager.add_range(s[0], s[0])

            # Checkpoint dedupe index periodically or per batch
            self.dedupe_manager.save_checkpoint(session_id, sensor)

            return len(new_samples)

    def iter_samples(
        self, session_id: Optional[str] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over all samples in the WAL in order of recording.
        Each yielded item: {"session_id": str, "sensor": int, "index": int, "values": List[int]}
        """
        with self._lock:
            if self.current_file:
                self.current_file.flush()

        log_files = sorted(glob.glob(os.path.join(self.wal_dir, "wal_*.log")))
        wal_files = sorted(glob.glob(os.path.join(self.wal_dir, "wal_*.wal")))
        all_segments = sorted(log_files + wal_files)

        for path in all_segments:
            if not os.path.exists(path):
                continue
            with open(path, "rb") as f:
                content = f.read()

            offset = 0
            content_len = len(content)
            while offset < content_len:
                if content_len - offset < HEADER_SIZE:
                    break
                payload_len, expected_crc = struct.unpack_from(HEADER_FORMAT, content, offset)
                frame_end = offset + HEADER_SIZE + payload_len
                if content_len < frame_end:
                    break
                payload = content[offset + HEADER_SIZE : frame_end]
                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    break

                try:
                    rec = json.loads(payload.decode("utf-8"))
                except Exception:
                    break

                rec_sess = rec.get("session_id", "")
                if session_id is not None and rec_sess != session_id:
                    offset = frame_end
                    continue

                sensor = rec.get("sensor", 1)
                for s in rec.get("samples", []):
                    yield {
                        "session_id": rec_sess,
                        "sensor": sensor,
                        "index": s[0],
                        "values": s[1],
                    }

                offset = frame_end

    def count_samples(self, session_id: Optional[str] = None) -> int:
        """Count total persisted samples."""
        count = 0
        for _ in self.iter_samples(session_id=session_id):
            count += 1
        return count

    def close(self) -> None:
        """Flush and close active segment."""
        with self._lock:
            if self.current_file:
                self.current_file.flush()
                os.fsync(self.current_file.fileno())
                self.current_file.close()
                self.current_file = None
