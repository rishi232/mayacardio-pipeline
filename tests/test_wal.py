"""
Crash-safety, Deduplication, and Recovery Tests for WAL and Loss Ledger.
"""

import os
import shutil
import struct
import tempfile
import pytest

from src.dedupe import DedupeManager, IntervalIndex
from src.ledger import LossLedger
from src.wal import HEADER_FORMAT, HEADER_SIZE, SegmentedWAL, WALRecord


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="wal_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_interval_index_dedupe():
    """Verify interval index logic, containment, merging, and filtering."""
    idx = IntervalIndex()
    assert not idx.contains(10)

    idx.add_range(10, 20)
    assert idx.contains(10)
    assert idx.contains(15)
    assert idx.contains(20)
    assert not idx.contains(9)
    assert not idx.contains(21)

    # Adjacent merge
    idx.add_range(21, 30)
    assert idx.intervals == [[10, 30]]

    # Overlapping merge
    idx.add_range(5, 12)
    assert idx.intervals == [[5, 30]]

    # Filter new
    assert idx.filter_new([4, 5, 15, 30, 31]) == [4, 31]


def test_wal_write_and_replay(temp_dir):
    """Verify batch writes, persistence, fsync, and iteration."""
    wal = SegmentedWAL(temp_dir)
    samples_s1 = [(0, [720, 1200, 1560]), (1, [710, 1190, 1540])]
    samples_s2 = [(0, [400000, 150000]), (1, [400100, 150100])]

    n1 = wal.write_batch("sess-1", 1, samples_s1)
    n2 = wal.write_batch("sess-1", 2, samples_s2)
    assert n1 == 2
    assert n2 == 2

    # Verify deduplication: writing same batch again should return 0 new samples
    n_dup = wal.write_batch("sess-1", 1, samples_s1)
    assert n_dup == 0

    replayed = list(wal.iter_samples("sess-1"))
    assert len(replayed) == 4
    assert replayed[0]["session_id"] == "sess-1"
    assert replayed[0]["sensor"] == 1
    assert replayed[0]["index"] == 0
    assert replayed[0]["values"] == [720, 1200, 1560]
    wal.close()


def test_wal_segment_rotation(temp_dir):
    """Verify segment rotation with low max_segment_bytes."""
    # Set max segment bytes small (e.g. 500 bytes)
    wal = SegmentedWAL(temp_dir, max_segment_bytes=500)
    for b in range(10):
        samples = [(b * 20 + i, [1, 2, 3]) for i in range(20)]
        wal.write_batch("sess-rot", 1, samples)

    wal.close()

    # Re-open: verify all samples exist across rotated segments
    wal2 = SegmentedWAL(temp_dir)
    assert wal2.count_samples("sess-rot") == 200
    wal2.close()


def test_wal_torn_tail_header_recovery(temp_dir):
    """Simulate a power cut during header write (partial 4 bytes appended)."""
    wal = SegmentedWAL(temp_dir)
    wal.write_batch("sess-crash", 1, [(0, [1, 2, 3]), (1, [4, 5, 6])])
    wal.close()

    # Find the active wal file and append 4 junk bytes (torn header)
    wal_files = [os.path.join(temp_dir, "wal", f) for f in os.listdir(os.path.join(temp_dir, "wal"))]
    active_file = wal_files[-1]
    orig_size = os.path.getsize(active_file)
    with open(active_file, "ab") as f:
        f.write(b"\x99\x88\x77")  # 3 bytes only

    # Reopen WAL: recovery should truncate the torn tail and successfully resume
    wal_recovered = SegmentedWAL(temp_dir)
    assert os.path.getsize(active_file) == orig_size
    assert wal_recovered.count_samples("sess-crash") == 2

    # Should be able to append new data without error
    n = wal_recovered.write_batch("sess-crash", 1, [(2, [7, 8, 9])])
    assert n == 1
    assert wal_recovered.count_samples("sess-crash") == 3
    wal_recovered.close()


def test_wal_torn_tail_payload_recovery(temp_dir):
    """Simulate a power cut during payload write (header says 200 bytes, only 20 written)."""
    wal = SegmentedWAL(temp_dir)
    wal.write_batch("sess-crash2", 1, [(0, [1, 2, 3])])
    wal.close()

    wal_files = [os.path.join(temp_dir, "wal", f) for f in os.listdir(os.path.join(temp_dir, "wal"))]
    active_file = wal_files[-1]
    orig_size = os.path.getsize(active_file)

    # Append header specifying 200 bytes payload, but write only 15 bytes of data
    header = struct.pack(HEADER_FORMAT, 200, 0x12345678)
    with open(active_file, "ab") as f:
        f.write(header + b"incomplete_data")

    # Reopen WAL: must detect torn tail and truncate back to orig_size
    wal_recovered = SegmentedWAL(temp_dir)
    assert os.path.getsize(active_file) == orig_size
    assert wal_recovered.count_samples("sess-crash2") == 1
    wal_recovered.close()


def test_wal_corrupt_crc_tail_recovery(temp_dir):
    """Simulate a bit-flip / corrupt CRC in the trailing record."""
    wal = SegmentedWAL(temp_dir)
    wal.write_batch("sess-crc", 1, [(0, [1, 2, 3])])
    wal.close()

    wal_files = [os.path.join(temp_dir, "wal", f) for f in os.listdir(os.path.join(temp_dir, "wal"))]
    active_file = wal_files[-1]
    orig_size = os.path.getsize(active_file)

    # Append header with WRONG CRC
    fake_payload = b'{"type":"batch","session_id":"sess-crc","sensor":1,"samples":[[1,[9,9,9]]]}'
    wrong_crc = 0xDEADBEEF
    header = struct.pack(HEADER_FORMAT, len(fake_payload), wrong_crc)
    with open(active_file, "ab") as f:
        f.write(header + fake_payload)

    # Reopen WAL: must reject corrupted CRC and truncate
    wal_recovered = SegmentedWAL(temp_dir)
    assert os.path.getsize(active_file) == orig_size
    assert wal_recovered.count_samples("sess-crc") == 1
    wal_recovered.close()


def test_dedupe_rebuild_from_wal(temp_dir):
    """Simulate losing the index directory; restart must rebuild dedupe index from WAL."""
    wal = SegmentedWAL(temp_dir)
    wal.write_batch("sess-rebuild", 1, [(i, [i, i, i]) for i in range(100)])
    wal.close()

    # Wipe the dedupe index directory
    index_dir = os.path.join(temp_dir, "index")
    shutil.rmtree(index_dir, ignore_errors=True)

    # Restart WAL: must rebuild index from WAL
    wal2 = SegmentedWAL(temp_dir)
    idx = wal2.dedupe_manager.get_index("sess-rebuild", 1)
    for i in range(100):
        assert idx.contains(i)

    # Attempting to write duplicate range returns 0
    dup_n = wal2.write_batch("sess-rebuild", 1, [(i, [i, i, i]) for i in range(50)])
    assert dup_n == 0
    wal2.close()


def test_loss_ledger(temp_dir):
    """Verify append-only loss ledger persistence and crash safety."""
    ledger = LossLedger(temp_dir)
    e1 = ledger.record_loss("sess-1", 1, 4000, 4399, "buffer_overflow")
    e2 = ledger.record_loss("sess-1", 2, 5000, None, "mock_restart")

    assert e1["from_index"] == 4000
    assert e1["to_index"] == 4399
    assert e2["to_index"] is None

    # Reopen ledger
    ledger2 = LossLedger(temp_dir)
    losses = ledger2.get_losses("sess-1")
    assert len(losses) == 2
    assert losses[0]["reason"] == "buffer_overflow"
    assert losses[1]["reason"] == "mock_restart"
    assert losses[1]["to_index"] is None
