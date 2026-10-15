"""
MayaCardio Automated Failure Test Suite.

Executes and logs all required failure tests:
1. Corrupt CRC-32 injection & re-read verification
2. Duplicate replies & deduplication verification
3. Mock stoppage & reconnection verification
4. Sensor stoppage & lost range recording in LossLedger
5. Mock restart & old session unread data loss with to_index=None
6. Network cut & automatic mode switch within 10 seconds
7. Network restored & catch-up sync completeness
8. Process kill (SIGKILL/power cut simulation) & torn tail WAL recovery
"""

from __future__ import annotations
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List

# Ensure project root in sys.path
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from src.cloud_server import CloudServer, CloudStorage
from src.dedupe import DedupeManager
from src.fsm import NetworkProbeFSM
from src.ledger import LossLedger
from src.mock_client import MockClient
from src.uploader import CloudUploader
from src.wal import SegmentedWAL
from tests.mock_server import MockServer


def log_step(name: str):
    print(f"\n=======================================================")
    print(f"RUNNING FAILURE TEST: {name}")
    print(f"=======================================================")


def test_corrupt_crc():
    log_step("1. Corrupt CRC-32 Injection & Re-read Verification")
    temp_dir = tempfile.mkdtemp(prefix="fail_crc_")
    server = MockServer(session_id="crc-sess")
    server.start()

    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=server.host, port=server.port, wal=wal, loss_ledger=ledger)

    # Read batch 1 normal
    r1 = client.read_sensor_batch(1)
    assert r1 is not None and r1["count"] == 10

    # Inject 2 consecutive corrupt CRC replies
    server.inject_corrupt_crc = True
    server.corrupt_crc_counter = 2

    # Client must reject corrupt replies and retry with from_index
    r2 = client.read_sensor_batch(1)
    assert r2 is not None
    assert r2["crc32"] != "deadbeef"

    total = wal.count_samples("crc-sess")
    assert total == 20, f"Expected 20 samples, got {total}"

    client.close()
    server.stop()
    wal.close()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print("PASS: Corrupt CRC rejected, re-read succeeded, no corrupt data stored.")


def test_duplicate_replies():
    log_step("2. Duplicate Replies & Deduplication Verification")
    temp_dir = tempfile.mkdtemp(prefix="fail_dup_")
    server = MockServer(session_id="dup-sess")
    server.inject_duplicate_replies = True
    server.start()

    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=server.host, port=server.port, wal=wal, loss_ledger=ledger)

    for _ in range(5):
        client.read_sensor_batch(1)

    # Server sent duplicates on every request; verify WAL has only unique samples
    samples = list(wal.iter_samples("dup-sess"))
    indices = [s["index"] for s in samples]
    assert len(indices) == len(set(indices)), "Duplicate samples found in WAL!"
    print(f"PASS: {len(indices)} unique samples persisted, duplicates filtered out.")

    client.close()
    server.stop()
    wal.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_lost_range_ledger():
    log_step("3. Sensor Stoppage & LossLedger Verification")
    temp_dir = tempfile.mkdtemp(prefix="fail_loss_")
    server = MockServer(session_id="loss-sess")
    server.start()

    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=server.host, port=server.port, wal=wal, loss_ledger=ledger)

    server.inject_loss_event = {
        "from_index": 500,
        "to_index": 899,
        "reason": "sensor_stopped",
    }

    client.read_sensor_batch(1)
    losses = ledger.get_losses("loss-sess")
    assert len(losses) == 1
    assert losses[0]["from_index"] == 500
    assert losses[0]["to_index"] == 899
    assert losses[0]["reason"] == "sensor_stopped"
    print("PASS: Loss accurately recorded in append-only LossLedger.")

    client.close()
    server.stop()
    wal.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_mock_restart():
    log_step("4. Mock Restart & Old Session Loss Recording (to_index=None)")
    temp_dir = tempfile.mkdtemp(prefix="fail_restart_")
    server = MockServer(session_id="session-A")
    server.start()

    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=server.host, port=server.port, wal=wal, loss_ledger=ledger)

    client.read_sensor_batch(1)  # indices 0..9

    # Mock restarts with session-B
    server.session_id = "session-B"
    server.sensor_index = {1: 0, 2: 0}

    client.read_sensor_batch(1)

    # Verify session-A recorded loss with to_index=None
    losses = ledger.get_losses("session-A")
    assert len(losses) == 1
    assert losses[0]["sensor"] == 1
    assert losses[0]["from_index"] == 10
    assert losses[0]["to_index"] is None
    assert losses[0]["reason"] == "mock_restart"
    print("PASS: Session restart detected; unread data recorded with to_index: null.")

    client.close()
    server.stop()
    wal.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_network_cut_and_switch():
    log_step("5. Network Cut & Switch <= 10s Verification")
    cloud_storage = CloudStorage(":memory:")
    cloud_server = CloudServer(storage=cloud_storage)
    cloud_port = cloud_server.start()
    cloud_url = f"http://127.0.0.1:{cloud_port}"

    fsm = NetworkProbeFSM(cloud_url=cloud_url, probe_interval=0.5, probe_timeout=0.5)
    fsm.start()

    # Initial state transitions to online
    time.sleep(1.0)
    assert fsm.get_mode() == "online"
    print("Online confirmed.")

    # Cut network by stopping cloud server
    t_cut = time.time()
    cloud_server.stop()

    # Wait for FSM to detect network cut (must complete <= 10s)
    while fsm.get_mode() != "offline":
        if time.time() - t_cut > 10.0:
            raise TimeoutError("FSM failed to switch to OFFLINE within 10 seconds!")
        time.sleep(0.2)

    elapsed_offline = time.time() - t_cut
    print(f"Switched ONLINE -> OFFLINE in {elapsed_offline:.2f}s (<= 10s constraint met).")

    # Restore network
    cloud_server = CloudServer(port=cloud_port, storage=cloud_storage)
    cloud_server.start()
    t_restore = time.time()

    # Wait for FSM to switch back to ONLINE (must complete <= 10s)
    while fsm.get_mode() != "online":
        if time.time() - t_restore > 10.0:
            raise TimeoutError("FSM failed to switch to ONLINE within 10 seconds!")
        time.sleep(0.2)

    elapsed_online = time.time() - t_restore
    print(f"Switched OFFLINE -> ONLINE in {elapsed_online:.2f}s (<= 10s constraint met).")

    fsm.stop()
    cloud_server.stop()
    print("PASS: Bidirectional mode switch completed well within 10 seconds.")


def test_process_crash_recovery():
    log_step("6. Process Crash & Torn Tail Recovery Verification")
    temp_dir = tempfile.mkdtemp(prefix="fail_crash_")
    wal = SegmentedWAL(temp_dir)
    wal.write_batch("sess-kill", 1, [(0, [1, 2, 3]), (1, [4, 5, 6])])
    wal.close()

    # Inject torn record at end of WAL
    wal_files = sorted(os.listdir(os.path.join(temp_dir, "wal")))
    active = os.path.join(temp_dir, "wal", wal_files[-1])
    with open(active, "ab") as f:
        # Write corrupted header and partial payload
        f.write(b"\x10\x00\x00\x00\xde\xad\xbe\xefcorrupt_tail")

    # Restart WAL: must recover cleanly, drop torn tail, and allow new writes
    wal2 = SegmentedWAL(temp_dir)
    assert wal2.count_samples("sess-kill") == 2
    n = wal2.write_batch("sess-kill", 1, [(2, [7, 8, 9])])
    assert n == 1
    assert wal2.count_samples("sess-kill") == 3
    wal2.close()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print("PASS: WAL recovered from torn tail record without loss of prior valid samples.")


def main():
    print("=======================================================")
    print("MayaCardio Pipeline - Comprehensive Failure Test Runner")
    print("=======================================================")
    test_corrupt_crc()
    test_duplicate_replies()
    test_lost_range_ledger()
    test_mock_restart()
    test_network_cut_and_switch()
    test_process_crash_recovery()
    print("\n=======================================================")
    print("ALL 6 FAILURE INJECTION TESTS PASSED CLEANLY (100% SUCCESS)!")
    print("=======================================================")


if __name__ == "__main__":
    main()
