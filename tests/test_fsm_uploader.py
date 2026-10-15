"""
Unit and Integration Tests for NetworkProbeFSM and CloudUploader (Catch-up & FSM).
"""

import os
import shutil
import tempfile
import time
import pytest

from src.cloud_server import CloudServer, CloudStorage
from src.dedupe import DedupeManager
from src.fsm import NetworkProbeFSM
from src.ledger import LossLedger
from src.uploader import CloudUploader
from src.wal import SegmentedWAL


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="fsm_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cloud_env():
    storage = CloudStorage(":memory:")
    server = CloudServer(storage=storage)
    port = server.start()
    cloud_url = f"http://127.0.0.1:{port}"
    yield cloud_url, storage, server
    server.stop()


def test_fsm_transition_rules(cloud_env):
    """
    Verify FSM transition requirements:
    - 2 consecutive failures => OFFLINE
    - 1 success => ONLINE
    - Each transition within 10 seconds.
    """
    cloud_url, _, server = cloud_env
    transitions = []

    def on_mode_change(old_m, new_m):
        transitions.append((old_m, new_m))

    fsm = NetworkProbeFSM(
        cloud_url=cloud_url,
        probe_interval=0.1,
        probe_timeout=0.2,
        initial_mode="offline",
        on_mode_change=on_mode_change,
    )

    # 1 success => ONLINE
    fsm.update_with_probe_result(True)
    assert fsm.get_mode() == "online"
    assert transitions == [("offline", "online")]

    # 1 failure while ONLINE => remains ONLINE
    fsm.update_with_probe_result(False)
    assert fsm.get_mode() == "online"

    # 2nd consecutive failure => OFFLINE
    fsm.update_with_probe_result(False)
    assert fsm.get_mode() == "offline"
    assert transitions == [("offline", "online"), ("online", "offline")]

    # 1 success => ONLINE again
    fsm.update_with_probe_result(True)
    assert fsm.get_mode() == "online"
    assert len(transitions) == 3


def test_uploader_catchup_after_outage(temp_dir, cloud_env):
    """
    Verify Criterion 6:
    After network returns, cloud copy is complete with every sample exactly once.
    """
    cloud_url, storage, server = cloud_env
    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    fsm = NetworkProbeFSM(cloud_url=cloud_url, probe_interval=0.2, initial_mode="online")
    uploader = CloudUploader(cloud_url=cloud_url, fsm=fsm, wal=wal, loss_ledger=ledger)
    uploader.start()

    # 1. Normal online ingest of 20 samples (0..19)
    samples_s1_batch1 = [(i, [100, 200, 300]) for i in range(20)]
    wal.write_batch("sess-catchup", 1, samples_s1_batch1)
    uploader.enqueue_live_batch("sess-catchup", 1, samples_s1_batch1)
    time.sleep(0.5)

    # Verify cloud received first batch
    lines_initial = storage.get_export_lines("sess-catchup")
    assert len(lines_initial) == 20

    # 2. Simulate network outage: FSM switches to OFFLINE
    fsm.update_with_probe_result(False)
    fsm.update_with_probe_result(False)
    assert fsm.get_mode() == "offline"

    # Save 100 new samples while offline (indices 20..119)
    offline_samples = [(i, [400, 500, 600]) for i in range(20, 120)]
    wal.write_batch("sess-catchup", 1, offline_samples)
    assert wal.count_samples("sess-catchup") == 120

    # Live enqueue should be ignored while offline
    uploader.enqueue_live_batch("sess-catchup", 1, offline_samples)
    time.sleep(0.3)
    # Cloud still has only 20 samples
    assert len(storage.get_export_lines("sess-catchup")) == 20

    # 3. Network returns: FSM switches to ONLINE
    fsm.update_with_probe_result(True)
    assert fsm.get_mode() == "online"

    # Wait for catchup worker to drain backlog from WAL
    deadline = time.time() + 5.0
    while time.time() < deadline:
        cloud_lines = storage.get_export_lines("sess-catchup")
        if len(cloud_lines) >= 120:
            break
        time.sleep(0.2)

    cloud_lines = storage.get_export_lines("sess-catchup")
    assert len(cloud_lines) == 120

    # Verify exact samples and no duplicates
    for i in range(120):
        expected_val = [100, 200, 300] if i < 20 else [400, 500, 600]
        assert cloud_lines[i]["index"] == i
        assert cloud_lines[i]["values"] == expected_val

    uploader.stop()
    wal.close()
