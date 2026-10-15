"""
Unit and Integration Tests for pipeline CLI subcommands:
- pipeline status
- pipeline export --session ID --from device|cloud
- pipeline results --session ID
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import pytest

from src.cloud_server import CloudServer, CloudStorage
from src.ledger import LossLedger
from src.wal import SegmentedWAL


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="cli_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cloud_env():
    storage = CloudStorage(":memory:")
    server = CloudServer(storage=storage)
    port = server.start()
    url = f"http://127.0.0.1:{port}"
    yield url, storage, server
    server.stop()


def run_pipeline(cmd_args, cwd=None):
    cli_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pipeline"))
    full_cmd = [sys.executable, cli_path] + cmd_args
    res = subprocess.run(full_cmd, cwd=cwd, capture_output=True, text=True)
    return res


def test_cli_status_initial(temp_dir):
    """Verify `pipeline status` produces exactly ONE JSON line with required keys."""
    res = run_pipeline(["status", "--data-dir", temp_dir])
    assert res.returncode == 0
    lines = [l for l in res.stdout.strip().split("\n") if l]
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert "mode" in data
    assert "samples_saved" in data
    assert "samples_in_cloud" in data
    assert "backlog" in data
    assert data["samples_saved"] == 0


def test_cli_export_from_device(temp_dir):
    """
    Verify `pipeline export --session ID --from device`
    outputs samples followed by losses in required JSONL format.
    """
    # Write samples and losses to temp_dir
    wal = SegmentedWAL(temp_dir)
    wal.write_batch("test-sess", 1, [(120000, [720, 1200, 1560])])
    wal.close()

    ledger = LossLedger(temp_dir)
    ledger.record_loss("test-sess", 2, 4000, 4399, "buffer_overflow")

    res = run_pipeline(["export", "--session", "test-sess", "--from", "device", "--data-dir", temp_dir])
    assert res.returncode == 0
    lines = [l for l in res.stdout.strip().split("\n") if l]
    assert len(lines) == 2

    sample_line = json.loads(lines[0])
    assert sample_line == {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}

    loss_line = json.loads(lines[1])
    assert loss_line == {
        "sensor": 2,
        "from_index": 4000,
        "to_index": 4399,
        "reason": "buffer_overflow",
    }


def test_cli_export_from_cloud(temp_dir, cloud_env):
    """Verify `pipeline export --session ID --from cloud` fetches from cloud endpoint."""
    cloud_url, storage, _ = cloud_env

    # Ingest sample into cloud
    storage.upsert_samples("cloud-sess", 1, [{"index": 500, "values": [1, 2, 3]}])
    storage.record_loss("cloud-sess", 1, 600, 699, "network_drop")

    res = run_pipeline([
        "export",
        "--session", "cloud-sess",
        "--from", "cloud",
        "--cloud-url", cloud_url,
        "--data-dir", temp_dir,
    ])
    assert res.returncode == 0
    lines = [l for l in res.stdout.strip().split("\n") if l]
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"sensor": 1, "index": 500, "values": [1, 2, 3]}
    assert json.loads(lines[1]) == {
        "sensor": 1,
        "from_index": 600,
        "to_index": 699,
        "reason": "network_drop",
    }


def test_cli_results(temp_dir):
    """Verify `pipeline results --session ID` prints JSON lines with all §2 fields."""
    results_path = os.path.join(temp_dir, "results.jsonl")
    sample_result = {
        "session_id": "res-sess",
        "window": 0,
        "mode": "offline",
        "missing_1": 0,
        "missing_2": 0,
        "complete": True,
        "mean_1": [720.0, 1200.0, 1560.0],
        "min_1": [700, 1150, 1500],
        "max_1": [740, 1250, 1620],
        "mean_2": [400000.0, 150000.0],
        "min_2": [399000, 149000],
        "max_2": [401000, 151000],
        "magnitude": 427200.2,
        "heart_rate": None,
    }
    with open(results_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(sample_result) + "\n")

    res = run_pipeline(["results", "--session", "res-sess", "--data-dir", temp_dir])
    assert res.returncode == 0
    lines = [l for l in res.stdout.strip().split("\n") if l]
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["session_id"] == "res-sess"
    assert data["window"] == 0
    assert data["mode"] == "offline"
    assert data["complete"] is True
    assert data["magnitude"] == 427200.2
