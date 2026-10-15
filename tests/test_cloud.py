"""
Unit and Integration Tests for Cloud Service (Section 5).
"""

import json
import urllib.request
import pytest

from src.cloud_server import CloudServer, CloudStorage


@pytest.fixture
def cloud_service():
    storage = CloudStorage(":memory:")
    server = CloudServer(storage=storage)
    port = server.start()
    base_url = f"http://127.0.0.1:{port}"
    yield base_url, storage, server
    server.stop()


def test_cloud_probe(cloud_service):
    base_url, _, _ = cloud_service
    req = urllib.request.Request(f"{base_url}/probe")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "healthy"


def test_cloud_ingest_idempotence_and_ack(cloud_service):
    base_url, storage, _ = cloud_service

    # Ingest 10 samples of sensor 1 (indices 0..9)
    samples = [{"index": i, "values": [10, 20, 30]} for i in range(10)]
    payload = {
        "session_id": "test-sess",
        "sensor": 1,
        "samples": samples,
    }
    req = urllib.request.Request(
        f"{base_url}/ingest",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "ok"
        assert data["acked_up_to"] == 9

    # Send duplicate payload: must remain idempotent with same acked_up_to
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["acked_up_to"] == 9


def test_cloud_window_computation(cloud_service):
    base_url, storage, _ = cloud_service

    # Ingest complete window 0 (samples 0..399 for sensor 1 and sensor 2)
    s1_samples = [{"index": i, "values": [10, 20, 30]} for i in range(400)]
    s2_samples = [{"index": i, "values": [3, 4]} for i in range(400)]

    for payload in [
        {"session_id": "sess-win", "sensor": 1, "samples": s1_samples},
        {"session_id": "sess-win", "sensor": 2, "samples": s2_samples},
    ]:
        req = urllib.request.Request(
            f"{base_url}/ingest",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200

    # Request GET /window?session_id=sess-win&window=0
    req_win = urllib.request.Request(f"{base_url}/window?session_id=sess-win&window=0")
    with urllib.request.urlopen(req_win) as resp:
        assert resp.status == 200
        win_res = json.loads(resp.read().decode("utf-8"))
        assert win_res["window"] == 0
        assert win_res["mode"] == "online"
        assert win_res["complete"] is True
        assert win_res["magnitude"] == 5.0
        assert win_res["mean_1"] == [10.0, 20.0, 30.0]


def test_cloud_export_format(cloud_service):
    base_url, storage, _ = cloud_service

    # Ingest 2 samples
    samples = [
        {"index": 0, "values": [1, 2, 3]},
        {"index": 1, "values": [4, 5, 6]},
    ]
    payload = {"session_id": "export-sess", "sensor": 1, "samples": samples}
    req = urllib.request.Request(
        f"{base_url}/ingest",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)

    # Ingest 1 loss
    loss_payload = {
        "session_id": "export-sess",
        "sensor": 2,
        "from_index": 4000,
        "to_index": 4399,
        "reason": "buffer_overflow",
    }
    req_loss = urllib.request.Request(
        f"{base_url}/loss",
        data=json.dumps(loss_payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req_loss)

    # Request GET /export?session_id=export-sess
    req_exp = urllib.request.Request(f"{base_url}/export?session_id=export-sess")
    with urllib.request.urlopen(req_exp) as resp:
        assert resp.status == 200
        content = resp.read().decode("utf-8")
        lines = [json.loads(line) for line in content.strip().split("\n")]
        assert len(lines) == 3
        # First 2 are samples
        assert lines[0] == {"sensor": 1, "index": 0, "values": [1, 2, 3]}
        assert lines[1] == {"sensor": 1, "index": 1, "values": [4, 5, 6]}
        # Last is loss
        assert lines[2] == {
            "sensor": 2,
            "from_index": 4000,
            "to_index": 4399,
            "reason": "buffer_overflow",
        }
