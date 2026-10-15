"""
Unit and Integration Tests for Biosignal Mock TCP Client and Fault Injections.
"""

import os
import shutil
import tempfile
import time
import pytest

from src.dedupe import DedupeManager
from src.ledger import LossLedger
from src.mock_client import MockClient, extract_data_and_verify_crc
from src.wal import SegmentedWAL
from tests.mock_server import MockServer


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="mock_client_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def test_server():
    server = MockServer()
    port = server.start()
    yield server
    server.stop()


def test_crc_verification_logic():
    """Verify raw CRC-32 parsing and matching."""
    # Good payload
    line = b'{"session_id":"s1","sensor":1,"first_index":0,"count":2,"lost":[],"session_end":false,"crc32":"b11fa2ca","data":[[1,2,3],[4,5,6]]}\n'
    valid, reply = extract_data_and_verify_crc(line)
    # Let's check whether b11fa2ca matches:
    # data text is b'[[1,2,3],[4,5,6]]'
    import zlib
    expected = f"{zlib.crc32(b'[[1,2,3],[4,5,6]]') & 0xffffffff:08x}"
    line_correct = f'{{"session_id":"s1","sensor":1,"first_index":0,"count":2,"lost":[],"session_end":false,"crc32":"{expected}","data":[[1,2,3],[4,5,6]]}}\n'.encode("utf-8")
    valid_correct, reply_correct = extract_data_and_verify_crc(line_correct)
    assert valid_correct is True
    assert reply_correct["session_id"] == "s1"

    # Bad CRC
    line_corrupt = b'{"session_id":"s1","sensor":1,"first_index":0,"count":2,"lost":[],"session_end":false,"crc32":"deadbeef","data":[[1,2,3],[4,5,6]]}\n'
    valid_corrupt, reply_corrupt = extract_data_and_verify_crc(line_corrupt)
    assert valid_corrupt is False
    assert reply_corrupt is None


def test_mock_client_read_and_persist(temp_dir, test_server):
    """Verify normal read of sensors and persistence into WAL."""
    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=test_server.host, port=test_server.port, wal=wal, loss_ledger=ledger)

    reply1 = client.read_sensor_batch(sensor=1)
    assert reply1 is not None
    assert reply1["sensor"] == 1
    assert reply1["first_index"] == 0
    assert reply1["count"] == 10

    reply2 = client.read_sensor_batch(sensor=2)
    assert reply2 is not None
    assert reply2["sensor"] == 2
    assert reply2["first_index"] == 0
    assert reply2["count"] == 10

    assert wal.count_samples(test_server.session_id) == 20
    client.close()
    wal.close()


def test_mock_client_corrupt_crc_retry(temp_dir, test_server):
    """Verify client rejects corrupt reply, retries with from_index, and persists good data."""
    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=test_server.host, port=test_server.port, wal=wal, loss_ledger=ledger)

    # First read normal
    client.read_sensor_batch(sensor=1)
    assert wal.count_samples(test_server.session_id) == 10

    # Inject 1 corrupt reply from server on next read
    test_server.inject_corrupt_crc = True
    test_server.corrupt_crc_counter = 1

    reply = client.read_sensor_batch(sensor=1)
    # The client must have rejected corrupt payload and retried to get good data!
    assert reply is not None
    assert reply["crc32"] != "deadbeef"
    # Persisted samples must be 20, no corrupt data stored
    assert wal.count_samples(test_server.session_id) == 20
    client.close()
    wal.close()


def test_mock_client_loss_recording(temp_dir, test_server):
    """Verify that any loss reported by the mock is recorded in the LossLedger."""
    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=test_server.host, port=test_server.port, wal=wal, loss_ledger=ledger)

    test_server.inject_loss_event = {
        "from_index": 100,
        "to_index": 199,
        "reason": "buffer_overflow",
    }

    client.read_sensor_batch(sensor=1)
    losses = ledger.get_losses(test_server.session_id)
    assert len(losses) == 1
    assert losses[0]["sensor"] == 1
    assert losses[0]["from_index"] == 100
    assert losses[0]["to_index"] == 199
    assert losses[0]["reason"] == "buffer_overflow"
    client.close()
    wal.close()


def test_mock_client_restart_handling(temp_dir, test_server):
    """
    Verify mock restart detection:
    When session_id changes, unread data of old session must be listed in LossLedger
    with to_index: null.
    """
    wal = SegmentedWAL(temp_dir)
    ledger = LossLedger(temp_dir)
    client = MockClient(host=test_server.host, port=test_server.port, wal=wal, loss_ledger=ledger)

    # Read batch in first session (sess 1, last index = 9)
    client.read_sensor_batch(sensor=1)
    assert client.current_session_id == test_server.session_id

    # Simulate mock restart: change server session_id and reset indices
    test_server.session_id = "test-session-2-restarted"
    test_server.sensor_index = {1: 0, 2: 0}

    # Next read will receive new session_id
    client.read_sensor_batch(sensor=1)
    assert client.current_session_id == "test-session-2-restarted"

    # Verify that old session loss was recorded with to_index=None
    losses = ledger.get_losses("test-session-1")
    assert len(losses) == 1
    assert losses[0]["sensor"] == 1
    assert losses[0]["from_index"] == 10  # next after index 9
    assert losses[0]["to_index"] is None
    assert losses[0]["reason"] == "mock_restart"
    client.close()
    wal.close()
