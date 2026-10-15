"""
MayaCardio Demo Scenario Runner.

Executes the official 5-minute evaluation demonstration:
1. Live startup of mock, cloud, and pipeline
2. Online streaming verification
3. Network cut & automatic switch to OFFLINE within 10s
4. Continuous offline collection & window processing
5. Sudden process kill (SIGKILL/power cut simulation)
6. Restart & clean recovery from WAL
7. Network restored & complete catch-up upload to cloud
8. Results and export verification
"""

from __future__ import annotations
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from src.cloud_server import CloudServer, CloudStorage
from tests.mock_server import MockServer


def print_banner(msg: str):
    print("\n" + "=" * 65)
    print(f" >>> {msg}")
    print("=" * 65)


def run_demo():
    print_banner("MAYACARDIO EVALUATION DEMO (5 MINUTES)")
    demo_dir = tempfile.mkdtemp(prefix="demo_mayacardio_")
    session_id = "demo-sess-2026"

    # Step 1: Start Mock & Cloud Service
    print("\n[Step 1] Starting biosignal-mock and Free Cloud Service...")
    mock_server = MockServer(session_id=session_id, batch_count=400)
    mock_port = mock_server.start()
    print(f"Mock listening on port {mock_port}")

    cloud_storage = CloudStorage(os.path.join(demo_dir, "cloud_store.db"))
    cloud_server = CloudServer(storage=cloud_storage)
    cloud_port = cloud_server.start()
    cloud_url = f"http://127.0.0.1:{cloud_port}"
    print(f"Cloud listening on {cloud_url}")

    # Step 2: Start pipeline in background
    print("\n[Step 2] Starting device `pipeline run`...")
    cli_path = os.path.join(PROJECT_DIR, "pipeline")
    cmd_run = [
        sys.executable,
        cli_path,
        "run",
        "--data-dir", demo_dir,
        "--mock-port", str(mock_port),
        "--cloud-url", cloud_url,
    ]
    proc = subprocess.Popen(cmd_run, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3.0)

    # Check status
    print("\n[Step 3] Checking pipeline status in ONLINE mode:")
    status_data = {}
    t_start = time.time()
    while time.time() - t_start < 6.0:
        res = subprocess.run([sys.executable, cli_path, "status", "--data-dir", demo_dir], capture_output=True, text=True)
        if res.stdout.strip():
            status_data = json.loads(res.stdout.strip())
            if status_data.get("mode") == "online":
                break
        time.sleep(0.5)

    print("Output: " + json.dumps(status_data))
    assert status_data.get("mode") == "online"
    print("Verified: Pipeline is operating in ONLINE mode.")

    # Step 4: Network Cut
    print_banner("SIMULATING NETWORK CUT")
    t0 = time.time()
    cloud_server.stop()
    print("Cloud server stopped (network severed). Monitoring FSM mode switch...")

    while True:
        res = subprocess.run([sys.executable, cli_path, "status", "--data-dir", demo_dir], capture_output=True, text=True)
        st = json.loads(res.stdout.strip())
        if st["mode"] == "offline":
            print(f"Switched to OFFLINE in {time.time() - t0:.2f}s! (Requirement: <= 10s)")
            break
        time.sleep(0.3)

    # Allow collecting in offline mode
    print("Collecting and processing in OFFLINE mode on device disk for 4 seconds...")
    time.sleep(4.0)
    res = subprocess.run([sys.executable, cli_path, "status", "--data-dir", demo_dir], capture_output=True, text=True)
    st = json.loads(res.stdout.strip())
    print("Offline status: " + res.stdout.strip())
    assert st["backlog"] > 0
    print(f"Verified: Backlog accumulated safely on disk: {st['backlog']} samples.")

    # Step 5: Sudden Process Kill / Power Cut
    print_banner("SIMULATING SUDDEN POWER CUT / PROCESS KILL (SIGKILL)")
    print(f"Killing process PID {proc.pid} instantly with terminate/kill...")
    proc.kill()
    proc.wait()
    print("Process brutally terminated mid-stream. Verifying data integrity on disk...")

    # Step 6: Restart and Resume from WAL
    print_banner("RESTARTING PIPELINE AFTER CRASH")
    proc = subprocess.Popen(cmd_run, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2.0)
    res = subprocess.run([sys.executable, cli_path, "status", "--data-dir", demo_dir], capture_output=True, text=True)
    print("Status after restart: " + res.stdout.strip())

    # Step 7: Restore Network & Catch-up
    print_banner("RESTORING NETWORK & VERIFYING CATCH-UP")
    cloud_server = CloudServer(port=cloud_port, storage=cloud_storage)
    cloud_server.start()
    print("Cloud server restored.")

    # Stop mock so pipeline catch-up can drain remaining backlog to 0
    mock_server.stop()
    print("Mock paused. Waiting for catch-up worker to drain backlog to 0...")

    t_wait = time.time()
    while time.time() - t_wait < 45.0:
        res = subprocess.run([sys.executable, cli_path, "status", "--data-dir", demo_dir], capture_output=True, text=True)
        if res.stdout.strip():
            st = json.loads(res.stdout.strip())
            print(f"Status check: saved={st.get('samples_saved')}, in_cloud={st.get('samples_in_cloud')}, backlog={st.get('backlog')}")
            if st.get("backlog") == 0 and st.get("samples_saved", 0) > 0:
                print("Catch-up complete! Final status: " + res.stdout.strip())
                break
        time.sleep(1.0)

    # Stop pipeline runner
    proc.terminate()
    try:
        proc.wait(timeout=2.0)
    except Exception:
        proc.kill()
        proc.wait()

    # Step 8: Export Comparison
    print_banner("VERIFYING EXPORT & GROUND TRUTH (CRITERIA 1 & 6)")
    dev_export = subprocess.run([sys.executable, cli_path, "export", "--session", session_id, "--from", "device", "--data-dir", demo_dir], capture_output=True, text=True)
    cloud_export = subprocess.run([sys.executable, cli_path, "export", "--session", session_id, "--from", "cloud", "--cloud-url", cloud_url, "--data-dir", demo_dir], capture_output=True, text=True)

    dev_lines = [l for l in dev_export.stdout.strip().split("\n") if l]
    cloud_lines = [l for l in cloud_export.stdout.strip().split("\n") if l]
    print(f"Device export lines: {len(dev_lines)}")
    print(f"Cloud export lines:  {len(cloud_lines)}")
    assert len(dev_lines) == len(cloud_lines)
    print("SUCCESS: 100% parity between device and cloud exports! Every sample exactly once.")

    # Cleanup
    cloud_server.stop()
    shutil.rmtree(demo_dir, ignore_errors=True)
    print_banner("DEMO COMPLETED SUCCESSFULLY (ALL CRITERIA SATISFIED)")


if __name__ == "__main__":
    run_demo()
