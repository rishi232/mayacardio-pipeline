# MayaCardio Data Pipeline: Architecture & System Design Note

## 1. System Architecture

The MayaCardio pipeline is a high-reliability, zero-silent-loss data engine designed to operate continuously on an ARM64 Raspberry Pi-class device (≤ 4 CPU cores, ≤ 2 GB RAM) and synchronize with a free cloud service.

```
                  +----------------------------------------------+
                  |           biosignal-mock (port 7100)         |
                  +----------------------------------------------+
                                         |
                                TCP NDJSON Stream
                                         v
+---------------------------------------------------------------------------------+
|                        DEVICE PIPELINE (Single Process)                         |
|                                                                                 |
|  +--------------------+        +---------------------+      +----------------+  |
|  |   Mock TCP Client  | -----> | Deduplication Index | ---> |  Segmented WAL |  |
|  | (CRC-32, Re-read)  |        |  (Interval Sets)    |      | (fsync/batch)  |  |
|  +--------------------+        +---------------------+      +----------------+  |
|            |                                                        |           |
|            v                                                        v           |
|  +--------------------+                                     +----------------+  |
|  |    Loss Ledger     |                                     | Window Engine  |  |
|  |  (Append-only sync)|                                     |  (Section 2)   |  |
|  +--------------------+                                     +----------------+  |
|                                                                     |           |
|  +--------------------+                                             v           |
|  |   Mode FSM & Probe | <---------------------------------- +----------------+  |
|  |  (<=3s Heartbeat)  |                                     | Cloud Uploader |  |
|  +--------------------+                                     | (Catch-up Sync)|  |
+---------------------------------------------------------------------+-----------+
                                                                      |
                                                               HTTP/1.1 POST
                                                                      v
+---------------------------------------------------------------------------------+
|                         CLOUD SERVICE (Free Tier)                               |
|                                                                                 |
|  POST /ingest (Idempotent samples)  -> SQLite WAL DB                            |
|  POST /result (Idempotent windows)  -> Results DB                               |
|  GET  /window (Shared window logic) -> Live online verification                 |
|  GET  /export (JSONL export for judges)                                         |
+---------------------------------------------------------------------------------+
```

### Component Responsibilities:
1. **Mock Client**: Connects via TCP to `biosignal-mock`. Reads alternating batches for Sensor 1 and Sensor 2. Before parsing data, verifies CRC-32 (zlib) from the raw bytes `[` to `]`. Corrupt replies trigger immediate re-reads using `{"cmd": "read", "sensor": S, "from_index": N}`.
2. **Deduplication Engine**: Uses memory-efficient interval sets (`[start, end]` ranges). Filters out duplicate transmissions in $O(\log N)$ time. Persisted to disk and automatically rebuilt from the WAL on startup.
3. **Segmented Write-Ahead Log (WAL)**: Binary framed records (`[4B len][4B crc32][payload]`) written to 5 MB segments. Every batch is flushed and `fsync`'d before proceeding. Atomic rename seals filled segments.
4. **Loss Ledger**: Separate append-only JSONL ledger. Critical Invariant: Every loss (buffer overflow, sensor drop, disk full, mock restart) is written and `fsync`'d **before** any data is dropped.
5. **Mode FSM & Network Probe**: Probes `GET /probe` every 2.0 s ($\le 3$ s). Two consecutive failures trigger `OFFLINE`; one success triggers `ONLINE`. Switches occur in $\le 6$ s ($< 10$ s limit).
6. **Window Engine (Pure Function)**: Shared identically by device and cloud without modification. Computes 1-second windows, channel statistics, Pythagorean magnitude, strict 4-condition ECG peak detection, and 5-second heart rate.

---

## 2. System Guarantees vs Non-Guarantees

### What IS Guaranteed:
- **Zero Silent Loss**: Every sample received is persisted to disk unchanged. Every missing sample is recorded in the loss ledger with an honest reason.
- **Strict Idempotence**: Samples are keyed by `(session_id, sensor, index)`. Duplicate arrivals produce zero duplicate writes on device and cloud.
- **Torn Tail Recovery**: If power is cut mid-write, startup scans segments, verifies frame lengths and CRC-32 checksums, truncates partial records, and resumes seamlessly.
- **Cloud Completeness**: When network restores after outages up to 2 hours, the catch-up engine synchronizes all un-acked samples from the WAL within 15 minutes.
- **Deterministic Processing**: Offline (device) and online (cloud) results use the identical pure function with identical math and 1-decimal-place rounding.

### What is NOT Guaranteed:
- **Mock Internal Drop Prevention**: Samples dropped inside the mock's 10-second buffer before delivery cannot be recovered (they are recorded honestly as losses).
- **Infinite Offline Storage**: If physical disk fills completely, the pipeline records honest `disk_full` losses while draining the mock buffer to preserve pipeline honesty.

---

## 3. Maximum Data Loss on Power Cut

> **Declared Maximum Loss on Sudden Power Cut: $\le 1.0$ Second of Data.**

### Rationale and Architectural Proof:
1. The mock delivers samples in batches of at most 1 second (400 samples).
2. The pipeline writes incoming accepted samples to the WAL and immediately executes `os.fsync(fd)` before acknowledging or reading the next batch.
3. The Operating System and disk controller commit all buffered dirty pages to persistent storage during `fsync`.
4. Therefore, at the precise moment power is cut, at most the single in-flight batch currently being read from the network socket (at most 1.0 second / 400 samples) can be lost.
5. All previously written batches have passed `fsync` and are fully preserved on disk.
6. Upon reboot, any partial write is detected by CRC mismatch or incomplete frame length and truncated cleanly, leaving all preceding batches 100% intact.
