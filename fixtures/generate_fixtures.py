"""Deterministic micro-batch Parquet fixture generator for the logs_events dataset.

Run from the project root:

    python -m fixtures.generate_fixtures

Output: many small Parquet files under `fixtures/parquet/`:
    fixtures/parquet/batch_0000.parquet
    fixtures/parquet/batch_0001.parquet
    ...
    fixtures/parquet/batch_NNNN.parquet

Each file holds a single micro-batch (~30 rows by default → ~1-3 KB on disk
once snappy-compressed; closer to ~30 KB in CI because the payload column
inflates with the dataset size knob). They are NOT meant to be queried
directly — they are the raw material the seeder will `add_files()`-attach to
the Iceberg table as the "small-files mess" the learner must compact away.

Why one Parquet per micro-batch (and not one big file we then split):
- It mirrors the real production pattern: a streaming writer flushing every
  N seconds produces one Parquet per flush.
- It lets the Iceberg seeder do a single `add_files([...all paths...])` call
  and get exactly N data files in the table — one per source Parquet — which
  is exactly the file count the pre-compaction baseline check asserts on.

Sizing knobs (override via env vars; defaults tuned for CI < 90 s):
    IAMDATAENG_SEED_FILES   : number of micro-batch files. Default 600.
    IAMDATAENG_SEED_ROWS    : rows per micro-batch.        Default 30.

The CI rubric counts files dynamically against `IAMDATAENG_SEED_FILES`, so a
learner who wants to push to 3000 files locally can do so without breaking
the test. The baseline threshold (`pre_compaction_baseline`) requires > 100
files in the table to ensure there's actually a problem to solve.

Determinism:
- One `random.Random(SEED + batch_idx)` per micro-batch.
- pyarrow writes Parquet with stable defaults; same input → byte-stable output.
- 10 tenants with a skewed distribution (tenant 1 gets ~40% of traffic) so
  that the sort-order check has something concrete to assert on.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42

# Number of micro-batch Parquet files to generate. Tuned so the seed step
# stays under ~30 s in CI and the baseline pre-compaction check (>100 files)
# is satisfied with margin. Override with IAMDATAENG_SEED_FILES locally.
SEED_FILES = int(os.environ.get("IAMDATAENG_SEED_FILES", "600"))

# Rows per micro-batch. ~30 rows × ~50 bytes payload ≈ 1.5 KB/row → ~3 KB
# per file after snappy. Bumping rows up produces fatter files; the project
# is built around the file-count problem, not the per-file weight.
ROWS_PER_BATCH = int(os.environ.get("IAMDATAENG_SEED_ROWS", "30"))

# Window the events live in: 1 hour, sliced across all micro-batches. Each
# micro-batch covers ~ (3600 / SEED_FILES) seconds of wall-time.
WINDOW_START = datetime(2026, 1, 1, 12, 0, 0)
WINDOW_SECONDS = 3600

# 10 tenants with skewed traffic: tenant 1 is the noisy neighbour.
# Probability weights sum to 1.0. This skew matters for the sort-order check:
# after sort-by-(tenant_id, event_time), files at the start of the layout
# should hold ONLY tenant 1 rows, and lower_bounds/upper_bounds shouldn't
# overlap across the rewritten files.
TENANT_WEIGHTS = [
    (1, 0.40),  # noisy neighbour
    (2, 0.15),
    (3, 0.10),
    (4, 0.08),
    (5, 0.07),
    (6, 0.05),
    (7, 0.05),
    (8, 0.04),
    (9, 0.03),
    (10, 0.03),
]
TENANTS = [t for t, _ in TENANT_WEIGHTS]
TENANT_PROBS = [w for _, w in TENANT_WEIGHTS]

EVENT_TYPES = ["page_view", "page_view", "page_view", "click", "click", "purchase", "error", "signup"]

FIXTURES_DIR = Path(__file__).resolve().parent / "parquet"


def _gen_batch(batch_idx: int) -> pa.Table:
    """Build one micro-batch of synthetic log events as an Arrow table.

    Deterministic per batch_idx via `Random(SEED + batch_idx)`.

    event_id is densely packed and globally unique:
        event_id = batch_idx * ROWS_PER_BATCH + row_offset
    so the total row count is exactly SEED_FILES * ROWS_PER_BATCH.

    event_time is within the slice
        [WINDOW_START + offset_for_this_batch, +slice_seconds)
    so micro-batches arrive in chronological order (matches the streaming
    micro-batch scenario in the README).
    """
    rng = random.Random(SEED + batch_idx)

    slice_seconds = max(1, WINDOW_SECONDS // SEED_FILES)
    batch_start = WINDOW_START + timedelta(seconds=batch_idx * slice_seconds)

    event_ids: list[int] = []
    tenant_ids: list[int] = []
    event_times: list[datetime] = []
    event_types: list[str] = []
    payloads: list[str] = []

    for row in range(ROWS_PER_BATCH):
        event_ids.append(batch_idx * ROWS_PER_BATCH + row)
        tenant_ids.append(rng.choices(TENANTS, weights=TENANT_PROBS, k=1)[0])
        event_times.append(batch_start + timedelta(microseconds=rng.randint(0, slice_seconds * 1_000_000 - 1)))
        event_types.append(rng.choice(EVENT_TYPES))
        # A short but non-trivial payload string — gives each row real weight
        # so the bytes-scanned metric meaningfully drops post-compaction.
        payloads.append(f"req={rng.randint(10_000_000, 99_999_999)};ua={rng.choice(['firefox', 'chrome', 'safari', 'edge'])};dur_ms={rng.randint(1, 5000)}")

    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int64()),
            "tenant_id": pa.array(tenant_ids, type=pa.int32()),
            "event_time": pa.array(event_times, type=pa.timestamp("us")),
            "event_type": pa.array(event_types, type=pa.string()),
            "payload": pa.array(payloads, type=pa.string()),
        }
    )


def generate() -> list[Path]:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for batch_idx in range(SEED_FILES):
        table = _gen_batch(batch_idx)
        # Zero-padded so file ordering on disk matches batch order.
        path = FIXTURES_DIR / f"batch_{batch_idx:04d}.parquet"
        # Single row-group, snappy: stable Parquet output. Small row_group_size
        # is fine — each file holds only ROWS_PER_BATCH rows total anyway.
        pq.write_table(table, path, compression="snappy", row_group_size=ROWS_PER_BATCH)
        written.append(path)
    return written


def main() -> None:
    paths = generate()
    total_bytes = sum(p.stat().st_size for p in paths)
    print(f"wrote {len(paths)} micro-batch Parquet files under {FIXTURES_DIR}")
    print(f"  rows/file        : {ROWS_PER_BATCH}")
    print(f"  total rows       : {len(paths) * ROWS_PER_BATCH}")
    print(f"  total disk bytes : {total_bytes}")
    print(f"  avg file size    : {total_bytes // max(1, len(paths))} bytes")


if __name__ == "__main__":
    main()
