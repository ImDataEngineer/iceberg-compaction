"""Concurrent writer harness — for the snapshot-isolation check.

This module is NOT the work the learner must produce. It is the harness used
by the rubric to assert that a concurrent append, committed AFTER the
learner's compaction reads the snapshot but BEFORE it commits, is not lost
when the compaction commits.

Why this exists, in production terms:
  Iceberg uses optimistic concurrency control. When rewrite_data_files
  commits, the catalog checks the table's current snapshot id against the
  one the rewrite started from. If a concurrent writer appended in between,
  the catalog tells rewrite to retry (or merge), preserving the concurrent
  append. A rewrite that *blindly overwrites* the table with its computed
  result — instead of using the rewrite_data_files action which understands
  this conflict resolution — would lose the concurrent rows.

The check that consumes this harness:
  tests/test_evaluate.py::test_snapshot_isolation_during_rewrite

How the test orchestrates the race deterministically:
  We can't reliably interleave a compaction and an append on a wall clock
  in CI. Instead we do this:

    1. Run the learner's compact_table() to completion → table is compacted.
       Record snapshot id S_compacted.
    2. Call append_canary_batch() → appends a small "canary" batch as a
       brand-new snapshot S_canary on top of S_compacted.
    3. Replay a SECOND compaction (still via the learner's compact_table()).
       The rewrite must NOT lose the canary rows.
    4. Assert: post-second-compaction, the canary rows are still visible.

  This sidesteps the timing flakiness while exercising the exact failure
  mode (a compaction that doesn't respect concurrent commits). The semantics
  are: "your compaction is safe to interleave with concurrent appends" — and
  if it isn't, the canary disappears on the second pass.

The canary batch is small (50 rows), with event_id values in a range
disjoint from the seeded fixtures (10^9 + offset), so its presence/absence
is checked with a single filtered scan.
"""

from __future__ import annotations

import pyarrow as pa

from src.catalog import TABLE_IDENTIFIER, get_catalog

# event_id values for canary rows live in a range disjoint from the seed
# (seed uses event_id < SEED_FILES * ROWS_PER_BATCH, default 18 000).
CANARY_BASE_EVENT_ID = 1_000_000_000
CANARY_ROW_COUNT = 50
CANARY_TENANT_ID = 1  # falls into the reference-query tenant on purpose


def append_canary_batch() -> int:
    """Append CANARY_ROW_COUNT rows to default.logs_events as a new snapshot.

    Returns the snapshot id of the resulting commit. The rows are designed
    to be uniquely identifiable by `event_id >= CANARY_BASE_EVENT_ID`.
    """
    from datetime import datetime, timedelta

    catalog = get_catalog()
    table = catalog.load_table(TABLE_IDENTIFIER)

    base_ts = datetime(2026, 1, 1, 13, 0, 0)
    arrow = pa.table(
        {
            "event_id": pa.array(
                [CANARY_BASE_EVENT_ID + i for i in range(CANARY_ROW_COUNT)],
                type=pa.int64(),
            ),
            "tenant_id": pa.array(
                [CANARY_TENANT_ID] * CANARY_ROW_COUNT,
                type=pa.int32(),
            ),
            "event_time": pa.array(
                [base_ts + timedelta(microseconds=i) for i in range(CANARY_ROW_COUNT)],
                type=pa.timestamp("us"),
            ),
            "event_type": pa.array(["canary"] * CANARY_ROW_COUNT, type=pa.string()),
            "payload": pa.array(
                [f"canary-row-{i:04d}" for i in range(CANARY_ROW_COUNT)],
                type=pa.string(),
            ),
        }
    )

    table.append(arrow)
    table.refresh()
    return table.current_snapshot().snapshot_id


def count_canary_rows() -> int:
    """How many canary rows are currently visible in the table."""
    from pyiceberg.expressions import GreaterThanOrEqual

    catalog = get_catalog()
    table = catalog.load_table(TABLE_IDENTIFIER)
    table.refresh()

    scan = table.scan(row_filter=GreaterThanOrEqual("event_id", CANARY_BASE_EVENT_ID))
    return scan.to_arrow().num_rows


def main() -> None:
    snap = append_canary_batch()
    print(f"appended {CANARY_ROW_COUNT} canary rows; snapshot id = {snap}")
    print(f"visible canary rows: {count_canary_rows()}")


if __name__ == "__main__":
    main()


__all__ = [
    "CANARY_BASE_EVENT_ID",
    "CANARY_ROW_COUNT",
    "CANARY_TENANT_ID",
    "append_canary_batch",
    "count_canary_rows",
]
