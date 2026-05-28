"""Measurement helpers for the compaction project.

These functions are PROVIDED by IAmDataEng — they are NOT the work the learner
must produce. They let you (and the CI rubric) inspect the state of the
Iceberg table at a point in time:

  - file_count(table)        : how many data files exist right now
  - total_file_bytes(table)  : sum of file_size_in_bytes across data files
  - bytes_scanned_for_query(table, predicate): how many bytes would a reader
                               touch to answer this predicate, given the
                               current file layout? Iceberg lets us compute
                               this from manifest stats WITHOUT actually
                               running a query, which makes the measurement
                               deterministic across machines (no CPU clock
                               variance, no warm/cold cache surprises).
  - lower_upper_bounds_for_column(table, column_name): per-file (lower, upper)
                               bounds for a given column, used by the rubric
                               to assert that post-compaction files don't
                               overlap on tenant_id (i.e. sort order applied).

Why we measure bytes, not query time:
  Wall-clock query latency on a learner's CI runner is noisy — caches warm
  up between runs, the runner kernel changes, DuckDB does its own I/O
  buffering. A SENIOR rubric cannot fail learners on flake. So we measure
  what Iceberg can tell us deterministically: the number of bytes a reader
  with predicate pushdown would have to crack open. If that number drops
  meaningfully post-compaction, the speedup is real — the only thing that
  can hide it is a slower storage layer, which is out of scope.

The bytes_scanned helper relies on Iceberg's per-file column-level statistics
(`lower_bounds` / `upper_bounds`) which the writer (PyIceberg via add_files)
populates from the Parquet column statistics. That's why the seed step
generates Parquet with column stats enabled (the default).
"""

from __future__ import annotations

from typing import Any

from pyiceberg.expressions import BooleanExpression
from pyiceberg.table import Table


def file_count(table: Table, content_type: int = 0) -> int:
    """Number of data files in the current snapshot of `table`.

    content_type == 0 → data files (we want these)
    content_type == 1 → position deletes
    content_type == 2 → equality deletes
    """
    table.refresh()
    files = table.inspect.files()
    if "content" not in files.column_names:
        # Older Iceberg metadata schemas; assume all are data files.
        return files.num_rows
    contents = files.column("content").to_pylist()
    return sum(1 for c in contents if c == content_type)


def total_file_bytes(table: Table, content_type: int = 0) -> int:
    """Sum of `file_size_in_bytes` across data files in the current snapshot."""
    table.refresh()
    files = table.inspect.files()
    sizes = files.column("file_size_in_bytes").to_pylist()
    if "content" in files.column_names:
        contents = files.column("content").to_pylist()
        return sum(s for s, c in zip(sizes, contents) if c == content_type)
    return sum(sizes)


def bytes_scanned_for_query(table: Table, row_filter: BooleanExpression | str | None = None) -> int:
    """Bytes a reader would touch to answer this row_filter, given current layout.

    Computed from the planned file scan — Iceberg uses each file's
    lower/upper bounds to prune files that cannot match the predicate. The
    returned number is `sum(file_size_in_bytes)` over the files NOT pruned.

    This is the central post-compaction metric: a well-sorted, well-sized
    layout should let MORE files be pruned for a tenant-scoped query,
    dropping bytes_scanned by 1-2 orders of magnitude even with the same
    underlying data.
    """
    table.refresh()
    scan = table.scan(row_filter=row_filter) if row_filter is not None else table.scan()
    total = 0
    # plan_files() returns FileScanTask objects, one per data file the scan
    # actually needs to open. Their `.file.file_size_in_bytes` is the on-disk
    # size that would be physically read (modulo column pruning, which
    # Iceberg does separately at the Parquet layer — we are conservative
    # here and count the whole file, same as Trino's `bytes_scanned` metric).
    for task in scan.plan_files():
        total += task.file.file_size_in_bytes
    return total


def lower_upper_bounds_for_column(table: Table, column_name: str) -> list[tuple[Any, Any]]:
    """Return [(lower, upper), ...] across data files for `column_name`.

    Used by the sort-order check: after a sort-aware rewrite_data_files on
    `tenant_id`, the (lower, upper) tuples across files should be largely
    non-overlapping — a strong, deterministic signal that the sort was
    actually applied (vs the post-merge bins that would overlap if the
    rewrite just concatenated by arrival order).

    Returns an empty list if Iceberg metadata didn't record bounds (e.g. an
    older Parquet writer skipped column stats).
    """
    table.refresh()
    files = table.inspect.files()

    schema = table.schema()
    field = schema.find_field(column_name)
    if field is None:
        raise KeyError(f"column `{column_name}` not in table schema")
    field_id = field.field_id

    if "lower_bounds" not in files.column_names or "upper_bounds" not in files.column_names:
        return []

    lowers = files.column("lower_bounds").to_pylist()
    uppers = files.column("upper_bounds").to_pylist()
    contents = files.column("content").to_pylist() if "content" in files.column_names else [0] * len(lowers)

    # lower_bounds / upper_bounds in PyIceberg's inspect output are dicts
    # {field_id: bytes_repr}. We decode the int32/int64 ones here — that's
    # all we need for tenant_id (int) and event_id (long).
    out: list[tuple[Any, Any]] = []
    for low, up, content in zip(lowers, uppers, contents):
        if content != 0:
            continue
        if low is None or up is None:
            continue
        low_v = _decode_int_bound(low.get(field_id))
        up_v = _decode_int_bound(up.get(field_id))
        if low_v is None or up_v is None:
            continue
        out.append((low_v, up_v))
    return out


def _decode_int_bound(b: bytes | None) -> int | None:
    """Decode a little-endian int (4 or 8 bytes) from Iceberg's bound bytes.

    Iceberg stores per-column bounds as little-endian byte strings (per the
    spec). For int32 / int64 columns the payload is 4 or 8 bytes respectively.
    Strings would need a different decoder; we only need ints for the sort
    check on tenant_id.
    """
    if b is None:
        return None
    if not isinstance(b, (bytes, bytearray)):
        # PyIceberg ≥ 0.7 already decodes some scalar bounds; pass through.
        try:
            return int(b)
        except (TypeError, ValueError):
            return None
    if len(b) == 4:
        return int.from_bytes(b, "little", signed=True)
    if len(b) == 8:
        return int.from_bytes(b, "little", signed=True)
    return None
