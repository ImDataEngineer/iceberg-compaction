"""Seed `default.logs_events` with the small-files problem the learner will fix.

Run from the project root (after `python -m fixtures.generate_fixtures`):

    python -m fixtures.seed_iceberg

What this does, in order:

1. Drops `default.logs_events` if it exists. We want a clean baseline every
   time the seeder runs — idempotent and safe to re-run from the devcontainer
   on each rebuild.

2. Creates `default.logs_events` with the schema from contracts/logs_events.json.
   No partition spec on purpose (see the contract's `notes` field).

3. Attaches every Parquet file under `fixtures/parquet/batch_*.parquet` to the
   table in ONE Iceberg commit via `Table.add_files([...paths...])`.

   Why `add_files` and not 600 separate `table.append()` calls:
   - 600 round-trips to the REST catalog over docker-compose loopback takes
     several minutes and is flaky in CI containers.
   - The shape we want — one tiny data file per micro-batch — is achievable
     in a single commit because `add_files` registers existing Parquet
     files as data files without rewriting them.
   - The CI rubric counts the resulting data files, not the number of
     snapshots. From the table's point of view post-seed, the file layout
     is identical to "600 micro-batches each committed separately" (which
     is what the README scenario describes); only the snapshot history
     differs.

4. Prints a recap: file count attached, total bytes, average file size. This
   IS the diagnostic the learner reads before deciding how to compact.

The seeder is intentionally NOT something the learner touches. They consume
its output (a broken table) and produce src/compact.py to fix it.
"""

from __future__ import annotations

from pathlib import Path

from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from src.catalog import TABLE_IDENTIFIER, ensure_namespace, get_catalog

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = PROJECT_ROOT / "fixtures" / "parquet"


# The schema mirrors contracts/logs_events.json. Field ids are stable and
# match the Parquet column names so `add_files` can attach without coercion.
LOGS_EVENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="tenant_id", field_type=IntegerType(), required=True),
    NestedField(field_id=3, name="event_time", field_type=TimestampType(), required=True),
    NestedField(field_id=4, name="event_type", field_type=StringType(), required=True),
    NestedField(field_id=5, name="payload", field_type=StringType(), required=True),
)


def _list_batch_files() -> list[str]:
    """Return absolute paths of every batch Parquet, sorted by batch index."""
    files = sorted(PARQUET_DIR.glob("batch_*.parquet"))
    if not files:
        raise SystemExit(
            "No micro-batch fixtures found under fixtures/parquet/. "
            "Run `python -m fixtures.generate_fixtures` first."
        )
    return [str(p.resolve()) for p in files]


def _absolute_paths_to_s3a_urls(local_paths: list[str]) -> list[str]:
    """`add_files` requires the URIs PyIceberg's FileIO can read. Since the
    table's FileIO is configured against MinIO (s3://), but the local Parquet
    files we generated live on the host filesystem, we need to UPLOAD them
    into MinIO first and then pass back the s3:// URIs.

    This helper performs the upload via the same s3fs that PyIceberg uses
    under the hood, so we stay on a single S3 client config and don't
    introduce yet another auth dance.
    """
    import s3fs

    fs = s3fs.S3FileSystem(
        endpoint_url="http://localhost:9000",
        key="admin",
        secret="password",
        client_kwargs={"region_name": "us-east-1"},
    )

    s3_urls: list[str] = []
    for local in local_paths:
        name = Path(local).name
        # We upload under a dedicated prefix so the learner can poke at the
        # raw Parquet from the MinIO console if curious.
        s3_path = f"warehouse/logs_events_seed/{name}"
        fs.put_file(local, s3_path)
        s3_urls.append(f"s3://{s3_path}")
    return s3_urls


def seed() -> None:
    catalog = get_catalog()
    ensure_namespace(catalog)

    # 1. Wipe any previous attempt — idempotent reseed.
    try:
        catalog.drop_table(TABLE_IDENTIFIER)
        print(f"dropped existing {TABLE_IDENTIFIER}")
    except Exception:  # noqa: BLE001
        # First seed of this lifecycle. Nothing to drop.
        pass

    # 2. Create the table with no partition spec, no sort order (the learner
    #    will install a sort order during compaction).
    table = catalog.create_table(
        identifier=TABLE_IDENTIFIER,
        schema=LOGS_EVENTS_SCHEMA,
    )
    print(f"created {TABLE_IDENTIFIER}")

    # 3. Upload all batch Parquet files into MinIO so the catalog can see them.
    local_files = _list_batch_files()
    print(f"uploading {len(local_files)} micro-batch files to MinIO...")
    s3_urls = _absolute_paths_to_s3a_urls(local_files)

    # 4. ONE commit, N data files attached. Post-seed the table holds exactly
    #    len(s3_urls) tiny data files — the broken state the project is built
    #    around.
    print(f"attaching {len(s3_urls)} files to the table in one commit...")
    table.add_files(file_paths=s3_urls)

    # 5. Read back metadata and print the diagnostic the learner reads first.
    table.refresh()
    files_inspect = table.inspect.files()
    file_count = files_inspect.num_rows
    total_bytes = sum(files_inspect.column("file_size_in_bytes").to_pylist())
    avg = total_bytes // max(1, file_count)

    print()
    print(f"=== seed done — {TABLE_IDENTIFIER} ===")
    print(f"  data files       : {file_count}")
    print(f"  total file bytes : {total_bytes}")
    print(f"  avg file size    : {avg} bytes")
    print()
    print("Your queries are going to crawl on this layout. That's the point.")
    print("Implement src/compact.py and run `pytest tests/ -v`.")


def main() -> None:
    seed()


if __name__ == "__main__":
    main()
