# Small files: every data engineer's fight — `storage.iceberg-compaction`

> **Level**: senior · **Estimated time**: ~10 h · **Paid IAmDataEng project (€49)**
> **Framework axes**: `storage`, `software_engineering_dataops`
> **Prerequisites**: you've already done `storage.partitioned-lakehouse` (or equivalent).
> You know what an Iceberg snapshot, a manifest, a partition spec is.

This is a table-operations project — not an initialization project. You
take an Iceberg table broken by its own writes (600 micro-batches of ~30
rows each, i.e. ~600 data files of a few KB each) and you bring its
queries back to their target latency.

It's not a feature — it's a chore every senior eventually has to
master. The rubric measures it cleanly.

---

## The context

A log analytics platform ingests ~10k events per micro-batch, 5
micro-batches per minute, written straight into an unpartitioned Iceberg
table `default.logs_events`. After an hour you've got **600 Parquet
files of ~30 KB each**. Your Trino dashboards — which always scope by
`tenant_id` — take **8 seconds** to scan what should take **200 ms**.

Cause: the reader's planning cost (reading manifests + opening files)
dominates when each data file weighs less than its Parquet footer.
Iceberg can prune NO file because each file holds a bit of every
tenant. The fact table is small; the metadata and round-trip cost are
big.

Your job:

1. **Diagnose** the initial state (file count, average size, bytes
   scanned for the reference query).
2. **Install a sort order** on `(tenant_id, event_time)` — that's what
   makes post-compaction pruning efficient.
3. **Compact** via `rewrite_data_files(target_file_size_bytes=128 MB, ...)`
   while respecting Iceberg's optimistic concurrency (a concurrent
   writer must not lose its rows).
4. **Measure** the gain: bytes scanned / 5 minimum, file count / 30
   minimum, row count strictly preserved.

You DON'T need to touch the seeding — it's shipped (`fixtures/seed_iceberg.py`).
Essentially, you write `src/compact.py`.

---

## What you ship

| Deliverable | Where |
|---|---|
| Compaction + sort order | `src/compact.py` (read the header: 6 explicit steps) |
| Results bench | `bench/results.json` (written by your `compact.py`) |
| Explanatory note | `notebooks/explain.md` (≤ 250 words, template provided) |
| Local stack | `docker-compose.yml` (provided — don't touch) |
| Deterministic fixtures | `fixtures/generate_fixtures.py` + `fixtures/seed_iceberg.py` (provided) |
| Measurement helpers | `src/measure.py` (provided — use it, don't rewrite it) |

The rubric reads `default.logs_events` through the REST catalog after
your compact. It does NOT read `bench/results.json` — that's a portfolio
deliverable, not a CI gate.

---

## Getting started

If you're in GitHub Codespaces (one-click open from the IAmDataEng app),
everything is ready:

- MinIO + Iceberg REST are running (`docker compose ps` should show 2
  services).
- The 600 Parquet fixtures are generated under `fixtures/parquet/`.
- The `default.logs_events` table is seeded and **already broken** —
  that's your starting point.

Check the initial state by hand:

```python
from src.catalog import TABLE_IDENTIFIER, get_catalog
from src.measure import file_count, total_file_bytes, bytes_scanned_for_query
from pyiceberg.expressions import EqualTo

t = get_catalog().load_table(TABLE_IDENTIFIER)
print(f"files   : {file_count(t)}")
print(f"bytes   : {total_file_bytes(t)}")
print(f"scanned : {bytes_scanned_for_query(t, EqualTo('tenant_id', 1))}")
```

You should see ~600 files, ~5-15 MB total, and a `scanned` close to the
total (because without a sort order, NO file can be pruned for tenant
1).

Locally (outside Codespaces):

```bash
docker compose up -d
pip install -r requirements.txt
python -m fixtures.generate_fixtures
python -m fixtures.seed_iceberg
# Implement src/compact.py — it raises NotImplementedError while empty.
pytest tests/ -v
```

Once the 6 checks pass locally, **commit + push** to your fork. GitHub
Actions CI replays the rubric (re-seed + your compact.py + tests). The
verdict appears in your IAmDataEng dashboard.

---

## The 6 rubric checks

Defined in `tests/test_evaluate.py`. All deterministic — they rely on
Iceberg metadata, not on wall-clock timing.

| # | Id | What we check |
|---|---|---|
| 1 | `pre_compaction_baseline` | The seeded table has **> 100 files** of **< 1 MB** each. Without that, the project measures nothing. If this fails, it's a seed/fixture problem. |
| 2 | `compaction_reduces_file_count` | Post-compaction, **< 20 files** remain. On this volume, the ideal target is ≤ 5 (a single big file is enough). |
| 3 | `compaction_preserves_data` | `COUNT(*)` before == `COUNT(*)` after. No loss. No duplication. This is the "your compaction didn't silently explode" check. |
| 4 | `bytes_scanned_decreased` | For the reference query (`tenant_id = 1`), you must scan **≤ 30%** of the bytes you scanned before. This is the metric that proves the sort order + the rewrite made pruning effective. |
| 5 | `sort_order_applied` | Post-compaction files have `lower_bounds` / `upper_bounds` on `tenant_id` that do not overlap **significantly** (≤ 20% overlapping pairs). That's what ENABLES the pruning in check 4. |
| 6 | `snapshot_isolation_during_rewrite` | A concurrent append (50 "canary" rows from another process) is **NOT** lost if you re-run the compaction over it. This is the check that separates a senior from a junior: `rewrite_data_files` handles optimistic concurrency; a DIY `overwrite(table.scan())` does not. |

---

## The senior-grade traps

Seen in production code review, not in training:

- **Installing the sort order AFTER the rewrite.**
  The sort order applies to FUTURE writes. Your compacted files have
  already been written in arrival order — you missed the boat and the
  per-file bounds still overlap. Both `bytes_scanned_decreased` and
  `sort_order_applied` fail.

- **`table.scan().to_arrow()` then `table.overwrite(compacted_arrow)`.**
  Technically compacts. "Works" on a simple test. But it breaks
  isolation: if a writer appends between your scan and your overwrite,
  you wipe its rows. Check 6 catches that. The correct API is
  `rewrite_data_files()`, which rewrites at the DATA FILE level, not the
  row level, and handles the retry/merge through optimistic concurrency.

- **target_file_size_bytes = 1 GB on 10 MB of data.**
  You get 1 file. No reader parallelism. The rubric doesn't punish you
  here (see the note in `notebooks/explain.md` §5), but in production
  this is an anti-pattern: you sacrifice Trino throughput to save one
  manifest entry. 128 MB is the industry default.

- **Compacting hot without `EXPIRE SNAPSHOTS` afterwards.**
  Out of CI scope here, but worth a mention in `notebooks/explain.md`
  §2: after your rewrite, the old layout (the 600 small files) is still
  on disk inside the old snapshot. You have not reclaimed the space
  until you've expired the historical snapshots. Quantifying this cost
  is part of the job.

- **Forgetting that `rewrite_data_files` reads BEFORE it writes.**
  You'll re-read the whole table (the 600 files) to recompact them.
  The compute cost is NOT free. On a real multi-TB table, you shard
  your job by partition (or by range) — not applicable here, the table
  is intentionally unpartitioned, but worth knowing.

---

## The local stack

Same as the `storage.partitioned-lakehouse` project. If you walk out of
this one without remembering the endpoints, go back there.

- **MinIO** (`localhost:9000`, console `localhost:9001`, creds
  `admin / password`) — S3-compatible.
- **Iceberg REST catalog** (`localhost:8181`, image
  `tabulario/iceberg-rest`).
- Warehouse `s3://warehouse/`, namespace `default`, table
  `default.logs_events`.

All names are centralized in `src/catalog.py` — don't touch.

Sizing knobs (useful if you want to push the local setup):

```bash
# Crank it up to 3000 files of 30 rows like the spec mentions:
IAMDATAENG_SEED_FILES=3000 python -m fixtures.generate_fixtures
python -m fixtures.seed_iceberg
```

The rubric stays valid as long as `IAMDATAENG_SEED_FILES > 100`. CI uses
the default (600) to stay under 2 minutes per run.

---

## Going further (references)

No reading is mandatory, but if you want these patterns in context:

- Joe Reis & Matt Housley, *Fundamentals of Data Engineering* (O'Reilly,
  2022) — **ch. 6 "Storage", pp. 218-228** — compaction, maintenance,
  small-files problem.
- Apache Iceberg spec, [Maintenance section](https://iceberg.apache.org/docs/latest/maintenance/)
  and [PyIceberg rewrite-files](https://py.iceberg.apache.org/api/#rewrite-files).
- Martin Kleppmann, *Designing Data-Intensive Applications* (O'Reilly,
  2017) — **ch. 3 on LSM trees** — same compaction problem, at another
  layer of the stack.
- Trino docs, [Iceberg connector — file size tuning](https://trino.io/docs/current/connector/iceberg.html#table-properties)
  — to tie the 128 MB target back to what the reader actually sees in
  production.

---

## If you're stuck

The point is for you to struggle a bit — that's what operating an
Iceberg table in production feels like. But if you've been spinning on
the same check for more than an hour:

1. Re-read the error message — it almost always points at the cause.
2. Inspect the table by hand:

   ```python
   from src.catalog import get_catalog, TABLE_IDENTIFIER
   t = get_catalog().load_table(TABLE_IDENTIFIER)
   print(t.inspect.files().to_pandas())   # data files list + sizes + bounds
   print(t.inspect.snapshots().to_pandas()) # commit history
   print(t.sort_order())                   # current sort order
   ```

3. Verify the compose: `docker compose ps`, `docker compose logs
   iceberg-rest --tail=50`.
4. Open an issue on your fork with the `help-wanted` label.

Good luck. When you come back in 2 years to compact a real production
table on a Sunday evening, you'll remember this project.
