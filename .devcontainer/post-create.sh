#!/usr/bin/env bash
# IAmDataEng — first-boot bootstrap for `storage.iceberg-compaction`.
#
# 1. Install Python deps
# 2. Pull + start MinIO and the Iceberg REST catalog (docker compose)
# 3. Wait for both health checks to go green
# 4. Generate the deterministic micro-batch Parquet fixtures
# 5. Seed the `default.logs_events` Iceberg table with the small-files mess
#
# Step 5 is the expensive part (~600 file additions in one Iceberg commit by
# default; configurable via IAMDATAENG_SEED_FILES). It is the test scaffold
# that produces the "small files problem" the learner must then solve via
# compaction. We do it once at devcontainer boot so the learner is staring at
# the broken table from second one.
#
# Designed to be re-runnable: docker compose is idempotent and the seed
# scripts re-create the table from scratch via drop_table + create_table.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "[1/5] Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "[2/5] Starting the local lakehouse stack (MinIO + Iceberg REST)..."
docker compose up -d

echo "[3/5] Waiting for services to be healthy..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 \
     && curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1; then
    echo "  services ready (after ${i}s)"
    break
  fi
  if [ "$i" = "60" ]; then
    echo "  ERROR: services did not become healthy within 60s."
    docker compose logs --tail=50
    exit 1
  fi
  sleep 1
done

echo "[4/5] Generating deterministic micro-batch Parquet fixtures..."
python -m fixtures.generate_fixtures

echo "[5/5] Seeding default.logs_events Iceberg table with the small-files mess..."
python -m fixtures.seed_iceberg

echo
echo "Setup done. The table default.logs_events now has hundreds of tiny files —"
echo "that's the problem you're here to solve. Next steps:"
echo "  1. Read README.fr.md"
echo "  2. Implement src/compact.py"
echo "  3. Run: pytest tests/ -v"
