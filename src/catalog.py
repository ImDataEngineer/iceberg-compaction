"""Shared helper — connect to the local Iceberg REST catalog backed by MinIO.

Same pattern as iceberg-partitioning: one connection helper, hardcoded local
endpoints, no secrets to leak (local-only stack), no env vars to misconfigure.

Centralising the connection means a single endpoint definition that stays in
sync between the devcontainer, the CI workflow, the fixtures seeder, the
learner code in src/, and the test rubric.
"""

from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog

CATALOG_NAME = "local"
NAMESPACE = "default"
TABLE_NAME = "logs_events"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

REST_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
WAREHOUSE = "s3://warehouse/"


def get_catalog() -> RestCatalog:
    """Return a PyIceberg `RestCatalog` configured against the local stack."""
    return load_catalog(
        CATALOG_NAME,
        **{
            "type": "rest",
            "uri": REST_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.path-style-access": "true",
            "warehouse": WAREHOUSE,
        },
    )


def ensure_namespace(catalog: RestCatalog) -> None:
    """Create the `default` namespace if it doesn't exist. Idempotent."""
    existing = {ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()}
    if NAMESPACE not in existing:
        catalog.create_namespace(NAMESPACE)
