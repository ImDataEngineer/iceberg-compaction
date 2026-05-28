"""IAmDataEng — rubric d'évaluation pour `storage.iceberg-compaction` (senior).

Six checks déterministes alignés sur la spec projet :

  1. pre_compaction_baseline           — la table seedée a > 100 fichiers (< 1 MB).
  2. compaction_reduces_file_count     — après rewrite, < 20 fichiers.
  3. compaction_preserves_data         — COUNT(*) avant == COUNT(*) après.
  4. bytes_scanned_decreased           — la requête de référence scanne ≤ 30%
                                          des bytes d'avant.
  5. sort_order_applied                — les fichiers post-compaction ne se
                                          chevauchent pas sur tenant_id.
  6. snapshot_isolation_during_rewrite — un append concurrent (« canary »)
                                          n'est jamais perdu par la
                                          recompaction.

Tous les checks reposent uniquement sur la métadonnée Iceberg (file counts,
file_size_in_bytes, lower/upper bounds). Aucun timing wall-clock. Aucune mesure
de débit. Aucun appel réseau public. La stack docker-compose locale est la
source de vérité pour les deux environnements (devcontainer + CI).

Ordre des tests :
- pytest exécute par ordre de définition dans le module.
- baseline → compaction → preserves_data → bytes_scanned → sort_order :
  même fixture `compacted_table` (scope="module"), elle lance le seed +
  compact UNE fois et tous les checks dépendant de l'état post-compaction
  partagent ce setup.
- snapshot_isolation : appende une canary puis relance la compaction. Ce
  test modifie irréversiblement l'état, il vient en DERNIER.
"""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

REST_HEALTH_URL = "http://localhost:8181/v1/config"
MINIO_HEALTH_URL = "http://localhost:9000/minio/health/live"

# Seuils déterministes — voir docs/PROJECTS_CATALOG_V1.md §8 pour le contexte.
BASELINE_MIN_FILE_COUNT = 100      # check 1 : faut au moins 100 petits fichiers pour qu'il y ait un problème
BASELINE_MAX_AVG_BYTES = 1_000_000 # check 1 : taille moyenne < 1 MB par fichier
POST_COMPACT_MAX_FILES = 20        # check 2 : seuil senior — la cible idéale est ≤ 5
BYTES_SCANNED_MAX_RATIO = 0.30     # check 4 : on doit scanner ≤ 30% des bytes d'avant
SORT_ORDER_MAX_OVERLAP_RATIO = 0.20  # check 5 : ≤ 20% de paires de fichiers peuvent se chevaucher


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wait_for_url(url: str, timeout_s: int = 60) -> bool:
    """Poll an HTTP endpoint until it returns 2xx or the timeout elapses."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 300:
                    return True
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(1)
    return False


def _run(module: str) -> None:
    """Run `python -m <module>` as a subprocess; fail with full output on error."""
    result = subprocess.run(
        [sys.executable, "-m", module],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"`python -m {module}` a planté.\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}\n"
            "Astuce : ce module lève NotImplementedError tant qu'il n'est pas "
            "implémenté ; lis l'en-tête du fichier pour la marche à suivre."
        )


# ---------------------------------------------------------------------------
# Module-level fixture : (1) stack up, (2) seed, (3) capture baseline metrics,
# (4) compact, (5) capture post-compaction metrics. Tous les checks 1-5
# consomment ce setup.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compacted_table():
    """Drive the full pipeline once and return a metrics bundle for all checks.

    Returns a dict :
        {
          "table": pyiceberg.table.Table,
          "rows_before": int,
          "rows_after": int,
          "files_before": int,
          "files_after": int,
          "bytes_before": int,
          "bytes_after": int,
          "scanned_before": int,
          "scanned_after": int,
          "avg_file_size_before": int,
        }
    """
    if not _wait_for_url(MINIO_HEALTH_URL, timeout_s=30):
        pytest.fail(
            "MinIO n'est pas joignable sur http://localhost:9000. "
            "Vérifie `docker compose ps`. La stack doit être démarrée — le "
            "devcontainer le fait au postCreateCommand ; en CI le job le fait "
            "avant pytest."
        )
    if not _wait_for_url(REST_HEALTH_URL, timeout_s=30):
        pytest.fail(
            "Le catalog Iceberg REST n'est pas joignable sur http://localhost:8181. "
            "Vérifie `docker compose logs iceberg-rest`."
        )

    # Étape 1 — (re)seed la table avec le « small files mess ».
    # Idempotent : drop_table + create_table + add_files.
    _run("fixtures.seed_iceberg")

    # Étape 2 — capture des métriques AVANT compaction.
    from src.catalog import TABLE_IDENTIFIER, get_catalog
    from src.measure import (
        bytes_scanned_for_query,
        file_count,
        total_file_bytes,
    )
    from pyiceberg.expressions import EqualTo

    catalog = get_catalog()
    table = catalog.load_table(TABLE_IDENTIFIER)

    rows_before = table.scan().to_arrow().num_rows
    files_before = file_count(table)
    bytes_before = total_file_bytes(table)
    scanned_before = bytes_scanned_for_query(table, EqualTo("tenant_id", 1))
    avg_before = bytes_before // max(1, files_before)

    if files_before == 0:
        pytest.fail(
            "La table default.logs_events est vide juste après le seed. "
            "Soit le seed a échoué silencieusement, soit `fixtures.seed_iceberg` "
            "n'a pas attaché les fichiers via add_files(). Regarde la sortie "
            "stdout de l'étape précédente."
        )

    # Étape 3 — lance la compaction du learner.
    _run("src.compact")

    # Étape 4 — re-mesure APRÈS compaction.
    table.refresh()
    rows_after = table.scan().to_arrow().num_rows
    files_after = file_count(table)
    bytes_after = total_file_bytes(table)
    scanned_after = bytes_scanned_for_query(table, EqualTo("tenant_id", 1))

    return {
        "table": table,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "files_before": files_before,
        "files_after": files_after,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "scanned_before": scanned_before,
        "scanned_after": scanned_after,
        "avg_file_size_before": avg_before,
    }


# ---------------------------------------------------------------------------
# Check 1 — pre_compaction_baseline
# ---------------------------------------------------------------------------


def test_pre_compaction_baseline(compacted_table):
    """La table seedée doit avoir > 100 fichiers de < 1 MB chacun.

    Sans ça, le projet ne mesure rien : il faut un vrai « small files
    problem » pour que les autres checks aient un sens.
    """
    files_before = compacted_table["files_before"]
    avg_before = compacted_table["avg_file_size_before"]

    if files_before <= BASELINE_MIN_FILE_COUNT:
        pytest.fail(
            f"Baseline insuffisante : seulement {files_before} fichiers data "
            f"avant compaction (seuil minimum : > {BASELINE_MIN_FILE_COUNT}).\n"
            "Pour que le projet mesure quelque chose, il faut une vraie scène "
            "de « petits fichiers ». Régénère les fixtures avec plus de "
            "micro-batches :\n"
            "  IAMDATAENG_SEED_FILES=600 python -m fixtures.generate_fixtures\n"
            "  python -m fixtures.seed_iceberg\n"
            "Si tu vois ce message, c'est probablement que tu as relancé un "
            "compact PUIS un re-seed sans purger correctement — vérifie que "
            "fixtures.seed_iceberg drop bien la table avant de la recréer."
        )

    if avg_before > BASELINE_MAX_AVG_BYTES:
        pytest.fail(
            f"Baseline incorrecte : la taille moyenne des fichiers est de "
            f"{avg_before} bytes (seuil max : {BASELINE_MAX_AVG_BYTES}).\n"
            "Tes fixtures produisent déjà des gros fichiers — il n'y a rien "
            "à compacter. Diminue IAMDATAENG_SEED_ROWS dans la génération."
        )


# ---------------------------------------------------------------------------
# Check 2 — compaction_reduces_file_count
# ---------------------------------------------------------------------------


def test_compaction_reduces_file_count(compacted_table):
    """Après rewrite_data_files, le nombre de fichiers doit chuter sous 20."""
    files_before = compacted_table["files_before"]
    files_after = compacted_table["files_after"]

    if files_after >= files_before:
        pytest.fail(
            f"La compaction n'a rien réduit : {files_before} → {files_after} fichiers.\n"
            "Hypothèses probables :\n"
            "  - tu as oublié de commit la transaction (table.refresh() te montrerait "
            "    l'état d'avant) ;\n"
            "  - tu as appelé une mauvaise stratégie de rewrite_data_files ;\n"
            "  - tu as compacté une autre table que default.logs_events.\n"
            "Vérifie que src.compact.compact_table() appelle bien "
            "`table.rewrite_data_files(...)` et que le commit a lieu."
        )

    if files_after > POST_COMPACT_MAX_FILES:
        pytest.fail(
            f"Compaction sous-dimensionnée : {files_after} fichiers post-compaction "
            f"(seuil senior : ≤ {POST_COMPACT_MAX_FILES}).\n"
            "Causes courantes :\n"
            "  - target_file_size_bytes trop petit (par ex. 1 MB au lieu de 128 MB) ;\n"
            "  - la stratégie « bin-pack » de rewrite a vu tes fichiers comme déjà "
            "    assez gros (vérifie min_input_files / delete_file_threshold dans la doc).\n"
            f"Détail : avant {files_before} fichiers, après {files_after}."
        )


# ---------------------------------------------------------------------------
# Check 3 — compaction_preserves_data
# ---------------------------------------------------------------------------


def test_compaction_preserves_data(compacted_table):
    """Aucune ligne perdue, aucun doublon."""
    rows_before = compacted_table["rows_before"]
    rows_after = compacted_table["rows_after"]

    if rows_after == rows_before:
        return  # OK

    diff = rows_after - rows_before
    if diff < 0:
        hint = (
            f"Tu as perdu {-diff} lignes. Causes courantes :\n"
            "  - tu as fait `table.overwrite(...)` au lieu de `rewrite_data_files(...)`. "
            "    overwrite remplace, rewrite réécrit en place ;\n"
            "  - tu as filtré par mégarde (un row_filter passé à la stratégie de rewrite ?) ;\n"
            "  - tu as compacté pendant qu'un autre process supprimait des lignes."
        )
    else:
        hint = (
            f"Tu as {diff} lignes en trop. Causes courantes :\n"
            "  - tu as appendé le résultat de la compaction (`table.append(compacted)`) "
            "    au lieu de remplacer les data files. C'est exactement le piège du "
            "    « DIY compaction » : rewrite_data_files évite ce doublonnage ;\n"
            "  - tu as relancé src.compact deux fois sans purger entre temps."
        )

    pytest.fail(
        f"Row count incorrect après compaction : avant {rows_before}, après {rows_after}.\n{hint}"
    )


# ---------------------------------------------------------------------------
# Check 4 — bytes_scanned_decreased
# ---------------------------------------------------------------------------


def test_bytes_scanned_decreased(compacted_table):
    """La requête de référence (tenant_id = 1) scanne ≤ 30% des bytes d'avant."""
    scanned_before = compacted_table["scanned_before"]
    scanned_after = compacted_table["scanned_after"]

    if scanned_before == 0:
        pytest.fail(
            "scanned_before == 0 : la mesure de référence n'a rien retourné. "
            "Probable cause : la métadonnée Iceberg n'a pas de file_size_in_bytes "
            "ou la table est vide. Régénère le seed et relance."
        )

    ratio = scanned_after / scanned_before

    if ratio > BYTES_SCANNED_MAX_RATIO:
        pct_after = ratio * 100
        pct_target = BYTES_SCANNED_MAX_RATIO * 100
        pytest.fail(
            f"Gain insuffisant en bytes scannés pour la requête (tenant_id = 1) :\n"
            f"  avant  : {scanned_before:>12} bytes\n"
            f"  après  : {scanned_after:>12} bytes ({pct_after:.1f}% de l'avant)\n"
            f"  cible  : ≤ {pct_target:.0f}% de l'avant.\n"
            "Causes probables :\n"
            "  - tu n'as pas installé de sort order sur tenant_id avant le rewrite, "
            "    donc les bounds par fichier post-compaction couvrent TOUS les tenants — "
            "    Iceberg ne peut prune aucun fichier ;\n"
            "  - tu as installé le sort order après le rewrite — il faut l'installer AVANT.\n"
            "Vérifie l'ordre dans src.compact : (1) replace_sort_order, (2) rewrite_data_files."
        )


# ---------------------------------------------------------------------------
# Check 5 — sort_order_applied
# ---------------------------------------------------------------------------


def test_sort_order_applied(compacted_table):
    """Les fichiers post-compaction ne se chevauchent pas (significativement)
    sur tenant_id.

    Méthode : pour chaque paire de fichiers (i, j) avec i < j, on vérifie que
    les intervalles [lower_i, upper_i] et [lower_j, upper_j] sur tenant_id
    ne se croisent pas. Avec un sort order ASC sur tenant_id appliqué à la
    compaction, on attend ZÉRO chevauchement si on a un seul fichier final
    (la cible nominale ici) ou au pire 1-2 chevauchements aux frontières si
    la stratégie a produit plusieurs fichiers.

    Pourquoi on tolère jusqu'à 20% de paires chevauchantes plutôt que zéro :
    selon la version de PyIceberg, la bin-pack strategy peut produire un
    fichier final qui mélange tenant 1 et tenant 2 aux frontières même
    avec un sort order — la spec Iceberg ne garantit pas l'absence absolue
    de chevauchement, seulement l'ordre intra-fichier. 20% est tolérant
    sans être inutilement permissif.
    """
    from src.measure import lower_upper_bounds_for_column

    table = compacted_table["table"]
    bounds = lower_upper_bounds_for_column(table, "tenant_id")

    if not bounds:
        pytest.fail(
            "Impossible de lire les lower_bounds/upper_bounds pour tenant_id. "
            "Probable cause : le rewrite n'a pas écrit de stats colonnes "
            "Parquet, OU le décodeur d'integer bounds ne reconnaît pas le "
            "format de cette version PyIceberg. Vérifie src.measure._decode_int_bound."
        )

    if len(bounds) == 1:
        # Un seul fichier post-compaction : pas de question de chevauchement.
        # C'est le cas idéal sur ce volume de fixtures.
        return

    total_pairs = 0
    overlapping = 0
    for i in range(len(bounds)):
        lo_i, hi_i = bounds[i]
        for j in range(i + 1, len(bounds)):
            lo_j, hi_j = bounds[j]
            total_pairs += 1
            # Intervals overlap iff max(lo) <= min(hi).
            if max(lo_i, lo_j) <= min(hi_i, hi_j):
                overlapping += 1

    overlap_ratio = overlapping / total_pairs if total_pairs else 0.0

    if overlap_ratio > SORT_ORDER_MAX_OVERLAP_RATIO:
        # Mettre en clair les bornes pour aider le debug.
        pretty = "\n  ".join(f"file[{i}]: [{lo}, {hi}]" for i, (lo, hi) in enumerate(bounds))
        pytest.fail(
            f"Sort order non appliqué : {overlapping}/{total_pairs} paires de "
            f"fichiers se chevauchent sur tenant_id ({overlap_ratio:.0%}).\n"
            f"Seuil max : {SORT_ORDER_MAX_OVERLAP_RATIO:.0%}.\n"
            "Bornes par fichier :\n  " + pretty + "\n"
            "Cause classique : tu as lancé rewrite_data_files SANS installer de sort order "
            "via `table.transaction().replace_sort_order(...)` d'abord. Le bin-pack "
            "concatène alors les petits fichiers dans l'ordre d'arrivée — donc avec tous "
            "les tenants mélangés dans chaque fichier final."
        )


# ---------------------------------------------------------------------------
# Check 6 — snapshot_isolation_during_rewrite
# ---------------------------------------------------------------------------


def test_snapshot_isolation_during_rewrite(compacted_table):
    """Un append concurrent (canary) n'est pas perdu si on recompacte derrière.

    Voir src/concurrent_writer.py pour l'explication détaillée de pourquoi
    on test-orchestre la race comme ça plutôt que sur du wall-clock.
    """
    from src.concurrent_writer import (
        CANARY_ROW_COUNT,
        append_canary_batch,
        count_canary_rows,
    )

    # État de départ : on est juste après la première compaction. Pas de canary.
    canary_at_start = count_canary_rows()
    if canary_at_start != 0:
        pytest.fail(
            f"État inattendu : {canary_at_start} lignes canary visibles avant l'append "
            "de canary. Le seed ne les a pas filtrées correctement, OU un test précédent "
            "a laissé un état sale. Relance la stack avec `docker compose down -v && "
            "docker compose up -d`."
        )

    # Étape 1 — un writer concurrent appende sa canary.
    append_canary_batch()

    canary_after_append = count_canary_rows()
    if canary_after_append != CANARY_ROW_COUNT:
        pytest.fail(
            f"L'append canary n'a inséré que {canary_after_append} lignes "
            f"sur {CANARY_ROW_COUNT} attendues. Le harness de test est cassé — "
            "regarde src/concurrent_writer.py."
        )

    # Étape 2 — on relance la compaction du learner. Une compaction qui ne
    # gère pas l'isolation va ÉCRIRE par-dessus l'append canary et le perdre.
    _run("src.compact")

    # Étape 3 — la canary doit toujours être là.
    canary_after_recompact = count_canary_rows()

    if canary_after_recompact != CANARY_ROW_COUNT:
        pytest.fail(
            f"Snapshot isolation cassée : après la seconde compaction, il ne reste "
            f"que {canary_after_recompact} lignes canary sur {CANARY_ROW_COUNT}.\n"
            "Diagnostic : ta `compact_table()` n'utilise pas la concurrence "
            "optimiste d'Iceberg. Cause typique : tu lis l'ensemble des lignes "
            "avec `table.scan().to_arrow()` puis tu fais un `overwrite(...)` "
            "avec ce snapshot — c'est un classic « lost update » : tu écrases "
            "les rows appendées entre ta lecture et ton commit.\n"
            "Fix : utilise `table.rewrite_data_files(...)` (l'action Iceberg "
            "officielle), qui base sa compaction sur les data FILES, pas sur "
            "les ROWS, et résout les conflits via la concurrence optimiste du "
            "catalog REST. Voir https://py.iceberg.apache.org/api/#rewrite-files."
        )
