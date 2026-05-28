"""Compact the `default.logs_events` table — your job.

Objectif final :

    python -m src.compact

doit transformer la table `default.logs_events` (qui contient des centaines de
fichiers data de quelques KB chacun) en une table compactée :

  - moins de 20 fichiers data au total (cible ≤ 10 pour une vraie note),
  - chacun proche de la taille cible (par défaut ~128 MB ; ici les données
    sont trop petites pour atteindre 128 MB en un fichier — on vise plutôt
    quelques gros fichiers de taille proche les uns des autres),
  - sortés par `(tenant_id, event_time)` pour que les requêtes scopées par
    tenant n'aient à ouvrir QUE les fichiers contenant ce tenant,
  - sans perdre une seule ligne, sans dupliquer (transactionnel, snapshot-isolé).

Tu disposes des aides suivantes :

  - src.catalog.get_catalog()              → un PyIceberg RestCatalog connecté à MinIO + REST.
  - src.catalog.TABLE_IDENTIFIER           → "default.logs_events".
  - src.measure.file_count(table)          → compte les data files.
  - src.measure.total_file_bytes(table)    → somme des file_size_in_bytes.
  - src.measure.bytes_scanned_for_query(table, predicate) → bytes scannés
                                              pour une requête donnée.

Tu N'AS PAS à écrire ces helpers. Concentre-toi sur compact_table().

----------------------------------------------------------------------------
Ce que tu dois implémenter
----------------------------------------------------------------------------

1. Charger la table via le catalog.

2. AVANT compaction, mémoriser :
   - row_count_before  : table.scan().to_arrow().num_rows
   - file_count_before : measure.file_count(table)
   - bytes_scanned_before : measure.bytes_scanned_for_query(
                                table, "tenant_id = 1")
     (la requête de référence — un dashboard analytics qui scope sur tenant 1)

3. Installer le sort order (tenant_id ASC, event_time ASC) sur la table.

   PyIceberg ≥ 0.7 expose ça via la transaction API :

       from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
       from pyiceberg.transforms import IdentityTransform

       with table.transaction() as txn:
           txn.replace_sort_order(SortOrder(
               SortField(
                   source_id=<field_id de tenant_id>,
                   transform=IdentityTransform(),
                   direction=SortDirection.ASC,
                   null_order=NullOrder.NULLS_LAST,
               ),
               SortField(
                   source_id=<field_id de event_time>,
                   transform=IdentityTransform(),
                   direction=SortDirection.ASC,
                   null_order=NullOrder.NULLS_LAST,
               ),
           ))

   Astuce : récupère les field ids depuis table.schema().find_field("tenant_id").field_id.

4. Lancer la compaction.

   PyIceberg ≥ 0.7 expose `Table.rewrite_data_files(...)` qui regroupe les
   petits fichiers en fichiers proches de `target_file_size_bytes`. Lis la
   doc :
       https://py.iceberg.apache.org/api/#rewrite-files

   Cible :
       - target_file_size_bytes : 128 * 1024 * 1024  (128 MB nominal)
       - L'action doit appliquer le sort order que tu viens d'installer.
       - Tout doit tenir dans une transaction (le snapshot d'origine reste
         lisible jusqu'au commit final).

   NB : sur ce jeu de fixtures, le volume total est largement sous 128 MB.
   PyIceberg va donc produire UN SEUL gros fichier — c'est attendu et la
   rubric en tient compte (seuil : < 20 fichiers post-compaction).

5. APRÈS compaction, re-mesurer et printer le récap :

       === compact done ===
       files       : N_BEFORE → N_AFTER
       bytes       : B_BEFORE → B_AFTER
       scanned(t1) : S_BEFORE → S_AFTER   (gain: XX%)
       rows        : R_BEFORE → R_AFTER   (DOIT être égal)

6. Écrire le résultat dans bench/results.json (la rubric ne le lit pas, mais
   c'est un livrable de portfolio — un recruteur voit le gain en clair).

----------------------------------------------------------------------------
Pièges classiques senior
----------------------------------------------------------------------------

- Lancer rewrite_data_files AVANT d'avoir installé le sort order : le rewrite
  produira des fichiers compactés mais NON triés. Le check `sort_order_applied`
  tombera. Ordre = sort order d'abord, rewrite ensuite.

- Cibler target_file_size_bytes = 1 GB sur ce dataset : tu obtiens 1 fichier,
  zéro parallélisme côté reader. La rubric ne te punit pas ici, mais en prod
  ça fait mal. Documente pourquoi tu cibles 128 MB.

- Oublier la transaction : si tu fais sort_order puis rewrite en deux commits
  séparés, un reader concurrent peut tomber sur l'état intermédiaire (sort
  order installé mais fichiers pas encore réécrits). Ce n'est pas catastrophique
  ici mais c'est sale.

- Faire un `table.append(table.scan().to_arrow())` puis supprimer les anciens
  fichiers à la main : ça compacte, mais tu sors du modèle Iceberg et tu
  exposes une fenêtre où les rows existent en double. `rewrite_data_files`
  fait ça proprement et atomiquement.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyiceberg.expressions import EqualTo

from src.catalog import TABLE_IDENTIFIER, get_catalog
from src.measure import (
    bytes_scanned_for_query,
    file_count,
    total_file_bytes,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = PROJECT_ROOT / "bench"

# Requête de référence : un dashboard analytics scope sur tenant 1. C'est
# CETTE requête sur laquelle on mesure le gain post-compaction.
REFERENCE_QUERY_PREDICATE = EqualTo("tenant_id", 1)

# Cible nominale pour la taille des fichiers compactés.
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB


def compact_table() -> dict:
    """Run the compaction job and return a metrics dict (before/after).

    Retourne un dict de la forme :
        {
          "rows":    {"before": int, "after": int},
          "files":   {"before": int, "after": int},
          "bytes":   {"before": int, "after": int},
          "scanned": {"before": int, "after": int, "reduction_pct": float},
        }

    `main()` se charge d'écrire ce dict dans bench/results.json.
    """
    catalog = get_catalog()
    table = catalog.load_table(TABLE_IDENTIFIER)

    # 1. Mesures AVANT compaction.
    rows_before = table.scan().to_arrow().num_rows
    files_before = file_count(table)
    bytes_before = total_file_bytes(table)
    scanned_before = bytes_scanned_for_query(table, REFERENCE_QUERY_PREDICATE)

    # 2. TODO: installer le sort order (tenant_id ASC, event_time ASC) via
    #          table.transaction() + txn.replace_sort_order(...).
    #          Voir l'en-tête du module pour le snippet.

    # 3. TODO: lancer la compaction avec target_file_size_bytes = TARGET_FILE_SIZE_BYTES.
    #          PyIceberg ≥ 0.7 : `table.rewrite_data_files(...)`.

    # 4. table.refresh() puis re-mesurer.

    raise NotImplementedError(
        "compact_table() pas encore implémenté. "
        "Lis l'en-tête de ce module — étapes 1 à 6 explicites."
    )


def write_results(metrics: dict) -> Path:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    out = BENCH_DIR / "results.json"
    out.write_text(json.dumps(metrics, indent=2, sort_keys=True))
    return out


def main() -> None:
    metrics = compact_table()
    path = write_results(metrics)

    print()
    print("=== compact done ===")
    print(f"  files       : {metrics['files']['before']} → {metrics['files']['after']}")
    print(f"  bytes       : {metrics['bytes']['before']} → {metrics['bytes']['after']}")
    print(
        f"  scanned(t1) : {metrics['scanned']['before']} → {metrics['scanned']['after']}"
        f"   (reduction: {metrics['scanned']['reduction_pct']:.1f}%)"
    )
    print(f"  rows        : {metrics['rows']['before']} → {metrics['rows']['after']}")
    print(f"  → wrote {path}")


if __name__ == "__main__":
    main()
