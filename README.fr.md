# Petits fichiers : le combat de l'ingénieur data — `storage.iceberg-compaction`

> **Niveau** : senior · **Durée estimée** : ~10 h · **Projet payant IAmDataEng (49 €)**
> **Axes framework** : `storage`, `software_engineering_dataops`
> **Prérequis** : tu as déjà fait `storage.partitioned-lakehouse` (ou équivalent).
> Tu sais ce qu'est un snapshot Iceberg, un manifest, un partition spec.

C'est un projet d'opérations sur table — pas un projet d'initialisation. Tu
prends une table Iceberg cassée par ses propres écritures (600 micro-batches
de ~30 lignes chacun, soit ~600 data files de quelques KB) et tu lui rends
ses requêtes en latence cible.

Ce n'est pas une feature — c'est une corvée que tout senior finit par devoir
maîtriser. La rubric le mesure proprement.

---

## Le contexte

Une plateforme de logs analytics ingère ~10k events par micro-batch, 5
micro-batches par minute, écrits directement dans une table Iceberg
`default.logs_events` non partitionnée. Au bout d'une heure tu as **600
fichiers Parquet de ~30 KB chacun**. Tes dashboards Trino — qui scopent
toujours par `tenant_id` — mettent **8 secondes** à scanner ce qui devrait
prendre **200 ms**.

Cause : le planning du reader (lecture des manifests + ouverture des
fichiers) domine quand chaque data file pèse moins que son footer Parquet.
Iceberg ne peut prune AUCUN fichier parce que chaque fichier contient un peu
de chaque tenant. La file table est petite ; le métadonnée et le round-trip
sont gros.

Ton job :

1. **Diagnostiquer** l'état initial (fichiers, taille moyenne, bytes scannés
   pour la requête de référence).
2. **Installer un sort order** sur `(tenant_id, event_time)` — c'est ce qui
   va rendre le pruning post-compaction efficace.
3. **Compacter** via `rewrite_data_files(target_file_size_bytes=128 MB, ...)`
   en respectant la concurrence optimiste d'Iceberg (un writer concurrent
   ne doit pas perdre ses rows).
4. **Mesurer** le gain : bytes scannés / 5 minimum, file count / 30 minimum,
   row count strictement préservé.

Tu N'AS PAS besoin de toucher au seeding — il est livré (`fixtures/seed_iceberg.py`).
Tu écris essentiellement `src/compact.py`.

---

## Ce que tu vas livrer

| Livrable | Où |
|---|---|
| Compaction + sort order | `src/compact.py` (lis l'en-tête : 6 étapes explicites) |
| Bench des résultats | `bench/results.json` (écrit par ton `compact.py`) |
| Note explicative | `notebooks/explain.md` (≤ 250 mots, template fourni) |
| Stack locale | `docker-compose.yml` (déjà fourni — n'y touche pas) |
| Fixtures déterministes | `fixtures/generate_fixtures.py` + `fixtures/seed_iceberg.py` (déjà fournis) |
| Helpers de mesure | `src/measure.py` (déjà fourni — utilise-le, ne le réécris pas) |

La rubric lit `default.logs_events` via le catalog REST après ton compact.
Elle ne lit PAS `bench/results.json` — c'est un livrable de portfolio, pas un
gating de CI.

---

## Comment commencer

Si tu es dans GitHub Codespaces (ouverture en un clic depuis l'app
IAmDataEng), tout est prêt :

- MinIO + Iceberg REST tournent (`docker compose ps` doit montrer 2 services).
- Les 600 fixtures Parquet sont générées sous `fixtures/parquet/`.
- La table `default.logs_events` est seedée et **déjà cassée** — c'est ton
  point de départ.

Vérifie l'état initial à la main :

```python
from src.catalog import TABLE_IDENTIFIER, get_catalog
from src.measure import file_count, total_file_bytes, bytes_scanned_for_query
from pyiceberg.expressions import EqualTo

t = get_catalog().load_table(TABLE_IDENTIFIER)
print(f"files   : {file_count(t)}")
print(f"bytes   : {total_file_bytes(t)}")
print(f"scanned : {bytes_scanned_for_query(t, EqualTo('tenant_id', 1))}")
```

Tu devrais voir ~600 fichiers, ~5-15 MB total, et un `scanned` proche du
total (parce que sans sort order, AUCUN fichier ne peut être prune pour
tenant 1).

En local (hors Codespaces) :

```bash
docker compose up -d
pip install -r requirements.txt
python -m fixtures.generate_fixtures
python -m fixtures.seed_iceberg
# Implémente src/compact.py — il lève NotImplementedError tant que vide.
pytest tests/ -v
```

Quand les 6 checks passent en local, **commit + push** sur ton fork. La CI
GitHub Actions rejoue la rubric (re-seed + ton compact.py + tests). Le
verdict apparaît dans ton dashboard IAmDataEng.

---

## Les 6 checks de la rubric

Définis dans `tests/test_evaluate.py`. Tous déterministes — ils reposent sur
les métadonnées Iceberg, pas sur du wall-clock.

| # | Id | Ce qu'on vérifie |
|---|---|---|
| 1 | `pre_compaction_baseline` | La table seedée a **> 100 fichiers** de **< 1 MB** chacun. Sans ça, le projet ne mesure rien. Si ça casse, c'est un problème de seed/fixtures. |
| 2 | `compaction_reduces_file_count` | Post-compaction, **< 20 fichiers** restent. Sur ce volume, la cible idéale est ≤ 5 (un seul gros fichier suffit). |
| 3 | `compaction_preserves_data` | `COUNT(*)` avant == `COUNT(*)` après. Aucune perte. Aucun doublon. C'est le check « ta compaction n'a pas explosé en silence ». |
| 4 | `bytes_scanned_decreased` | Pour la requête de référence (`tenant_id = 1`), tu dois scanner **≤ 30 %** des bytes d'avant. C'est la mesure qui prouve que le sort order + le rewrite ont rendu le pruning efficace. |
| 5 | `sort_order_applied` | Les fichiers post-compaction ont des bornes (`lower_bounds`, `upper_bounds`) sur `tenant_id` qui ne se chevauchent **pas significativement** (≤ 20 % de paires chevauchantes). C'est ce qui PERMET le pruning du check 4. |
| 6 | `snapshot_isolation_during_rewrite` | Un append concurrent (50 lignes « canary » d'un autre process) n'est **PAS** perdu si tu relances la compaction par-dessus. C'est le check qui sépare un senior d'un junior : `rewrite_data_files` gère la concurrence optimiste ; un DIY `overwrite(table.scan())` ne le fait pas. |

---

## Les pièges senior

Vu en code review en prod, pas en formation :

- **Installer le sort order APRÈS le rewrite.**
  Le sort order vaut pour les FUTURES écritures. Tes fichiers compactés ont
  déjà été écrits dans l'ordre d'arrivée — tu as raté ton coup et les
  bornes par fichier se chevauchent encore. Le check `bytes_scanned_decreased`
  et `sort_order_applied` tombent tous les deux.

- **`table.scan().to_arrow()` puis `table.overwrite(compacted_arrow)`.**
  Ça compacte techniquement. Ça « marche » sur un test simple. Mais ça
  casse l'isolation : si un writer appende entre ton scan et ton overwrite,
  tu écrases ses rows. Le check 6 attrape ça. La bonne API est
  `rewrite_data_files()` qui réécrit au niveau des DATA FILES, pas des rows,
  et gère le retry/merge via la concurrence optimiste.

- **target_file_size_bytes = 1 GB sur 10 MB de données.**
  Tu obtiens 1 fichier. Pas de parallélisme reader. La rubric ne te punit
  pas ici (cf. la note dans `notebooks/explain.md` §5), mais en prod c'est
  un anti-pattern : tu sacrifies le throughput Trino pour économiser un
  manifest entry. 128 MB est le défaut de l'industrie.

- **Compacter à chaud sans `EXPIRE SNAPSHOTS` derrière.**
  Hors scope CI ici, mais à mentionner dans `notebooks/explain.md` §2 :
  après ton rewrite, l'ancien layout (les 600 petits fichiers) est encore
  sur disque dans l'ancien snapshot. Tu n'as pas reclaimé l'espace tant que
  tu n'as pas expiré les snapshots historiques. Quantifier ce coût fait
  partie du job.

- **Oublier que `rewrite_data_files` lit AVANT d'écrire.**
  Tu vas relire intégralement la table (les 600 fichiers) pour les
  recompacter. Le coût compute n'est PAS gratuit. Sur une vraie table
  multi-TB, tu fragmentes ton job par partition (ou par range) — pas
  applicable ici, table non partitionnée volontairement, mais à savoir.

---

## La stack locale

Identique au projet `storage.partitioned-lakehouse`. Si tu sors d'ici sans
te souvenir des endpoints, retourne-y.

- **MinIO** (`localhost:9000`, console `localhost:9001`, creds
  `admin / password`) — S3-compatible.
- **Iceberg REST catalog** (`localhost:8181`, image `tabulario/iceberg-rest`).
- Warehouse `s3://warehouse/`, namespace `default`, table `default.logs_events`.

Tous les noms sont centralisés dans `src/catalog.py` — n'y touche pas.

Knobs de sizing (utiles pour aller plus loin localement) :

```bash
# Pousse à 3000 fichiers de 30 rows comme dans la spec :
IAMDATAENG_SEED_FILES=3000 python -m fixtures.generate_fixtures
python -m fixtures.seed_iceberg
```

La rubric reste valide tant que `IAMDATAENG_SEED_FILES > 100`. CI utilise
le défaut (600) pour rester sous 2 minutes par run.

---

## Pour aller plus loin (références)

Aucune lecture obligatoire, mais si tu veux mettre ces patterns dans un cadre :

- Joe Reis & Matt Housley, *Fundamentals of Data Engineering* (O'Reilly,
  2022) — **chap. 6 « Storage », pp. 218-228** — compaction, maintenance,
  small-files problem.
- Apache Iceberg spec, [Maintenance section](https://iceberg.apache.org/docs/latest/maintenance/)
  et la doc [PyIceberg rewrite-files](https://py.iceberg.apache.org/api/#rewrite-files).
- Martin Kleppmann, *Designing Data-Intensive Applications* (O'Reilly,
  2017) — **chap. 3 sur les LSM trees** — c'est le même problème de
  compaction, à une autre couche de la stack.
- Trino docs, [Iceberg connector — file size tuning](https://trino.io/docs/current/connector/iceberg.html#table-properties)
  — pour relier la cible 128 MB à ce que voit le reader en prod.

---

## Si tu es bloqué

L'objectif est que tu galères un peu — c'est ça, opérer une table Iceberg
en prod. Mais si tu tournes en rond > 1h sur un check précis :

1. Relis le message d'erreur — il pointe presque toujours la cause.
2. Inspecte la table à la main :

   ```python
   from src.catalog import get_catalog, TABLE_IDENTIFIER
   t = get_catalog().load_table(TABLE_IDENTIFIER)
   print(t.inspect.files().to_pandas())   # liste des data files + sizes + bounds
   print(t.inspect.snapshots().to_pandas()) # historique des commits
   print(t.sort_order())                   # le sort order courant
   ```

3. Vérifie le compose : `docker compose ps`, `docker compose logs iceberg-rest --tail=50`.
4. Ouvre une issue dans ton fork avec le label `help-wanted`.

Bonne route. Quand tu reviendras dans 2 ans pour compacter une vraie table
prod un dimanche soir, tu te souviendras de ce projet.
