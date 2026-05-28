# Petits fichiers : le combat de l'ingénieur data

> Note du learner : remplis cette page (≤ 250 mots) une fois que les 6 checks
> de la rubric passent. C'est ce qu'un recruteur lira sur ton fork — vise la
> clarté brutale, pas la pédagogie de manuel. La CI ne lit pas ce fichier,
> mais il vaut son poids en entretien technique.

## 1. Le coût réel des petits fichiers

<!--
2-3 phrases. Pourquoi un reader s'effondre face à 3000 fichiers Parquet de
30 KB ? Évoque : ouverture (round-trips S3), planning Iceberg (lecture des
manifests), parallélisme côté reader, et le fait que le footer Parquet
domine quand le data block est minuscule.
-->

…

## 2. Ce que fait `rewrite_data_files` (et ce que ça ne fait PAS)

<!--
2-3 phrases. Explique la stratégie bin-pack et la cible `target_file_size_bytes`.
Précise ce qui est ATOMIQUE (un seul commit catalog en fin de course) et ce
qui ne l'est pas (le coût compute du rewrite lui-même n'est PAS gratuit).
-->

…

## 3. Pourquoi le sort order AVANT le rewrite, pas après

<!--
2-3 phrases. Quel est le mécanisme qui permet à Iceberg de skipper des fichiers
post-compaction quand tu requêtes par tenant_id ? Réponse : per-file
lower_bounds/upper_bounds + un sort order qui rend ces bornes non
chevauchantes. Si tu installes le sort order APRÈS, les fichiers existants
ne sont pas réécrits — l'attribute du sort order vaut pour les FUTURES
écritures, pas pour celles déjà sur disque.
-->

…

## 4. Snapshot isolation pendant la compaction

<!--
2-3 phrases. Comment Iceberg garantit qu'un append concurrent n'est pas perdu
par un rewrite_data_files qui a commencé avant ? Mots-clés : optimistic
concurrency control, manifest list immuable, retry/merge au commit.
-->

…

## 5. Quand NE PAS compacter

<!--
2-3 phrases. Sois honnête : la compaction coûte de la CPU et du I/O. Quand
est-ce que le jeu n'en vaut pas la chandelle ? Pense : table déjà bien
dimensionnée, fenêtre de maintenance saturée, partitions tièdes que personne
n'interroge, ratio reads/writes très bas.
-->

…
