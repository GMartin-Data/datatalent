# Runbook — Pipeline DataTalent

Procédures opérationnelles pour surveiller, diagnostiquer et vérifier le pipeline en production.

**Architecture rappelée (D19, D72)** : Cloud Scheduler `datatalent-ingestion-scheduler` (cron `0 6 * * 1` Europe/Paris) → Cloud Run Job `datatalent-ingestion` (exécute `ingestion/main.py`) → invocation programmatique du Cloud Run Job `datatalent-dbt` en fin de pipeline (D108) → matérialisation des marts BigQuery.

**Prérequis CLI** : `gcloud` authentifié sur le projet `datatalent-glaq-2`, `bq` configuré, `jq` installé (`apt install jq` ou équivalent).

---

## 1. Surveillance hebdomadaire (mitigation D109)

Le scénario « le Scheduler ne déclenche rien et personne ne le sait » n'est pas couvert par les alertes Cloud Monitoring Niveau 1 (cf. D109 — métrique scheduler native inexistante, log-based metric reportée Bloc 2 via TODO-4). Mitigation humaine en attendant : check visuel **chaque lundi après-midi**.

```bash
gcloud scheduler jobs describe datatalent-ingestion-scheduler \
  --location=europe-west1 \
  --format=json \
  | jq '{state, lastAttemptTime, scheduleTime, status: (.status // "ok-implicit")}'
```

**Sortie nominale attendue** :

```json
{
  "state": "ENABLED",
  "lastAttemptTime": "2026-05-25T04:00:03.788291Z",
  "scheduleTime": "2026-06-01T04:00:02.923924Z",
  "status": "ok-implicit"
}
```

Critères de succès :
- `state` = `ENABLED` (pas `PAUSED` ni `DISABLED`).
- `lastAttemptTime` dans les dernières 24h après un lundi 04:00 UTC.
- `scheduleTime` pointe sur le lundi suivant.
- `status` absent du JSON = succès implicite (Cloud Scheduler ne crée le champ qu'en cas d'erreur). On le force à `"ok-implicit"` dans le `jq` ci-dessus pour expliciter la sémantique.

Si l'un de ces critères est faux, basculer sur la section 2 (diagnostic).

---

## 2. Diagnostic d'un run cassé (4 steps)

Méthodologie validée sur l'incident D113 (2026-05-25 — premier run nominal post-PR #81 cassé sur env vars manquantes côté Terraform).

**Principe** : remonter la chaîne d'invocation maillon par maillon, en s'arrêtant dès qu'un maillon est anormal. Chaque step coûte une commande.

### Step 1 — Vérifier l'exécution `datatalent-ingestion`

```bash
gcloud run jobs executions list \
  --job=datatalent-ingestion \
  --region=europe-west1 \
  --limit=3 \
  --format=json \
  | jq '[.[] | {execution: .metadata.name, status: (.status.completionTime // "running"), created: .metadata.creationTimestamp, runBy: .spec.template.metadata.annotations."run.googleapis.com/triggerName" // .metadata.annotations."run.googleapis.com/triggerName" // "unknown"}]'
```

**Ce qu'on cherche** :
- Une exécution récente (timestamp dans la fenêtre attendue).
- Status `SUCCEEDED` (visible via la présence de `completionTime` non-nulle).

**Sortie anormale** :
- **Aucune exécution récente** → le Scheduler n'a pas déclenché. Sauter au Step 3.
- **Exécution `FAILED`** → ingestion plantée. Lire les logs (Step 2.5 ci-dessous).

### Step 2 — Vérifier l'exécution `datatalent-dbt`

L'invocation dbt est déclenchée par `main.py` en fin d'ingestion (D108, fire-and-forget). Une exécution `datatalent-dbt` doit suivre chaque ingestion réussie de quelques secondes.

```bash
gcloud run jobs executions list \
  --job=datatalent-dbt \
  --region=europe-west1 \
  --limit=3 \
  --format='table(name,status.completionTime,metadata.creationTimestamp,spec.template.spec.taskCount)'
```

**Critère de succès** : une exécution `datatalent-dbt-XXXXX` créée dans les ~1-5 min après l'exécution ingestion correspondante.

**Sortie anormale** :
- **Aucune exécution dbt après une ingestion réussie** → `main.py` n'a pas réussi à invoquer dbt. Sauter au Step 2.4 puis Step 2.5.

### Step 2.4 — Vérifier l'image en production

Si on suspecte un déploiement CD incomplet (l'image en prod ne contient pas le code attendu) :

```bash
gcloud run jobs describe datatalent-ingestion \
  --region=europe-west1 \
  --format=json \
  | jq -r '.spec.template.spec.template.spec.containers[0].image'
```

**Piège connu** : `gcloud run jobs describe` retourne du **Knative v1** (chemin imbriqué 5 niveaux : `spec.template.spec.template.spec.containers`), même quand la ressource est gérée en `google_cloud_run_v2_job` côté Terraform. Réflexe : lister les clés top-level d'abord, puis descendre.

**Sortie attendue** :

```
europe-west1-docker.pkg.dev/datatalent-glaq-2/datatalent/ingestion:<SHA>
```

Le `<SHA>` doit correspondre à un commit `main` postérieur à la dernière merge attendue. Le CD `cd-ingestion.yml` met à jour ce tag à chaque merge sur `main` touchant `ingestion/**`.

### Step 2.5 — Lire les logs structlog de l'exécution

Lecture des logs filtrés sur une exécution précise. C'est l'étape la plus puissante : `main.py` émet des events structurés à chaque transition d'état.

```bash
EXEC_NAME=datatalent-ingestion-XXXXX  # remplacer par le nom de l'exécution observée

gcloud logging read \
  "resource.type=cloud_run_job \
   AND resource.labels.job_name=datatalent-ingestion \
   AND labels.\"run.googleapis.com/execution_name\"=$EXEC_NAME" \
  --limit=200 \
  --format=json \
  --freshness=2d \
  | jq -r '.[] | [.timestamp, .severity, .jsonPayload.event // .textPayload // "<empty>"] | @tsv' \
  | column -t -s $'\t' \
  | head -50
```

**Events à chercher en fin de pipeline** (ordre du dernier au premier, intéressants en priorité) :

| Event structlog | Sémantique | Action |
|---|---|---|
| `dbt_invoked` | `JobsClient.run_job()` a réussi, operation ID retourné. Pipeline nominal. | Continuer Step 3/4 si problème ailleurs. |
| `dbt_invocation_skipped_critical_failure` | Garde métier : `errors & CRITICAL_SOURCES` non vide. France Travail ou Adzuna en échec. | Investiguer logs sources critiques. |
| `dbt_invocation_skipped_missing_env` | Garde environnementale : `GCP_PROJECT_ID` ou `GCP_REGION` absente du container. Cf. D113. | Vérifier `infra/main.tf` `static_env_vars`. `terraform plan` ne doit montrer aucun diff sur ces vars. |
| `dbt_invocation_failed` | Garde technique : `JobsClient.run_job()` a levé. IAM `run.invoker` cassé, network, etc. | Vérifier binding IAM `sa-ingestion` → `datatalent-dbt`. |
| `ingestion_end` (du module `__main__`) | Pipeline fini. | Vérifier qu'un des 4 events ci-dessus le précède. |

### Step 3 — Vérifier Cloud Scheduler

Si Step 1 ne montre aucune exécution récente :

```bash
gcloud scheduler jobs describe datatalent-ingestion-scheduler \
  --location=europe-west1 \
  --format=json \
  | jq '{state, lastAttemptTime, scheduleTime, schedule, timeZone, status: (.status // "ok-implicit"), attemptDeadline}'
```

**Critères croisés** :
- `state` = `ENABLED`.
- `lastAttemptTime` proche de l'heure attendue (cron `0 6 * * 1` Europe/Paris = `04:00 UTC` en CEST mai-octobre, `05:00 UTC` en CET novembre-avril).
- `status` absent = succès. Si `status.code = 5` (NOT_FOUND) ou `13` (INTERNAL) ou `7` (PERMISSION_DENIED), creuser.

### Step 4 — Vérifier la fraîcheur BigQuery

Confirme que les marts ont bien été matérialisés. Aussi utile pour observer l'asymétrie `raw frais / marts stale` qui signale une orchestration cassée entre ingestion et dbt.

```bash
for ds in marts raw; do
  echo "=== $ds ==="
  bq query --use_legacy_sql=false --project_id=datatalent-glaq-2 --format=json "
    SELECT table_id, row_count,
           FORMAT_TIMESTAMP('%F %T UTC', TIMESTAMP_MILLIS(last_modified_time)) AS last_modified
    FROM \`datatalent-glaq-2.$ds.__TABLES__\`
    ORDER BY last_modified_time DESC
  " 2>/dev/null | jq -r '.[] | [.table_id, .row_count, .last_modified] | @tsv' | column -t -s $'\t'
done
```

**Sortie nominale post-run lundi matin** :
- `raw.adzuna`, `raw.france_travail`, `raw.bmo`, `raw.geo_*`, `raw.urssaf_*` : `last_modified` ≈ lundi 04:05 UTC.
- `raw.sirene_*` : `last_modified` ≈ il y a 7-30 jours (D40 — refresh mensuel, skippé si < 30j).
- `marts.fct_offres` : `last_modified` ≈ lundi 04:10 UTC, postérieur aux raw.

**Sortie anormale typique** (incident D113 reproduit) :
```
raw.adzuna           12714   2026-05-25 04:05:14 UTC   ← frais
marts.fct_offres     10269   2026-05-23 10:03:33 UTC   ← stale J-2
```
Asymétrie raw frais / marts stale = orchestration cassée entre les deux maillons. Remonter via Step 2.5 pour identifier la cause exacte.

**Note `__TABLES__` vs `INFORMATION_SCHEMA.TABLE_STORAGE`** : `__TABLES__` (legacy, par dataset) est toujours disponible. `INFORMATION_SCHEMA.TABLE_STORAGE` (vue cross-dataset) demande `ALTER PROJECT SET OPTIONS (region-eu.enable_info_schema_storage = TRUE)` + ~1 jour de latence — non activée sur `datatalent-glaq-2` au 2026-05-25. Pour les audits ponctuels, `__TABLES__` suffit.

---

## 3. Alertes automatisées en place

Deux alertes Cloud Monitoring Niveau 1 strict provisionnées par le module `infra/modules/monitoring/` (D105, D106, D109) :

| Alerte | Condition | Destinataire |
|---|---|---|
| `ingestion_job_failed` | `run.googleapis.com/job/completed_execution_count` avec label `result=failed` sur `datatalent-ingestion` | `datatalent-alerts@googlegroups.com` |
| `dbt_job_failed` | Idem sur `datatalent-dbt` | Idem |

**Délai de notification** : ~5-7 min entre l'événement `failed` et la réception du mail (cf. D110 — contraintes API Cloud Monitoring + propagation Monarch + relais SMTP). Si > 10 min, suspecter une dégradation du pipeline d'alerting.

**Alertes non couvertes** (signalées explicitement pour ne pas créer de fausse confiance) :
- **Scheduler qui ne déclenche pas silencieusement** → reporté en Niveau 2 log-based metric (TODO-4 Bloc 2). Mitigation humaine en attendant (section 1 de ce runbook).
- **Garde environnementale `_invoke_dbt()` qui court-circuite silencieusement** → warning structlog uniquement, pas d'alerte. Durcissement reporté en TODO-8 Bloc 2 (promotion en `error` + `sys.exit(1)`). Mitigation : section 2 Step 2.5 de ce runbook.

---

## 4. Référence rapide — chemins JSON gcloud

Aide-mémoire pour `gcloud ... --format=json | jq '...'`.

### Cloud Run Job (`gcloud run jobs describe`)

L'API gcloud retourne **Knative v1** (legacy serving), pas v2. Conséquence : chemins imbriqués.

| Champ logique | Chemin JSON |
|---|---|
| Image | `.spec.template.spec.template.spec.containers[0].image` |
| Service account | `.spec.template.spec.template.spec.serviceAccountName` |
| Env vars | `.spec.template.spec.template.spec.containers[0].env` |
| Timeout | `.spec.template.spec.template.spec.timeoutSeconds` |
| Memory | `.spec.template.spec.template.spec.containers[0].resources.limits.memory` |

### Cloud Run Job Execution (`gcloud run jobs executions describe`)

| Champ logique | Chemin JSON |
|---|---|
| Status complet | `.status.conditions` (array, chercher `type=Completed` ou `type=Ready`) |
| Image effective | `.spec.template.spec.containers[0].image` |
| Override command | `.spec.template.spec.containers[0].command` |
| Override args | `.spec.template.spec.containers[0].args` |

### Cloud Scheduler (`gcloud scheduler jobs describe`)

API plus moderne, chemins plats.

| Champ logique | Chemin JSON |
|---|---|
| État | `.state` |
| Dernier attempt | `.lastAttemptTime` |
| Prochain run | `.scheduleTime` |
| Erreur dernier attempt | `.status` (absent si succès) |

### BigQuery `__TABLES__`

| Champ | Note |
|---|---|
| `creation_time` / `last_modified_time` | Microsecondes Unix. Convertir avec `TIMESTAMP_MILLIS()`. |
| `row_count` | Rafraîchi à chaque DDL/DML. Pas en temps réel pour streaming. |

---

## Pour aller plus loin

- **D109** dans `notes-projet.md` — justification de la mitigation humaine et report Niveau 2.
- **D110** dans `notes-projet.md` — décomposition du délai de notification.
- **D113** dans `notes-projet.md` — incident type ayant donné naissance à ce runbook.
- **TODO-4 / TODO-5 / TODO-8** dans `~/explain/rétro-todo.md` — extensions futures (alerte scheduler Niveau 2, durcissement garde env var).
