# Makefile — facilitateur de démo DataTalent (D114)
#
# Raccourcis des gestes de la démonstration de soutenance : déclencher le
# pipeline et prouver le bout-en-bout (enchaînement ingestion -> dbt, fraîcheur
# des marts). Commandes volontairement LÉGÈRES, optimisées pour la lisibilité à
# l'écran et l'exécution rapide en live.
#
# Périmètre : démo uniquement. Le diagnostic d'incident (variantes plus riches :
# jq triggerName, asymétrie raw/marts, chemins Knative v1) vit dans
# docs/runbook.md, pas ici. Voir D114 dans notes-projet.md.

PROJECT  := datatalent-glaq-2
REGION   := europe-west1
JOB      := datatalent-ingestion
SCHEDULER := $(JOB)-scheduler

.PHONY: help trigger last-run logs fresh

help: ## Liste les cibles disponibles
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

trigger: ## Déclenche le pipeline via le Scheduler (chaîne exacte du cron, fire-and-forget)
	gcloud scheduler jobs run $(SCHEDULER) \
	  --location=$(REGION) --project=$(PROJECT)

last-run: ## Affiche la dernière exécution ingestion (succeededCount=1 attendu)
	gcloud run jobs executions list \
	  --job=$(JOB) --region=$(REGION) --project=$(PROJECT) --limit=1 \
	  --format='table(name, status.succeededCount, status.completionTime, metadata.creationTimestamp)'

# --freshness=7d : gcloud logging read ne lit que les dernières 24h par défaut.
# Le run est hebdomadaire (cron lundi), donc 24h laisse la fenêtre vide hors du
# lendemain d'un run. 7d couvre le dernier run hebdo quoi qu'il arrive.
logs: ## Prouve l'enchaînement ingestion -> dbt (events structlog)
	gcloud logging read \
	  'resource.type=cloud_run_job AND jsonPayload.event=("ingestion_end" OR "dbt_invoked")' \
	  --project=$(PROJECT) --limit=10 --freshness=7d \
	  --format='value(timestamp, jsonPayload.event)'

fresh: ## Prouve la fraîcheur des marts (fct_offres reconstruit au dernier run)
	bq query --use_legacy_sql=false --project_id=$(PROJECT) --format=prettyjson \
	  "SELECT table_id, row_count, \
	          FORMAT_TIMESTAMP('%F %T UTC', TIMESTAMP_MILLIS(last_modified_time)) AS last_modified \
	   FROM \`$(PROJECT).marts.__TABLES__\`"
