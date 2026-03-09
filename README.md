# DataTalent

Pipeline data end-to-end GCP : "Où recrute-t-on des Data Engineers en France, dans quelles entreprises et à quels salaires ?"

## Sources de données

- **France Travail** — Offres d'emploi Data Engineer (API OAuth2)
- **Sirene INSEE** — Données entreprises (fichier Parquet)
- **API Géo** — Référentiel géographique (API libre)

## Stack

- **Ingestion** — Python 3.12, httpx, tenacity → GCS → BigQuery (raw)
- **Transformation** — dbt-bigquery (staging → intermediate → marts)
- **Infrastructure** — Terraform (GCS, BigQuery, Cloud Run, Scheduler)
- **Orchestration** — Cloud Run Job + Cloud Scheduler (cron hebdo)
- **CI/CD** — GitHub Actions
- **Visualisation** — Looker Studio connecté aux marts

## Démarrage rapide

Voir [docs/setup.md](docs/setup.md) pour l'installation complète.
```bash
git clone git@github.com:GMartin-Data/datatalent.git && cd datatalent
cd ingestion && uv sync && cd ..
pre-commit install && pre-commit install --hook-type commit-msg
```

## Structure du repo
```
datatalent/
├── ingestion/      # Pipeline Python : extract → GCS → BigQuery
├── dbt/            # Transformations SQL (Bloc 2)
├── infra/          # Terraform IaC (Bloc 2-3)
├── docs/           # Documentation
└── .github/        # CI/CD workflows (Bloc 2-3)
```

## Documentation

- [Setup développeur](docs/setup.md)
