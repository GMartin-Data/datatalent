# Setup — Onboarding développeur

## Prérequis

- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) — voir `docs/onboarding-gcp.md` pour l'installation et l'authentification (ADC)
- [uv](https://docs.astral.sh/uv/) (gère Python + dépendances)
```bash
  # Linux / macOS
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # Windows
  powershell -ExecutionPolicy BypassProcess -c "irm https://astral.sh/uv/install.ps1 | iex"
```
- pre-commit
```bash
  uv tool install pre-commit
```
- Git
- Docker + Docker Compose

## Installation
```bash
# 1. Clone
git clone git@github.com:GMartin-Data/datatalent.git && cd datatalent

# 2. Dépendances ingestion
cd ingestion && uv sync && cd ..

# 3. Dépendances dbt
cd dbt && uv sync && uv run dbt deps && cd ..

# 4. Hooks pre-commit
pre-commit install

# 5. Fichiers locaux (ne seront jamais committés)
cp dbt/profiles.yml.example dbt/profiles.yml
cp infra/terraform.tfvars.example infra/terraform.tfvars

# 6. Variables d'environnement
cp .env.example .env
```

Ouvrir `.env` et remplir les credentials :
- `FT_CLIENT_ID` / `FT_CLIENT_SECRET` — OAuth2 France Travail
- `ADZUNA_APP_ID` / `ADZUNA_APP_KEY` — API Adzuna

Les autres sources (Sirene, Géo, URSSAF ×2, BMO) sont publiques et n'ont pas besoin de credentials.

Ouvrir `dbt/profiles.yml` et `infra/terraform.tfvars`, remplacer `<YOUR_GCP_PROJECT_ID>` par votre project ID GCP.

m
# dbt compile
cd dbt && uv run dbt compile && cd ..
```

## Conventions à connaître

### Git

- Messages de commit : [Conventional Commits](https://www.conventionalcommits.org/) en anglais — validé automatiquement par un hook
- Branches : `{type}/{scope}` (ex: `feature/ingestion-france-travail`)
- Merge : squash merge sur `main` via PR avec 1 approval minimum

### Python

- Linting et formatting par Ruff — exécuté automatiquement à chaque commit
- Config partagée : `ruff.toml` à la racine
- Dépendances : `ingestion/pyproject.toml` — ajouter via `uv add`, jamais manuellement

### dbt

- Environnement Python isolé : `dbt/pyproject.toml`
- Dépendances dbt : `dbt/packages.yml` (lock : `dbt/package-lock.yml`)
- Profil : `dbt/profiles.yml` (gitignored, copié depuis `profiles.yml.example`)

### Fichiers sensibles

Ne jamais committer : `.env`, `dbt/profiles.yml`, `infra/terraform.tfvars`. Le `.gitignore` les bloque, mais restez vigilants.

## Commandes courantes

| Action | Commande |
|--------|----------|
| Ajouter une dépendance ingestion | `cd ingestion && uv add <package>` |
| Ajouter une dépendance dbt | `cd dbt && uv add <package>` |
| Lancer les tests Python | `cd ingestion && uv run pytest` |
| Compiler dbt | `cd dbt && uv run dbt compile` |
| Tester dbt | `cd dbt && uv run dbt test` |
| Lancer un modèle dbt | `cd dbt && uv run dbt run -s <model>` |
| Linter manuellement | `ruff check .` |
| Formater manuellement | `ruff format .` |
| Build Docker local | `docker compose build ingestion` |
| Run Docker local | `docker compose up ingestion` |