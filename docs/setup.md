# Setup — Onboarding développeur

## Prérequis

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

# 2. Dépendances Python (uv installe Python 3.12 automatiquement si absent)
cd ingestion && uv sync && cd ..

# 3. Hooks pre-commit (les deux commandes sont nécessaires)
pre-commit install
pre-commit install --hook-type commit-msg

# 4. Fichiers locaux (ne seront jamais committés)
cp dbt/profiles.yml.example dbt/profiles.yml
cp infra/terraform.tfvars.example infra/terraform.tfvars
```

Ouvrir `dbt/profiles.yml` et `infra/terraform.tfvars`, remplacer `<YOUR_GCP_PROJECT_ID>` par votre project ID GCP.

## Vérification
```bash
# Pre-commit fonctionne
pre-commit run --all-files

# Python fonctionne
cd ingestion && uv run python -c "import httpx; print('OK')" && cd ..
```

## Conventions à connaître

### Git

- Messages de commit : [Conventional Commits](https://www.conventionalcommits.org/) — validé automatiquement par un hook
- Branches : `{type}/{scope}` (ex: `feat/ingestion-france-travail`)
- Merge : squash merge sur `main` via PR avec 1 approval minimum

### Python

- Linting et formatting par Ruff — exécuté automatiquement à chaque commit
- Config partagée : `ruff.toml` à la racine
- Dépendances : `ingestion/pyproject.toml` — ajouter via `uv add`, jamais manuellement

### Fichiers sensibles

Ne jamais committer : `.env`, `dbt/profiles.yml`, `infra/terraform.tfvars`. Le `.gitignore` les bloque, mais restez vigilants.

## Commandes courantes

| Action | Commande |
|--------|----------|
| Ajouter une dépendance | `cd ingestion && uv add <package>` |
| Lancer les tests | `cd ingestion && uv run pytest` |
| Linter manuellement | `ruff check .` |
| Formater manuellement | `ruff format .` |
| Build Docker local | `docker compose build ingestion` |
| Run Docker local | `docker compose up ingestion` |
