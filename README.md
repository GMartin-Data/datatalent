# DataTalent

> Pipeline data end-to-end sur GCP pour répondre à une question :
> **« Où recrute-t-on des Data Engineers en France, dans quelles entreprises et à quels salaires ? »**

Projet de formation Data Engineer (équipe de 4). Chaque semaine, un pipeline
automatisé collecte 7 sources publiques, les transforme avec dbt en un mart
analytique unique, et alimente un dashboard Looker Studio. L'ensemble de
l'infrastructure GCP est gérée en Terraform.

---

## Sources de données

Le brief impose 3 sources. À elles seules, elles ne répondent pas à la question :
les offres France Travail **ne contiennent pas le SIRET** de l'employeur (absent à
100 % des offres collectées), ce qui rend impossible la jointure avec Sirene et donc
l'enrichissement entreprise visé au départ. Quatre sources complémentaires ont été
sélectionnées pour combler ce trou — chacune répond à un manque précis du use case,
pas au remplissage.

| Source | Rôle dans le use case | Accès |
|---|---|---|
| **France Travail** | Offres d'emploi Data Engineer (volume, métier, géo, salaire déclaré) | API OAuth2 |
| **Adzuna** | Offres complémentaires : **volume** (2171 offres data vs 20 via FT) et **qualité entreprise** (99 % renseigné vs 31 %) | API REST |
| **Sirene INSEE** | Référentiel entreprises — conservé comme livrable technique, **sans jointure** (SIRET absent des offres) | Parquet (~2 Go) |
| **API Géo** | Référentiel géographique (communes, départements, régions) pour normaliser la localisation des offres | API libre |
| **URSSAF effectifs** | **Densité IT communale** — seule source NAF5 × commune, remplace l'enrichissement sectoriel perdu avec Sirene | API Opendatasoft |
| **URSSAF masse salariale** | **Benchmark salarial** du secteur informatique (NA88 = 62) comme repère national | API Opendatasoft |
| **BMO** | **Tensions de recrutement** IT par bassin d'emploi (besoins en main-d'œuvre) | XLSX annuel |

> Décisions associées : abandon de la jointure Sirene (D14-bis), sélection des sources
> complémentaires (D35, D46). Voir `datatalent_docs/_docs/notes-projet.md`.

---

## Architecture

Le pipeline suit un flux unique, déclenché chaque lundi à 6 h :

```
Cloud Scheduler (cron 0 6 * * 1)
  └─> Cloud Run Job ingestion (python main.py)
        ├─ 7 sources : extract → GCS → BigQuery raw
        └─ invoque le Cloud Run Job dbt (si sources critiques OK)
              └─> dbt build : staging → intermediate → marts
                    └─> Looker Studio (dashboard)
```

L'orchestration ingestion → dbt est **applicative** : `main.py` invoque le Job dbt
via `run_job()` en fin d'ingestion, conditionnellement à la réussite des sources
critiques (`france_travail`, `adzuna`). Pas de second Scheduler — dbt ne tourne que si
l'ingestion a réussi, ce qui évite un dashboard construit silencieusement sur des
données périmées (D72, D108).

**Schémas détaillés** (acquisition, transformations dbt, infrastructure) :
voir [docs/architecture.md](docs/architecture.md).

---

## Stack technique

| Couche | Technologies | Choix structurants |
|---|---|---|
| **Ingestion** | Python 3.12, httpx, structlog | Orchestrateur séquentiel `main.py`, best-effort par source (D19) |
| **Stockage** | GCS (landing zone) + BigQuery (4 datasets Medallion : raw, staging, intermediate, marts) | Free tier BigQuery (D1, D5, D26) |
| **Transformation** | dbt-bigquery (≥ 1.11) | Medallion : staging (view) → intermediate (table) → mart unifié `fct_offres` (D94) |
| **Infrastructure** | Terraform, backend GCS distant | IaC modulaire, state versionné (D2, D30) |
| **Orchestration** | Cloud Run Job + Cloud Scheduler | Job (batch one-shot), pas de Service HTTP (D19) |
| **CI/CD** | GitHub Actions (`ci.yml` + 2 CD path-scoped) | Validation sur PR, déploiement sur merge (D22, D70) |
| **Monitoring** | Cloud Monitoring (2 alertes natives + email) | Échec ingestion / échec dbt → mail (D105, D106) |
| **Visualisation** | Looker Studio | Connecté au mart `fct_offres` (D21) |

Le pipeline opère **entièrement dans le free tier GCP** (budget mensuel < 10 €, avec
alertes à 50/90/100 %).

---

## Structure du repo

```
datatalent/
├── ingestion/      # Pipeline Python : 7 sources, extract → GCS → BigQuery raw
├── dbt/            # Transformations SQL : staging → intermediate → marts
├── infra/          # Terraform IaC (6 modules + ressources racine)
├── docs/           # Documentation livrable (setup, architecture, runbook)
└── .github/        # CI/CD (ci.yml, cd-ingestion.yml, cd-dbt.yml)
```

---

## Déploiement

L'infrastructure GCP est intégralement provisionnée par Terraform
(`terraform apply`), pas par des commandes `gcloud` manuelles (D28).

### Prérequis

- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) authentifié en ADC
  (`gcloud auth application-default login`)
- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5
- Docker (pour construire et pousser les images des Cloud Run Jobs)
- Un projet GCP avec la facturation activée

### 1. Bootstrap du bucket de state (une seule fois, hors Terraform)

Terraform ne peut pas gérer le bucket qui contient son propre state — il s'agit du
*bootstrap problem*. Ce bucket est la seule ressource GCP créée manuellement (D30) :

```bash
gcloud storage buckets create gs://datatalent-glaq-2-tfstate \
  --location=europe-west1 \
  --default-storage-class=STANDARD \
  --uniform-bucket-level-access
gcloud storage buckets update gs://datatalent-glaq-2-tfstate --versioning
```

### 2. Variables

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# Renseigner project_id, billing_account_id, et les 4 credentials sources
# (FT OAuth2 + Adzuna). terraform.tfvars est gitignored — ne jamais le committer.
```

### 3. Image placeholder des Cloud Run Jobs (avant le premier apply)

Cloud Run v2 valide l'existence de l'image **à la création** du Job. Une image
placeholder `:initial` doit donc exister dans Artifact Registry avant le premier
`terraform apply`, sinon la création échoue (D73). Le CD remplacera ensuite cette
image par `:$GITHUB_SHA` à chaque merge sur `main`.

> Procédure détaillée (pull / retag / push) : voir [docs/runbook.md](docs/runbook.md).

### 4. Apply

```bash
cd infra
terraform init      # configure le backend GCS distant
terraform plan      # vérifier le diff avant d'appliquer
terraform apply
```

Le `plan` doit afficher zéro diff après un `apply` réussi — règle de garde-fou du
projet (D31). Le déploiement applicatif (images Docker à jour) est ensuite pris en
charge par les workflows CD au merge sur `main`.

> **Développement local** (onboarding, dépendances, conventions) :
> voir [docs/setup.md](docs/setup.md).

---

## Exploitation

- **Cadence** : le Cloud Scheduler déclenche le pipeline chaque lundi à 6 h.
- **Monitoring** : deux alertes natives Cloud Run (échec du Job ingestion, échec du
  Job dbt) notifient `datatalent-alerts@googlegroups.com` (D105, D106).
- **Exploitation et incidents** : procédures de surveillance et de diagnostic dans
  [docs/runbook.md](docs/runbook.md).

---

## Liens utiles

| Ressource | Lien |
|---|---|
| Dashboard analytique (Looker Studio) | [ouvrir](https://datastudio.google.com/reporting/8d729c5a-d3aa-47e2-aaed-da3ccf3011d2/page/QVjzF) |
| Dashboard coûts | [ouvrir](https://datastudio.google.com/reporting/1da67a3c-ebb6-404e-afdc-e36ad17a31d0/page/4lzuF) |
| Suivi projet (Trello) | [ouvrir](https://trello.com/b/45dfMWTF/team-glaq) |

---

## Documentation

| Document | Contenu |
|---|---|
| [docs/setup.md](docs/setup.md) | Onboarding développeur, installation, conventions |
| [docs/architecture.md](docs/architecture.md) | Schémas détaillés (acquisition, dbt, infra) |
| [docs/runbook.md](docs/runbook.md) | Exploitation, surveillance, diagnostic d'incident |

### Catalogue de données et lignage

Le catalogue (descriptions des tables et colonnes, propriétaires, fréquences de
mise à jour, tags `pii`) et le lignage interactif (DAG `staging → intermediate →
marts`) sont produits par dbt. Une fois l'environnement dbt configuré
(voir [docs/setup.md](docs/setup.md)) :

```bash
cd dbt
uv run dbt docs generate   # construit le catalogue (manifest + catalog)
uv run dbt docs serve       # ouvre le site sur http://localhost:8080
```

Le DAG de lignage est accessible via l'icône en bas à droite du site généré.
