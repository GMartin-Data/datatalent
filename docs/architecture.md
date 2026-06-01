# Architecture — DataTalent

Trois vues complémentaires du pipeline, chacune lisible indépendamment :
acquisition des données, transformations dbt, infrastructure et déploiement.
Pour une vue d'ensemble du projet, voir le [README](../README.md).

> Les diagrammes ci-dessous sont au format Mermaid, rendu nativement par GitHub.

---

## 1. Acquisition des données

Un seul chemin d'ingestion : les 7 sources passent par Cloud Run → GCS →
BigQuery `raw`. Aucune exception.

```mermaid
graph TD
    subgraph brief["Sources brief"]
        FT["API France Travail<br/>OAuth2 · 101 depts × 4 ROME"]
        SI["Stock Sirene INSEE<br/>Parquet · ~2 Go"]
        GEO["API Géo<br/>Snapshot 3 niveaux"]
    end

    subgraph compl["Sources complémentaires"]
        UE["URSSAF effectifs<br/>API Opendatasoft<br/>filtré 4 codes APE IT"]
        UM["URSSAF masse salariale<br/>API Opendatasoft<br/>~30 lignes · NA88 = 62"]
        BMO["BMO France Travail<br/>XLSX annuel · FAP M1X/M2X"]
        ADZ["Adzuna<br/>API REST paginée<br/>data engineer FR"]
    end

    subgraph pipeline["Pipeline d'ingestion uniforme"]
        CR["Cloud Run Job"]
        GCS["GCS<br/>datatalent-glaq-2-raw/"]
        RAW["BigQuery raw"]
    end

    FT --> CR
    SI --> CR
    GEO --> CR
    UE --> CR
    UM --> CR
    BMO --> CR
    ADZ --> CR
    CR --> GCS --> RAW

    classDef brief fill:#dfe6f0,stroke:#5e81ac,stroke-width:1.5px,color:#2e3440
    classDef compl fill:#e1f5ee,stroke:#0f6e56,stroke-width:1.5px,color:#2e3440
    classDef pipeline fill:#f5edda,stroke:#c9a95a,stroke-width:1.5px,color:#2e3440

    class FT,SI,GEO brief
    class UE,UM,BMO,ADZ compl
    class CR,GCS,RAW pipeline
```

**Points clés :**

- Chaque source suit le même chemin : script Python dans `ingestion/{source}/` →
  `shared/gcs.py` → `shared/bigquery.py` → `raw`.
- France Travail et Adzuna utilisent `WRITE_APPEND` (accumulation hebdomadaire,
  déduplication en staging). Les autres sources utilisent `WRITE_TRUNCATE`.
- URSSAF masse salariale (~30 lignes) passe par le workflow classique malgré son
  faible volume, par souci d'uniformité architecturale.

---

## 2. Transformations dbt

Une couche analytique partagée (`int_offres_analytiques`) isole les dérivées métier
au grain offre, et un mart unifié (`fct_offres`) dénormalise les enrichissements
territoriaux pour servir Looker Studio en table large.

```mermaid
flowchart LR
    subgraph staging["Staging — nettoyage mono-source"]
        STG_FT["stg_france_travail__offres<br/>categorie_metier · salaire parsé<br/>source = 'france_travail'"]
        STG_ADZ["stg_adzuna__offres<br/>salaire annualisé<br/>source = 'adzuna'"]
        STG_SI["stg_sirene__etablissements<br/>filtre actifs · masquage RGPD"]
        STG_GEO["stg_geo__communes<br/>+ departements + regions"]
        STG_UE["stg_urssaf__effectifs<br/>_commune_ape"]
        STG_BMO["stg_bmo__projets<br/>_recrutement"]
        STG_UM["stg_urssaf__masse<br/>_salariale_na88"]
    end

    subgraph intermediate["Intermediate — croisements + couche analytique"]
        INT_OFF["int_offres_enrichies<br/>UNION ALL FT + Adzuna<br/>+ enrichissement géo"]
        INT_ANA["int_offres_analytiques<br/>multi-vues salaire · regroupement codeNAF<br/>flags qualité · dimensions temporelles"]
        INT_DENS["int_densite_sectorielle<br/>_commune"]
        INT_TENS["int_tensions_bassin<br/>_emploi"]
    end

    subgraph marts["Mart unifié — wide fact table grain offre"]
        FCT_OFF["fct_offres<br/>métriques fenêtrées · audit · partitioning<br/>+ densité IT commune (LEFT JOIN)<br/>+ tensions BMO départementales (LEFT JOIN)<br/>+ benchmark salaire IT national (CROSS JOIN)<br/>+ statut_donnee fiabilité"]
    end

    STG_FT -->|"UNION ALL"| INT_OFF
    STG_ADZ -->|"UNION ALL"| INT_OFF
    STG_GEO -->|"LEFT JOIN<br/>code_commune"| INT_OFF
    STG_UE -->|"WHERE APE IT<br/>GROUP BY commune"| INT_DENS
    STG_BMO -->|"WHERE FAP M1X/M2X<br/>GROUP BY dept"| INT_TENS

    INT_OFF --> INT_ANA
    INT_ANA --> FCT_OFF
    INT_DENS -->|"LEFT JOIN<br/>code_commune"| FCT_OFF
    INT_TENS -->|"LEFT JOIN<br/>code_dept"| FCT_OFF
    STG_UM -.->|"CROSS JOIN<br/>NA88 = 62"| FCT_OFF

    STG_SI ~~~ staging

    classDef stg fill:#dfe6f0,stroke:#5e81ac,stroke-width:1px,color:#2e3440
    classDef int fill:#f5edda,stroke:#c9a95a,stroke-width:1px,color:#2e3440
    classDef int_new fill:#eeedfe,stroke:#534ab7,stroke-width:1.5px,color:#2e3440
    classDef mart fill:#d8ebdd,stroke:#6b9e78,stroke-width:1.5px,color:#2e3440
    classDef dead fill:#e8e8e8,stroke:#888888,stroke-width:1px,color:#2e3440,stroke-dasharray: 5 5

    class STG_FT,STG_ADZ,STG_GEO,STG_UE,STG_BMO,STG_UM stg
    class STG_SI dead
    class INT_OFF,INT_DENS,INT_TENS int
    class INT_ANA int_new
    class FCT_OFF mart
```

**Points clés :**

- `int_offres_enrichies` reste minimaliste : UNION ALL France Travail + Adzuna +
  enrichissement géographique.
- `int_offres_analytiques` (en violet) porte les dérivées analytiques partageables au
  grain offre (multi-vues salaire, regroupement par code NAF, flags qualité,
  dimensions temporelles). Couche d'isolation entre logique métier et logique de
  présentation.
- `fct_offres` (mart unifié) est la **seule table consommée par Looker Studio**. Elle
  combine les dérivées analytiques, les métriques fenêtrées (que Looker ne peut pas
  calculer), les enrichissements territoriaux dénormalisés et les flags de fiabilité.
- `stg_sirene__etablissements` (grisé) est maintenu comme livrable technique mais ne
  participe à aucune jointure : le SIRET est absent des offres, la jointure Sirene a
  été abandonnée.

---

## 3. Infrastructure et déploiement

Les composants transverses qui font tourner le pipeline. L'intégralité est
provisionnée par Terraform, à l'exception du bucket de state (bootstrap problem).
Quatre vues complémentaires : le flux d'exécution nominal, puis trois focus
transverses (sécurité, monitoring, déploiement) qui partagent le même squelette.

### 3.1 Flux d'exécution nominal

```mermaid
flowchart LR
    CS(["Cloud Scheduler<br/>lundi 6 h"])
    CR["Job ingestion<br/>main.py · 7 sources"]
    GCS["GCS<br/>landing zone"]
    RAW["BigQuery<br/>raw"]
    DBT["Job dbt<br/>dbt build"]
    MARTS["BigQuery<br/>marts · fct_offres"]
    DASH(["Looker Studio<br/>dashboard"])

    CS -->|"HTTP trigger"| CR
    CR -->|"upload"| GCS
    GCS -->|"load"| RAW
    CR -->|"invoque (si critiques OK)"| DBT
    RAW --> DBT
    DBT -->|"build"| MARTS
    MARTS --> DASH

    linkStyle default stroke:#888,stroke-width:1.5px
    classDef trig fill:#f2dbd8,stroke:#b55a50,stroke-width:1.5px,color:#2e3440
    classDef proc fill:#dfe6f0,stroke:#5e81ac,stroke-width:1.5px,color:#2e3440
    classDef store fill:#f5edda,stroke:#c9a95a,stroke-width:1.5px,color:#2e3440
    classDef out fill:#d8ebdd,stroke:#6b9e78,stroke-width:1.5px,color:#2e3440

    class CS trig
    class CR,DBT proc
    class GCS,RAW,MARTS store
    class DASH out
```

### 3.2 Focus — Sécurité et habilitation

```mermaid
flowchart LR
    CR["Job ingestion"]
    DBT["Job dbt"]
    BQ["BigQuery"]

    SAI(["sa-ingestion<br/>identité du Job ingestion"])
    SAD(["sa-dbt<br/>identité du Job dbt"])
    SM["Secret Manager<br/>FT : client_id/secret<br/>Adzuna : app_id/key"]

    SAI -->|"run.invoker → dbt<br/>+ accès GCS/BQ"| CR
    SAD -->|"BQ dataEditor/jobUser"| DBT
    SM -->|"credentials montés"| CR
    CR -->|"écrit raw"| BQ
    DBT -->|"écrit staging→marts"| BQ

    linkStyle default stroke:#888,stroke-width:1.5px
    classDef proc fill:#dfe6f0,stroke:#5e81ac,stroke-width:1.5px,color:#2e3440
    classDef sec fill:#e8e8e8,stroke:#888,stroke-width:1.5px,color:#2e3440
    classDef store fill:#f5edda,stroke:#c9a95a,stroke-width:1.5px,color:#2e3440

    class CR,DBT proc
    class SAI,SAD,SM sec
    class BQ store
```

Chaque Job a sa propre identité (`sa-ingestion`, `sa-dbt`) — séparation des
privilèges runtime (D60). Seuls France Travail et Adzuna ont des credentials,
isolés dans Secret Manager ; les autres sources sont des API ouvertes. ADC en
dev local (D27).

### 3.3 Focus — Monitoring et robustesse

```mermaid
flowchart LR
    CR["Job ingestion"]
    DBT["Job dbt"]
    CS(["Cloud Scheduler"])

    AP1{{"alert policy<br/>ingestion_job_failed"}}
    AP2{{"alert policy<br/>dbt_job_failed"}}
    MAIL(["email<br/>datatalent-alerts@…"])

    CR -.->|"failed execution"| AP1
    DBT -.->|"failed execution"| AP2
    AP1 --> MAIL
    AP2 --> MAIL

    GAP["Trou assumé : échec scheduler non couvert<br/>(métrique native inexistante · D109) → reporté Niveau 2"]
    CS -.->|"non couvert"| GAP

    linkStyle default stroke:#888,stroke-width:1.5px
    classDef proc fill:#dfe6f0,stroke:#5e81ac,stroke-width:1.5px,color:#2e3440
    classDef trig fill:#f2dbd8,stroke:#b55a50,stroke-width:1.5px,color:#2e3440
    classDef alert fill:#f5edda,stroke:#c9a95a,stroke-width:1.5px,color:#2e3440
    classDef out fill:#d8ebdd,stroke:#6b9e78,stroke-width:1.5px,color:#2e3440
    classDef gap fill:#f7d9d4,stroke:#b55a50,stroke-width:2px,color:#2e3440

    class CR,DBT proc
    class CS trig
    class AP1,AP2 alert
    class MAIL out
    class GAP gap
```

Deux alert policies natives Cloud Run (`ingestion_job_failed`, `dbt_job_failed`)
routées vers un notification channel email (D105/D106). L'échec du Scheduler n'a
pas de métrique native (D109) — trou documenté, reporté en Niveau 2 log-based.

### 3.4 Focus — Déploiement (IaC + CI/CD)

```mermaid
flowchart LR
    MODS["Terraform · provisionne tout<br/>(sauf bucket state)<br/>6 modules : gcs · bigquery · cloud_run<br/>iam · secret_manager · monitoring"]

    STATE[("GCS backend<br/>tfstate · bootstrap manuel")]
    INFRA["Infra GCP<br/>(Jobs, datasets, IAM,<br/>secrets, alertes)"]

    MODS --- STATE
    MODS ==>|"terraform apply"| INFRA

    CICD["CI/CD GitHub Actions<br/>CI valide sur PR (path-scoped + ci-success)<br/>CD déploie les images sur merge"]
    CICD ==>|"push image :SHA<br/>gcloud run jobs update"| INFRA

    linkStyle default stroke:#888,stroke-width:1.5px
    classDef iac fill:#eeedfe,stroke:#534ab7,stroke-width:1.5px,color:#2e3440
    classDef state fill:#e8e8e8,stroke:#888,stroke-width:1.5px,color:#2e3440
    classDef infra fill:#dfe6f0,stroke:#5e81ac,stroke-width:1.5px,color:#2e3440
    classDef cicd fill:#d6eae4,stroke:#5d9e8a,stroke-width:1.5px,color:#2e3440

    class MODS iac
    class STATE state
    class INFRA infra
    class CICD cicd
```

**Points clés :**

- Cloud Scheduler déclenche le Job ingestion chaque lundi à 6 h. `main.py` exécute
  l'ingestion puis **invoque le Cloud Run Job dbt séparé via `run_job()`** (cross-Job,
  fire-and-forget), conditionnellement à la réussite des sources critiques
  `france_travail` / `adzuna`. Le Job dbt exécute `dbt build`.
- BigQuery héberge les **4 datasets Medallion** (`raw`, `staging`, `intermediate`,
  `marts`) consommés par le pipeline, plus deux datasets annexes hors flux
  analytique : `marts_archive` (marts historiques figés) et `billing_export`
  (monitoring des coûts).
- Terraform provisionne l'intégralité de l'infra (6 modules), sauf le bucket de state
  — seule ressource gérée manuellement (bootstrap problem).
- **Monitoring :** 2 alert policies natives Cloud Run (`ingestion_job_failed`,
  `dbt_job_failed`) routées vers un notification channel email. L'alerte sur le
  Scheduler n'est pas couverte en natif (métrique inexistante côté Cloud Monitoring) ;
  elle est reportée à une extension log-based.
- **CI/CD :** `ci.yml` valide sur PR (agrégé via le check `ci-success`,
  path-scoped) ; deux workflows CD séparés (`cd-ingestion.yml`, `cd-dbt.yml`)
  déploient les images sur merge.
- Les variables `GCP_PROJECT_ID` / `GCP_REGION` sont injectées par Terraform dans le
  Job ingestion pour permettre l'invocation cross-Job.

---

> **Source des diagrammes :** ces vues sont maintenues en cohérence avec le code de
> `infra/` et `dbt/`. En cas de divergence, le code fait foi.
