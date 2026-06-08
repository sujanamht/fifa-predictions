# FIFA World Cup 2026 — Match Outcome Predictor

A **production-ready, end-to-end data engineering pipeline** that ingests 150+ years of
international football match data, engineers predictive features using distributed computing,
applies machine learning to predict 2026 World Cup group stage outcomes, and serves live
predictions through an interactive dashboard — all containerized and orchestrated.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Project Structure](#project-structure)
5. [Data Sources](#data-sources)
6. [Phase 1 — Ingestion & Environment Setup](#phase-1--ingestion--environment-setup)
7. [Phase 2 — PySpark Processing & Feature Engineering](#phase-2--pyspark-processing--feature-engineering)
8. [Phase 3 — dbt Transformation & Data Quality](#phase-3--dbt-transformation--data-quality)
9. [Phase 4 — ML Model: Match Outcome Predictor](#phase-4--ml-model-match-outcome-predictor)
10. [Phase 5 — Airflow Orchestration & Streamlit Dashboard](#phase-5--airflow-orchestration--streamlit-dashboard)
11. [Database Tables Reference](#database-tables-reference)
12. [Setup & Installation](#setup--installation)
13. [Running Each Phase](#running-each-phase)
14. [Access URLs](#access-urls)
15. [Docker Reference](#docker-reference)
16. [Windows-Specific Notes](#windows-specific-notes)
17. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project builds a complete data engineering pipeline around a single question:

> **Can we predict the outcome of a 2026 FIFA World Cup group stage match, given 150+ years of international football history?**

The answer involves the full modern data engineering stack:

- Raw CSV data is ingested into **PostgreSQL** using **pandas**
- **PySpark** cleans the data and computes rolling form statistics using **window functions**
- **dbt** transforms raw tables into analytics-ready **mart models** with embedded data quality tests
- **XGBoost** trains a multi-class classifier (Win / Draw / Loss) on the engineered features
- **Apache Airflow** orchestrates the entire pipeline on a daily schedule
- **Streamlit** serves predictions through an interactive dashboard with charts and team comparisons
- Everything runs in **Docker** with a single `docker-compose up --build` command

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                                  │
│  results.csv (49k rows) | goalscorers.csv | shootouts.csv | former_names │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 │  Phase 1 — pandas + SQLAlchemy
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     POSTGRESQL 15  (Docker, port 5432)                   │
│                                                                          │
│  raw_results | raw_goalscorers | raw_shootouts | raw_former_names        │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
       ┌───────┴────────────────────────────────────────────┐
       │ Phase 2 — PySpark 3.5                              │ Phase 3 — dbt 1.8
       ▼                                                    ▼
┌──────────────────────┐              ┌──────────────────────────────────────┐
│  data/processed/     │              │  PostgreSQL — dbt models             │
│  ─────────────────   │              │  ─────────────────────────────────   │
│  matches_clean       │              │  staging.stg_match_results  (view)   │
│  goalscorers_clean   │              │  staging.stg_rankings        (view)   │
│  shootouts_clean     │              │  staging.stg_fixtures         (view)   │
│  match_features      │              │  marts.mart_team_form        (table)  │
│  (Parquet)           │              │  marts.mart_head_to_head     (table)  │
└──────────┬───────────┘              │  marts.mart_predictions_input(table)  │
           │                          └─────────────────────┬────────────────┘
           │                                                │
           └───────────────────────┬────────────────────────┘
                                   │  Phase 4 — XGBoost
                                   ▼
                    ┌──────────────────────────────┐
                    │  PostgreSQL                  │
                    │  wc2026_predictions (48 rows)│
                    │                              │
                    │  models/                     │
                    │  ├── xgb_fifa_model.pkl       │
                    │  ├── scaler.pkl               │
                    │  └── model_metadata.json      │
                    └──────────────┬───────────────┘
                                   │  Phase 5
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
        ┌───────────────────────┐   ┌────────────────────────┐
        │  Streamlit Dashboard  │   │  Apache Airflow         │
        │  localhost:8501       │   │  localhost:8080         │
        │                       │   │                         │
        │  - Outcome gauge      │   │  DAG: fifa_wc2026       │
        │  - Form badges        │   │  Schedule: @daily       │
        │  - H2H bar chart      │   │  6 tasks in sequence    │
        │  - Feature drivers    │   │  Fails on dbt test fail │
        │  - Predictions table  │   └────────────────────────┘
        └───────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Ingestion | pandas + SQLAlchemy | 2.2.2 / 2.0.30 | Read CSVs, write to PostgreSQL |
| Processing | PySpark | 3.5.1 | Distributed cleaning, window functions |
| Transformation | dbt-core + dbt-postgres | 1.8.3 | SQL models, data quality tests, documentation |
| Orchestration | Apache Airflow | 2.9.2 | Daily pipeline scheduling, task dependencies |
| ML | XGBoost + scikit-learn | 2.0.3 / 1.4.2 | Multi-class outcome prediction |
| Dashboard | Streamlit + Plotly | 1.35.0 / 5.22.0 | Interactive predictions dashboard |
| Storage | PostgreSQL | 15.6 | Central data store |
| Serialization | pyarrow + joblib | 15.0.2 / 1.4.2 | Parquet files, model artifacts |
| Containerization | Docker + Compose | — | One-command full stack startup |
| Language | Python | 3.10+ | All pipeline code |

---

## Project Structure

```
fifa-pipeline/
│
├── data/
│   ├── raw/                         # Source CSVs (you place these here)
│   │   ├── results.csv              # 49,411 international match results (1872–present)
│   │   ├── goalscorers.csv          # Individual goal events per match
│   │   ├── shootouts.csv            # Penalty shootout outcomes
│   │   └── former_names.csv         # Country name changes (West Germany → Germany)
│   └── processed/                   # PySpark parquet outputs (auto-generated)
│       ├── matches_clean.parquet
│       ├── goalscorers_clean.parquet
│       ├── shootouts_clean.parquet
│       └── match_features.parquet
│
├── notebooks/
│   ├── 01_ingestion.ipynb           # Phase 1: Load CSVs → PostgreSQL
│   ├── 02_spark_processing.ipynb    # Phase 2a: PySpark cleaning → Parquet
│   ├── 03_feature_engineering.ipynb # Phase 2b: Window functions → match_features
│   ├── 02b_dbt_runner.ipynb         # Phase 3: Run dbt from notebook
│   ├── 04_ml_model.ipynb            # Phase 4: Train XGBoost, predict WC 2026
│   └── 05_dashboard_test.ipynb      # Phase 5: End-to-end verification
│
├── dbt/
│   ├── dbt_project.yml              # Project config (staging=view, marts=table)
│   ├── profiles.yml                 # DB connection (reads from .env)
│   ├── seeds/
│   │   └── wc2026_fixtures.csv      # 48 WC 2026 group stage fixtures
│   └── models/
│       ├── staging/
│       │   ├── stg_match_results.sql  # Clean types, derive W/D/L result
│       │   ├── stg_rankings.sql       # Derived strength score (no rankings CSV)
│       │   ├── stg_fixtures.sql       # 2026 WC fixtures from seed
│       │   └── schema.yml             # Source definitions + tests
│       └── marts/
│           ├── mart_team_form.sql     # Last 10/20 match stats per team
│           ├── mart_head_to_head.sql  # All-time H2H record per team pair
│           ├── mart_predictions_input.sql  # ML-ready feature table
│           └── schema.yml            # Tests + documentation
│
├── airflow/
│   └── dags/
│       └── fifa_pipeline_dag.py     # 6-task daily DAG
│
├── dashboard/
│   └── app.py                       # Streamlit dashboard
│
├── models/                          # Model artifacts (auto-generated by Phase 4)
│   ├── xgb_fifa_model.pkl           # Trained XGBoost classifier
│   ├── scaler.pkl                   # StandardScaler (fitted on train only)
│   └── model_metadata.json          # Feature names, label map, accuracy
│
├── drivers/                         # JDBC driver (auto-downloaded by Phase 2)
│   └── postgresql-42.7.3.jar
│
├── docker-compose.yml               # Full stack: Postgres + Airflow + Streamlit
├── Dockerfile.airflow               # Custom Airflow image with Java + PySpark
├── requirements.txt                 # Local dev dependencies (pinned)
├── requirements-airflow.txt         # Airflow container runtime deps
├── .env                             # Secrets (never commit — in .gitignore)
├── .gitignore
└── README.md
```

---

## Data Sources

All raw data lives in `data/raw/`. These are open-source international football datasets.

| File | Rows | Key Columns | Used For |
|------|------|-------------|----------|
| `results.csv` | 49,411 | date, home_team, away_team, home_score, away_score, tournament | Primary training data — every international match 1872–2025 |
| `goalscorers.csv` | ~40k | date, team, scorer, minute, own_goal, penalty | Goal-level events per match |
| `shootouts.csv` | ~300 | date, home_team, away_team, winner | Penalty shootout outcomes |
| `former_names.csv` | ~50 | current, former, start_date, end_date | Team name normalization (West Germany → Germany) |

> **Note:** There is no FIFA rankings CSV in the dataset. A strength score derived from recent match results (`form_last6m`) is used as a ranking proxy throughout the pipeline.

---

## Phase 1 — Ingestion & Environment Setup

**Notebook:** `notebooks/01_ingestion.ipynb`

### What it does

Reads the 4 raw CSV files from `data/raw/` and loads them into PostgreSQL as the starting point for all downstream processing.

### Step-by-step

| Cell | Action |
|------|--------|
| 1 | Markdown — explains the phase, prerequisites, and why we use PostgreSQL |
| 2 | Loads all 4 CSVs using pandas, prints shape and `head()` for each |
| 3 | Data quality checks: null counts per table, date ranges, unique teams, result distribution |
| 4 | Writes 4 tables to PostgreSQL via SQLAlchemy (`if_exists='replace'` for idempotency) |
| 5 | Reads row counts back from DB and cross-checks against CSV to verify |

### Tables created

| PostgreSQL Table | Source File | Rows |
|-----------------|-------------|------|
| `raw_results` | results.csv | ~49,411 |
| `raw_goalscorers` | goalscorers.csv | ~40,000 |
| `raw_shootouts` | shootouts.csv | ~300 |
| `raw_former_names` | former_names.csv | ~50 |

### Key design decisions

- Uses `if_exists='replace'` — safe to re-run, never creates duplicates
- Derives the `result` column (W/D/L) during ingestion — this becomes the ML target variable
- Secrets loaded from `.env` via `python-dotenv` — never hardcoded
- Writes in `chunksize=1000` with `method='multi'` for memory efficiency

---

## Phase 2 — PySpark Processing & Feature Engineering

**Notebooks:** `notebooks/02_spark_processing.ipynb` + `notebooks/03_feature_engineering.ipynb`

### Why PySpark over pandas

| Aspect | pandas | PySpark |
|--------|--------|---------|
| Execution | Single CPU, in-memory | Multi-core, distributed |
| Scale | GB limit | TB capable |
| File format | CSV/Excel | Parquet (columnar, compressed) |
| Schema | Inferred | Explicit, enforced |
| Production path | Re-write everything | Just change `.master("local[4]")` to cluster URL |

### Notebook 02 — PySpark Processing

| Cell | Action |
|------|--------|
| 1 | Markdown — why PySpark, Windows setup (Java 8/11, winutils.exe) |
| 2 | Starts `SparkSession(local[4])` with auto-detection of Java install path |
| 3 | Reads all 4 CSVs with **explicit schemas** (no `inferSchema` — faster and safer) |
| 4 | Cleans data: `StringType → DateType`, `'TRUE'/'FALSE' → BooleanType`, normalizes team names using `former_names.csv` join, drops null scores, adds `result` and `goal_diff` columns |
| 5 | Writes 3 Parquet files to `data/processed/` with `coalesce(1)` and `mode='overwrite'` |
| 6 | Reads parquet back, verifies schemas, confirms "West Germany" replaced by "Germany" |

### Notebook 03 — Feature Engineering

The core of Phase 2. Uses **Spark window functions** to compute rolling statistics per team.

**Critical concept — data leakage prevention:**
All windows use `.rowsBetween(-N, -1)` — the `-1` excludes the current row. This ensures
a match's own result is never used to predict itself.

| Cell | Action |
|------|--------|
| 1 | Markdown — full feature table with explanations, window function concept, leakage warning |
| 2 | Restarts Spark with JDBC driver, loads `matches_clean.parquet` |
| 3 | Unpivots matches into team-centric view (home + away UNION = ~98k rows), computes all rolling features using window functions |
| 4 | Computes cumulative H2H stats using `rowsBetween(Window.unboundedPreceding, -1)`, joins home features + away features + H2H → master feature table |
| 5 | Downloads PostgreSQL JDBC `.jar`, writes `match_features` to PostgreSQL, saves `match_features.parquet` |
| 6 | Verifies feature ranges, shows Brazil vs Argentina sample, null check |

### Features Engineered

| Feature | Window | Description |
|---------|--------|-------------|
| `home_win_rate_last10` | last 10 rows | Fraction of last 10 matches won |
| `away_win_rate_last10` | last 10 rows | Same for away team |
| `home_draw_rate_last10` | last 10 rows | Draw tendency (high → cagey team) |
| `home_avg_goals_scored_last20` | last 20 rows | Offensive capability |
| `home_avg_goals_conceded_last20` | last 20 rows | Defensive fragility |
| `home_avg_goal_diff_last10` | last 10 rows | Net dominance per match |
| `home_goal_diff_trend` | last5 − prev5 | Improving (+) or declining (−) form |
| `home_form_last6m` | 6-month range | Win rate using `rangeBetween` on unix timestamps |
| `home_ranking_proxy` | 6-month range | Strength score proxy (no external FIFA rankings) |
| `home_ranking_change` | last6m − prev6m | Ranking momentum |
| `h2h_team_a_win_rate` | unbounded | Historical win rate vs this specific opponent |
| `h2h_total` | unbounded | H2H sample size (reliability signal) |

All features are computed for both `home_` and `away_` perspectives. The final `match_features`
table has ~40 columns and ~40,000 rows (post-1950 matches only).

---

## Phase 3 — dbt Transformation & Data Quality

**Notebook:** `notebooks/02b_dbt_runner.ipynb`
**Folder:** `dbt/`

### What dbt adds

| Without dbt | With dbt |
|-------------|----------|
| Raw SQL scripts, no tracking | Models tracked as a dependency graph |
| Manual test queries | Automated tests run with `dbt test` |
| No documentation | Auto-generated searchable data catalog |
| Order of execution is manual | Execution order derived from `{{ ref() }}` |

### Model Dependency Graph

```
raw_results (source)
    └── stg_match_results (view)
            ├── stg_rankings (view)         → mart_predictions_input (table)
            ├── mart_team_form (table)      → mart_predictions_input
            └── mart_head_to_head (table)   → mart_predictions_input

wc2026_fixtures (seed)
    └── stg_fixtures (view)                → mart_predictions_input
```

### Models

**Staging (materialized as views — lightweight, rebuilt on each run)**

| Model | Source | Key Transformations |
|-------|--------|---------------------|
| `stg_match_results` | `raw_results` | Cast dates, booleans; derive W/D/L; add `goal_diff`; drop nulls |
| `stg_rankings` | `stg_match_results` | Derive strength score = `(wins×3 + draws) / (total×3) × 100` from last 12 months. Replaces missing FIFA rankings CSV. |
| `stg_fixtures` | `wc2026_fixtures` seed | Normalize group to uppercase, type-cast date |

**Marts (materialized as tables — persisted, fast for downstream queries)**

| Model | Description |
|-------|-------------|
| `mart_team_form` | Last 10 and last 20 match aggregates per team: win rate, goals, goal diff trend |
| `mart_head_to_head` | All-time H2H record for every team pair. Pairs normalized with `least()`/`greatest()` so (Brazil, Argentina) and (Argentina, Brazil) are the same row |
| `mart_predictions_input` | Final ML-ready table: 48 WC 2026 fixtures joined with home/away form, H2H stats, derived ranking scores, and computed differentials |

### Data Quality Tests (schema.yml)

25 tests across all models. Examples:

```yaml
- not_null        on: match_date, home_team, away_team, home_score, result, fixture_id
- unique          on: team (mart_team_form), team_a+team_b (mart_head_to_head), fixture_id
- accepted_values on: result IN ['W', 'D', 'L'], group_name IN ['A'..'L']
```

The Airflow DAG (`dbt_test` task) **fails the entire pipeline** if any test fails — ensuring the ML model is never retrained on corrupt data.

### Running dbt

```bash
cd fifa-pipeline/dbt/

# All commands require --profiles-dir . because profiles.yml is in dbt/ not ~/.dbt/
dbt seed  --profiles-dir .   # Load wc2026_fixtures.csv into PostgreSQL
dbt run   --profiles-dir .   # Build all 6 models
dbt test  --profiles-dir .   # Run all 25 quality tests
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8081   # View data catalog
```

---

## Phase 4 — ML Model: Match Outcome Predictor

**Notebook:** `notebooks/04_ml_model.ipynb`

### Problem Definition

Multi-class classification with 3 output classes:
- **W** — Home team wins
- **D** — Draw
- **L** — Home team loses (away team wins)

### Why XGBoost

| Algorithm | Verdict |
|-----------|---------|
| Logistic Regression | Too simple — football outcomes are non-linear |
| Neural Network | Overkill for 18 tabular features and 40k rows |
| Random Forest | Good baseline but XGBoost consistently outperforms it on tabular data |
| **XGBoost** | State-of-the-art for tabular classification. Gradient boosting corrects tree errors iteratively. Native missing value handling. Built-in feature importance. |

### Training Details

| Setting | Value |
|---------|-------|
| Training data | `match_features` — post-1990, ~40,000 rows |
| Target encoding | W=0, D=1, L=2 (fixed mapping, not LabelEncoder) |
| Train/test split | 80/20, stratified by result class |
| Feature scaling | StandardScaler fit on train set only |
| n_estimators | 300 with `early_stopping_rounds=20` |
| max_depth | 4 |
| learning_rate | 0.05 |
| objective | `multi:softprob` → outputs probabilities |
| Expected accuracy | 53–58% (typical for football prediction literature) |

### Feature Set (18 columns)

```
home_win_rate_last10         away_win_rate_last10
home_draw_rate_last10        away_draw_rate_last10
home_avg_goals_scored_last20 away_avg_goals_scored_last20
home_avg_goals_conceded_last20 away_avg_goals_conceded_last20
home_avg_goal_diff_last10    away_avg_goal_diff_last10
home_goal_diff_trend         away_goal_diff_trend
home_ranking_proxy           away_ranking_proxy
home_ranking_change          away_ranking_change
h2h_team_a_win_rate          h2h_total
```

### Notebook Step-by-Step

| Cell | Action |
|------|--------|
| 1 | Markdown — ML approach, why XGBoost, feature→prediction flow |
| 2 | Loads `match_features` (training) from PostgreSQL |
| 3 | Feature selection, label encoding, 80/20 stratified split, StandardScaler |
| 4 | Trains XGBoost with early stopping, prints classification report + Plotly confusion matrix heatmap, baseline comparison |
| 5 | Feature importance horizontal bar chart (Plotly), color-coded home/away/H2H |
| 6 | Builds inference features using `DISTINCT ON (team) ... ORDER BY date DESC` for latest form per team; `predict_proba()` → saves to `wc2026_predictions`; serializes model artifacts |
| 7 | Top 10 most confident predictions with feature contribution reasoning (z_score × importance); group-by-group winner summary |

### Output Artifacts

| File | Description |
|------|-------------|
| `models/xgb_fifa_model.pkl` | Serialized XGBoost classifier (joblib) |
| `models/scaler.pkl` | Fitted StandardScaler |
| `models/model_metadata.json` | Feature names, label map, accuracy, training date |
| `wc2026_predictions` table | 48 rows: fixture + predicted_result + win/draw/loss probabilities + confidence |

### `wc2026_predictions` Schema

```
fixture_id       group_name    home_team      away_team
match_date       predicted_result             (W / D / L)
win_prob         draw_prob      loss_prob     confidence
predicted_at
```

---

## Phase 5 — Airflow Orchestration & Streamlit Dashboard

### Airflow DAG — `fifa_wc2026_pipeline`

**File:** `airflow/dags/fifa_pipeline_dag.py`
**Schedule:** `@daily` | **Max active runs:** 1

```
ingest_raw_data
    → spark_process
        → dbt_run
            → dbt_test          ← Fails here if data quality breaks
                → retrain_model
                    → update_predictions
```

| Task | `task_id` | What happens | On failure |
|------|-----------|-------------|-----------|
| 1 | `ingest_raw_data` | pandas reads 4 CSVs → PostgreSQL raw tables | Retry once, then fail |
| 2 | `spark_process` | PySpark cleans data + computes all window features → `match_features` | Fail |
| 3 | `dbt_run` | `dbt seed && dbt run` — builds staging views + mart tables | Fail |
| 4 | `dbt_test` | `dbt test` — raises `AirflowException` if any of 25 tests fail | **Blocks all downstream tasks** |
| 5 | `retrain_model` | Full XGBoost retrain on latest `match_features`, saves `.pkl` | Fail |
| 6 | `update_predictions` | Scores all 48 WC fixtures → `wc2026_predictions` table | Fail |

All tasks use `PythonOperator`. PySpark runs inside the Airflow container (Java installed via `Dockerfile.airflow`). dbt runs via `subprocess`.

### Streamlit Dashboard — `dashboard/app.py`

**URL:** `http://localhost:8501`

**Sidebar:**
- Team selector dropdowns (Home Team, Away Team) — populated from `wc2026_predictions`
- Last pipeline run timestamp (from Airflow `dag_run` table)
- Model accuracy display

**Main content:**

| Section | Chart Type | Data Source |
|---------|-----------|-------------|
| Predicted outcome | Plotly Gauge (0–100% confidence) | `wc2026_predictions` |
| Win/Draw/Loss probabilities | Stacked horizontal bar | `wc2026_predictions` |
| Home team last 10 form | Colored W/D/L HTML badges + table | `match_features` SQL |
| Away team last 10 form | Colored W/D/L HTML badges + table | `match_features` SQL |
| Head-to-head record | Plotly grouped bar chart | `match_features` aggregate |
| Top 5 feature drivers | Plotly horizontal bar chart | Model importance × feature values |
| Full group stage table | Sortable Streamlit DataFrame with group filter | `wc2026_predictions` |

**Performance:** Uses `@st.cache_resource` for DB engine and model (loaded once), and `@st.cache_data(ttl=300)` for queries (refreshed every 5 minutes).

**Live prediction:** If the selected teams are not in the WC fixture list, the dashboard computes an on-the-fly prediction using the loaded model.

### Test Notebook — `notebooks/05_dashboard_test.ipynb`

| Cell | Action |
|------|--------|
| 1 | Markdown — complete startup guide, architecture recap |
| 2 | Checks all 9 PostgreSQL tables exist and meet minimum row count thresholds |
| 3 | HTTP GET to `localhost:8501` (Streamlit) and `localhost:8080/health` (Airflow) |
| 4 | Full project summary — table row counts, model accuracy, predictions, group-by-group results, file artifacts |

---

## Database Tables Reference

| Table | Schema | Phase | Description | Key Columns |
|-------|--------|-------|-------------|-------------|
| `raw_results` | public | 1 | Raw match results CSV | date, home_team, away_team, home_score, away_score, tournament |
| `raw_goalscorers` | public | 1 | Raw goal events CSV | date, team, scorer, minute, own_goal, penalty |
| `raw_shootouts` | public | 1 | Raw shootout CSV | date, home_team, away_team, winner |
| `raw_former_names` | public | 1 | Country name changes | current, former |
| `match_features` | public | 2 | Master feature table (one row per historical match) | All 18 feature columns + result |
| `wc2026_fixtures` | public | 3 | 48 WC 2026 group stage fixtures (seed) | fixture_id, group_name, home_team, away_team, match_date |
| `stg_match_results` | staging | 3 | Cleaned results (dbt view) | match_date, result, goal_diff |
| `stg_rankings` | staging | 3 | Derived strength scores (dbt view) | team, strength_score, derived_rank |
| `stg_fixtures` | staging | 3 | WC fixtures staging (dbt view) | fixture_id, group_name |
| `mart_team_form` | marts | 3 | Last 10/20 stats per team (dbt table) | win_rate_last10, avg_goals, goal_diff_trend |
| `mart_head_to_head` | marts | 3 | All-time H2H records (dbt table) | team_a, team_b, total_matches, team_a_win_rate |
| `mart_predictions_input` | marts | 3 | ML-ready 2026 fixtures (dbt table) | All features for home + away teams |
| `wc2026_predictions` | public | 4 | Model predictions (48 rows) | predicted_result, win_prob, draw_prob, loss_prob, confidence |

---

## Setup & Installation

### Prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker Desktop | Enable WSL 2 backend on Windows for better performance |
| Python 3.10+ | For running notebooks locally |
| Java 8 or 11 | **Required for PySpark** — Java 17 breaks PySpark. Download from https://adoptium.net/ |
| winutils.exe | **Windows only** — Required for PySpark file I/O. Download: https://github.com/cdarlint/winutils |

### Step 1 — Clone / set up the project

```bash
# The folder already exists from this build session:
cd fifa-pipeline/
```

### Step 2 — Configure environment

The `.env` file contains all secrets. Review and update if needed:

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fifa_db
POSTGRES_USER=fifa_user
POSTGRES_PASSWORD=fifa_pass123
DATABASE_URL=postgresql://fifa_user:fifa_pass123@localhost:5432/fifa_db
```

### Step 3 — Windows: Set JAVA_HOME

```cmd
REM Verify Java version (must be 8 or 11, NOT 17+)
java -version

REM Set JAVA_HOME permanently (adjust path to your install)
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11.0.23.9-hotspot"
setx PATH "%PATH%;%JAVA_HOME%\bin"

REM Set HADOOP_HOME for winutils.exe
setx HADOOP_HOME "C:\hadoop"
```

Place `winutils.exe` from https://github.com/cdarlint/winutils (hadoop-3.3.x folder) into `C:\hadoop\bin\`.

### Step 4 — Install Python dependencies

```bash
# Local notebook development
pip install -r requirements.txt

# Airflow (install with constraints for clean dependency resolution)
pip install apache-airflow==2.9.2 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.10.txt"
```

### Step 5 — Start Docker infrastructure

```bash
cd fifa-pipeline/

# Start everything (first run takes ~5 minutes to build + pull images)
docker-compose up --build

# Or start only PostgreSQL for local notebook development:
docker-compose up postgres -d
```

---

## Running Each Phase

Run notebooks in order. Each phase depends on the previous.

### Phase 1 — Ingest raw data

```bash
# Prerequisites: postgres running (docker-compose up postgres -d)
jupyter notebook notebooks/01_ingestion.ipynb
```

Outputs: `raw_results`, `raw_goalscorers`, `raw_shootouts`, `raw_former_names` tables in PostgreSQL.

### Phase 2 — PySpark processing

```bash
# Prerequisites: Phase 1 complete, JAVA_HOME set, winutils.exe in place
jupyter notebook notebooks/02_spark_processing.ipynb
jupyter notebook notebooks/03_feature_engineering.ipynb
```

Outputs: `data/processed/*.parquet` files + `match_features` PostgreSQL table.

### Phase 3 — dbt transformation

**Option A — via notebook (recommended for learning):**
```bash
jupyter notebook notebooks/02b_dbt_runner.ipynb
```

**Option B — via terminal:**
```bash
cd dbt/
dbt seed  --profiles-dir .   # Load WC 2026 fixtures
dbt run   --profiles-dir .   # Build all 6 models
dbt test  --profiles-dir .   # Run 25 quality tests (all should pass)
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8081   # View docs at http://localhost:8081
```

Outputs: `wc2026_fixtures` table + staging views + mart tables in PostgreSQL.

### Phase 4 — Train ML model

```bash
# Prerequisites: Phase 1 + 2 + 3 complete
jupyter notebook notebooks/04_ml_model.ipynb
```

Outputs: `wc2026_predictions` PostgreSQL table + `models/` artifacts (`.pkl`, `.json`).

### Phase 5 — Dashboard and orchestration

**Start dashboard locally:**
```bash
streamlit run dashboard/app.py
# Open http://localhost:8501
```

**Trigger Airflow pipeline:**
```bash
# Airflow must be running (docker-compose up)
# Visit http://localhost:8080 → login: admin / admin
# Find 'fifa_wc2026_pipeline' → toggle ON → Trigger DAG
```

**Run end-to-end test notebook:**
```bash
jupyter notebook notebooks/05_dashboard_test.ipynb
```

---

## Access URLs

| Service | URL | Credentials | Notes |
|---------|-----|-------------|-------|
| Streamlit Dashboard | http://localhost:8501 | None | Starts after `docker-compose up` or `streamlit run` |
| Airflow UI | http://localhost:8080 | admin / admin | Toggle DAG ON, then trigger |
| PostgreSQL | localhost:5432 | fifa_user / fifa_pass123 | Connect with any Postgres client |
| dbt Docs | http://localhost:8081 | None | Run `dbt docs serve --port 8081` |

---

## Docker Reference

```bash
# Start all services
docker-compose up --build

# Start only specific services
docker-compose up postgres -d
docker-compose up postgres airflow-webserver airflow-scheduler -d
docker-compose up streamlit -d

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f streamlit

# Stop everything (keeps volumes/data)
docker-compose down

# Stop and DELETE all data (fresh start)
docker-compose down -v

# Rebuild after code changes
docker-compose up --build airflow-webserver airflow-scheduler
```

### Container Overview

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `fifa_postgres` | postgres:15.6 | 5432 | Primary database |
| `fifa_airflow_init` | custom (Dockerfile.airflow) | — | One-time DB migration + admin user |
| `fifa_airflow_webserver` | custom (Dockerfile.airflow) | 8080 | Airflow UI |
| `fifa_airflow_scheduler` | custom (Dockerfile.airflow) | — | Executes DAG tasks |
| `fifa_streamlit` | python:3.10-slim | 8501 | Dashboard |

The `Dockerfile.airflow` custom image installs:
- OpenJDK 11 (for PySpark)
- PostgreSQL JDBC driver
- All packages from `requirements-airflow.txt` (PySpark, dbt, XGBoost, etc.)

---

## Windows-Specific Notes

### 1. Java for PySpark (most common issue)

```cmd
# Check Java version — must be 8 or 11, NOT 17+
java -version

# If wrong version, install from: https://adoptium.net/
# Choose: OpenJDK 11 LTS → Windows x64 → .msi installer

# After installing, set in System Environment Variables:
# JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.0.23.9-hotspot
# Add to PATH: %JAVA_HOME%\bin
```

### 2. winutils.exe for Hadoop

Without this, PySpark throws `java.io.IOException: Could not locate executable` when writing files.

```
1. Download winutils from: https://github.com/cdarlint/winutils
2. Navigate to hadoop-3.3.x/bin/
3. Copy winutils.exe to C:\hadoop\bin\
4. Set environment variable: HADOOP_HOME = C:\hadoop
```

### 3. PySpark SparkSession on Windows

The notebooks auto-detect common Java install paths. If auto-detection fails, add this before `SparkSession.builder`:

```python
import os
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.23.9-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
```

### 4. dbt profiles.yml location

dbt normally looks for `~/.dbt/profiles.yml`. This project stores it in `dbt/profiles.yml`.
Always pass `--profiles-dir .` when running dbt from the `dbt/` directory:

```bash
cd dbt/
dbt run --profiles-dir .
```

### 5. Docker Desktop

Use the **WSL 2 backend** (Settings → General → Use WSL 2 based engine) for significantly faster container builds and file I/O on Windows.

---

## Troubleshooting

### `JAVA_HOME not set` when running PySpark

```bash
# Windows CMD
echo %JAVA_HOME%
# Should output something like: C:\Program Files\Eclipse Adoptium\jdk-11...

# If empty: set it manually in notebook Cell 2 or System Environment Variables
```

### `PSQLException: Connection refused` when running notebooks

```bash
# Make sure PostgreSQL container is running
docker-compose up postgres -d
docker-compose ps   # Should show fifa_postgres as "healthy"
```

### `dbt could not find profile 'fifa_pipeline'`

```bash
# Always run dbt from inside the dbt/ directory with --profiles-dir flag
cd fifa-pipeline/dbt/
dbt run --profiles-dir .
```

### PySpark JDBC write fails (`No suitable driver found`)

The PostgreSQL JDBC `.jar` must be present before SparkSession starts.
Notebook Cell 5 of `03_feature_engineering.ipynb` auto-downloads it:

```python
# Manual download if needed:
import urllib.request
urllib.request.urlretrieve(
    "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar",
    "../drivers/postgresql-42.7.3.jar"
)
```

### `Airflow DAG not appearing in UI`

```bash
# Check scheduler logs for import errors
docker-compose logs airflow-scheduler | grep "ERROR\|CRITICAL"

# Common cause: syntax error in the DAG file
# Fix the DAG, then the scheduler auto-picks it up within 30 seconds
```

### Streamlit `ModuleNotFoundError`

```bash
# Install missing package
pip install <package>==<version>   # Use pinned version from requirements.txt

# Or rebuild the Docker image
docker-compose up --build streamlit
```

### `wc2026_predictions` table is empty

Run Phase 4 notebook `04_ml_model.ipynb` completely, or trigger the Airflow DAG.
The dashboard shows a warning and prompts you to run the pipeline if the table is empty.

---

## Project Stats

| Metric | Value |
|--------|-------|
| Total files created | 25 |
| Lines of code | ~3,500 |
| Raw data rows ingested | ~90,000 |
| Match features table rows | ~40,000 (post-1950) |
| Feature columns | 18 |
| dbt models | 6 (3 staging views + 3 mart tables) |
| dbt tests | 25 |
| WC 2026 fixtures predicted | 48 |
| Airflow DAG tasks | 6 |
| Docker services | 5 |
| Training data date range | 1990–2025 |
| Historical data date range | 1872–2025 (153 years) |

---

## Resume Talking Points

This project demonstrates the following production data engineering skills:

- **Distributed computing**: PySpark window functions for temporal feature engineering across 90k+ rows
- **Data modeling**: dbt staging/mart pattern with source definitions, documentation, and 25 automated tests
- **Data quality**: dbt tests gate the ML retraining step — pipeline refuses to train on dirty data
- **ML engineering**: XGBoost multi-class classification with proper train/test stratification, feature importance, and prediction confidence scoring
- **Orchestration**: Airflow DAG with task dependencies, failure handling, and `AirflowException` on test failures
- **Containerization**: Full stack Docker Compose with health checks, named volumes, and custom Airflow image
- **API design**: Streamlit dashboard with `@st.cache_resource` + `@st.cache_data(ttl=300)` for performance
- **Security**: All secrets in `.env` with `python-dotenv`, never hardcoded, excluded from git
- **Reproducibility**: Fully pinned `requirements.txt`, idempotent pipeline (safe to re-run any phase)
