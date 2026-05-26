# FIFA World Cup 2026 — Match Outcome Predictor

A production-ready data engineering pipeline that ingests 150+ years of
international football data, engineers predictive features with PySpark,
transforms with dbt, orchestrates with Airflow, and serves predictions
through a Streamlit dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│  results.csv  |  goalscorers.csv  |  shootouts.csv  |  ...      │
└────────────────────────────┬────────────────────────────────────┘
                             │ pandas (Phase 1)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      POSTGRESQL (Docker)                        │
│  raw_results | raw_goalscorers | raw_shootouts | raw_former_names│
└──────────┬──────────────────────────────┬───────────────────────┘
           │ PySpark (Phase 2)            │ dbt (Phase 3)
           ▼                              ▼
┌──────────────────────┐    ┌─────────────────────────────────────┐
│  data/processed/     │    │  POSTGRESQL — Transformed Tables    │
│  (Parquet files)     │    │  stg_match_results | stg_rankings   │
│                      │    │  mart_team_form | mart_head_to_head  │
│  match_features table│    │  mart_predictions_input             │
└──────────┬───────────┘    └──────────────────┬──────────────────┘
           │                                   │
           └──────────────┬────────────────────┘
                          │ XGBoost (Phase 4)
                          ▼
              ┌───────────────────────┐
              │  wc2026_predictions   │
              │  (PostgreSQL table)   │
              │  models/xgb_fifa.pkl  │
              └───────────┬───────────┘
                          │ Streamlit (Phase 5)
                          ▼
              ┌───────────────────────┐
              │  Dashboard            │
              │  localhost:8501       │
              └───────────────────────┘

Orchestration: Apache Airflow (localhost:8080) runs the full pipeline daily
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | pandas + SQLAlchemy |
| Processing | PySpark 3.5 |
| Transformation | dbt-core + dbt-postgres |
| Orchestration | Apache Airflow 2.9 |
| ML | scikit-learn + XGBoost |
| Dashboard | Streamlit + Plotly |
| Storage | PostgreSQL 15 |
| Containerization | Docker + Docker Compose |

---

## Setup

### 1. Prerequisites

- Docker Desktop (running)
- Python 3.10+
- Java 8 or 11 (**required for PySpark on Windows**)
  - Download: https://adoptium.net/
  - Set `JAVA_HOME` environment variable to your JDK path
  - Add `%JAVA_HOME%\bin` to your `PATH`
- winutils.exe for Hadoop on Windows:
  - Download from: https://github.com/cdarlint/winutils
  - Place in `C:\hadoop\bin\` and set `HADOOP_HOME=C:\hadoop`

### 2. Clone and configure

```bash
cd fifa-pipeline
cp .env.example .env    # Edit .env with your preferred passwords if needed
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

> **Airflow note:** Airflow has complex dependencies. For a clean install:
> ```bash
> pip install apache-airflow==2.9.2 \
>   --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.10.txt"
> ```

### 4. Start infrastructure

```bash
# Start all services (PostgreSQL, Airflow, Streamlit)
docker-compose up --build

# Or start only PostgreSQL for local notebook development:
docker-compose up postgres -d
```

---

## Running Each Phase

### Phase 1 — Ingestion
```bash
# Make sure postgres is running, then open Jupyter:
jupyter notebook notebooks/01_ingestion.ipynb
```

### Phase 2 — PySpark Processing & Feature Engineering
```bash
jupyter notebook notebooks/02_spark_processing.ipynb
jupyter notebook notebooks/03_feature_engineering.ipynb
```

### Phase 2b — dbt Runner
```bash
jupyter notebook notebooks/02b_dbt_runner.ipynb
```

### Phase 3 — dbt Transformation
```bash
cd dbt/
dbt run
dbt test
dbt docs generate && dbt docs serve  # Opens docs at localhost:8080
```

### Phase 4 — ML Model
```bash
jupyter notebook notebooks/04_ml_model.ipynb
```

### Phase 5 — Dashboard Test
```bash
jupyter notebook notebooks/05_dashboard_test.ipynb
```

---

## Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | See .env |

---

## Project Structure

```
fifa-pipeline/
├── data/
│   ├── raw/                  # Source CSVs (place your data here)
│   └── processed/            # PySpark parquet outputs
├── notebooks/
│   ├── 01_ingestion.ipynb    # Phase 1: Load CSVs → PostgreSQL
│   ├── 02_spark_processing.ipynb  # Phase 2: PySpark cleaning
│   ├── 02b_dbt_runner.ipynb       # Phase 3: Run dbt via notebook
│   ├── 03_feature_engineering.ipynb  # Phase 2: Feature computation
│   ├── 04_ml_model.ipynb     # Phase 4: XGBoost training
│   └── 05_dashboard_test.ipynb   # Phase 5: End-to-end test
├── dbt/
│   ├── models/staging/       # Raw → clean staging models
│   ├── models/marts/         # Business-level aggregations
│   └── dbt_project.yml
├── airflow/dags/
│   └── fifa_pipeline_dag.py  # Full pipeline DAG
├── dashboard/
│   └── app.py                # Streamlit app
├── models/                   # Trained model artifacts (.pkl)
├── docker-compose.yml
├── requirements.txt
├── .env                      # Secrets (never commit)
└── README.md
```

---

## Windows-Specific Notes

1. **PySpark + Java**: PySpark will silently fail without Java. Always verify:
   ```bash
   java -version   # Should show 1.8.x or 11.x
   echo %JAVA_HOME%
   ```

2. **Hadoop winutils**: Without winutils, PySpark will throw errors when writing files.
   Set `HADOOP_HOME` and ensure `winutils.exe` is in `%HADOOP_HOME%\bin`.

3. **Line endings**: Git may convert line endings. Add `.gitattributes` if dbt/Airflow
   behave unexpectedly on Windows paths.

4. **Docker Desktop**: Use WSL 2 backend for better performance with Linux containers.
