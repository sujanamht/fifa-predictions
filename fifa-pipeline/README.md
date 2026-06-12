# FIFA World Cup 2026 — Match Outcome Predictor

An end-to-end data engineering portfolio project. It ingests 150 years of international football results, engineers predictive features, trains a match outcome classifier, and serves live predictions through an interactive dashboard — all orchestrated by Airflow and containerised with Docker.

The goal is a realistic showcase of a modern data stack, not a production system. Every layer (ingestion, transformation, testing, ML, orchestration, serving) is intentionally explicit so the architecture is easy to read and discuss.

---

## Stack

PostgreSQL · PySpark · dbt · XGBoost · Apache Airflow · Streamlit · Docker Compose

---

## What it builds

Raw CSVs (`results.csv`, ~49k rows of international match history) flow through a six-step pipeline:

1. **Ingest** — pandas loads 4 CSVs into PostgreSQL raw tables
2. **Process** — PySpark cleans the data and computes rolling team form features (win rates, goal differentials, H2H records) using window functions
3. **Transform** — dbt builds staging views and mart tables with embedded data quality tests
4. **Test** — dbt tests act as a data contract; the pipeline halts if any check fails
5. **Train** — XGBoost trains a multiclass classifier (Win / Draw / Loss) on the engineered features
6. **Predict** — the model scores all 48 WC 2026 group stage fixtures and writes predictions to Postgres

The Streamlit dashboard reads those predictions and lets you compare any two teams: predicted outcome, win/draw/loss probabilities, last-10-match form, head-to-head record, and top feature drivers.

---

## How to run

**Requirements:** Docker Desktop. That's it.

```bash
git clone <repo>
cd fifa-pipeline
docker-compose up --build
```

This starts five services:

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| Streamlit dashboard | http://localhost:8501 |
| PostgreSQL | localhost:5432 |

**First run — populate the database and train the model:**

The containers start cleanly but the database is empty and no model artifacts exist yet. Trigger the Airflow DAG (`fifa_wc2026_pipeline`) from the UI to run the full pipeline end-to-end, or run the notebooks in order if you want to step through each phase manually:

```
notebooks/01_ingestion.ipynb
notebooks/02_spark_processing.ipynb
notebooks/03_feature_engineering.ipynb
notebooks/04_ml_model.ipynb
```

Once the model is trained, the dashboard loads predictions automatically.

**To run notebooks locally (outside Docker):**

```bash
pip install -r requirements.txt
```

You also need Java 11 installed and `JAVA_HOME` set (PySpark requirement). On Windows you additionally need `winutils.exe` — see the [winutils repo](https://github.com/cdarlint/winutils). The Postgres container must be running before you start any notebook.

---

## Project structure

```
fifa-pipeline/
├── data/raw/                 # Source CSVs (committed)
├── notebooks/                # One notebook per pipeline phase
├── dbt/
│   ├── models/staging/       # Cleaned views (stg_match_results, stg_rankings, stg_fixtures)
│   └── models/marts/         # Aggregated tables (team_form, head_to_head, predictions_input)
├── airflow/dags/             # Single DAG: fifa_wc2026_pipeline
├── dashboard/                # Streamlit app (app.py)
├── docker-compose.yml
├── Dockerfile.airflow        # Custom Airflow image with Java + Python deps
├── requirements.txt          # Local dev dependencies
└── requirements-airflow.txt  # Runtime deps installed inside the Airflow container
```

Model artifacts (`models/*.pkl`, `models/model_metadata.json`) are excluded from git. They are generated when the pipeline runs.

---

## Airflow DAG

The DAG (`fifa_wc2026_pipeline`) runs on a daily schedule and chains six tasks:

```
ingest_raw_data → spark_process → dbt_run → dbt_test → retrain_model → update_predictions
```

`dbt_test` is the gate: if any data quality check fails, the DAG stops and the model is not retrained on dirty data.

---

## dbt models

dbt handles transformation and testing inside Postgres. Staging models are views; marts are materialised tables.

| Model | What it produces |
|---|---|
| `stg_match_results` | Typed, cleaned match rows with W/D/L result derived |
| `stg_rankings` | Team strength proxy from last 12 months using a FIFA-style points formula |
| `stg_fixtures` | Cleaned WC 2026 group stage fixture list |
| `mart_team_form` | Last-10 and last-20 match aggregates per team |
| `mart_head_to_head` | All-time H2H record per team pair, normalised alphabetically |
| `mart_predictions_input` | Fixtures joined with all team features — one row per match, ML-ready |

---

## Known design trade-offs

*PySpark is overkill for this dataset.* The source data is ~49k rows — pandas handles it in milliseconds. PySpark is here to demonstrate distributed processing patterns (window functions, JDBC writes, partitioned Spark sessions), not because the data requires it.

*Features are computed in two places.* The Spark job builds `match_features` with rolling stats that the ML model reads directly. The dbt marts compute equivalent stats in SQL. The DAG uses the Spark output; the dbt representation exists to show both approaches side by side.

*Three source CSVs are ingested but not used in modelling.* `goalscorers.csv`, `shootouts.csv`, and `former_names.csv` are loaded to Postgres in phase 1. They are available for future feature work (goal scorer patterns, penalty shootout history, historical name resolution) but are currently unused downstream.

---

## Data source

Match results from [Kaggle — International Football Results 1872–present](https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017). WC 2026 fixtures are illustrative group stage matchups stored in `dbt/seeds/wc2026_fixtures.csv`.
