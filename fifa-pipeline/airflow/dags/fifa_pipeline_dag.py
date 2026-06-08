"""
FIFA WC 2026 Pipeline — Airflow DAG
====================================
DAG ID : fifa_wc2026_pipeline
Schedule: Daily (@daily)

Task Order:
    ingest_raw_data
        → spark_process
            → retrain_model
                → update_predictions

Why Airflow?
  Without orchestration, you would manually run 4 steps in order, check
  each succeeded, and remember to re-run them every day. Airflow:
  - Schedules the pipeline automatically
  - Retries failed tasks
  - Gives a UI to monitor every run
  - Keeps a full audit log of every run

Usage:
  docker-compose up airflow-webserver airflow-scheduler -d
  Then visit http://localhost:8080 (admin/admin)
  Toggle the DAG 'fifa_wc2026_pipeline' to ON, then trigger a run.
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# ============================================================
# Logger — every task writes to Airflow's task log
# ============================================================
log = logging.getLogger(__name__)

# ============================================================
# Paths — inside Docker containers these are mounted volumes
# ============================================================
DATA_RAW       = "/opt/airflow/data/raw"
DATA_PROCESSED = "/opt/airflow/data/processed"
MODELS_DIR     = "/opt/airflow/models"

# ============================================================
# Default arguments applied to every task
# ============================================================
default_args = {
    "owner":            "fifa_pipeline",
    "depends_on_past":  False,             # Don't wait for yesterday's run to succeed
    "start_date":       datetime(2026, 6, 1),
    "retries":          1,                 # Retry once on failure before giving up
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,             # Set to True + add email if you want alerts
    "email_on_retry":   False,
}


# ============================================================
# TASK 1 — Ingest raw data (CSV → PostgreSQL)
# ============================================================
def ingest_raw_data(**context):
    """
    Reads the raw CSVs from data/raw/ and writes them to PostgreSQL.
    Equivalent to running notebook 01_ingestion.ipynb.

    Uses pandas + SQLAlchemy (no heavy dependencies needed here).
    """
    import pandas as pd
    from sqlalchemy import create_engine

    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url)

    files = {
        "raw_results":   f"{DATA_RAW}/results.csv",
        "raw_fixtures":  f"{DATA_RAW}/fixtures.csv",
        "raw_rankings":  f"{DATA_RAW}/rankings.csv",
    }

    for table_name, path in files.items():
        if not Path(path).exists():
            raise AirflowException(f"Missing file: {path}")

        df = pd.read_csv(path)

        # Derive result column on raw_results (needed downstream)
        if table_name == "raw_results":
            df["result"] = df.apply(
                lambda r: "W" if r["home_score"] > r["away_score"]
                          else ("D" if r["home_score"] == r["away_score"] else "L"),
                axis=1
            )

        with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False,
                      chunksize=1000, method="multi")
        log.info(f"Wrote {len(df):,} rows to {table_name}")

    engine.dispose()
    log.info("ingest_raw_data complete.")



# TASK 2 — PySpark processing (clean + build match_features)

def spark_process(**context):
    """
    Runs PySpark to clean raw CSVs and build the match_features table.
    Equivalent to notebooks 02_spark_processing + 03_feature_engineering.

    We import pyspark directly here (requires pyspark in Airflow's
    Python environment). For a real production setup you would use
    SparkSubmitOperator pointing at a dedicated Spark cluster.
    """
    import sys

# Set JAVA_HOME if not already set (Docker sets it; locals may not)
    if not os.environ.get("JAVA_HOME"):
        import shutil
        java_candidates = [
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/java-11-openjdk",
            "/usr/lib/jvm/java-8-openjdk-amd64",
            "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
            "/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
        ]
        for j in java_candidates:
            if Path(j).exists():
                os.environ["JAVA_HOME"] = j
                break
        else:
            java_bin = shutil.which("java")
            if java_bin:
                os.environ["JAVA_HOME"] = str(Path(java_bin).resolve().parents[1])

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    spark = (
        SparkSession.builder
        .appName("FIFA_Airflow_Processing")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark version: {spark.version}")

    # ---- Clean results ----
    df_results = (
        spark.read.csv(f"{DATA_RAW}/results.csv", header=True, inferSchema=True)
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
        .withColumn(
            "result",
            F.when(F.col("home_score") > F.col("away_score"), "W")
             .when(F.col("home_score") == F.col("away_score"), "D")
             .otherwise("L")
        )
        .withColumn("goal_diff", F.col("home_score") - F.col("away_score"))
        .dropna(subset=["home_score", "away_score", "home_team", "away_team"])
        .filter(F.col("home_score") >= 0)
        .filter(F.col("date") >= F.lit("1950-01-01").cast(T.DateType()))
        .orderBy("date")
    )

    # ---- Build team-centric view ----
    home_view = df_results.select(
        "date", F.col("home_team").alias("team"), F.col("away_team").alias("opponent"),
        F.col("home_score").alias("goals_for"), F.col("away_score").alias("goals_against"),
        F.when(F.col("result") == "W", 1).otherwise(0).alias("win"),
        F.when(F.col("result") == "D", 1).otherwise(0).alias("draw"),
        F.when(F.col("result") == "L", 1).otherwise(0).alias("loss"),
        F.lit("home").alias("venue"),
    )
    away_view = df_results.select(
        "date", F.col("away_team").alias("team"), F.col("home_team").alias("opponent"),
        F.col("away_score").alias("goals_for"), F.col("home_score").alias("goals_against"),
        F.when(F.col("result") == "L", 1).otherwise(0).alias("win"),
        F.when(F.col("result") == "D", 1).otherwise(0).alias("draw"),
        F.when(F.col("result") == "W", 1).otherwise(0).alias("loss"),
        F.lit("away").alias("venue"),
    )
    team_matches = home_view.union(away_view)
    team_matches = team_matches.withColumn("ts", F.unix_timestamp("date").cast(T.LongType()))
    team_matches = team_matches.withColumn("gd", F.col("goals_for") - F.col("goals_against"))

    # ---- Window features ----
    w10   = Window.partitionBy("team").orderBy("ts").rowsBetween(-10, -1)
    w20   = Window.partitionBy("team").orderBy("ts").rowsBetween(-20, -1)
    w5    = Window.partitionBy("team").orderBy("ts").rowsBetween(-5,  -1)
    wp5   = Window.partitionBy("team").orderBy("ts").rowsBetween(-10, -6)
    SIX_M = 180 * 24 * 3600
    w6m   = Window.partitionBy("team").orderBy("ts").rangeBetween(-SIX_M, -1)
    w12m  = Window.partitionBy("team").orderBy("ts").rangeBetween(-2 * SIX_M, -SIX_M - 1)

    tf = (
        team_matches
        .withColumn("win_rate_last10",              F.avg("win").over(w10))
        .withColumn("draw_rate_last10",             F.avg("draw").over(w10))
        .withColumn("avg_goals_scored_last20",      F.avg("goals_for").over(w20))
        .withColumn("avg_goals_conceded_last20",    F.avg("goals_against").over(w20))
        .withColumn("avg_goal_diff_last10",         F.avg("gd").over(w10))
        .withColumn("avg_gd_last5",                 F.avg("gd").over(w5))
        .withColumn("avg_gd_prev5",                 F.avg("gd").over(wp5))
        .withColumn("goal_diff_trend",              F.col("avg_gd_last5") - F.col("avg_gd_prev5"))
        .withColumn("form_last6m",                  F.avg("win").over(w6m))
        .withColumn("ranking_proxy",                F.avg("win").over(w6m))
        .withColumn("ranking_change",
            F.avg("win").over(w6m) - F.coalesce(F.avg("win").over(w12m), F.lit(0.0)))
        .withColumn("matches_available",            F.count("win").over(w20))
    )

    fill_cols = ["win_rate_last10", "draw_rate_last10", "avg_goals_scored_last20",
                 "avg_goals_conceded_last20", "avg_goal_diff_last10",
                 "goal_diff_trend", "form_last6m", "ranking_proxy", "ranking_change"]
    for c in fill_cols:
        tf = tf.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    feat_cols = ["date", "team", "venue",
                 "win_rate_last10", "draw_rate_last10",
                 "avg_goals_scored_last20", "avg_goals_conceded_last20",
                 "avg_goal_diff_last10", "goal_diff_trend",
                 "ranking_proxy", "ranking_change", "matches_available"]
    tf_final = tf.select(feat_cols)

    # ---- H2H ----
    w_h2h = Window.partitionBy("team_a", "team_b").orderBy("date").rowsBetween(
        Window.unboundedPreceding, -1
    )
    h2h = (
        df_results.select("date", "home_team", "away_team", "result")
        .withColumn("team_a", F.least("home_team", "away_team"))
        .withColumn("team_b", F.greatest("home_team", "away_team"))
        .withColumn("ta_won",
            F.when((F.col("home_team") == F.col("team_a")) & (F.col("result") == "W"), 1)
             .when((F.col("away_team") == F.col("team_a")) & (F.col("result") == "L"), 1)
             .otherwise(0))
        .withColumn("is_draw", F.when(F.col("result") == "D", 1).otherwise(0))
        .withColumn("h2h_ta_wins", F.sum("ta_won").over(w_h2h))
        .withColumn("h2h_draws",   F.sum("is_draw").over(w_h2h))
        .withColumn("h2h_total",   F.count("*").over(w_h2h))
        .withColumn("h2h_team_a_win_rate",
            F.when(F.col("h2h_total") > 0, F.col("h2h_ta_wins") / F.col("h2h_total"))
             .otherwise(F.lit(0.33)))
        .fillna({"h2h_ta_wins": 0, "h2h_draws": 0, "h2h_total": 0})
        .select("date", "home_team", "away_team", "h2h_ta_wins",
                "h2h_draws", "h2h_total", "h2h_team_a_win_rate")
    )

    # ---- Join to build master feature table ----
    home_tf = tf_final.filter(F.col("venue") == "home")
    away_tf = tf_final.filter(F.col("venue") == "away")

    for c in ["win_rate_last10", "draw_rate_last10", "avg_goals_scored_last20",
              "avg_goals_conceded_last20", "avg_goal_diff_last10", "goal_diff_trend",
              "ranking_proxy", "ranking_change", "matches_available"]:
        home_tf = home_tf.withColumnRenamed(c, f"home_{c}")
        away_tf = away_tf.withColumnRenamed(c, f"away_{c}")

    master = (
        df_results.select("date", "home_team", "away_team",
                          "home_score", "away_score", "goal_diff", "result", "tournament")
        .join(home_tf.withColumnRenamed("team", "home_team"),  on=["date", "home_team"], how="left")
        .join(away_tf.withColumnRenamed("team", "away_team"),  on=["date", "away_team"], how="left")
        .join(h2h, on=["date", "home_team", "away_team"], how="left")
        .fillna(0.0)
        .orderBy("date")
    )

    # ---- Write parquet ----
    master.coalesce(1).write.mode("overwrite").parquet(f"{DATA_PROCESSED}/match_features.parquet")
    log.info(f"match_features.parquet written. Rows: {master.count():,}")

    # ---- Write to PostgreSQL via JDBC ----
    jdbc_jar = "/opt/airflow/drivers/postgresql-42.7.3.jar"
    if Path(jdbc_jar).exists():
        pg_host = os.getenv("POSTGRES_HOST", "postgres")
        jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{os.getenv('POSTGRES_DB', 'fifa_db')}"
        (
            master.write.jdbc(
                url=jdbc_url,
                table="match_features",
                mode="overwrite",
                properties={
                    "user": os.getenv("POSTGRES_USER", "fifa_user"),
                    "password": os.getenv("POSTGRES_PASSWORD", "fifa_pass123"),
                    "driver": "org.postgresql.Driver",
                }
            )
        )
        log.info("match_features written to PostgreSQL via JDBC.")
    else:
        # Fallback: use pandas if JDBC jar not present
        import pandas as pd
        from sqlalchemy import create_engine as ce
        pdf = master.toPandas()
        eng = ce(os.environ["DATABASE_URL"])
        with eng.connect() as conn:
            pdf.to_sql("match_features", conn, if_exists="replace", index=False,
                       chunksize=1000, method="multi")
        eng.dispose()
        log.info(f"match_features written via pandas fallback. Rows: {len(pdf):,}")

    spark.stop()
    log.info("spark_process complete.")



# ============================================================
# TASK 3 — Retrain XGBoost model on latest data
# ============================================================
def retrain_model(**context):
    """
    Retrains the XGBoost classifier on the latest match_features data.
    Overwrites the existing model artifact.

    Why retrain daily? As new international matches are played, the
    feature table grows. Retraining incorporates recent results.
    """
    import json
    import joblib
    import pandas as pd
    import numpy as np
    import xgboost as xgb
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import accuracy_score
    from sqlalchemy import create_engine

    FEATURE_COLS = [
        "home_win_rate_last10", "away_win_rate_last10",
        "home_draw_rate_last10", "away_draw_rate_last10",
        "home_avg_goals_scored_last20", "away_avg_goals_scored_last20",
        "home_avg_goals_conceded_last20", "away_avg_goals_conceded_last20",
        "home_avg_goal_diff_last10", "away_avg_goal_diff_last10",
        "home_goal_diff_trend", "away_goal_diff_trend",
        "home_ranking_proxy", "away_ranking_proxy",
        "home_ranking_change", "away_ranking_change",
        "h2h_team_a_win_rate", "h2h_total",
    ]
    LABEL_MAP = {"W": 0, "D": 1, "L": 2}

    engine = create_engine(os.environ["DATABASE_URL"])
    df = pd.read_sql(
        "SELECT * FROM match_features WHERE result IS NOT NULL AND date >= '1990-01-01'",
        engine
    )
    engine.dispose()

    df = df[FEATURE_COLS + ["result"]].fillna(0)
    X = df[FEATURE_COLS]
    y = df["result"].map(LABEL_MAP)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s  = scaler.transform(X_test)

    model = xgb.XGBClassifier(
        n_estimators=300, max_depth=4, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        objective="multi:softprob", num_class=3,
        eval_metric="mlogloss", early_stopping_rounds=20,
        random_state=42, n_jobs=-1,
    )
    model.fit(X_train_s, y_train, eval_set=[(X_test_s, y_test)], verbose=False)

    acc = accuracy_score(y_test, model.predict(X_test_s))
    log.info(f"Retrained model accuracy: {acc*100:.2f}%")

    os.makedirs(MODELS_DIR, exist_ok=True)
    joblib.dump(model,  f"{MODELS_DIR}/xgb_fifa_model.pkl")
    joblib.dump(scaler, f"{MODELS_DIR}/scaler.pkl")

    meta = {
        "feature_cols": FEATURE_COLS,
        "label_map": LABEL_MAP,
        "label_map_inv": {"0": "W", "1": "D", "2": "L"},
        "training_date": datetime.now().isoformat(),
        "n_training_samples": int(len(X_train)),
        "test_accuracy": float(acc),
        "best_iteration": int(model.best_iteration),
    }
    with open(f"{MODELS_DIR}/model_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    log.info(f"Model artifacts written to {MODELS_DIR}")


# ============================================================
# TASK 4 — Score 2026 WC fixtures and save predictions
# ============================================================
def update_predictions(**context):
    """
    Loads the latest trained model and scores all 48 WC 2026 fixtures.
    Overwrites the wc2026_predictions table in PostgreSQL.
    """
    import json
    import joblib
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine

    engine = create_engine(os.environ["DATABASE_URL"])

    # Load model + scaler + metadata
    model  = joblib.load(f"{MODELS_DIR}/xgb_fifa_model.pkl")
    scaler = joblib.load(f"{MODELS_DIR}/scaler.pkl")
    with open(f"{MODELS_DIR}/model_metadata.json") as f:
        meta = json.load(f)
    FEATURE_COLS   = meta["feature_cols"]
    LABEL_MAP_INV  = meta["label_map_inv"]

    # Latest team form
    df_home = pd.read_sql("""
        SELECT DISTINCT ON (home_team) home_team AS team,
            home_win_rate_last10 AS win_rate_last10,
            home_draw_rate_last10 AS draw_rate_last10,
            home_avg_goals_scored_last20 AS avg_goals_scored_last20,
            home_avg_goals_conceded_last20 AS avg_goals_conceded_last20,
            home_avg_goal_diff_last10 AS avg_goal_diff_last10,
            home_goal_diff_trend AS goal_diff_trend,
            home_ranking_proxy AS ranking_proxy,
            home_ranking_change AS ranking_change
        FROM match_features WHERE home_team IS NOT NULL
        ORDER BY home_team, date DESC
    """, engine).set_index("team")

    df_away = pd.read_sql("""
        SELECT DISTINCT ON (away_team) away_team AS team,
            away_win_rate_last10 AS win_rate_last10,
            away_draw_rate_last10 AS draw_rate_last10,
            away_avg_goals_scored_last20 AS avg_goals_scored_last20,
            away_avg_goals_conceded_last20 AS avg_goals_conceded_last20,
            away_avg_goal_diff_last10 AS avg_goal_diff_last10,
            away_goal_diff_trend AS goal_diff_trend,
            away_ranking_proxy AS ranking_proxy,
            away_ranking_change AS ranking_change
        FROM match_features WHERE away_team IS NOT NULL
        ORDER BY away_team, date DESC
    """, engine).set_index("team")

    # Merge perspectives
    form_cols = ["win_rate_last10", "draw_rate_last10", "avg_goals_scored_last20",
                 "avg_goals_conceded_last20", "avg_goal_diff_last10",
                 "goal_diff_trend", "ranking_proxy", "ranking_change"]
    df_form = df_home.merge(df_away, left_index=True, right_index=True,
                            how="outer", suffixes=("_h", "_a"))
    for c in form_cols:
        df_form[c] = df_form[[f"{c}_h", f"{c}_a"]].mean(axis=1)
    df_form = df_form[form_cols]

    df_h2h = pd.read_sql("""
        SELECT DISTINCT ON (LEAST(home_team, away_team), GREATEST(home_team, away_team))
            LEAST(home_team, away_team) AS team_a,
            GREATEST(home_team, away_team) AS team_b,
            h2h_team_a_win_rate, h2h_total
        FROM match_features
        ORDER BY LEAST(home_team, away_team), GREATEST(home_team, away_team), date DESC
    """, engine)

    df_fixtures = pd.read_sql("SELECT * FROM wc2026_fixtures", engine)

    rows = []
    for _, fix in df_fixtures.iterrows():
        home, away = fix["home_team"], fix["away_team"]
        neutral_form = {c: 0.33 if "rate" in c else 0.0 for c in form_cols}
        h = df_form.loc[home] if home in df_form.index else pd.Series(neutral_form)
        a = df_form.loc[away] if away in df_form.index else pd.Series(neutral_form)

        ta, tb = min(home, away), max(home, away)
        h2h_r = df_h2h[(df_h2h["team_a"] == ta) & (df_h2h["team_b"] == tb)]
        if not h2h_r.empty:
            h2h_wr = h2h_r.iloc[0]["h2h_team_a_win_rate"]
            if home != ta:
                h2h_wr = max(0.0, 1.0 - h2h_wr - 0.25)
            h2h_tot = h2h_r.iloc[0]["h2h_total"]
        else:
            h2h_wr, h2h_tot = 0.33, 0

        rows.append({
            "fixture_id": fix["fixture_id"], "group_name": fix["group_name"],
            "home_team": home, "away_team": away, "match_date": fix["match_date"],
            "home_win_rate_last10":           h["win_rate_last10"],
            "away_win_rate_last10":           a["win_rate_last10"],
            "home_draw_rate_last10":          h["draw_rate_last10"],
            "away_draw_rate_last10":          a["draw_rate_last10"],
            "home_avg_goals_scored_last20":   h["avg_goals_scored_last20"],
            "away_avg_goals_scored_last20":   a["avg_goals_scored_last20"],
            "home_avg_goals_conceded_last20": h["avg_goals_conceded_last20"],
            "away_avg_goals_conceded_last20": a["avg_goals_conceded_last20"],
            "home_avg_goal_diff_last10":      h["avg_goal_diff_last10"],
            "away_avg_goal_diff_last10":      a["avg_goal_diff_last10"],
            "home_goal_diff_trend":           h["goal_diff_trend"],
            "away_goal_diff_trend":           a["goal_diff_trend"],
            "home_ranking_proxy":             h["ranking_proxy"],
            "away_ranking_proxy":             a["ranking_proxy"],
            "home_ranking_change":            h["ranking_change"],
            "away_ranking_change":            a["ranking_change"],
            "h2h_team_a_win_rate":            h2h_wr,
            "h2h_total":                      h2h_tot,
        })

    df_inf = pd.DataFrame(rows)
    X = scaler.transform(df_inf[FEATURE_COLS].fillna(0))
    proba  = model.predict_proba(X)
    labels = model.predict(X)

    df_preds = df_inf[["fixture_id", "group_name", "home_team", "away_team", "match_date"]].copy()
    df_preds["predicted_result"] = [LABEL_MAP_INV[str(l)] for l in labels]
    df_preds["win_prob"]         = proba[:, 0].round(4)
    df_preds["draw_prob"]        = proba[:, 1].round(4)
    df_preds["loss_prob"]        = proba[:, 2].round(4)
    df_preds["confidence"]       = proba.max(axis=1).round(4)
    df_preds["predicted_at"]     = datetime.now().isoformat()

    with engine.connect() as conn:
        df_preds.to_sql("wc2026_predictions", conn, if_exists="replace",
                        index=False, method="multi")
    log.info(f"Saved {len(df_preds)} predictions to wc2026_predictions.")
    engine.dispose()


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="fifa_wc2026_pipeline",
    default_args=default_args,
    description="Daily FIFA WC 2026 prediction pipeline",
    schedule_interval="@daily",
    catchup=False,          # Don't run for every past day since start_date
    max_active_runs=1,      # Only one run at a time (avoid parallel DB writes)
    tags=["fifa", "ml", "wc2026"],
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=ingest_raw_data,
        doc_md="""Reads results.csv, fixtures.csv, rankings.csv and writes to PostgreSQL raw tables.""",
    )

    t_spark = PythonOperator(
        task_id="spark_process",
        python_callable=spark_process,
        doc_md="""Runs PySpark to clean data and build match_features table.""",
    )

    t_retrain = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_model,
        doc_md="""Retrains XGBoost classifier on latest match_features data.""",
    )

    t_predict = PythonOperator(
        task_id="update_predictions",
        python_callable=update_predictions,
        doc_md="""Scores all 48 WC 2026 fixtures and saves to wc2026_predictions.""",
    )

    # ---- Task dependencies (the pipeline order) ----
    # >> means "must succeed before the next task starts"
    t_ingest >> t_spark >> t_retrain >> t_predict
