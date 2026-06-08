"""
FIFA WC 2026 — Match Outcome Predictor Dashboard
=================================================
Streamlit app that displays:
  - Predicted match outcome with confidence gauge
  - Last 10 matches form badges per team
  - Head-to-head historical bar chart
  - Top 5 feature drivers (horizontal bar chart)
  - Full group stage predictions table

Run locally:
  cd fifa-pipeline/
  streamlit run dashboard/app.py

Run via Docker:
  docker-compose up streamlit
  Then open: http://localhost:8501
"""

import os
import json
import warnings
from pathlib import Path
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# ============================================================
# Page configuration (must be the FIRST Streamlit call)
# ============================================================
st.set_page_config(
    page_title="FIFA WC 2026 — Match Outcome Predictor",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# Load .env (works locally; in Docker, env vars come from compose)
# ============================================================
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB",   "fifa_db")
DB_USER = os.getenv("POSTGRES_USER", "fifa_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MODELS_DIR = os.path.join(os.path.dirname(__file__), 'model')

# ============================================================
# Cached resource loaders (run once, stay in memory)
# ============================================================
@st.cache_resource
def get_engine():
    """Create and cache the SQLAlchemy engine."""
    return create_engine(DB_URL, pool_pre_ping=True)


@st.cache_resource
def load_model_artifacts():
    """Load XGBoost model, scaler, and metadata from disk."""
    model_path = MODELS_DIR / "xgb_fifa_model.pkl"
    scaler_path = MODELS_DIR / "scaler.pkl"
    meta_path   = MODELS_DIR / "model_metadata.json"

    if not model_path.exists():
        return None, None, None

    model  = joblib.load(model_path)
    scaler = joblib.load(scaler_path) if scaler_path.exists() else None

    metadata = {}
    if meta_path.exists():
        with open(meta_path) as f:
            metadata = json.load(f)

    return model, scaler, metadata


@st.cache_data(ttl=300)   # Refresh every 5 minutes
def load_predictions():
    """Load all WC 2026 predictions from PostgreSQL."""
    try:
        return pd.read_sql("SELECT * FROM wc2026_predictions ORDER BY match_date, group_name",
                           get_engine())
    except Exception as e:
        st.warning(f"Could not load predictions: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_team_form(team: str) -> pd.DataFrame:
    """Get last 10 matches for a team (home or away perspective)."""
    try:
        df = pd.read_sql(f"""
            SELECT date, home_team, away_team,
                   home_score, away_score, result, tournament
            FROM match_features
            WHERE home_team = '{team}' OR away_team = '{team}'
            ORDER BY date DESC
            LIMIT 10
        """, get_engine())

        # Convert result to the team's perspective
        def team_result(row):
            if row["home_team"] == team:
                return row["result"]
            else:
                return {"W": "L", "L": "W", "D": "D"}.get(row["result"], "?")

        df["team_result"] = df.apply(team_result, axis=1)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_h2h(home_team: str, away_team: str) -> dict:
    """Get head-to-head record between two teams."""
    try:
        ta = min(home_team, away_team)
        tb = max(home_team, away_team)
        df = pd.read_sql(f"""
            SELECT
                SUM(CASE
                    WHEN home_team < away_team AND result = 'W' THEN 1
                    WHEN home_team > away_team AND result = 'L' THEN 1
                    ELSE 0 END) AS team_a_wins,
                SUM(CASE
                    WHEN home_team < away_team AND result = 'L' THEN 1
                    WHEN home_team > away_team AND result = 'W' THEN 1
                    ELSE 0 END) AS team_b_wins,
                SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS draws,
                COUNT(*) AS total
            FROM match_features
            WHERE (home_team = '{ta}' AND away_team = '{tb}')
               OR (home_team = '{tb}' AND away_team = '{ta}')
        """, get_engine())

        row = df.iloc[0]
        if row["total"] == 0:
            return {"team_a": ta, "team_b": tb,
                    "team_a_wins": 0, "team_b_wins": 0, "draws": 0, "total": 0}
        return {
            "team_a":      ta,
            "team_b":      tb,
            "team_a_wins": int(row["team_a_wins"] or 0),
            "team_b_wins": int(row["team_b_wins"] or 0),
            "draws":       int(row["draws"]       or 0),
            "total":       int(row["total"]        or 0),
        }
    except Exception:
        return {"team_a": home_team, "team_b": away_team,
                "team_a_wins": 0, "team_b_wins": 0, "draws": 0, "total": 0}


@st.cache_data(ttl=600)
def get_last_pipeline_run() -> str:
    """
    Query Airflow's metadata database for the last successful DAG run.
    Falls back to 'N/A' if Airflow is not running or table doesn't exist.
    """
    try:
        result = get_engine().connect().execute(text("""
            SELECT MAX(execution_date)
            FROM dag_run
            WHERE dag_id = 'fifa_wc2026_pipeline'
              AND state = 'success'
        """))
        ts = result.fetchone()[0]
        return str(ts) if ts else "No successful runs yet"
    except Exception:
        return "N/A (Airflow not connected)"


# ============================================================
# Helper functions
# ============================================================
def form_badge(result: str) -> str:
    """Return a colored HTML badge for a W/D/L result."""
    colors = {"W": "#28a745", "D": "#e6a817", "L": "#dc3545"}
    color  = colors.get(result, "#6c757d")
    return (f'<span style="background:{color};color:white;padding:3px 9px;'
            f'border-radius:4px;margin:2px;font-weight:bold;font-size:13px">{result}</span>')


def outcome_color(result: str) -> str:
    return {"W": "#28a745", "D": "#e6a817", "L": "#dc3545"}.get(result, "#6c757d")


def make_gauge_chart(win_p: float, draw_p: float, loss_p: float,
                     predicted: str, team: str) -> go.Figure:
    """Plotly gauge showing confidence for the predicted outcome."""
    conf  = max(win_p, draw_p, loss_p) * 100
    color = outcome_color(predicted)
    label = {"W": f"{team} Wins", "D": "Draw", "L": f"{team} Loses"}.get(predicted, predicted)

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=conf,
        title={"text": f"<b>{label}</b>", "font": {"size": 20}},
        number={"suffix": "%", "font": {"size": 40, "color": color}},
        gauge={
            "axis":  {"range": [0, 100], "tickwidth": 1},
            "bar":   {"color": color, "thickness": 0.3},
            "bgcolor": "white",
            "steps": [
                {"range": [0,  40], "color": "#f8f9fa"},
                {"range": [40, 60], "color": "#fff3cd"},
                {"range": [60, 100], "color": "#d4edda"},
            ],
            "threshold": {
                "line":  {"color": color, "width": 4},
                "thickness": 0.75,
                "value": conf,
            },
        },
    ))
    fig.update_layout(height=280, margin=dict(t=60, b=10, l=20, r=20))
    return fig


def make_prob_bar(win_p: float, draw_p: float, loss_p: float,
                  home: str, away: str) -> go.Figure:
    """Stacked probability bar for Win/Draw/Loss."""
    fig = go.Figure()
    for label, prob, color in [
        (f"{home} Win", win_p,  "#28a745"),
        ("Draw",        draw_p, "#e6a817"),
        (f"{away} Win", loss_p, "#dc3545"),
    ]:
        fig.add_trace(go.Bar(
            name=label,
            x=[prob * 100],
            y=["Outcome"],
            orientation="h",
            marker_color=color,
            text=f"{prob*100:.1f}%",
            textposition="inside",
            insidetextanchor="middle",
            hovertemplate=f"{label}: {prob*100:.1f}%<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack",
        height=90,
        showlegend=True,
        legend=dict(orientation="h", y=-0.5),
        margin=dict(t=5, b=40, l=5, r=5),
        xaxis=dict(range=[0, 100], showticklabels=False),
        yaxis=dict(showticklabels=False),
        plot_bgcolor="white",
    )
    return fig


def make_h2h_chart(h2h: dict, home: str, away: str) -> go.Figure:
    """Grouped bar chart for head-to-head wins, draws."""
    ta_wins = h2h["team_a_wins"] if h2h["team_a"] == home else h2h["team_b_wins"]
    tb_wins = h2h["team_b_wins"] if h2h["team_a"] == home else h2h["team_a_wins"]

    fig = go.Figure(data=[
        go.Bar(name=home,   x=["H2H Record"], y=[ta_wins], marker_color="#1f77b4"),
        go.Bar(name="Draw", x=["H2H Record"], y=[h2h["draws"]], marker_color="#aaaaaa"),
        go.Bar(name=away,   x=["H2H Record"], y=[tb_wins], marker_color="#ff7f0e"),
    ])
    fig.update_layout(
        barmode="group",
        title=f"All-Time H2H Record ({h2h['total']} meetings)",
        height=280,
        legend=dict(orientation="h"),
        margin=dict(t=50, b=10),
        yaxis_title="Number of Wins/Draws",
    )
    return fig


def make_feature_drivers_chart(feat_vals: pd.Series,
                                feat_importance: dict,
                                predicted: str) -> go.Figure:
    """
    Top 5 feature drivers for a specific prediction.
    Contribution = z_score × importance_pct (approximation of SHAP).
    """
    contribs = {}
    for feat, val in feat_vals.items():
        imp = feat_importance.get(feat, 0)
        # Heuristic z-score using typical ranges
        contribs[feat] = float(val) * imp

    top5 = sorted(contribs.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
    feats  = [t[0] for t in top5]
    values = [t[1] for t in top5]
    colors = ["#28a745" if v >= 0 else "#dc3545" for v in values]

    fig = go.Figure(go.Bar(
        x=values,
        y=feats,
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.3f}" for v in values],
        textposition="outside",
    ))
    fig.update_layout(
        title=f"Top 5 Feature Drivers for '{predicted}' Prediction",
        height=280,
        xaxis_title="Contribution Score",
        margin=dict(l=220, t=50, r=80),
    )
    return fig


# ============================================================
# Load data and model
# ============================================================
engine  = get_engine()
model, scaler, metadata = load_model_artifacts()
df_preds = load_predictions()

# All unique teams from predictions table
if not df_preds.empty:
    teams = sorted(set(df_preds["home_team"].tolist() + df_preds["away_team"].tolist()))
else:
    teams = ["Argentina", "Brazil", "France", "Germany", "England",
             "Spain", "Portugal", "Netherlands", "USA", "Mexico"]

FEATURE_COLS   = metadata.get("feature_cols", []) if metadata else []
LABEL_MAP_INV  = metadata.get("label_map_inv", {"0": "W", "1": "D", "2": "L"}) if metadata else {}

# ============================================================
# Sidebar
# ============================================================
st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/en/thumb/a/a0/2026_FIFA_World_Cup_logo.svg/200px-2026_FIFA_World_Cup_logo.svg.png",
    width=160, use_column_width=False
)
st.sidebar.title("Match Selector")
st.sidebar.markdown("Select any two nations to see the predicted outcome.")

home_team = st.sidebar.selectbox("Home Team", teams, index=0)
away_team = st.sidebar.selectbox("Away Team", teams, index=min(1, len(teams) - 1))

if home_team == away_team:
    st.sidebar.error("Home and Away teams must be different.")

st.sidebar.divider()
last_run = get_last_pipeline_run()
st.sidebar.caption(f"Last pipeline run: {last_run}")
st.sidebar.caption(f"Model accuracy: {metadata.get('test_accuracy', 0)*100:.1f}%" if metadata else "")

# ============================================================
# Main header
# ============================================================
st.title("FIFA WC 2026 — Match Outcome Predictor")
st.markdown(f"### {home_team}  vs  {away_team}")
st.divider()

if home_team == away_team:
    st.warning("Please select two different teams in the sidebar.")
    st.stop()

# ============================================================
# Look up prediction for this fixture (or compute on the fly)
# ============================================================
fixture_pred = pd.DataFrame()
if not df_preds.empty:
    fixture_pred = df_preds[
        (df_preds["home_team"] == home_team) & (df_preds["away_team"] == away_team)
    ]

if fixture_pred.empty and model is not None and scaler is not None and FEATURE_COLS:
    # Custom matchup not in WC fixtures — compute on the fly
    st.info(f"'{home_team} vs {away_team}' is not in the WC 2026 fixture list. Computing live prediction...")
    try:
        from sqlalchemy import text as sqlt

        def get_latest_form(team: str) -> dict:
            for perspective, t_col in [("home", "home_team"), ("away", "away_team")]:
                prefix = perspective
                row = pd.read_sql(f"""
                    SELECT DISTINCT ON ({t_col}) {t_col} AS team,
                        {prefix}_win_rate_last10 AS win_rate_last10,
                        {prefix}_draw_rate_last10 AS draw_rate_last10,
                        {prefix}_avg_goals_scored_last20 AS avg_goals_scored_last20,
                        {prefix}_avg_goals_conceded_last20 AS avg_goals_conceded_last20,
                        {prefix}_avg_goal_diff_last10 AS avg_goal_diff_last10,
                        {prefix}_goal_diff_trend AS goal_diff_trend,
                        {prefix}_ranking_proxy AS ranking_proxy,
                        {prefix}_ranking_change AS ranking_change
                    FROM match_features WHERE {t_col} = '{team}'
                    ORDER BY {t_col}, date DESC LIMIT 1
                """, engine)
                if not row.empty:
                    return row.iloc[0].to_dict()
            return {c: 0.33 if "rate" in c else 0.0 for c in [
                "win_rate_last10", "draw_rate_last10", "avg_goals_scored_last20",
                "avg_goals_conceded_last20", "avg_goal_diff_last10",
                "goal_diff_trend", "ranking_proxy", "ranking_change"
            ]}

        h_form = get_latest_form(home_team)
        a_form = get_latest_form(away_team)
        h2h    = load_h2h(home_team, away_team)
        ta     = min(home_team, away_team)
        h2h_wr = h2h["team_a_wins"] / h2h["total"] if h2h["total"] > 0 else 0.33
        if home_team != ta:
            h2h_wr = max(0.0, 1.0 - h2h_wr - 0.25)

        feat_row = {
            "home_win_rate_last10":           h_form["win_rate_last10"],
            "away_win_rate_last10":           a_form["win_rate_last10"],
            "home_draw_rate_last10":          h_form["draw_rate_last10"],
            "away_draw_rate_last10":          a_form["draw_rate_last10"],
            "home_avg_goals_scored_last20":   h_form["avg_goals_scored_last20"],
            "away_avg_goals_scored_last20":   a_form["avg_goals_scored_last20"],
            "home_avg_goals_conceded_last20": h_form["avg_goals_conceded_last20"],
            "away_avg_goals_conceded_last20": a_form["avg_goals_conceded_last20"],
            "home_avg_goal_diff_last10":      h_form["avg_goal_diff_last10"],
            "away_avg_goal_diff_last10":      a_form["avg_goal_diff_last10"],
            "home_goal_diff_trend":           h_form["goal_diff_trend"],
            "away_goal_diff_trend":           a_form["goal_diff_trend"],
            "home_ranking_proxy":             h_form["ranking_proxy"],
            "away_ranking_proxy":             a_form["ranking_proxy"],
            "home_ranking_change":            h_form["ranking_change"],
            "away_ranking_change":            a_form["ranking_change"],
            "h2h_team_a_win_rate":            h2h_wr,
            "h2h_total":                      h2h["total"],
        }
        X = scaler.transform(pd.DataFrame([feat_row])[FEATURE_COLS].fillna(0))
        proba  = model.predict_proba(X)[0]
        label  = int(model.predict(X)[0])
        fixture_pred = pd.DataFrame([{
            "home_team": home_team, "away_team": away_team,
            "predicted_result": LABEL_MAP_INV.get(str(label), "?"),
            "win_prob": proba[0], "draw_prob": proba[1], "loss_prob": proba[2],
            "confidence": max(proba),
            "group_name": "Custom",
        }])
    except Exception as e:
        st.error(f"Live prediction failed: {e}")

# ============================================================
# Main prediction section
# ============================================================
if not fixture_pred.empty:
    pred_row = fixture_pred.iloc[0]
    predicted = str(pred_row["predicted_result"])
    win_p     = float(pred_row["win_prob"])
    draw_p    = float(pred_row["draw_prob"])
    loss_p    = float(pred_row["loss_prob"])
    conf      = float(pred_row["confidence"])

    # Row 1: Gauge + Probability bar + Team form
    col_gauge, col_form_h, col_form_a = st.columns([2, 1.5, 1.5])

    with col_gauge:
        st.subheader("Predicted Outcome")
        outcome_label = {
            "W": f"{home_team} Wins",
            "D": "Draw",
            "L": f"{away_team} Wins"
        }.get(predicted, predicted)
        col_result, col_conf = st.columns(2)
        col_result.metric("Result", outcome_label)
        col_conf.metric("Confidence", f"{conf*100:.1f}%")
        st.plotly_chart(make_gauge_chart(win_p, draw_p, loss_p, predicted, home_team),
                        use_container_width=True)
        st.plotly_chart(make_prob_bar(win_p, draw_p, loss_p, home_team, away_team),
                        use_container_width=True)

    with col_form_h:
        st.subheader(f"{home_team} — Last 10")
        df_form_h = load_team_form(home_team)
        if not df_form_h.empty:
            badges_h = " ".join([form_badge(r) for r in df_form_h["team_result"]])
            st.markdown(badges_h, unsafe_allow_html=True)
            wins_h   = (df_form_h["team_result"] == "W").sum()
            draws_h  = (df_form_h["team_result"] == "D").sum()
            losses_h = (df_form_h["team_result"] == "L").sum()
            st.markdown(f"""
            <small>
            W: <b>{wins_h}</b> &nbsp; D: <b>{draws_h}</b> &nbsp; L: <b>{losses_h}</b><br>
            Win rate: <b>{wins_h/len(df_form_h)*100:.0f}%</b>
            </small>
            """, unsafe_allow_html=True)
            st.dataframe(
                df_form_h[["date", "home_team", "away_team", "home_score", "away_score"]],
                use_container_width=True, height=240, hide_index=True,
            )
        else:
            st.info("No recent match data.")

    with col_form_a:
        st.subheader(f"{away_team} — Last 10")
        df_form_a = load_team_form(away_team)
        if not df_form_a.empty:
            badges_a = " ".join([form_badge(r) for r in df_form_a["team_result"]])
            st.markdown(badges_a, unsafe_allow_html=True)
            wins_a   = (df_form_a["team_result"] == "W").sum()
            draws_a  = (df_form_a["team_result"] == "D").sum()
            losses_a = (df_form_a["team_result"] == "L").sum()
            st.markdown(f"""
            <small>
            W: <b>{wins_a}</b> &nbsp; D: <b>{draws_a}</b> &nbsp; L: <b>{losses_a}</b><br>
            Win rate: <b>{wins_a/len(df_form_a)*100:.0f}%</b>
            </small>
            """, unsafe_allow_html=True)
            st.dataframe(
                df_form_a[["date", "home_team", "away_team", "home_score", "away_score"]],
                use_container_width=True, height=240, hide_index=True,
            )
        else:
            st.info("No recent match data.")

    st.divider()

    # Row 2: H2H chart + Feature drivers
    col_h2h, col_drivers = st.columns(2)

    with col_h2h:
        h2h_data = load_h2h(home_team, away_team)
        st.plotly_chart(make_h2h_chart(h2h_data, home_team, away_team),
                        use_container_width=True)
        if h2h_data["total"] > 0:
            ta = h2h_data["team_a"]
            tb = h2h_data["team_b"]
            ta_w = h2h_data["team_a_wins"]
            tb_w = h2h_data["team_b_wins"]
            st.caption(
                f"All time: {ta} {ta_w}–{h2h_data['draws']}–{tb_w} {tb}"
            )
        else:
            st.caption("No prior meetings recorded.")

    with col_drivers:
        if model is not None and FEATURE_COLS:
            try:
                importance_raw = model.get_booster().get_score(importance_type="gain")
                feat_importance = {
                    f: importance_raw.get(f, importance_raw.get(f"f{i}", 0))
                    for i, f in enumerate(FEATURE_COLS)
                }
                # Build feature values from the first row of fixture_pred
                feat_lookup = {}
                for feat in FEATURE_COLS:
                    if feat in pred_row.index:
                        feat_lookup[feat] = float(pred_row[feat])
                    else:
                        feat_lookup[feat] = 0.0
                feat_series = pd.Series(feat_lookup)
                st.plotly_chart(
                    make_feature_drivers_chart(feat_series, feat_importance, outcome_label),
                    use_container_width=True
                )
            except Exception:
                st.info("Feature drivers require model artifacts in models/ folder.")
        else:
            st.info("Load model artifacts to see feature drivers.")

else:
    st.warning(
        f"No prediction found for {home_team} vs {away_team}. "
        f"Run the full pipeline (Phase 4 notebook or Airflow DAG) to generate predictions."
    )

# ============================================================
# Full Group Stage Predictions Table
# ============================================================
st.divider()
st.subheader("Full 2026 WC Group Stage Predictions")

if not df_preds.empty:
    # Format the table for display
    df_display = df_preds[[
        "group_name", "home_team", "away_team", "match_date",
        "predicted_result", "win_prob", "draw_prob", "loss_prob", "confidence"
    ]].copy()
    df_display["match_date"]       = pd.to_datetime(df_display["match_date"]).dt.strftime("%d %b")
    df_display["win_prob"]         = (df_display["win_prob"]   * 100).round(1).astype(str) + "%"
    df_display["draw_prob"]        = (df_display["draw_prob"]  * 100).round(1).astype(str) + "%"
    df_display["loss_prob"]        = (df_display["loss_prob"]  * 100).round(1).astype(str) + "%"
    df_display["confidence"]       = (df_display["confidence"] * 100).round(1).astype(str) + "%"
    df_display.columns             = ["Group", "Home Team", "Away Team", "Date",
                                       "Prediction", "Win%", "Draw%", "Loss%", "Confidence"]

    # Group filter
    groups = ["All"] + sorted(df_display["Group"].unique().tolist())
    selected_group = st.selectbox("Filter by Group", groups)
    if selected_group != "All":
        df_display = df_display[df_display["Group"] == selected_group]

    st.dataframe(df_display, use_container_width=True, hide_index=True, height=500)
    st.caption(f"Showing {len(df_display)} fixtures. Predictions updated: "
               f"{df_preds['predicted_at'].max()[:19] if 'predicted_at' in df_preds.columns else 'N/A'}")
else:
    st.info("No predictions found. Run the Phase 4 notebook or trigger the Airflow DAG.")

# ============================================================
# Footer
# ============================================================
st.divider()
st.markdown(
    "<small>Built with PySpark · dbt · XGBoost · Airflow · Streamlit · PostgreSQL · Docker "
    "| Powered by 150+ years of international football data</small>",
    unsafe_allow_html=True
)
