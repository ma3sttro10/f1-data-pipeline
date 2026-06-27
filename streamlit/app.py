"""
Dashboard F1 — Streamlit
Visualise les données analytiques depuis PostgreSQL.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# ── Configuration page ─────────────────────────────────────────
st.set_page_config(
    page_title="F1 Data Pipeline — Portfolio",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Connexion PostgreSQL (cachée = une seule connexion) ────────
@st.cache_resource
def get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ.get('POSTGRES_HOST', 'localhost')}:5432/"
        f"{os.environ['POSTGRES_DB']}"
    )
    return create_engine(url)

@st.cache_data(ttl=300)  # Cache 5 minutes (évite les requêtes répétées)
def query(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ── Sidebar ───────────────────────────────────────────────────
st.sidebar.title("🏎️ F1 Data Pipeline")
st.sidebar.markdown("_Portfolio Data Engineering_")

years = query("SELECT DISTINCT year FROM fact_race_result ORDER BY year DESC")
selected_year = st.sidebar.selectbox("Saison", years["year"].tolist(), index=0)

page = st.sidebar.radio(
    "Navigation",
    ["Classement pilotes", "Stratégies pit stops", "Comparaison pilotes", "Pipeline info"]
)


# ── Page 1 : Classement pilotes ───────────────────────────────
if page == "Classement pilotes":
    st.title(f"🏆 Classement Pilotes {selected_year}")

    standings = query(
        "SELECT * FROM vw_driver_standings WHERE year = :year ORDER BY championship_rank",
        year=selected_year
    )

    if standings.empty:
        st.warning("Aucune donnée disponible pour cette saison.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Champion",     standings.iloc[0]["driver_name"])
        col2.metric("Points max",   standings.iloc[0]["total_points"])
        col3.metric("Victoires",    standings.iloc[0]["wins"])

        # Bar chart
        fig = px.bar(
            standings.head(10),
            x="driver_name", y="total_points",
            color="wins",
            color_continuous_scale="Reds",
            title=f"Top 10 pilotes — Championnat {selected_year}",
            labels={"driver_name": "Pilote", "total_points": "Points", "wins": "Victoires"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # Tableau détaillé
        st.dataframe(
            standings[["championship_rank", "driver_name", "total_points", "wins", "points_finishes"]]
            .rename(columns={
                "championship_rank": "#",
                "driver_name": "Pilote",
                "total_points": "Points",
                "wins": "Victoires",
                "points_finishes": "Top 10",
            }),
            hide_index=True,
            use_container_width=True,
        )


# ── Page 2 : Stratégies pit stops ────────────────────────────
elif page == "Stratégies pit stops":
    st.title(f"🔧 Stratégies Pit Stops {selected_year}")

    gps = query(
        "SELECT DISTINCT gp_name FROM fact_pit_stop WHERE year = :year ORDER BY gp_name",
        year=selected_year
    )
    if gps.empty:
        st.warning("Pas de données pit stops disponibles.")
    else:
        selected_gp = st.selectbox("Grand Prix", gps["gp_name"].tolist())

        strategy = query(
            """
            SELECT *
            FROM vw_pit_stop_strategy
            WHERE year = :year AND gp_name = :gp
            ORDER BY total_stops, avg_pit_duration_sec
            """,
            year=selected_year, gp=selected_gp
        )

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Durée moyenne des arrêts")
            fig = px.bar(
                strategy.head(20),
                x="driver_id", y="avg_pit_duration_sec",
                color="total_stops",
                title="Durée pit stop par pilote (secondes)",
                labels={"driver_id": "Pilote", "avg_pit_duration_sec": "Durée moy. (s)"},
                color_continuous_scale="Blues",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Stratégies pneumatiques")
            st.dataframe(
                strategy[["driver_id", "total_stops", "fastest_pit_sec", "tyre_strategy"]]
                .rename(columns={
                    "driver_id": "Pilote",
                    "total_stops": "Arrêts",
                    "fastest_pit_sec": "Meilleur arrêt (s)",
                    "tyre_strategy": "Stratégie",
                }),
                hide_index=True,
                use_container_width=True,
            )


# ── Page 3 : Comparaison pilotes ─────────────────────────────
elif page == "Comparaison pilotes":
    st.title(f"📊 Comparaison Pilotes {selected_year}")

    all_drivers = query(
        "SELECT DISTINCT driver_id FROM fact_race_result WHERE year = :year ORDER BY driver_id",
        year=selected_year
    )["driver_id"].tolist()

    col1, col2 = st.columns(2)
    driver1 = col1.selectbox("Pilote A", all_drivers, index=0)
    driver2 = col2.selectbox("Pilote B", all_drivers, index=min(1, len(all_drivers)-1))

    if driver1 == driver2:
        st.warning("Sélectionne deux pilotes différents.")
    else:
        comparison_sql = """
            SELECT
                round,
                gp_name,
                driver_id,
                position,
                points
            FROM fact_race_result
            WHERE year = :year
              AND driver_id IN (:d1, :d2)
            ORDER BY round
        """
        df = query(comparison_sql, year=selected_year, d1=driver1, d2=driver2)

        fig = px.line(
            df, x="round", y="points",
            color="driver_id",
            markers=True,
            title=f"Points par GP — {driver1} vs {driver2}",
            labels={"round": "GP #", "points": "Points", "driver_id": "Pilote"},
            color_discrete_sequence=["#E10600", "#1E41FF"],
        )
        st.plotly_chart(fig, use_container_width=True)

        # Radar chart (positions moyennes, victoires, etc.)
        stats = (
            df.groupby("driver_id").agg(
                avg_position=("position", "mean"),
                total_points=("points", "sum"),
                wins=("position", lambda x: (x == 1).sum()),
                podiums=("position", lambda x: (x <= 3).sum()),
            )
            .reset_index()
        )
        st.dataframe(stats, hide_index=True, use_container_width=True)


# ── Page 4 : Info pipeline ────────────────────────────────────
elif page == "Pipeline info":
    st.title("🔧 Informations Pipeline")

    counts = query("""
        SELECT
            (SELECT COUNT(*) FROM fact_race_result) AS nb_race_results,
            (SELECT COUNT(*) FROM fact_pit_stop)    AS nb_pit_stops,
            (SELECT COUNT(*) FROM fact_weather)     AS nb_weather,
            (SELECT COUNT(*) FROM dim_driver)       AS nb_drivers,
            (SELECT COUNT(*) FROM dim_circuit)      AS nb_circuits
    """)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Résultats courses", counts.iloc[0]["nb_race_results"])
    c2.metric("Pit stops",         counts.iloc[0]["nb_pit_stops"])
    c3.metric("Points météo",      counts.iloc[0]["nb_weather"])
    c4.metric("Pilotes",           counts.iloc[0]["nb_drivers"])
    c5.metric("Circuits",          counts.iloc[0]["nb_circuits"])

    st.markdown("---")
    st.subheader("Stack technique")
    stack = {
        "Ingestion": "FastF1 3.3 → Apache Kafka 7.5",
        "Traitement": "Apache Spark 3.5",
        "Orchestration": "Apache Airflow 2.8",
        "Stockage": "PostgreSQL 15 (schéma en étoile)",
        "Visualisation": "Streamlit 1.32 + Plotly",
        "Infra": "Docker Compose",
    }
    for layer, tech in stack.items():
        st.markdown(f"**{layer}** — `{tech}`")