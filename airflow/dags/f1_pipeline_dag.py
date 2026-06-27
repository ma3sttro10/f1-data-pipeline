"""
DAG Airflow : orchestration du pipeline F1.
Stratégie : batch quotidien qui vérifie si un nouveau GP a eu lieu
et déclenche les jobs Spark correspondants.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

logger = logging.getLogger(__name__)

# ── Configuration du DAG ──────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Fonctions des tâches ──────────────────────────────────────

def check_new_race(**context) -> str:
    """
    Vérifie si un GP a eu lieu depuis la dernière exécution.
    Retourne le nom de la tâche suivante (branching).
    """
    import fastf1
    from datetime import date

    execution_date = context["ds"]  # Date d'exécution du DAG (YYYY-MM-DD)
    year = datetime.strptime(execution_date, "%Y-%m-%d").year

    fastf1.Cache.enable_cache("/tmp/fastf1_cache")

    try:
        schedule = fastf1.get_event_schedule(year)
        # Filtre les GP passés avant la date d'exécution
        past_events = schedule[schedule["EventDate"].dt.strftime("%Y-%m-%d") <= execution_date]

        if past_events.empty:
            logger.info("Aucun GP à traiter.")
            return "no_new_race"

        latest = past_events.iloc[-1]
        # Stocke le contexte dans XCom pour les tâches suivantes
        context["ti"].xcom_push(key="gp_round", value=int(latest["RoundNumber"]))
        context["ti"].xcom_push(key="gp_year",  value=int(latest["EventDate"].year))
        context["ti"].xcom_push(key="gp_name",  value=latest["EventName"])
        logger.info(f"GP à traiter : {latest['EventName']} {year}")
        return "run_producer"

    except Exception as e:
        logger.error(f"Erreur vérification GP : {e}")
        return "no_new_race"


def run_producer(**context) -> None:
    """Lance le producer FastF1 → Kafka pour le GP détecté."""
    import subprocess
    gp_round = context["ti"].xcom_pull(key="gp_round")
    gp_year  = context["ti"].xcom_pull(key="gp_year")

    result = subprocess.run(
        ["python", "/app/producer.py",
         "--year", str(gp_year),
         "--round", str(gp_round)],
        capture_output=True, text=True, timeout=600
    )
    if result.returncode != 0:
        raise RuntimeError(f"Producer échoué :\n{result.stderr}")
    logger.info(result.stdout)


def run_spark_job(job_name: str, **context) -> None:
    """Soumet un job Spark via spark-submit."""
    import subprocess
    result = subprocess.run(
        [
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1",
            f"/opt/spark_jobs/jobs/{job_name}.py",
        ],
        capture_output=True, text=True, timeout=900
    )
    if result.returncode != 0:
        raise RuntimeError(f"Spark job {job_name} échoué :\n{result.stderr}")
    logger.info(f"Job {job_name} terminé ✓")


# ── Définition du DAG ─────────────────────────────────────────
with DAG(
    dag_id="f1_pipeline",
    description="Pipeline complet F1 : ingestion → ETL → stockage",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * MON",  # Chaque lundi à 6h (lendemain des GP du dimanche)
    catchup=False,                     # Ne rejoue pas les exécutions passées
    tags=["f1", "data-engineering", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_race = BranchPythonOperator(
        task_id="check_new_race",
        python_callable=check_new_race,
    )

    no_new_race = EmptyOperator(task_id="no_new_race")

    producer_task = PythonOperator(
        task_id="run_producer",
        python_callable=run_producer,
    )

    spark_race_results = PythonOperator(
        task_id="spark_transform_race_results",
        python_callable=run_spark_job,
        op_kwargs={"job_name": "transform_race_results"},
    )

    spark_pit_stops = PythonOperator(
        task_id="spark_transform_pit_stops",
        python_callable=run_spark_job,
        op_kwargs={"job_name": "transform_pit_stops"},
    )

    spark_telemetry = PythonOperator(
        task_id="spark_transform_telemetry",
        python_callable=run_spark_job,
        op_kwargs={"job_name": "transform_telemetry"},
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # Termine même si branche "no_new_race"
    )

    # Dépendances
    start >> check_race >> [producer_task, no_new_race]
    producer_task >> [spark_race_results, spark_pit_stops, spark_telemetry]
    [spark_race_results, spark_pit_stops, spark_telemetry, no_new_race] >> end