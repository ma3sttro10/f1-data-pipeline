"""
Kafka Producer F1 — ingère les données FastF1 dans 4 topics.
On simule un "streaming" en envoyant course par course.
"""

import json
import time
import logging
from datetime import datetime

import fastf1
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"
TOPICS = ["f1.race_results", "f1.telemetry", "f1.pit_stops", "f1.weather"]

# Saisons et Grand Prix à ingérer (commencer par une saison récente complète)
SEASONS_GPS = {
    2023: list(range(1, 23)),  # 22 GP en 2023
    2024: list(range(1, 24)),  # 23 GP en 2024
}

# Active le cache FastF1 pour ne pas retélécharger
fastf1.Cache.enable_cache("/tmp/fastf1_cache")


def create_topics(bootstrap_servers: str, topics: list[str]) -> None:
    """Crée les topics Kafka s'ils n'existent pas."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics.keys()
    to_create = [
        NewTopic(t, num_partitions=3, replication_factor=1)
        for t in topics if t not in existing
    ]
    if to_create:
        futures = admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic créé : {topic}")
            except Exception as e:
                logger.warning(f"Topic {topic} existe déjà ou erreur : {e}")


def delivery_report(err, msg):
    """Callback appelé après chaque envoi Kafka."""
    if err:
        logger.error(f"Échec livraison : {err}")
    else:
        logger.debug(f"Message livré → {msg.topic()} [partition {msg.partition()}]")


def serialize(data: dict) -> bytes:
    """Sérialise un dict en JSON bytes."""
    return json.dumps(data, default=str).encode("utf-8")


def ingest_race_results(producer: Producer, session) -> int:
    """Envoie les résultats de course dans f1.race_results."""
    results = session.results
    if results is None or results.empty:
        return 0

    count = 0
    for _, row in results.iterrows():
        message = {
            "event_type": "race_result",
            "year": session.event["EventDate"].year,
            "gp_name": session.event["EventName"],
            "round": session.event["RoundNumber"],
            "driver_id": row.get("DriverId", ""),
            "driver_name": f"{row.get('FirstName', '')} {row.get('LastName', '')}",
            "team": row.get("TeamName", ""),
            "position": row.get("Position", None),
            "points": row.get("Points", 0),
            "status": row.get("Status", ""),
            "fastest_lap": str(row.get("FastestLapTime", "")),
            "ingested_at": datetime.utcnow().isoformat(),
        }
        producer.produce(
            "f1.race_results",
            key=f"{message['year']}_{message['round']}_{message['driver_id']}",
            value=serialize(message),
            callback=delivery_report,
        )
        count += 1

    producer.flush()
    return count


def ingest_pit_stops(producer: Producer, session) -> int:
    """Envoie les données de pit stops dans f1.pit_stops."""
    laps = session.laps
    # Filtre les tours avec un pit stop
    pit_laps = laps[laps["PitOutTime"].notna() | laps["PitInTime"].notna()].copy()

    count = 0
    for _, row in pit_laps.iterrows():
        message = {
            "event_type": "pit_stop",
            "year": session.event["EventDate"].year,
            "gp_name": session.event["EventName"],
            "round": session.event["RoundNumber"],
            "driver_id": row.get("Abbreviation", ""),
            "lap_number": int(row.get("LapNumber", 0)),
            "pit_duration_sec": row.get("PitOutTime", pd.NaT) - row.get("PitInTime", pd.NaT),
            "compound": row.get("Compound", ""),
            "tyre_life": int(row.get("TyreLife", 0)),
            "ingested_at": datetime.now(datetime.UTC).isoformat(),
        }
        # Convertit Timedelta en secondes float pour la sérialisation JSON
        if isinstance(message["pit_duration_sec"], pd.Timedelta):
            message["pit_duration_sec"] = message["pit_duration_sec"].total_seconds()
        else:
            message["pit_duration_sec"] = None

        producer.produce(
            "f1.pit_stops",
            key=f"{message['year']}_{message['round']}_{message['driver_id']}_{message['lap_number']}",
            value=serialize(message),
            callback=delivery_report,
        )
        count += 1

    producer.flush()
    return count


def ingest_telemetry(producer: Producer, session, sample_rate: int = 50) -> int:
    """
    Envoie la télémétrie dans f1.telemetry.
    ATTENTION : la télémétrie est massive (10k+ lignes/pilote).
    On sous-échantillonne pour le portfolio (1 point tous les N points).
    """
    laps = session.laps
    count = 0

    # Prend les 3 premiers pilotes (pour limiter le volume)
    drivers = laps["DriverId"].unique()[:3]

    for driver_id in drivers:
        try:
            driver_laps = laps.pick_driver(driver_id)
            telemetry = driver_laps.get_telemetry().iloc[::sample_rate]  # sous-échantillonnage

            for _, row in telemetry.iterrows():
                message = {
                    "event_type": "telemetry",
                    "year": session.event["EventDate"].year,
                    "gp_name": session.event["EventName"],
                    "driver_id": driver_id,
                    "speed_kmh": float(row.get("Speed", 0)),
                    "throttle_pct": float(row.get("Throttle", 0)),
                    "brake": bool(row.get("Brake", False)),
                    "drs": int(row.get("DRS", 0)),
                    "gear": int(row.get("nGear", 0)),
                    "rpm": float(row.get("RPM", 0)),
                    "x": float(row.get("X", 0)),
                    "y": float(row.get("Y", 0)),
                    "ingested_at": datetime.utcnow().isoformat(),
                }
                producer.produce(
                    "f1.telemetry",
                    key=f"{message['year']}_{message['gp_name']}_{driver_id}",
                    value=serialize(message),
                    callback=delivery_report,
                )
                count += 1

        except Exception as e:
            logger.warning(f"Télémétrie indisponible pour {driver_id} : {e}")

    producer.flush()
    return count


def ingest_weather(producer: Producer, session) -> int:
    """Envoie les données météo dans f1.weather."""
    try:
        weather = session.weather_data
    except Exception:
        return 0

    if weather is None or weather.empty:
        return 0

    count = 0
    for _, row in weather.iterrows():
        message = {
            "event_type": "weather",
            "year": session.event["EventDate"].year,
            "gp_name": session.event["EventName"],
            "air_temp_c": float(row.get("AirTemp", 0)),
            "track_temp_c": float(row.get("TrackTemp", 0)),
            "humidity_pct": float(row.get("Humidity", 0)),
            "wind_speed_ms": float(row.get("WindSpeed", 0)),
            "rainfall": bool(row.get("Rainfall", False)),
            "ingested_at": datetime.utcnow().isoformat(),
        }
        producer.produce(
            "f1.weather",
            key=f"{message['year']}_{message['gp_name']}",
            value=serialize(message),
            callback=delivery_report,
        )
        count += 1

    producer.flush()
    return count


def main():
    logger.info("Démarrage du producer F1...")

    # Attente que Kafka soit prêt (retry pattern)
    for attempt in range(10):
        try:
            create_topics(KAFKA_BOOTSTRAP, TOPICS)
            break
        except Exception as e:
            logger.warning(f"Kafka pas encore prêt ({attempt+1}/10) : {e}")
            time.sleep(10)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",              # Garantie de livraison maximale
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 5,            # Regroupe les messages pendant 5ms (throughput)
        "compression.type": "snappy",
    })

    for year, gp_list in SEASONS_GPS.items():
        for gp_round in gp_list:
            logger.info(f"Ingestion : {year} — GP #{gp_round}")
            try:
                session = fastf1.get_session(year, gp_round, "R")
                session.load(telemetry=True, weather=True, messages=False)

                n_results = ingest_race_results(producer, session)
                n_pits = ingest_pit_stops(producer, session)
                n_telemetry = ingest_telemetry(producer, session)
                n_weather = ingest_weather(producer, session)

                logger.info(
                    f"  → {n_results} résultats | {n_pits} pit stops | "
                    f"{n_telemetry} points télémétrie | {n_weather} météo"
                )
                time.sleep(2)  # Respecte l'API F1

            except Exception as e:
                logger.error(f"Erreur GP {year}#{gp_round} : {e}")
                continue

    logger.info("Ingestion terminée.")


if __name__ == "__main__":
    main()