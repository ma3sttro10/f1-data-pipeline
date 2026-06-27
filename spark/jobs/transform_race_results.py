"""
Job Spark : lit depuis Kafka f1.race_results, transforme et charge dans PostgreSQL.
Pattern : Kafka → Spark DataFrame → transformations → JDBC → PostgreSQL
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, TimestampType
)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# Schéma JSON du message Kafka (doit correspondre exactement au producer)
RACE_RESULT_SCHEMA = StructType([
    StructField("event_type",   StringType(),  True),
    StructField("year",         IntegerType(), True),
    StructField("gp_name",      StringType(),  True),
    StructField("round",        IntegerType(), True),
    StructField("driver_id",    StringType(),  True),
    StructField("driver_name",  StringType(),  True),
    StructField("team",         StringType(),  True),
    StructField("position",     IntegerType(), True),
    StructField("points",       FloatType(),   True),
    StructField("status",       StringType(),  True),
    StructField("fastest_lap",  StringType(),  True),
    StructField("ingested_at",  StringType(),  True),
])

POSTGRES_URL = "jdbc:postgresql://postgres:5432/f1db"
POSTGRES_PROPS = {
    "user":     os.environ.get("POSTGRES_USER", "f1user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "f1password"),
    "driver":   "org.postgresql.Driver",
}


def read_from_kafka(spark: SparkSession) -> "DataFrame":
    """
    Lit TOUS les messages depuis le début du topic (batch mode).
    Pour du vrai streaming, utiliser readStream.
    """
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "f1.race_results")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def parse_messages(raw_df: "DataFrame") -> "DataFrame":
    """
    Kafka retourne des bytes. On désérialise le JSON dans une colonne structurée.
    """
    return (
        raw_df
        .select(F.col("value").cast("string").alias("json_str"))
        .select(F.from_json(F.col("json_str"), RACE_RESULT_SCHEMA).alias("data"))
        .select("data.*")
    )


def transform(df: "DataFrame") -> "DataFrame":
    """
    Transformations métier :
    - Normalise driver_id en majuscules
    - Calcule le rang dans chaque équipe par saison
    - Ajoute une colonne winner (position == 1)
    - Nettoie les valeurs nulles
    """
    return (
        df
        .filter(F.col("driver_id").isNotNull())
        .filter(F.col("position").isNotNull())
        .withColumn("driver_id", F.upper(F.col("driver_id")))
        .withColumn("is_winner", (F.col("position") == 1).cast("boolean"))
        .withColumn("is_points_finish", (F.col("position") <= 10).cast("boolean"))
        .withColumn(
            "season_team_rank",
            F.rank().over(
                Window.partitionBy("year", "team")
                      .orderBy(F.desc("points"))
            )
        )
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .dropDuplicates(["year", "round", "driver_id"])  # Idempotence
    )


def load_dimensions(spark: SparkSession, df: "DataFrame") -> None:
    """
    Charge les dimensions (pilotes, circuits) dans PostgreSQL.
    UPSERT via mode 'append' + contrainte UNIQUE sur driver_id.
    """
    # dim_driver
    dim_driver = (
        df.select(
            F.col("driver_id"),
            F.col("driver_name"),
            F.col("team").alias("current_team"),
        )
        .dropDuplicates(["driver_id"])
    )
    (
        dim_driver.write
        .jdbc(POSTGRES_URL, "dim_driver", mode="append", properties=POSTGRES_PROPS)
    )

    # dim_circuit (GP Name → circuit)
    dim_circuit = (
        df.select(
            F.col("gp_name"),
            F.col("round"),
            F.col("year"),
        )
        .dropDuplicates(["year", "round"])
    )
    (
        dim_circuit.write
        .jdbc(POSTGRES_URL, "dim_circuit", mode="append", properties=POSTGRES_PROPS)
    )


def load_facts(df: "DataFrame") -> None:
    """Charge la table de faits principale."""
    fact_cols = [
        "year", "round", "gp_name", "driver_id", "team",
        "position", "points", "status",
        "is_winner", "is_points_finish", "ingested_at",
    ]
    (
        df.select(fact_cols)
        .write
        .jdbc(POSTGRES_URL, "fact_race_result", mode="append", properties=POSTGRES_PROPS)
    )


def main():
    from pyspark.sql import Window  # Import ici pour éviter les conflits

    spark = create_spark_session("F1-RaceResults-ETL")
    logger.info("Lecture depuis Kafka...")

    raw_df = read_from_kafka(spark)
    parsed_df = parse_messages(raw_df)
    transformed_df = transform(parsed_df)

    logger.info(f"Messages traités : {transformed_df.count()}")

    load_dimensions(spark, transformed_df)
    load_facts(transformed_df)

    logger.info("ETL race_results terminé.")
    spark.stop()


if __name__ == "__main__":
    main()