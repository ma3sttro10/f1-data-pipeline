from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "F1Pipeline") -> SparkSession:
    """
    Crée une SparkSession configurée pour lire depuis Kafka
    et écrire dans PostgreSQL.
    Les JARs Kafka + PostgreSQL sont chargés depuis Maven.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1"
        )
        .config("spark.sql.shuffle.partitions", "4")  # Réduit pour local (défaut = 200)
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )