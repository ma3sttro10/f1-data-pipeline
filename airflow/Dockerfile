FROM apache/airflow:2.8.4-python3.11
USER root
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir fastf1==3.3.9