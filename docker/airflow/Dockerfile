FROM apache/airflow:3.1.8-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN curl -fsSL https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.11.txt -o /tmp/constraints-original.txt \
    && sed -E '/^(pyspark|py4j)==/d' /tmp/constraints-original.txt > /tmp/constraints.txt \
    && pip install --no-cache-dir --constraint /tmp/constraints.txt \
        apache-airflow==3.1.8 \
        apache-airflow-providers-fab==3.4.0 \
        apache-airflow-providers-amazon==9.22.0 \
        apache-airflow-providers-apache-spark==5.5.1 \
        apache-airflow-providers-postgres==6.6.0 \
        pyspark==4.0.5 \
    && pip check
