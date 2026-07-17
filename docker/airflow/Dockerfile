FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

COPY --chown=airflow:airflow . /opt/airflow/project

ENV PYTHONPATH="/opt/airflow/project"
ENV AIRFLOW__CORE__DAGS_FOLDER="/opt/airflow/project/dags"
ENV AIRFLOW__CORE__LOAD_EXAMPLES="false"
