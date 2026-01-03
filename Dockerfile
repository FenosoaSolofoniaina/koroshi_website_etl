FROM apache/airflow:3.1.5

WORKDIR /opt/airflow

COPY ./requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt