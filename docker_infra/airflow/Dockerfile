FROM apache/airflow:slim-3.1.0-python3.12

COPY docker/airflow/requirements.txt .

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir pyspark==3.5.1 

WORKDIR /home/airflow