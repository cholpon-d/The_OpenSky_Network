FROM jupyter/pyspark-notebook:spark-3.4.1

USER root  


RUN pip install --no-cache-dir psycopg2-binary clickhouse-driver && \
    wget -q -O /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    mv /tmp/postgresql.jar /usr/local/spark/jars/ && \
    chown jovyan:users /usr/local/spark/jars/postgresql.jar

USER jovyan  

WORKDIR /opt/scripts
COPY scripts/ /opt/scripts/