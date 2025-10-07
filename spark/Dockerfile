FROM jupyter/pyspark-notebook:spark-3.4.1

USER root  

RUN pip install --no-cache-dir psycopg2-binary clickhouse-driver && \
    wget -q -O /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    wget -q -O /tmp/clickhouse-jdbc.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar && \
    mv /tmp/*.jar /usr/local/spark/jars/ && \
    chown jovyan:users /usr/local/spark/jars/

USER jovyan  

WORKDIR /opt/scripts
COPY scripts/ /opt/scripts/