FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu
USER root
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN python3 -m pip install --no-cache-dir '.[pipeline]' \
 && python3 -c "import psycopg, confluent_kafka; print('Spark runtime dependencies verified')"
COPY jobs ./jobs
COPY scripts ./scripts
COPY tests ./tests
RUN mkdir -p /opt/spark/work-dir /tmp/ivy && chmod -R 777 /opt/spark/work-dir /tmp/ivy
ENV PYTHONPATH=/app/src
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_NO_DAEMONIZE=true
ENTRYPOINT []
