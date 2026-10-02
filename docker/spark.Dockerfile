FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu
USER root
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[pipeline]'
COPY jobs ./jobs
COPY scripts ./scripts
RUN mkdir -p /opt/spark/work-dir /tmp/ivy && chmod -R 777 /opt/spark/work-dir /tmp/ivy
ENV PYTHONPATH=/app/src
ENV PYSPARK_PYTHON=python3
ENV SPARK_NO_DAEMONIZE=true
ENTRYPOINT []
