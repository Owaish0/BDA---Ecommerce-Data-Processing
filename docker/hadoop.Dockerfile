FROM eclipse-temurin:11-jre-jammy
ARG HADOOP_VERSION=3.4.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates procps && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
 && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512" -o /tmp/hadoop.sha512 \
 && EXPECTED=$(grep -Eo '[a-fA-F0-9]{128}' /tmp/hadoop.sha512 | head -1) \
 && test -n "$EXPECTED" && echo "$EXPECTED  /tmp/hadoop.tar.gz" | sha512sum -c - \
 && tar -xzf /tmp/hadoop.tar.gz -C /opt && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm /tmp/hadoop.tar.gz /tmp/hadoop.sha512
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
COPY docker/hadoop-entrypoint.sh /entrypoint.sh
COPY docker/hdfs-init.sh /hdfs-init.sh
RUN sed -i 's/\r$//' /entrypoint.sh /hdfs-init.sh && chmod +x /entrypoint.sh /hdfs-init.sh
ENTRYPOINT ["/entrypoint.sh"]
