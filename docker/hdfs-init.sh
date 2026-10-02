#!/bin/sh
set -eu
attempt=0
while [ "$attempt" -lt 30 ]; do
  attempt=$((attempt + 1))
  if hdfs dfs -fs hdfs://namenode:9000 -mkdir -p /ecommerce /checkpoints; then
    hdfs dfs -fs hdfs://namenode:9000 -chmod 777 /ecommerce /checkpoints
    echo 'HDFS analytics and checkpoint directories are ready.'
    exit 0
  fi
  echo "HDFS is not writable yet (attempt $attempt/30); retrying in five seconds."
  sleep 5
done
echo 'HDFS initialization failed. Inspect NameNode safe mode and DataNode health.' >&2
exit 1
