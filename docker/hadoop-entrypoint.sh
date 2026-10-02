#!/bin/sh
set -eu
cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<'EOF'
<configuration>
 <property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>
</configuration>
EOF
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<'EOF'
<configuration>
 <property><name>dfs.namenode.name.dir</name><value>file:///data/name</value></property>
 <property><name>dfs.datanode.data.dir</name><value>file:///data/data</value></property>
 <property><name>dfs.replication</name><value>1</value></property>
 <property><name>dfs.permissions.enabled</name><value>false</value></property>
 <property><name>dfs.namenode.rpc-address</name><value>namenode:9000</value></property>
 <property><name>dfs.namenode.http-address</name><value>0.0.0.0:9870</value></property>
 <property><name>dfs.datanode.hostname</name><value>datanode</value></property>
 <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
 <property><name>dfs.datanode.use.datanode.hostname</name><value>true</value></property>
</configuration>
EOF
if [ "$1" = "namenode" ]; then
 mkdir -p /data/name
 if [ ! -f /data/name/current/VERSION ]; then
   hdfs namenode -format -nonInteractive
 fi
 exec hdfs namenode
fi
mkdir -p /data/data
exec hdfs datanode
