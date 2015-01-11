#! /bin/bash

#num-workers 代表使用的节点数 最多5个
#worker-cores 代表每个节点的CPU核数 最多12个
#worker-memory 节点使用的内存最多100000m

export YARN_CONF_DIR=/opt/hadoop-2.2.0/etc/hadoop

$SPARK_HOME/bin/spark-submit --class Closeness \
--master yarn-cluster \
--num-executors 8 \
--driver-memory 30g \
--executor-memory 30g \
--executor-cores 8 \
target/Closeness-1.0.jar \

