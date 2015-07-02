#! /bin/bash

#num-workers 代表使用的节点数 最多5个
#worker-cores 代表每个节点的CPU核数 最多12个
#worker-memory 节点使用的内存最多100000m

export YARN_CONF_DIR=/opt/hadoop-2.2.0/etc/hadoop

$SPARK_HOME/bin/spark-submit --class InterestTFIDF \
--master dell01:7077 \
--driver-memory 20g \
--executor-memory 15g \
--total-executor-cores 72 \
../target/LiuShifeng-2.0.jar