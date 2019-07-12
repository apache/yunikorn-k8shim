#!/bin/bash

# replace these env var values accordingly
SPARK_HOME=/Users/example/repository/spark
K8S_ENDPOINT=http://localhost:8001
SPARK_EXECUTOR_NUM=1
SPARK_DOCKER_IMAGE=yunikorn/spark:latest
SPARK_EXAMPLE_JAR=local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0-SNAPSHOT.jar

# spark submit command
${SPARK_HOME}/bin/spark-submit \
  --master k8s://${K8S_ENDPOINT} --deploy-mode cluster --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=${SPARK_EXECUTOR_NUM} \
  --conf spark.kubernetes.container.image=${SPARK_DOCKER_IMAGE} \
  --conf spark.kubernetes.driver.podTemplateFile=../driver.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=../executor.yaml \
  ${SPARK_EXAMPLE_JAR}
