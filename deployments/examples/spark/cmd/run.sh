#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
