#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [[ $# -gt 8 ]]; then
  echo "expecting >= 5 parameters for starting the spark jobs"
  exit 1
fi

MASTER_URL=$1
SPARK_IMAGE=$2
NAMESPACE=$3
SVC_ACC=$4
EXEC_COUNT=3
END=${6:-3}

if [[ -z "${SPARK_HOME}" ]]; then
  SPARK_SUBMIT_CMD="spark-submit"
else
  SPARK_SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit"
fi

for i in $(seq 1 "$END"); do
  nohup "$SPARK_SUBMIT_CMD" \
      --master k8s://"$MASTER_URL" \
      --deploy-mode cluster \
      --name spark-yk-example-"$i" \
      --conf spark.executor.instances=$EXEC_COUNT \
      --conf spark.kubernetes.container.image="$SPARK_IMAGE" \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SVC_ACC" \
      --conf spark.pyspark.python=python3 \
      --conf spark.pyspark.driver.python=python3 \
      --conf spark.kubernetes.file.upload.path=/opt/spark/upload-temp \
      --conf spark.kubernetes.driver.podTemplateFile=../testdata/spark_pod_template.yaml \
      --conf spark.kubernetes.executor.podTemplateFile=../testdata/spark_pod_template.yaml \
      --conf spark.kubernetes.namespace="$NAMESPACE" \
      --conf spark.kubernetes.driver.limit.cores=0.5 \
      --conf spark.kubernetes.driver.request.cores=0.1 \
      --conf spark.driver.memory=500m \
      --conf spark.driver.memoryOverhead=500m \
      --conf spark.kubernetes.executor.limit.cores=0.5 \
      --conf spark.kubernetes.executor.request.cores=0.1 \
      --conf spark.executor.memory=500m \
      --conf spark.executor.memoryOverhead=500m \
      local:///opt/spark/examples/src/main/python/pi.py \
      100 &
  sleep 2
done;
