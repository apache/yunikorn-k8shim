#!/usr/bin/env bash

#
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
#

# gangDeploy.sh <job amount> <gang member> <task run time(sec)>
set -o errexit
set -o nounset
set -o pipefail

# check parameter is integer or not
if { [ "$1" -gt 0 ] && [ "$2" -gt 0 ] && [ "$3" -gt 0 ]; } 2>/dev/null
then
    JOBAMOUNT=$1
    GANGMEMBER=$2
    RUNTIMESEC=$3
else    
    echo "ERROR: input parameters must be an integer."
    exit 1
fi

TIMEOUT=0

# create service and job counter web server
WORKDIR=$(cd "$(dirname "$0")/../"; pwd)
if [[ -f $WORKDIR/gang-coordinator.yaml ]]; then
  kubectl apply -f "$WORKDIR"/gang-coordinator.yaml
else
  echo "ERROR: gang-coordinator.yaml is not found in path $WORKDIR"
  exit
fi

# wait for web server to be running
until grep -q 'Running' <(kubectl get pod gangweb -o=jsonpath='{.status.phase}'); do
  sleep 1
  TIMEOUT=$((TIMEOUT + 1))
  if [ $TIMEOUT -ge 20 ]; then
    echo "ERROR: Timeout for waiting web server"
    kubectl delete -f "$WORKDIR"/gang-coordinator.yaml
    exit
  fi
done

# create gang jobs
for i in $(seq "$JOBAMOUNT"); do
  kubectl create -f <(cat << EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: gang-job-$i
  labels: 
    app: gang
    queue: root.sandbox
spec:
  completions: $GANGMEMBER
  parallelism: $GANGMEMBER
  template:
    spec:
      containers:
      - name: gang
        image: apache/yunikorn:simulation-gang-worker-latest
        imagePullPolicy: Never
        env:
        - name: JOB_ID
          value: gang-job-$i
        - name: SERVICE_NAME
          value: gangservice
        - name: MEMBER_AMOUNT
          value: "$GANGMEMBER"
        - name: TASK_EXECUTION_SECONDS
          value: "$RUNTIMESEC"
      restartPolicy: Never
      schedulerName: yunikorn
EOF
)
done
