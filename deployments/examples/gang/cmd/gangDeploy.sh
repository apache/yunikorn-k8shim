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
#limitations under the License.
#

# gangDeploy.sh <job amount> <pod amount> <gang member> <task run time(sec)>
set -o errexit
set -o nounset
set -o pipefail

JOBAMOUNT=$1
GANGMEMBER=$2
RUNTIMESEC=$3

# create service
kubectl create -f <(cat << EOF
apiVersion: v1
kind: Service
metadata:
  name: gangservice
  labels:
    app: gang
spec:
  selector:
    app: gang
  type: ClusterIP
  ports:
  - protocol: TCP
    port: 8863
    targetPort: 8863
EOF) 
# create job counter web server
kubectl create -f <(cat << EOF
apiVersion: v1
kind: Pod
metadata:
  name: gangweb
  labels:
    app: gang
    queue: root.sandbox
spec:
  schedulerName: yunikorn
  containers:
    - name: gangweb
      image: apache/yunikorn:simulation-gang-coordinator-latest
      imagePullPolicy: Never
      ports:
        - containerPort: 8863
EOF)
# wait for web server to be running
until grep 'Running' <(kubectl get pod gangweb -o=jsonpath='{.status.phase}'); do
  sleep 1
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
EOF)
done