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

# deploy.sh <job amount> <pod amount> <gang member> <job run time(min)>
set -o errexit
set -o nounset
set -o pipefail

JOBAMOUNT=$1
PODAMOUNT=$2
GANGMEMBER=$3
RUNTIMEMIN=$4

if [ $GANGMEMBER -gt $PODAMOUNT ]
    then
    GANGMEMBER=$PODAMOUNT
    echo "gangMember > podAmount"
    echo "Set GangMember to "$GANGMEMBER
fi

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
spec:
  containers:
    - name: gangweb
      image: apache/gang:webserver
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
spec:
  completions: $PODAMOUNT
  parallelism: $PODAMOUNT
  template:
    spec:
      containers:
      - name: gang
        image: apache/gang:client
        imagePullPolicy: Never
        env:
        - name: jobName
          value: gang-job-$i
        - name: serviceName
          value: gangservice
        - name: memberAmount
          value: "$GANGMEMBER"
        - name: runtimeMin
          value: "$RUNTIMEMIN"
      restartPolicy: Never
EOF)
done