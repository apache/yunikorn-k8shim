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

show_help() {
  cat << EOF
Invalid option: -$OPTARG
Usage: $0 [-d] [-i <interval>] <deployment_count> <replicas_count>

Options:
  -d, --delete               Delete the specified number of deployments.
  -i, --interval <interval>  Set the interval between deployments in seconds.

Arguments:
  <deployment_count>         Number of deployments to create or delete (required).
  <replicas_count>           Number of replicas for each deployment (required).
EOF
}

deploy_deployments() {
  for (( i=0; i<deployment_count; i++ )); do
    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleep-deployment-$i
  labels:
    app: sleep
    applicationId: "sleep-deployment-$i"
    queue: root.default
spec:
  replicas: $replicas_count
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
        applicationId: "sleep-deployment-$i"
        queue: root.default
    spec:
      containers:
      - name: sleep300
        image: "alpine:latest"
      tolerations:
      - key: "kwok.x-k8s.io/node"
        operator: "Exists"
        effect: "NoSchedule"
EOF
    sleep "$interval"
  done
}

delete_deployments(){
    for (( i=0; i<deployment_count; i++ )); do
        kubectl delete deploy/sleep-deployment-$i
    done
}

# Default values
delete=false
interval=0

# Process command-line options
while getopts ":di:" opt; do
  case $opt in
    d)
      delete=true
      ;;
    i)
      interval=$OPTARG
      ;;
    \?)
      show_help
      exit 1
      ;;
    :)
      show_help
      exit 1
      ;;
  esac
done

# Shift the processed options out of the command-line arguments
shift $((OPTIND-1))

# Check if deployment count and replicas count are provided
if [ $# -ne 2 ]; then
  show_help
  exit 1
fi

# Assign provided values to variables
deployment_count=$1
replicas_count=$2

# Check if delete flag is set
if [ "$delete" = true ]; then
  echo "Deleting $deployment_count deployments with $replicas_count replicas each."
  delete_deployments
else
  echo "Deploying $deployment_count deployments with $replicas_count replicas each."
  deploy_deployments 
fi

exit 0
