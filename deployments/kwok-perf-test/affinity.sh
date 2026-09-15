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

RANDOM=0
OUTPUT_PATH="./output/affinity.yaml"
NUM_PODS=0
NUM_NODES=0
OPERATORS=("In" "NotIn")
OPERATORS_STATE=0
j=0

show_help() {
  cat << EOF
Invalid option: -$OPTARG
Usage: $0 <pod_count> <node_count>

Options:
  -o,             Specifies the location of the output yaml file (default is ./output/affinity.yaml)

Arguments:
  <pod_count>     Number of pod to create (required).
  <node_count>    Number of kwok nodes (required).
EOF
}

# Process command-line options
while getopts ":o:" opt; do
  case $opt in
    o)
      OUTPUT_PATH=$OPTARG
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

# Check if pod count and node count are provided
if [ $# -ne 2 ]; then
  show_help
  exit 1
fi

NUM_PODS=$1
NUM_NODES=$2

if [ -f "$OUTPUT_PATH" ]; then
  # clear origin content in file
  echo "" > "$OUTPUT_PATH"
  echo "The original content of the file located at $OUTPUT_PATH has been cleared."
else
  echo "The file $OUTPUT_PATH does not exist."
  mkdir -p "$(dirname "$OUTPUT_PATH")"
  touch "$OUTPUT_PATH"
  echo "The create $OUTPUT_PATH."
fi

# create pods assigned with random node affinity 
echo "Create $((NUM_PODS/2/2)) pods, each with random node affinity using the required rule."
echo "Create $((NUM_PODS/2/2)) pods, each with random node affinity using the preferred rule."
for (( ;j<NUM_PODS/2; j+=2))
do
  operator=${OPERATORS[$OPERATORS_STATE]}
  OPERATORS_STATE=$(( (OPERATORS_STATE + 1) % 2 ))
  randHost1=$((RANDOM % NUM_NODES))
  randHost2=$((RANDOM % NUM_NODES))
  randHost3=$((RANDOM % NUM_NODES))
  randHost4=$((RANDOM % NUM_NODES))
  cat <<EOF >> "$OUTPUT_PATH"
apiVersion: v1
kind: Pod
metadata:
  name: nginx-$j
  labels:
    applicationId: nginx-$j
spec:
  containers:
  - name: sleep300
    image: "alpine:latest"
    command: ["sleep", "0"]
    ports:
    - containerPort: 80
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/hostname
            operator: $operator
            values:
            - kwok-node-$randHost1
            - kwok-node-$randHost2
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
---
apiVersion: v1
kind: Pod
metadata:
  name: nginx-$((j+1))
  labels:
    applicationId: nginx-$((j+1))
spec:
  containers:
  - name: sleep300
    image: "alpine:latest"
    command: ["sleep", "0"]
    ports:
    - containerPort: 80
  affinity:
    nodeAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 1
          preference:
            matchExpressions:
            - key: kubernetes.io/hostname
              operator: $operator
              values:
              - kwok-node-$randHost3
              - kwok-node-$randHost4
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
---
EOF
done

# create pods assigned with random pod affinity 
echo "Create $((NUM_PODS-j)) pods, each with random node affinity using the preferred rule."
for (( ;j<NUM_PODS; j+=1))
do
  operator=${OPERATORS[$OPERATORS_STATE]}
  OPERATORS_STATE=$(( (OPERATORS_STATE + 1) % 2 ))
  randAppID1=$((RANDOM % NUM_PODS))
  randAppID2=$((RANDOM % NUM_PODS))
  cat <<EOF >> "$OUTPUT_PATH"
apiVersion: v1
kind: Pod
metadata:
  name: nginx-$((j))
  labels:
    applicationId: nginx-$((j))
spec:
  containers:
  - name: sleep300
    image: "alpine:latest"
    command: ["sleep", "0"]
    ports:
    - containerPort: 80
  affinity:
    podAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: applicationId
              operator: $operator
              values:
              - nginx-$randAppID1
              - nginx-$randAppID2
          topologyKey: kubernetes.io/role
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
---
EOF
done
