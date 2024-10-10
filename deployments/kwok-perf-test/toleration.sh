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
OUTPUT_PATH="./output/toleration.yaml"
NUM_PODS=0
NUM_NODES=0
ACTION=""

show_help() {
  cat << EOF
Invalid option: -$OPTARG
Usage: $0 <pod_count> <priorityClass_count>

Commands:
  -t,   Taint kwok nodes with its index
  -u,   Untaint kwok nodes with its index
  -o,   Specifies the location of the output yaml file (default is ./output/priority.yaml)

Arguments:
  <pod_count>     Number of pod to create (required).
  <node_count>    Number of kwok nodes (required).
EOF
}

taint(){
  for (( i=0;i<NUM_NODES;i++))
  do
    kubectl taint nodes kwok-node-$i key-$i=value-$i:NoSchedule
  done
}

untaint(){
  for (( i=0;i<NUM_NODES;i++))
  do
    kubectl taint nodes kwok-node-$i key-$i=value-$i:NoSchedule-
  done
}

createYaml(){
  if [ -f "$OUTPUT_PATH" ]; then
    # clear origin content in file
    echo "" > "$OUTPUT_PATH"
    echo "The original content of the file located at $OUTPUT_PATH has been cleared."
  else
    echo "The file $OUTPUT_PATH does not exist."
    mkdir -p "$(dirname "$OUTPUT_PATH")"
    touch "$OUTPUT_PATH"
    echo "Create file $OUTPUT_PATH."
  fi

  echo "Create $((NUM_PODS)) Pods and save them to a YAML file, each Pod with a randomly assigned toleration."
  for (( j=0;j<NUM_PODS; j++))
  do
    randValue=$((RANDOM % NUM_NODES))
  cat <<EOF >> "$OUTPUT_PATH"
apiVersion: v1
kind: Pod
metadata:
  name: nginx-$j
spec:
  containers:
  - name: sleep300
    image: "alpine:latest"
    command: ["sleep", "0"]
    ports:
    - containerPort: 80
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
  - key: key-$randValue
    operator: "Exists"
---
EOF
done
}

# Process command-line options
while getopts ":tuo:" opt; do
  case $opt in
    t)
      ACTION="taint"
      ;;
    u)
      ACTION="untaint"
      ;;
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

# Execute the chosen action
case $ACTION in
  taint)
    taint
    exit 0
    ;;
  untaint)
    untaint
    exit 0
    ;;
  "")
    createYaml
    ;;
  *)
    show_help
    ;;
esac

