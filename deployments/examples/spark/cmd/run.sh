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

#########################
# The command line help #
#########################
usage() {
    echo "Usage: $0"
    echo "   -h | --hadoop, hadoop-version"
    echo "   -s | --spark, spark version"
    echo "   -d | --directory, workspace directory"
    exit 1
}

case "$1" in
   --help)
       usage
       exit 0
       ;;
esac

while getopts ":h:s:d:-:" opt; do
  case $opt in
    h) SPARK_HADOOP_VERSION="$OPTARG"
    printf "Specified hadoop version is: %s\n" "$SPARK_HADOOP_VERSION"
    ;;
    s) SPARK_VERSION="$OPTARG"
    printf "Specified spark version is: %s\n" "$SPARK_VERSION"
    ;;
    d) WORK_SPACE_ROOT="$OPTARG"
    printf "Specified workspace directory is: %s\n" "$WORK_SPACE_ROOT"
    ;;
    -)
      case "$OPTARG" in
        hadoop)
          SPARK_HADOOP_VERSION="${!OPTIND}"; OPTIND=$(( OPTIND + 1 ))
          printf "Specified hadoop version is: %s\n" "$SPARK_HADOOP_VERSION"
          ;;
        spark)
          SPARK_VERSION="${!OPTIND}"; OPTIND=$(( OPTIND + 1 ))
          printf "Specified hadoop version is %s\n" "$SPARK_VERSION"
          ;;
        directory)
          WORK_SPACE_ROOT="${!OPTIND}"; OPTIND=$(( OPTIND + 1 ))
          printf "Specified workspace directory is: %s\n" "$WORK_SPACE_ROOT"
          ;;
        *) echo "Invalid option --$OPTARG" >&2
          ;;
    esac ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

if [ -z "$WORK_SPACE_ROOT" ]; then
  SCRIPT_PATH=$(cd "$(dirname "$0")" || pwd)
  WORK_SPACE_ROOT=$(dirname "$SCRIPT_PATH")
  echo "Using default workspace dir: $WORK_SPACE_ROOT"
fi

if [ -z "$SPARK_HADOOP_VERSION" ]; then
  SPARK_HADOOP_VERSION=2.7
  echo "Using default spark hadoop version: $SPARK_HADOOP_VERSION"
fi

if [ -z "$SPARK_VERSION" ]; then
  SPARK_VERSION=2.4.4
  echo "Using default spark version: $SPARK_VERSION"
fi

SPARK_BINARY_FILE_NAME=spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz
SPARK_BINARY_FILE_PATH=$WORK_SPACE_ROOT/$SPARK_BINARY_FILE_NAME
SPARK_BINARY_FILE_CHECKSUM_FILE_NAME=$SPARK_BINARY_FILE_NAME.sha512
SPARK_BINARY_FILE_CHECKSUM_FILE_PATH=$WORK_SPACE_ROOT/$SPARK_BINARY_FILE_CHECKSUM_FILE_NAME
FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_NAME=$SPARK_BINARY_FILE_CHECKSUM_FILE_NAME.formatted
FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_PATH=$WORK_SPACE_ROOT/$FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_NAME

SPARK_HOME=$WORK_SPACE_ROOT/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}
SPARK_EXAMPLE_JAR=local://$SPARK_HOME/examples/jars/spark-examples_2.12-${SPARK_VERSION}.jar

K8S_ENDPOINT=http://localhost:8001
SPARK_EXECUTOR_NUM=1
SPARK_DOCKER_IMAGE=yunikorn/spark:$SPARK_VERSION

if [ -f "$SPARK_BINARY_FILE_PATH" ]; then
  echo "The binary file $SPARK_BINARY_FILE_NAME has been cached!"
else
  echo "The binary file $SPARK_BINARY_FILE_NAME did not exist, try to download."
  wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_BINARY_FILE_NAME} -O "${SPARK_BINARY_FILE_PATH}"
fi

if [ -f "$SPARK_BINARY_FILE_CHECKSUM_FILE_PATH" ]; then
  echo "The binary checksum file $SPARK_BINARY_FILE_CHECKSUM_FILE_NAME has been cached!"
else
  echo "The binary checksum file $SPARK_BINARY_FILE_CHECKSUM_FILE_NAME did not exist, try to download."
  wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_BINARY_FILE_CHECKSUM_FILE_NAME} -O "${SPARK_BINARY_FILE_CHECKSUM_FILE_PATH}"
fi

if [ -f "$FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_PATH" ]; then
  echo "The formatted binary checksum file $FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_NAME has been cached!"
else
  # format the official checksum file for verifying
  echo "The formatted binary checksum file $FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_NAME did not exist, try to generate."
  < "$SPARK_BINARY_FILE_CHECKSUM_FILE_PATH" tr -d " \t\n\r" | awk -v awkvar="$SPARK_BINARY_FILE_PATH" -F: '{print $2 "  " awkvar}' > "$FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_PATH"
fi

# check signature to verify the completeness
if [[ 'OK' == $(shasum -c -a 512 "$FORMATTED_SPARK_BINARY_FILE_CHECKSUM_FILE_PATH"  | awk '{print $2}') ]]; then
  echo "The checksum is matched!"
  echo "Try to remove the old unpacked dir and re-uncompress it"
  rm -rf "$WORK_SPACE_ROOT"/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}
  tar -xvzf "$SPARK_BINARY_FILE_PATH" -C "$WORK_SPACE_ROOT"
else
  echo "The checksum is not matched, Removing the incompleted file, please download it again."
  rm -f "$SPARK_BINARY_FILE_PATH"
  exit 0
fi

# spark submit command
"${SPARK_HOME}"/bin/spark-submit \
  --master k8s://${K8S_ENDPOINT} --deploy-mode cluster --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=${SPARK_EXECUTOR_NUM} \
  --conf spark.kubernetes.container.image=${SPARK_DOCKER_IMAGE} \
  --conf spark.kubernetes.driver.podTemplateFile=../driver.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=../executor.yaml \
  "${SPARK_EXAMPLE_JAR}"
