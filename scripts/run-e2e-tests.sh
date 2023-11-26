#!/usr/bin/env bash

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

HELM=tools/helm
KIND=tools/kind
KUBECTL=tools/kubectl
GO="${GO:-go}"
export GO

function check_cmd() {
  CMD=$1
  if ! command -v "${CMD}" &> /dev/null
  then
    echo "command ${CMD} could not be found"
    exit 1
  fi
}

function exit_on_error() {
  CMD_CODE=$?
  ERR_MSG=$1
  if [[ ${CMD_CODE} -ne 0 ]]; then
    echo "command execution failed: ${ERR_MSG}"
    exit ${CMD_CODE}
  fi
}

# check options that must have values
function check_opt() {
  OPTION=$1
  VALUE=$2
  if [[ "${VALUE}" == "" ]]; then
    echo "ERROR: option ${OPTION} cannot be empty"
    echo
    print_usage
    exit 1
  fi
}

# only support linux and darwin to run e2e tests
function check_os() {
  if [ "${OS}" != "linux" ] && [ "${OS}" != "darwin" ]; then
    echo "unsupported OS: ${OS}"
    exit 1
  fi
}

# check docker available and up
function check_docker() {
  check_cmd "docker"
  DOCKER_UP=$(docker version | grep "^Server:")
  if [ -z "${DOCKER_UP}" ]; then
    echo "docker daemon must be running"
    return 1
  fi
}

function install_tools() {
  make tools
}

function install_cluster() {
  echo "step 1/7: checking required configuration"
  if [ ! -r "${KIND_CONFIG}" ]; then
    exit_on_error "kind config not found: ${KIND_CONFIG}"
  fi

  echo "step 2/7: install tools"
  install_tools

  # use latest helm charts from the release repo to install yunikorn unless path is provided
  if [ "${GIT_CLONE}" = "true" ]; then
    check_cmd "git"
    rm -rf ./build/yunikorn-release
    # TODO: update following branch if PR in yunikorn-release is merged
    git clone --depth 1 https://github.com/FrankYang0529/yunikorn-release.git -b YUNIKORN-2135 ./build/yunikorn-release
  fi
  if [ ! -d "${CHART_PATH}" ]; then
    exit_on_error "helm charts not found in path: ${CHART_PATH}"
  fi

  # build docker images from latest code, so that we can install yunikorn with these latest images
  echo "step 3/7: building docker images from latest code"
  check_docker
  QUIET="--quiet" REGISTRY=local VERSION=latest ENABLE_GO_COVER_DIR=TRUE make image
  exit_on_error "build docker images failed"
  QUIET="--quiet" REGISTRY=local VERSION=latest make webtest_image
  exit_on_error "build test web images failed"

  # install ginkgo and gomega for e2e tests.
  echo "step 4/7: installing Ginkgo & Gomega at $("${GO}" env GOPATH)/bin"
  "${GO}" install github.com/onsi/ginkgo/v2/ginkgo@v2.9.1
  check_cmd "ginkgo"

  # create K8s cluster
  echo "step 5/7: installing K8s cluster using kind"
  "${KIND}" create cluster --name "${CLUSTER_NAME}" --image "${CLUSTER_VERSION}" --config="${KIND_CONFIG}"
  exit_on_error "install K8s cluster failed"
  "${KUBECTL}" cluster-info --context kind-"${CLUSTER_NAME}"
  exit_on_error "set K8s cluster context failed"
  "${KUBECTL}" create namespace yunikorn
  exit_on_error "failed to create yunikorn namespace"
  echo "cluster node definitions:"
  "${KUBECTL}" describe nodes

  # pre-load yunikorn docker images to kind
  echo "step 6/7: pre-load yunikorn images"
  "${KIND}" load docker-image "local/yunikorn:${SCHEDULER_IMAGE}" --name "${CLUSTER_NAME}"
  exit_on_error "pre-load scheduler image failed: ${SCHEDULER_IMAGE}"
  "${KIND}" load docker-image "local/yunikorn:${ADMISSION_IMAGE}" --name "${CLUSTER_NAME}"
  exit_on_error "pre-load admission controller image failed: ${ADMISSION_IMAGE}"
  "${KIND}" load docker-image "local/yunikorn:${WEBTEST_IMAGE}" --name "${CLUSTER_NAME}"
  exit_on_error "pre-load web image failed: ${WEBTEST_IMAGE}"

  echo "step 7/7: installing yunikorn"
  "${HELM}" install yunikorn "${CHART_PATH}" --namespace yunikorn \
    --set image.repository=local/yunikorn \
    --set image.tag="${SCHEDULER_IMAGE}" \
    --set image.pullPolicy=IfNotPresent \
    --set admissionController.image.repository=local/yunikorn \
    --set admissionController.image.tag="${ADMISSION_IMAGE}" \
    --set admissionController.image.pullPolicy=IfNotPresent \
    --set web.image.repository=local/yunikorn \
    --set web.image.tag="${WEBTEST_IMAGE}" \
    --set web.image.pullPolicy=IfNotPresent \
    --set enableGoCoverDir=true
  exit_on_error "failed to install yunikorn"
  "${KUBECTL}" wait --for=condition=available --timeout=300s deployment/yunikorn-scheduler -n yunikorn
  exit_on_error "failed to wait for yunikorn scheduler deployment being deployed"
  "${KUBECTL}" wait --for=condition=ready --timeout=300s pod -l app=yunikorn -n yunikorn
  exit_on_error "failed to wait for yunikorn scheduler pods being deployed"
}

function uninstall_yunikorn() {
  echo "uninstall yunikorn"
  install_tools
  "${HELM}" uninstall yunikorn --namespace yunikorn --wait --cascade foreground
  exit_on_error "failed to uninstall yunikorn"
}

function copy_go_cover_dir() {
  echo "copy coverage profile files from kind containers to local go-cover-dir-merged directory"
  install_tools
  mkdir -p go-cover-dir-merged
  GO_COVER_DIR_FOLDERS=()
  for i in $("${KIND}" get nodes --name "${CLUSTER_NAME}"); do
    if docker exec -i "$i" test -d /go-cover-dir
    then
      docker cp "$i":/go-cover-dir "$i"
      GO_COVER_DIR_FOLDERS+=("$i")
    fi
  done

  if [ ${#GO_COVER_DIR_FOLDERS[@]} -ne 0 ]; then
    GO_COVER_DIR_STR=$(printf ",%s" "${GO_COVER_DIR_FOLDERS[@]}")
    GO_COVER_DIR_STR=${GO_COVER_DIR_STR:1}
    go tool covdata merge -i="${GO_COVER_DIR_STR}" -o go-cover-dir-merged
    for i in "${GO_COVER_DIR_FOLDERS[@]}"; do
      rm -rf "$i"
    done
  fi
}

function delete_cluster() {
  echo "deleting K8s cluster: ${CLUSTER_NAME}"
  install_tools
  "${KIND}" delete cluster --name "${CLUSTER_NAME}"
  exit_on_error "failed to delete the cluster"
}

function print_usage() {
  NAME=$(basename "$0")
  cat <<EOF
Usage: ${NAME} -a <action> -n <kind-cluster-name> -v <kind-node-image-version> [-p <chart-path>] [--plugin]
  <action>                     the action to be executed, must be either "test" or "cleanup".
  <kind-cluster-name>          the name of the K8s cluster to be created by kind
  <kind-node-image-version>    the kind node image used to provision the K8s cluster, required for "test" action
  <chart-path>                 local path to helm charts path (default is to pull from GitHub master)
  --plugin                     use scheduler plugin image instead of default mode image

Examples:
  ${NAME} -a test -n yk8s -v kindest/node:v1.24.15
  ${NAME} -a test -n yk8s -v kindest/node:v1.25.11
  ${NAME} -a test -n yk8s -v kindest/node:v1.26.6
  ${NAME} -a test -n yk8s -v kindest/node:v1.27.3
  ${NAME} -a test -n yk8s -v kindest/node:v1.28.0

  Use a local helm chart path:
    ${NAME} -a test -n yk8s -v kindest/node:v1.28.0 -p ../yunikorn-release/helm-charts/yunikorn
EOF
}

# setup architectures and OS type
check_cmd "${GO}"
check_cmd "make"
eval "$(make arch)"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
check_os

KIND_CONFIG=./scripts/kind.yaml
CHART_PATH="./build/yunikorn-release/helm-charts/yunikorn"
GIT_CLONE=true
SCHEDULER_IMAGE="scheduler-${DOCKER_ARCH}-latest"
ADMISSION_IMAGE="admission-${DOCKER_ARCH}-latest"
WEBTEST_IMAGE="webtest-${DOCKER_ARCH}-latest"

while [[ $# -gt 0 ]]; do
key="$1"
case ${key} in
  -a|--action)
    ACTION="$2"
    shift
    shift
    ;;
  -n|--cluster-name)
    CLUSTER_NAME="$2"
    shift
    shift
    ;;
  -v|--cluster-version)
    CLUSTER_VERSION="$2"
    shift
    shift
    ;;
  -p|--charts-path)
    CHART_PATH="$2"
    GIT_CLONE=false
    shift
    shift
    ;;
  --plugin)
    SCHEDULER_IMAGE="scheduler-plugin-${DOCKER_ARCH}-latest"
    shift
    ;;
  -h|--help)
    print_usage
    exit 0
    ;;
  *)
    echo "unknown option: ${key}"
    print_usage
    exit 1
    ;;
esac
done

echo "e2e test run details"
echo "  action             : ${ACTION}"
echo "  kind cluster name  : ${CLUSTER_NAME}"
echo "  kind node version  : ${CLUSTER_VERSION}"
echo "  kind config        : ${KIND_CONFIG}"
echo "  git clone release  : ${GIT_CLONE}"
echo "  chart path         : ${CHART_PATH}"
echo "  operating system   : ${OS}"
echo "  processor arch     : ${EXEC_ARCH}"
echo "  docker arch        : ${DOCKER_ARCH}"
echo "  scheduler image    : ${SCHEDULER_IMAGE}"
echo "  admission image    : ${ADMISSION_IMAGE}"
echo "  web image          : ${WEBTEST_IMAGE}"
check_opt "action" "${ACTION}"
check_opt "kind-cluster-name" "${CLUSTER_NAME}"

# this script only supports 3 actions
#   1) test
#     - install a K8s cluster with kind
#     - install latest yunikorn
#     - run e2e tests
#   2) cleanup
#     - delete k8s cluster
#   3) install
#     - install a K8s cluster with kind
#     - install latest yunikorn
if [ "${ACTION}" == "test" ]; then
  # make will fail without go installed but we call it before that...
  check_cmd "${GO}"
  check_opt "kind-node-image-version" "${CLUSTER_VERSION}"
  check_opt "chart-path" "${CHART_PATH}"
  install_cluster
  echo "starting e2e tests"
  # Noticed regular unexplained failures in the tests when run directly after
  # the install. Running the test, via make, on the installed kind cluster
  # following the failed run passes. A short sleep seems to settle things down
  # and prevent the unexplained failures.
  if [ "${OS}" == "darwin" ]; then
    sleep 5
  fi
  make e2e_test
  exit_on_error "e2e tests failed"
  uninstall_yunikorn
  copy_go_cover_dir
elif [ "${ACTION}" == "install" ]; then
  check_cmd "${GO}"
  check_opt "kind-node-image-version" "${CLUSTER_VERSION}"
  check_opt "chart-path" "${CHART_PATH}"
  install_cluster
elif [ "${ACTION}" == "cleanup" ]; then
  echo "cleaning up the environment"
  delete_cluster
else
  echo "unknown action: ${ACTION}"
  print_usage
  exit 1
fi
