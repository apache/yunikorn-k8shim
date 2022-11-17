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

function install_kubectl() {
  if ! command -v kubectl &> /dev/null
  then
    check_cmd "curl"
    STABLE_RELEASE=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    exit_on_error "unable to retrieve latest stable version of kubectl"
    curl -LO https://storage.googleapis.com/kubernetes-release/release/"${STABLE_RELEASE}"/bin/"${OS}"/"${EXEC_ARCH}"/kubectl \
              && chmod +x kubectl && sudo mv kubectl /usr/local/bin/
    exit_on_error "install Kubectl failed"
  fi
  check_cmd "kubectl"
}

function install_kind() {
  KIND="$(go env GOPATH)/bin/kind"
  if [ ! -x "${KIND}" ]; then
    FORCE_KIND_INSTALL=true
  else
    # check kind version: v0.15.0 or later required for 1.25 tests
    KIND_MINOR=$(${KIND} version | cut -f2 -d" " | cut -f2 -d".")
    if [ "${KIND_MINOR}" -lt 15 ]; then
      FORCE_KIND_INSTALL=true
      echo "kind version found is too old: force new install"
    fi
  fi

  if [ "${FORCE_KIND_INSTALL}" == "true" ]
  then
    check_cmd "curl"
    curl -Lo ./kind "https://kind.sigs.k8s.io/dl/v0.15.0/kind-${OS}-${EXEC_ARCH}" \
      && chmod +x ./kind && mv ./kind "$(go env GOPATH)"/bin
    exit_on_error "install KIND failed"
  fi
  check_cmd "${KIND}"
}

function install_helm() {
  FORCE_HELM_INSTALL=false
  if ! command -v helm &> /dev/null
  then
    FORCE_HELM_INSTALL=true
  else
    # get the major helm version, must be v3
    HELM_VERSION=$(helm version --short | cut -f1 -d".")
    if [ "${HELM_VERSION}" != "v3" ]; then
      FORCE_HELM_INSTALL=true
      echo "helm version found is too old: force new install"
    fi
  fi

  if [ "${FORCE_HELM_INSTALL}" == "true" ]
  then
    check_cmd "curl"
    curl -L https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash
    exit_on_error "install helm-v3 failed"
  fi
  check_cmd "helm"
}

function install_cluster() {
  echo "step 1/9: checking required configuration"
  if [ ! -r "${KIND_CONFIG}" ]; then
    exit_on_error "kind config not found: ${KIND_CONFIG}"
  fi
  # use latest helm charts from the release repo to install yunikorn unless path is provided
  if [ "${GIT_CLONE}" = "true" ]; then
    check_cmd "git"
    git clone --depth 1 https://github.com/apache/yunikorn-release.git ./yunikorn-release
  fi
  if [ ! -d "${CHART_PATH}" ]; then
    exit_on_error "helm charts not found in path: ${CHART_PATH}"
  fi

  # build docker images from latest code, so that we can install yunikorn with these latest images
  echo "step 2/9: building docker images from latest code"
  check_docker
  QUIET="--quiet" REGISTRY=local VERSION=latest make image
  exit_on_error "build docker images failed"
  QUIET="--quiet" REGISTRY=local VERSION=latest make webtest_image
  exit_on_error "build test web images failed"

  # install ginkgo and gomega for e2e tests.
  echo "step 3/9: installing Ginkgo & Gomega at $(go env GOPATH)/bin"
  go install github.com/onsi/ginkgo/ginkgo
  go install github.com/onsi/gomega
  check_cmd "ginkgo"

  echo "step 4/9: installing helm-v3"
  install_helm

  # install kubectl
  echo "step 5/9: installing kubectl"
  install_kubectl

  # install KIND
  echo "step 6/9: installing kind"
  install_kind

  # create K8s cluster
  echo "step 7/9: installing K8s cluster using kind"
  "${KIND}" create cluster --name "${CLUSTER_NAME}" --image "${CLUSTER_VERSION}" --config="${KIND_CONFIG}"
  exit_on_error "install K8s cluster failed"
  kubectl cluster-info --context kind-"${CLUSTER_NAME}"
  exit_on_error "set K8s cluster context failed"
  kubectl create namespace yunikorn
  exit_on_error "failed to create yunikorn namespace"
  echo "cluster node definitions:"
  kubectl describe nodes

  # pre-load yunikorn docker images to kind
  echo "step 8/9: pre-load yunikorn images"
  "${KIND}" load docker-image "local/yunikorn:${SCHEDULER_IMAGE}" --name "${CLUSTER_NAME}"
  exit_on_error "pre-load scheduler image failed: ${SCHEDULER_IMAGE}"
  "${KIND}" load docker-image "local/yunikorn:${ADMISSION_IMAGE}" --name "${CLUSTER_NAME}"
  exit_on_error "pre-load admission controller image failed: ${ADMISSION_IMAGE}"
  "${KIND}" load docker-image "local/yunikorn:${WEBTEST_IMAGE}" --name "${CLUSTER_NAME}"
  exit_on_error "pre-load web image failed: ${WEBTEST_IMAGE}"

  echo "step 9/9: installing yunikorn"
  helm install yunikorn "${CHART_PATH}" --namespace yunikorn \
    --set image.repository=local/yunikorn \
    --set image.tag="${SCHEDULER_IMAGE}" \
    --set image.pullPolicy=IfNotPresent \
    --set admissionController.image.repository=local/yunikorn \
    --set admissionController.image.tag="${ADMISSION_IMAGE}" \
    --set admissionController.image.pullPolicy=IfNotPresent \
    --set web.image.repository=local/yunikorn \
    --set web.image.tag="${WEBTEST_IMAGE}" \
    --set web.image.pullPolicy=IfNotPresent
  exit_on_error "failed to install yunikorn"
  kubectl wait --for=condition=available --timeout=300s deployment/yunikorn-scheduler -n yunikorn
  exit_on_error "failed to wait for yunikorn scheduler deployment being deployed"
  kubectl wait --for=condition=ready --timeout=300s pod -l app=yunikorn -n yunikorn
  exit_on_error "failed to wait for yunikorn scheduler pods being deployed"
}

function delete_cluster() {
  echo "deleting K8s cluster: ${CLUSTER_NAME}"
  install_kind
  "${KIND}" delete cluster --name "${CLUSTER_NAME}"
  exit_on_error "failed to delete the cluster"
}

function print_usage() {
  NAME=$(basename "$0")
  cat <<EOF
Usage: ${NAME} -a <action> -n <kind-cluster-name> -v <kind-node-image-version> [-p <chart-path>] [--plugin] [--force-kind-install]
  <action>                     the action to be executed, must be either "test" or "cleanup".
  <kind-cluster-name>          the name of the K8s cluster to be created by kind
  <kind-node-image-version>    the kind node image used to provision the K8s cluster, required for "test" action
  <chart-path>                 local path to helm charts path (default is to pull from GitHub master)
  --force-kind-install         force Kind to be installed even if already present
  --plugin                     use scheduler plugin image instead of default mode image

Examples:
  ${NAME} -a test -n yk8s -v kindest/node:v1.21.14
  ${NAME} -a test -n yk8s -v kindest/node:v1.22.15
  ${NAME} -a test -n yk8s -v kindest/node:v1.23.13
  ${NAME} -a test -n yk8s -v kindest/node:v1.24.7
  ${NAME} -a test -n yk8s -v kindest/node:v1.25.3

  Use a local helm chart path:
    ${NAME} -a test -n yk8s -v kindest/node:v1.25.3 -p ./yunikorn-release/helm-charts/yunikorn
EOF
}

# setup architectures and OS type
check_cmd "make"
eval "$(make arch)"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
check_os

KIND_CONFIG=./scripts/kind.yaml
CHART_PATH="./yunikorn-release/helm-charts/yunikorn"
GIT_CLONE=true
SCHEDULER_IMAGE="scheduler-${DOCKER_ARCH}-latest"
ADMISSION_IMAGE="admission-${DOCKER_ARCH}-latest"
WEBTEST_IMAGE="webtest-${DOCKER_ARCH}-latest"
FORCE_KIND_INSTALL=false

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
  --force-kind-install)
    FORCE_KIND_INSTALL=true
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
echo "  force kind install : ${FORCE_KIND_INSTALL}"
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

# this script only supports 2 actions
#   1) test
#     - install a K8s cluster with kind
#     - install latest yunikorn
#     - run e2e tests
#   2) cleanup
#     - delete k8s cluster
if [ "${ACTION}" == "test" ]; then
  # make will fail without go installed but we call it before that...
  check_cmd "go"
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
elif [ "${ACTION}" == "cleanup" ]; then
  echo "cleaning up the environment"
  delete_cluster
else
  echo "unknown action: ${ACTION}"
  print_usage
  exit 1
fi
