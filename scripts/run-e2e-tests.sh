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

function check_cmd() {
  cmd=$1
  if ! command -v ${cmd} &> /dev/null
  then
    echo "command ${cmd} could not be found"
    exit 1
  fi
}

function exit_on_error() {
  cmd_code=$?
  err_msg=$1
  if [[ ${cmd_code} -ne 0 ]]; then
    echo "command execution failed: ${err_msg}"
    exit ${cmd_code}
  fi
}

function check_opt() {
    opt_to_check=$1
    if [[ "${opt_to_check}" == "" ]]; then
      echo "option cannot be empty"
      print_usage
      exit 1
    fi
}

function install_cluster() {
  # both arguments are required
  if [[ $# -ne 2 ]]; then
    echo "expecting exact 2 parameters for function install_cluster()"
    return 1
  fi

  k8s_cluster_name=$1
  kind_node_image=$2

  # install ginkgo and gomega for e2e tests
  check_cmd "go"
  go get -v github.com/onsi/ginkgo/ginkgo
  go get -v github.com/onsi/gomega
  export PATH=$PATH:$HOME/gopath/bin
  check_cmd "ginkgo"

  # build docker images from latest code, so that we can install yunikorn with these latest images
  echo "step 1/6: building docker images from latest code"
  make image REGISTRY=local VERSION=latest
  exit_on_error "build docker images failed"

  echo "step 2/6: installing helm-v3"
  check_cmd "curl"
  curl -L https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash
  exit_on_error "install helm-v3 failed"
  check_cmd "helm"

  # install kubectl
  echo "step 3/6: installing kubectl"
  stable_release=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
  exit_on_error "unable to retrieve latest stable version of kubectl"
  curl -LO https://storage.googleapis.com/kubernetes-release/release/${stable_release}/bin/linux/amd64/kubectl \
    && chmod +x kubectl && sudo mv kubectl /usr/local/bin/
  exit_on_error "install kubectl failed"

  # install KIND
  echo "step 4/6: installing kind"
  curl -Lo ./kind "https://kind.sigs.k8s.io/dl/v0.8.0/kind-linux-amd64" \
    && chmod +x ./kind && mv ./kind $(go env GOPATH)/bin
  exit_on_error "install KIND failed"
  check_cmd "kind"

  # create K8s cluster
  echo "step 5/6: installing K8s cluster using kind"
  kind create cluster --name ${k8s_cluster_name} --image ${kind_node_image} --config=./scripts/kind.yaml
  exit_on_error "instal K8s cluster failed"
  kubectl cluster-info --context kind-${k8s_cluster_name}
  exit_on_error "set K8s cluster context failed"
  echo "k8s installed, version:"
  kubectl version

  # install yunikorn
  echo "step 6/6: installing yunikorn scheduler"
  # load latest yunikorn docker images to kind
  kind load docker-image local/yunikorn:scheduler-latest --name ${k8s_cluster_name}
  kind load docker-image local/yunikorn:admission-latest --name ${k8s_cluster_name}

  kubectl create namespace yunikorn
  exit_on_error "failed to create yunikorn namespace"
  # use latest helm charts from the release repo to install yunikorn
  git clone https://github.com/apache/incubator-yunikorn-release.git
  helm install yunikorn ./incubator-yunikorn-release/helm-charts/yunikorn --namespace yunikorn \
    --set image.repository=local/yunikorn \
    --set image.tag=scheduler-latest \
    --set image.pullPolicy=Never \
    --set admission_controller_image.repository=local/yunikorn \
    --set admission_controller_image.tag=admission-latest \
    --set admission_controller_image.pullPolicy=Never
  exit_on_error "failed to install yunikorn"
  kubectl wait --for=condition=available --timeout=300s deployment/yunikorn-scheduler -n yunikorn
  exit_on_error "failed to wait for yunikorn scheduler deployment being deployed"
  kubectl wait --for=condition=ready --timeout=300s pod -l app=yunikorn -n yunikorn
  exit_on_error "failed to wait for yunikorn scheduler pods being deployed"

  # forward rest server port 9080 as long as yk ns exists, so the e2e test code can verify cluster state via rest calls.
  while kubectl get namespace | grep yunikorn >/dev/null 2>&1; \
    do kubectl port-forward svc/yunikorn-service 9080:9080 -n yunikorn > /dev/null 2>&1; \
    done &
  exit_on_error "failed to forward rest server port 9080"
  kubectl describe deployment yunikorn-scheduler -n yunikorn
  exit_on_error "failed to describe yunikorn scheduler deployment"
}

function delete_cluster() {
  if [[ $# -ne 1 ]]; then
    echo "expecting exact 1 parameters for function install_cluster()"
    return 1
  fi

  k8s_cluster_name=$1

  echo "step 1/2: deleting yunikorn scheduler helm charts"
  helm uninstall yunikorn --namespace yunikorn
  exit_on_error "failed to uninstall helm charts"
  echo "step 2/2: deleting K8s cluster: ${k8s_cluster_name}"
  kind delete cluster --name ${k8s_cluster_name}
  exit_on_error "failed to delete the cluster"

}

function print_usage() {
    cat <<EOF
Usage: $(basename "$0") -a <action> -n <kind-cluster-name> -v <kind-node-image-version>
  <action>                     the action that needs to be executed, must be either "test" or "cleanup".
  <kind-cluster-name>          the name of the K8s cluster that created by kind.
  <kind-node-image-version>    the kind node image used to provision the K8s cluster.

Examples:
  $(basename "$0") -n "yk8s" -v "kindest/node:v1.15.11"
EOF
}

while [[ $# -gt 0 ]]; do
key="$1"
case ${key} in
  -a|--action)
    action="$2"
    shift
    shift
    ;;
  -n|--cluster-name)
    cluster_name="$2"
    shift
    shift
    ;;
  -v|--cluster-version)
    cluster_version="$2"
    shift
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

echo "action: ${action}"
check_opt "${action}"
echo "kind cluster name: ${cluster_name}"
check_opt "${cluster_name}"
echo "kind node image version ${cluster_version}"

# this script only supports 2 actions
#   1) test
#     - install a K8s cluster with kind
#     - install latest yunikorn
#     - run e2e tests
#   2) cleanup
#     - delete yunikorn
#     - delete k8s cluster
if [ "${action}" == "test" ]; then
  install_cluster ${cluster_name} ${cluster_version}
  echo "running e2e tests"
  make e2e_test
  exit_on_error "e2e tests failed"
elif [ "${action}" == "cleanup" ]; then
  echo "cleaning up the evnvironment"
  delete_cluster ${cluster_name}
else
  echo "unknown action: ${action}"
  print_usage
  exit 1
fi

