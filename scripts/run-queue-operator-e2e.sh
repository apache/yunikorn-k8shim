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
# limitations under the License.

set -euo pipefail

CLUSTER_NAME=""
CLUSTER_VERSION=""
while getopts "n:v:" option; do
  case "${option}" in
    n) CLUSTER_NAME="${OPTARG}" ;;
    v) CLUSTER_VERSION="${OPTARG}" ;;
    *) exit 2 ;;
  esac
done

if [[ -z "${CLUSTER_NAME}" || -z "${CLUSTER_VERSION}" ]]; then
  echo "usage: $0 -n <kind-cluster-name> -v <kind-node-image>" >&2
  exit 2
fi

make tools
KIND="tools/kind-$(make -s print_kind_version)/kind"
KUBECTL="tools/kubectl-$(make -s print_kubectl_version)/kubectl"
DOCKER="$(make -s print_docker)"
DOCKER_ARCH="$(make -s arch | awk -F= '$1 == "DOCKER_ARCH" {print $2}')"
IMAGE_REGISTRY="local"
if [[ "${DOCKER}" == "podman" ]]; then
  IMAGE_REGISTRY="localhost/local"
  export KIND_EXPERIMENTAL_PROVIDER=podman
fi
IMAGE="${IMAGE_REGISTRY}/yunikorn:queue-operator-${DOCKER_ARCH}-latest"
ARCHIVE=""
KUBECONFIG=""

cleanup() {
  status=$?
  rm -f "${KUBECONFIG}" "${ARCHIVE}"
  "${KIND}" delete cluster --name "${CLUSTER_NAME}" || true
  return "${status}"
}
trap cleanup EXIT

KIND_CONFIG="scripts/kind.yaml"
VERSION="${CLUSTER_VERSION##*:}"
VERSION="${VERSION#v}"
if [[ "$(printf '%s\n' "${VERSION}" "1.36" | sort -V | head -n1)" == "1.36" ]]; then
  KIND_CONFIG="scripts/kind-1.36.yaml"
elif [[ "$(printf '%s\n' "${VERSION}" "1.32" | sort -V | head -n1)" == "1.32" ]]; then
  KIND_CONFIG="scripts/kind-1.32.yaml"
fi

QUIET="--quiet" REGISTRY="${IMAGE_REGISTRY}" VERSION=latest make qop_image
"${KIND}" create cluster --name "${CLUSTER_NAME}" --image "${CLUSTER_VERSION}" --config "${KIND_CONFIG}"
if [[ "${DOCKER}" == "podman" ]]; then
  ARCHIVE="$(mktemp -t yunikorn-queue-operator-image.XXXXXX)"
  "${DOCKER}" save "${IMAGE}" -o "${ARCHIVE}"
  "${KIND}" load image-archive "${ARCHIVE}" --name "${CLUSTER_NAME}"
else
  "${KIND}" load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
fi

"${KUBECTL}" create namespace yunikorn
"${KUBECTL}" create configmap yunikorn-configs -n yunikorn \
  --from-literal=queues.yaml=unmanaged --from-literal=unrelated=preserved
"${KUBECTL}" apply -f deployments/queue-operator/queue-crd.yaml
"${KUBECTL}" apply -n yunikorn -f deployments/queue-operator/queue-operator-rbac.yaml
"${KUBECTL}" apply -n yunikorn -f deployments/queue-operator/queue-operator.yaml
"${KUBECTL}" set image -n yunikorn deployment/yunikorn-queue-operator \
  yunikorn-queue-operator="${IMAGE}"
"${KUBECTL}" rollout status -n yunikorn deployment/yunikorn-queue-operator --timeout=300s

KUBECONFIG="$(mktemp -t yunikorn-queue-operator-kubeconfig.XXXXXX)"
"${KIND}" get kubeconfig --name "${CLUSTER_NAME}" > "${KUBECONFIG}"
KUBECONFIG="${KUBECONFIG}" E2E_TEST=queue_operator make e2e_test
