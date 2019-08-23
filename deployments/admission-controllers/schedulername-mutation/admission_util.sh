#!/bin/bash

# Copyright 2019 Cloudera, Inc.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

basedir="$(dirname "$0")"
CONF_FILE="configs.properties"

if [ ! -f ${CONF_FILE} ]; then
  echo "${CONF_FILE} is missing in current directory!"
  exit 1
fi

SERVICE=`cat ${CONF_FILE} | grep service | cut -d "=" -f 2`
SECRET=`cat ${CONF_FILE} | grep secret | cut -d "=" -f 2`
NAMESPACE=`cat ${CONF_FILE} | grep namespace | cut -d "=" -f 2`

delete_resources() {
  kubectl delete -f server.yaml
  kubectl delete -n ${NAMESPACE} secret ${SECRET}
  kubectl delete -n ${NAMESPACE} certificatesigningrequest.certificates.k8s.io ${SERVICE}.${NAMESPACE}
  rm -rf server.yaml
  return 0
}

create_resources() {
  KEY_DIR=$1
  # Generate keys into a temporary directory.
  ${basedir}/generate-signed-ca.sh "${KEY_DIR}"

  # Create the yunikorn namespace.
  echo "Creating namespace ${NAMESPACE}"
  kubectl create namespace ${NAMESPACE} &> /dev/null

  # Create the TLS secret for the generated keys.
  kubectl create secret generic ${SECRET} \
          --from-file=key.pem=${KEY_DIR}/server-key.pem \
          --from-file=cert.pem=${KEY_DIR}/server-cert.pem \
          --namespace=${NAMESPACE}

  # Replace the certificate in the template with a valid CA parsed from kube-config
  ca_pem_b64=$(kubectl config view --raw --flatten -o json | jq -r '.clusters[] | select(.name == "'$(kubectl config current-context)'") | .cluster."certificate-authority-data"')
  sed -e 's@${CA_PEM_B64}@'"$ca_pem_b64"'@g' <"${basedir}/server.yaml.template" > server.yaml
  kubectl create -f server.yaml

  echo "The webhook server has been deployed and configured!"
  return 0
}

usage() {
  echo "usage: ${0} [OPTION]"
  echo "  create"
  echo "      Create admission controller and other related resources"
  echo "  delete "
  echo "      Delete all resources previously created"
}

if [ $# -eq 1 ] && [ $1 == "delete" ]; then
  delete_resources
  exit $?
elif [ $# -eq 1 ] && [ $1 == "create" ]; then
  KEY_DIR="$(mktemp -d)"
  create_resources ${KEY_DIR}
  rm -rf "$keydir"
  exit $?
else
  usage
  exit 1
fi