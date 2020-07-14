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

basedir="$(dirname "$0")"
CONF_FILE="configs.properties"

if [ ! -f ${CONF_FILE} ]; then
  echo "${CONF_FILE} is missing in current directory!"
  exit 1
fi

if [ -z "$SERVICE" ]; then
  SERVICE=`cat ${CONF_FILE} | grep ^service | cut -d "=" -f 2`
fi
if [ -z "$SECRET" ]; then
  SECRET=`cat ${CONF_FILE} | grep ^secret | cut -d "=" -f 2`
fi
if [ -z "$NAMESPACE" ]; then
  NAMESPACE=`cat ${CONF_FILE} | grep ^namespace | cut -d "=" -f 2`
fi
if [ -z "$POLICY_GROUP" ]; then
  POLICY_GROUP=`cat ${CONF_FILE} | grep ^policyGroup | cut -d "=" -f 2`
fi
if [ -z "$REGISTERED_ADMISSIONS" ]; then
  REGISTERED_ADMISSIONS=`cat ${CONF_FILE} | grep ^registeredAdmissions | cut -d "=" -f 2`
fi
REGISTERED_ADMISSIONS=${REGISTERED_ADMISSIONS//,/ }
if [ -z "$SCHEDULER_SERVICE_NAME" ]; then
  SCHEDULER_SERVICE_NAME=`cat ${CONF_FILE} | grep ^schedulerServiceName | cut -d "=" -f 2`
fi

delete_resources() {
  kubectl delete -f server.yaml
  # cleanup admissions
  for admission in $REGISTERED_ADMISSIONS
  do
    kubectl delete -f ${admission}.yaml
    rm -rf ${admission}.yaml
  done
  kubectl delete -n ${NAMESPACE} secret ${SECRET}
  kubectl delete -n ${NAMESPACE} certificatesigningrequest.certificates.k8s.io ${SERVICE}.${NAMESPACE}
  rm -rf server.yaml
  return 0
}

precheck() {
  # depedency check
  command -v kubectl &> /dev/null
  if [ $? -ne 0 ]; then
    echo "dependency check failed: kubectl is not installed"
    exit 1
  fi

  command -v openssl &> /dev/null
  if [ $? -ne 0 ]; then
    echo "dependency check failed: openssl is not installed"
    exit 1
  fi

  command -v jq &> /dev/null
  if [ $? -ne 0 ]; then
    echo "dependency check failed: jq is not installed"
    exit 1
  fi
  # check registered admissions
  for admission in $REGISTERED_ADMISSIONS
  do
    if [ ! -f "templates/${admission}.yaml.template" ]; then
      echo "invalid registered admission: ${admission}, template not found: templates/${admission}.yaml.template"
      exit 1
    fi
  done
  if [ -z "$SCHEDULER_SERVICE_ADDRESS" ];  then
    # get port of REST API service in yunikorn scheduler automatically
    port=$(kubectl get service "${SCHEDULER_SERVICE_NAME}" -n "${NAMESPACE}" -o jsonpath="{.spec.ports[0].port}")
    if [ -z $port ]; then
      echo "failed to get port from service ${SCHEDULER_SERVICE_NAME} in namespace ${NAMESPACE}"
      exit 1
    fi
    SCHEDULER_SERVICE_ADDRESS="${SCHEDULER_SERVICE_NAME}.${NAMESPACE}.svc:${port}"
  fi
}

create_resources() {
  KEY_DIR=$1
  # Generate keys into a temporary directory.
  if ! ${basedir}/generate-signed-ca.sh "${KEY_DIR}"
  then
    echo "failed to generate signed ca!"
    exit 1
  fi

  # Create the yunikorn namespace.
  echo "Creating namespace ${NAMESPACE}"
  kubectl create namespace ${NAMESPACE} &> /dev/null

  # Create the TLS secret for the generated keys.
  kubectl create secret generic ${SECRET} \
          --from-file=key.pem=${KEY_DIR}/server-key.pem \
          --from-file=cert.pem=${KEY_DIR}/server-cert.pem \
          --namespace=${NAMESPACE}

  # Replace the certificate in the template with a valid CA parsed from security tokens
  ca_pem_b64=$(kubectl get secret -o jsonpath="{.items[?(@.type==\"kubernetes.io/service-account-token\")].data['ca\.crt']}" | cut -d " " -f 1)
  sed -e 's@${NAMESPACE}@'"$NAMESPACE"'@g' -e 's@${SERVICE}@'"$SERVICE"'@g' \
    -e 's@${POLICY_GROUP}@'"$POLICY_GROUP"'@g' \
    -e 's@${SCHEDULER_SERVICE_ADDRESS}@'"$SCHEDULER_SERVICE_ADDRESS"'@g' \
    <"${basedir}/templates/server.yaml.template" > server.yaml
  kubectl create -f server.yaml

  # register admissions
  for admission in $REGISTERED_ADMISSIONS
  do
    sed -e 's@${CA_PEM_B64}@'"$ca_pem_b64"'@g' -e 's@${NAMESPACE}@'"$NAMESPACE"'@g' -e 's@${SERVICE}@'"$SERVICE"'@g' \
      <"${basedir}/templates/${admission}.yaml.template" > ${admission}.yaml
    kubectl create -f ${admission}.yaml
  done

  # at this point, we should have the webhook installed and started
  # cleanup the tmp files
  rm -rf ${KEY_DIR}

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
  precheck
  KEY_DIR="$(mktemp -d)"
  create_resources ${KEY_DIR}
  rm -rf "$keydir"
  exit $?
else
  usage
  exit 1
fi