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
if [ -z "$SERVICE_ACCOUNT_NAME" ]; then
  SERVICE_ACCOUNT_NAME=`cat ${CONF_FILE} | grep ^schedulerServiceAccountName | cut -d "=" -f 2`
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
if [ -z "$ADMISSION_CONTROLLER_IMAGE_REGISTRY" ]; then
  ADMISSION_CONTROLLER_IMAGE_REGISTRY=`cat ${CONF_FILE} | grep ^dockerImageRegistry | cut -d "=" -f 2`
fi
if [ -z "$ADMISSION_CONTROLLER_IMAGE_TAG" ]; then
  ADMISSION_CONTROLLER_IMAGE_TAG=`cat ${CONF_FILE} | grep ^dockerImageTag | cut -d "=" -f 2`
fi
if [ -z "$ADMISSION_CONTROLLER_IMAGE_PULL_POLICY" ]; then
  ADMISSION_CONTROLLER_IMAGE_PULL_POLICY=`cat ${CONF_FILE} | grep ^dockerImagePullPolicy | cut -d "=" -f 2`
fi
if [ -z "$ENABLE_CONFIG_HOT_REFRESH" ]; then
  ENABLE_CONFIG_HOT_REFRESH=`cat ${CONF_FILE} | grep ^enableConfigHotRefresh | cut -d "=" -f 2`
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

  # clean up local key and cert files
  rm -rf ${KEY_DIR}

  # Replace the certificate in the template with a valid CA parsed from security tokens
  ca_pem_b64=$(kubectl get secret -o jsonpath="{.items[?(@.type==\"kubernetes.io/service-account-token\")].data['ca\.crt']}" | cut -d " " -f 1)
  sed -e 's@${NAMESPACE}@'"$NAMESPACE"'@g' -e 's@${SERVICE}@'"$SERVICE"'@g' \
    -e 's@${POLICY_GROUP}@'"$POLICY_GROUP"'@g' \
    -e 's@${SERVICE_ACCOUNT_NAME}@'"$SERVICE_ACCOUNT_NAME"'@g' \
    -e 's@${SCHEDULER_SERVICE_ADDRESS}@'"$SCHEDULER_SERVICE_ADDRESS"'@g' \
    -e 's@${ADMISSION_CONTROLLER_IMAGE_REGISTRY}@'"$ADMISSION_CONTROLLER_IMAGE_REGISTRY"'@g' \
    -e 's@${ADMISSION_CONTROLLER_IMAGE_TAG}@'"$ADMISSION_CONTROLLER_IMAGE_TAG"'@g' \
    -e 's@${ADMISSION_CONTROLLER_IMAGE_PULL_POLICY}@'"$ADMISSION_CONTROLLER_IMAGE_PULL_POLICY"'@g' \
    -e 's@${ENABLE_CONFIG_HOT_REFRESH}@'"$ENABLE_CONFIG_HOT_REFRESH"'@g' \
    <"${basedir}/templates/server.yaml.template" > server.yaml

if [ -n "$ADMISSION_CONTROLLER_IMAGE_PULL_SECRETS" ]; then
  touch .server_yaml_tmp_file
  secrets_array=`echo "${ADMISSION_CONTROLLER_IMAGE_PULL_SECRETS}" | cut -d']' -f 1 | cut -d'[' -f 2`
  echo ${secrets_array} | awk -F" " '{ split($0, arr, " "); }END{ for ( i in arr ) { print arr[i] } }' | while read line ; do
    echo "      - name: ${line}" >> .server_yaml_tmp_file
  done
  sed -i '/[\s]*imagePullSecrets:/r .server_yaml_tmp_file' server.yaml
  rm -rf .server_yaml_tmp_file
fi
  # ImagePullSecrets is an array with format [secret1 secret2 ...]

  kubectl create -f server.yaml

  # register admissions
  for admission in $REGISTERED_ADMISSIONS
  do
    sed -e 's@${CA_PEM_B64}@'"$ca_pem_b64"'@g' -e 's@${NAMESPACE}@'"$NAMESPACE"'@g' -e 's@${SERVICE}@'"$SERVICE"'@g' \
      <"${basedir}/templates/${admission}.yaml.template" > ${admission}.yaml
    kubectl create -f ${admission}.yaml
  done

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
  exit $?
else
  usage
  exit 1
fi
