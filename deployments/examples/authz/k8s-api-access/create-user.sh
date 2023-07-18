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

# USERS & USERS_GROUP should contain the same number of elements.
# If you wants no group or no user. You can use an empty string, i.e. ""
USERS=("admin" "sue" "bob" "kim" "yono" "anonymous")
USERS_GROUP=("admin" "group-a" "group-a" "group-b" "group-b" "anonymous")
AUTH_FOLDER=./auth
CERT_REQUEST_FILE=./certification_request.yaml

mkdir -p $AUTH_FOLDER
for ((i = 0; i < ${#USERS[@]}; ++i)); do
    USERNAME="${USERS[i]}"
    GROUP="${USERS_GROUP[i]}"
    AUTH_FILE=$AUTH_FOLDER/$USERNAME-$GROUP
    echo "username: $USERNAME , group: $GROUP"
    # create a CSR for the user
    openssl genrsa -out "$AUTH_FILE".key 2048
    openssl req -new -key "$AUTH_FILE".key -out "$AUTH_FILE".csr -subj "/CN=$USERNAME/O=$GROUP"
    # Kubernetes config only supports user assignments, not groups.
    # if the USERNAME is not assigned, the script will create a user based on your group's name.
    if [ "$USERNAME" = "" ]
    then
        USERNAME=$GROUP
    fi

    # write a file for certification request & use kubectl to approve the request
    {
        echo "apiVersion: certificates.k8s.io/v1"
        echo "kind: CertificateSigningRequest"
        echo "metadata:"
        echo "   name: $USERNAME-csr"
        echo "spec:"
        echo "   groups:"
        echo "   - system:authenticated"
        echo "   request: $(< "$AUTH_FILE".csr base64 | tr -d '\n')"
        echo "   signerName: kubernetes.io/kube-apiserver-client"
        echo "   usages:"
        echo "   - digital signature"
        echo "   - key encipherment"
        echo "   - client auth"
    } >  $CERT_REQUEST_FILE

    kubectl apply -f ${CERT_REQUEST_FILE}
    kubectl certificate approve "$USERNAME"-csr

    # get CRT for user
    kubectl get csr "$USERNAME"-csr -o jsonpath='{.status.certificate}' | base64 --decode > "$AUTH_FILE".crt
    
    # using CRT & key to set credentials & set context for user
    kubectl config set-credentials "$USERNAME" --client-certificate="$AUTH_FILE".crt --client-key="$AUTH_FILE".key
    kubectl config set-context "$USERNAME"-context --cluster=kubernetes --namespace="" --user="$USERNAME"
    
done
# apply RBAC for user
kubectl apply -f ./authorization.yaml

