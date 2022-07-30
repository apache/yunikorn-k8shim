#!/bin/sh
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

cd "${HOME}" || exit
exec "${HOME}"/bin/k8s_yunikorn_scheduler \
  -clusterId="${CLUSTER_ID}" \
  -clusterVersion="${CLUSTER_VERSION}" \
  -policyGroup="${POLICY_GROUP}" \
  -interval="${SCHEDULING_INTERVAL}" \
  -logLevel="${LOG_LEVEL}" \
  -logEncoding="${LOG_ENCODING}" \
  -volumeBindTimeout="${VOLUME_BINDING_TIMEOUT}" \
  -eventChannelCapacity="${EVENT_CHANNEL_CAPACITY}" \
  -dispatchTimeout="${DISPATCHER_TIMEOUT}" \
  -kubeQPS="${KUBE_CLIENT_QPS}" \
  -kubeBurst="${KUBE_CLIENT_BURST}" \
  -operatorPlugins="${OPERATOR_PLUGINS}" \
  -enableConfigHotRefresh="${ENABLE_CONFIG_HOT_REFRESH}" \
  -disableGangScheduling="${DISABLE_GANG_SCHEDULING}" \
  -userLabelKey="${USER_LABEL_KEY}" \
  -placeHolderImage="${PLACEHOLDER_IMAGE}"
