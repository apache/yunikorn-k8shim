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

cd /opt/yunikorn/work
exec /opt/yunikorn/bin/kube-scheduler \
  --address="${ADDRESS}" \
  --leader-elect="${LEADER_ELECT}" \
  --config="${SCHEDULER_CONFIG}" \
  -v="${VERBOSITY}" \
  --scheduler-name="${SCHEDULER_NAME}" \
  --yk-cluster-id="${CLUSTER_ID}" \
  --yk-cluster-version="${CLUSTER_VERSION}" \
  --yk-policy-group="${POLICY_GROUP}" \
  --yk-scheduling-interval="${SCHEDULING_INTERVAL}" \
  --yk-log-level="${LOG_LEVEL}" \
  --yk-log-encoding="${LOG_ENCODING}" \
  --yk-event-channel-capacity="${EVENT_CHANNEL_CAPACITY}" \
  --yk-dispatcher-timeout="${DISPATCHER_TIMEOUT}" \
  --yk-kube-qps="${KUBE_CLIENT_QPS}" \
  --yk-kube-burst="${KUBE_CLIENT_BURST}" \
  --yk-enable-config-hot-refresh="${ENABLE_CONFIG_HOT_REFRESH}" \
  --yk-disable-gang-scheduling="${DISABLE_GANG_SCHEDULING}" \
  --yk-user-label-key="${USER_LABEL_KEY}" \
  --yk-scheduler-name="${SCHEDULER_NAME}"
