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

[ -n "$CLUSTER_ID" ] && ARGS="$ARGS --yk-cluster-id=$CLUSTER_ID"
[ -n "$CLUSTER_VERSION" ] && ARGS="$ARGS --yk-cluster-version=$CLUSTER_VERSION"
[ -n "$POLICY_GROUP" ] && ARGS="$ARGS --yk-policy-group=$POLICY_GROUP"
[ -n "$SCHEDULING_INTERVAL" ] && ARGS="$ARGS --yk-scheduling-interval=$SCHEDULING_INTERVAL"
[ -n "$LOG_LEVEL" ] && ARGS="$ARGS --yk-log-level=$LOG_LEVEL"
[ -n "$LOG_ENCODING" ] && ARGS="$ARGS --yk-log-encoding=$LOG_ENCODING"
[ -n "$EVENT_CHANNEL_CAPACITY" ] && ARGS="$ARGS --yk-event-channel-capacity=$EVENT_CHANNEL_CAPACITY"
[ -n "$DISPATCHER_TIMEOUT" ] && ARGS="$ARGS --yk-dispatcher-timeout=$DISPATCHER_TIMEOUT"
[ -n "$KUBE_CLIENT_QPS" ] && ARGS="$ARGS --yk-kube-qps=$KUBE_CLIENT_QPS"
[ -n "$KUBE_CLIENT_BURST" ] && ARGS="$ARGS --yk-kube-burst=$KUBE_CLIENT_BURST"
[ -n "$ENABLE_CONFIG_HOT_REFRESH" ] && ARGS="$ARGS --yk-enable-config-hot-refresh=$ENABLE_CONFIG_HOT_REFRESH"
[ -n "$DISABLE_GANG_SCHEDULING" ] && ARGS="$ARGS --yk-disable-gang-scheduling=$DISABLE_GANG_SCHEDULING"
[ -n "$USER_LABEL_KEY" ] && ARGS="$ARGS --yk-user-label-key=$USER_LABEL_KEY"
[ -n "$PLACEHOLDER_IMAGE" ] && ARGS="$ARGS --yk-placeholder-image=$PLACEHOLDER_IMAGE"

echo "Arguments passed to Yunikorn scheduler plugin: $ARGS"

exec /opt/yunikorn/bin/kube-scheduler \
  --address="${ADDRESS}" \
  --leader-elect="${LEADER_ELECT}" \
  --config="${SCHEDULER_CONFIG}" \
  -v="${VERBOSITY}" \
  --scheduler-name="${SCHEDULER_NAME}" \
  --yk-scheduler-name="${SCHEDULER_NAME}" $ARGS
