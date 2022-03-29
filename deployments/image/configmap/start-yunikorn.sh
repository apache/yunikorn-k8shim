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

[ -n "$CLUSTER_ID" ] && ARGS="$ARGS -clusterId=$CLUSTER_ID"
[ -n "$CLUSTER_VERSION" ] && ARGS="$ARGS -clusterVersion=$CLUSTER_VERSION"
[ -n "$POLICY_GROUP" ] && ARGS="$ARGS -policyGroup=$POLICY_GROUP"
[ -n "$SCHEDULING_INTERVAL" ] && ARGS="$ARGS -interval=$SCHEDULING_INTERVAL"
[ -n "$LOG_LEVEL" ] && ARGS="$ARGS -logLevel=$LOG_LEVEL"
[ -n "$LOG_ENCODING" ] && ARGS="$ARGS -logEncoding=$LOG_ENCODING"
[ -n "$VOLUME_BINDING_TIMEOUT" ] && ARGS="$ARGS -volumeBindTimeout=$VOLUME_BINDING_TIMEOUT"
[ -n "$EVENT_CHANNEL_CAPACITY" ] && ARGS="$ARGS -eventChannelCapacity=$EVENT_CHANNEL_CAPACITY"
[ -n "$DISPATCHER_TIMEOUT" ] && ARGS="$ARGS -dispatchTimeout=$DISPATCHER_TIMEOUT"
[ -n "$KUBE_CLIENT_QPS" ] && ARGS="$ARGS -kubeQPS=$KUBE_CLIENT_QPS"
[ -n "$KUBE_CLIENT_BURST" ] && ARGS="$ARGS -kubeBurst=$KUBE_CLIENT_BURST"
[ -n "$OPERATOR_PLUGINS" ] && ARGS="$ARGS -operatorPlugins=$OPERATOR_PLUGINS"
[ -n "$ENABLE_CONFIG_HOT_REFRESH" ] && ARGS="$ARGS -enableConfigHotRefresh=$ENABLE_CONFIG_HOT_REFRESH"
[ -n "$DISABLE_GANG_SCHEDULING" ] && ARGS="$ARGS -disableGangScheduling=$DISABLE_GANG_SCHEDULING"
[ -n "$USER_LABEL_KEY" ] && ARGS="$ARGS -userLabelKey=$USER_LABEL_KEY"
[ -n "$PLACEHOLDER_IMAGE" ] && ARGS="$ARGS -placeHolderImage=$PLACEHOLDER_IMAGE"

echo "Arguments passed to Yunikorn: $ARGS"
exec /opt/yunikorn/bin/k8s_yunikorn_scheduler $ARGS