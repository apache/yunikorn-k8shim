#!/bin/sh
cd /opt/yunikorn/work
exec /opt/yunikorn/bin/k8s_yunikorn_scheduler \
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
  -userLabelKey="${USER_LABEL_KEY}"
