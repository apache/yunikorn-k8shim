#!/bin/sh
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
