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

FROM alpine:latest

# scheduler binary
ADD kube-scheduler /bin/kube-scheduler
ADD scheduler-config.yaml /bin/scheduler-config.yaml
WORKDIR /bin
ENV CLUSTER_ID "mycluster"
ENV CLUSTER_VERSION "latest"
ENV POLICY_GROUP "queues"
ENV SCHEDULING_INTERVAL "1s"
ENV LOG_LEVEL "0"
ENV LOG_ENCODING "console"
ENV EVENT_CHANNEL_CAPACITY "1048576"
ENV DISPATCHER_TIMEOUT "300s"
ENV KUBE_CLIENT_QPS "1000"
ENV KUBE_CLIENT_BURST "1000"
ENV OPERATOR_PLUGINS "general"
ENV ENABLE_CONFIG_HOT_REFRESH "true"
ENV DISABLE_GANG_SCHEDULING "false"
ENV USER_LABEL_KEY "yunikorn.apache.org/username"
ENV ADDRESS "0.0.0.0"
ENV LEADER_ELECT "false"
ENV SCHEDULER_CONFIG "scheduler-config.yaml"
ENV VERBOSITY "2"
ENV SCHEDULER_NAME "yunikorn"

ENTRYPOINT ["/bin/sh", "-c", "/bin/kube-scheduler \
--address=\"${ADDRESS}\" \
--leader-elect=\"${LEADER_ELECT}\" \
--config=\"${SCHEDULER_CONFIG}\" \
-v=\"${VERBOSITY}\" \
--scheduler-name=\"${SCHEDULER_NAME}\" \
--yk-cluster-id=\"${CLUSTER_ID}\" \
--yk-cluster-version=\"${CLUSTER_VERSION}\" \
--yk-policy-group=\"${POLICY_GROUP}\" \
--yk-scheduling-interval=\"${SCHEDULING_INTERVAL}\" \
--yk-log-level=\"${LOG_LEVEL}\" \
--yk-log-encoding=\"${LOG_ENCODING}\" \
--yk-event-channel-capacity=\"${EVENT_CHANNEL_CAPACITY}\" \
--yk-dispatcher-timeout=\"${DISPATCHER_TIMEOUT}\" \
--yk-kube-qps=\"${KUBE_CLIENT_QPS}\" \
--yk-kube-burst=\"${KUBE_CLIENT_BURST}\" \
--yk-enable-config-hot-refresh=\"${ENABLE_CONFIG_HOT_REFRESH}\" \
--yk-disable-gang-scheduling=\"${DISABLE_GANG_SCHEDULING}\" \
--yk-user-label-key=\"${USER_LABEL_KEY}\" \
--yk-scheduler-name=\"${SCHEDULER_NAME}\""]
