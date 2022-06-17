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

ARG ARCH=
FROM ${ARCH}alpine:latest

RUN addgroup -S -g 4444 yunikorn && \
    adduser -S -h /opt/yunikorn/work -G yunikorn -u 4444 yunikorn -s /bin/sh && \
    mkdir -p /opt/yunikorn/bin /opt/yunikorn/work && \
    chown -R yunikorn:yunikorn /opt/yunikorn/work && \
    chmod 755 /opt/yunikorn/work

ADD k8s_yunikorn_scheduler /opt/yunikorn/bin/k8s_yunikorn_scheduler
ADD start-yunikorn.sh /opt/yunikorn/bin/start-yunikorn.sh
RUN chmod 755 /opt/yunikorn/bin/*

ENV CLUSTER_ID "mycluster"
ENV CLUSTER_VERSION "latest"
ENV POLICY_GROUP "queues"
ENV SCHEDULING_INTERVAL "1s"
ENV LOG_LEVEL "0"
ENV LOG_ENCODING "console"
ENV VOLUME_BINDING_TIMEOUT "10s"
ENV EVENT_CHANNEL_CAPACITY "1048576"
ENV DISPATCHER_TIMEOUT "300s"
ENV KUBE_CLIENT_QPS "1000"
ENV KUBE_CLIENT_BURST "1000"
ENV OPERATOR_PLUGINS "general"
ENV ENABLE_CONFIG_HOT_REFRESH "true"
ENV DISABLE_GANG_SCHEDULING "false"
ENV USER_LABEL_KEY "yunikorn.apache.org/username"
ENV PLACEHOLDER_IMAGE "k8s.gcr.io/pause"

WORKDIR /opt/yunikorn/work
USER yunikorn
ENTRYPOINT /opt/yunikorn/bin/start-yunikorn.sh
