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

# Home location for all data and executables
ENV HOME=/opt/yunikorn

# config for the scheduler
ENV CLUSTER_ID="mycluster"
ENV CLUSTER_VERSION="latest"
ENV POLICY_GROUP="queues"
ENV SCHEDULING_INTERVAL="1s"
ENV LOG_LEVEL="0"
ENV LOG_ENCODING="console"
ENV EVENT_CHANNEL_CAPACITY="1048576"
ENV DISPATCHER_TIMEOUT="300s"
ENV KUBE_CLIENT_QPS="1000"
ENV KUBE_CLIENT_BURST="1000"
ENV OPERATOR_PLUGINS="general"
ENV ENABLE_CONFIG_HOT_REFRESH="true"
ENV DISABLE_GANG_SCHEDULING="false"
ENV USER_LABEL_KEY="yunikorn.apache.org/username"
ENV ADDRESS="0.0.0.0"
ENV LEADER_ELECT="false"
ENV SCHEDULER_CONFIG="scheduler-config.yaml"
ENV VERBOSITY="2"
ENV SCHEDULER_NAME="yunikorn"
ENV PLACEHOLDER_IMAGE="k8s.gcr.io/pause"

RUN mkdir -p $HOME && \
    addgroup -S -g 4444 yunikorn && \
    adduser -S -h $HOME -G yunikorn -u 4444 yunikorn -s /bin/sh && \
    chown -R 4444:0 $HOME && \
    chmod -R g=u $HOME

COPY --chown=4444:0 start-yunikorn-plugin.sh $HOME/bin/
COPY --chown=4444:0 scheduler-config.yaml $HOME/
COPY --chown=4444:0 kube-scheduler $HOME/bin/

WORKDIR $HOME
USER yunikorn
ENTRYPOINT $HOME/bin/start-yunikorn-plugin.sh
