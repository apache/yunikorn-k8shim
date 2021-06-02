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

# admission controller bundles
RUN apk add curl
RUN apk add jq
RUN apk add --update openssl
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.19.7/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl
COPY admission-controller-init-scripts/admission_util.sh /
COPY admission-controller-init-scripts/configs.properties /
COPY admission-controller-init-scripts/generate-signed-ca.sh /
COPY admission-controller-init-scripts/templates /templates
RUN chmod +x /admission_util.sh
RUN chmod +x /generate-signed-ca.sh

# scheduler binary
ADD k8s_yunikorn_scheduler /k8s_yunikorn_scheduler
WORKDIR /
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
ENV PREDICATES ""
ENV OPERATOR_PLUGINS "general"
ENV ENABLE_CONFIG_HOT_REFRESH "true"
ENV USER_LABEL_KEY "yunikorn.apache.org/username"
ENTRYPOINT ["sh", "-c", "/k8s_yunikorn_scheduler \
-clusterId=${CLUSTER_ID} \
-clusterVersion=${CLUSTER_VERSION} \
-policyGroup=${POLICY_GROUP} \
-interval=${SCHEDULING_INTERVAL} \
-logLevel=${LOG_LEVEL} \
-logEncoding=${LOG_ENCODING} \
-volumeBindTimeout=${VOLUME_BINDING_TIMEOUT} \
-eventChannelCapacity=${EVENT_CHANNEL_CAPACITY} \
-dispatchTimeout=${DISPATCHER_TIMEOUT} \
-kubeQPS=${KUBE_CLIENT_QPS} \
-kubeBurst=${KUBE_CLIENT_BURST} \
-predicates=${PREDICATES} \
-operatorPlugins=${OPERATOR_PLUGINS} \
-enableConfigHotRefresh=${ENABLE_CONFIG_HOT_REFRESH} \
-userLabelKey=${USER_LABEL_KEY}"]
