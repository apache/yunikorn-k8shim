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

FROM scratch
ADD k8s_yunikorn_scheduler /k8s_yunikorn_scheduler
ADD queues.yaml /queues.yaml
ENV LOG_ENCODING "console"
ENV LOG_LEVEL "-1"
ENV CLUSTER_ID "mycluster"
ENV CLUSTER_VERSION "latest"
ENTRYPOINT ["sh", "-c", "/k8s_yunikorn_scheduler -logEncoding=${LOG_ENCODING} \
-logLevel=${LOG_LEVEL} -clusterId=${CLUSTER_ID} -clusterVersion=${CLUSTER_VERSION}"]