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

RUN apk add curl
RUN apk add jq
RUN apk add --update openssl
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.19.11/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl

COPY admission-controller-init-scripts/admission_util.sh /
COPY admission-controller-init-scripts/configs.properties /
COPY admission-controller-init-scripts/generate-signed-ca.sh /
COPY admission-controller-init-scripts/templates /templates
RUN chmod +x /admission_util.sh
RUN chmod +x /generate-signed-ca.sh

ADD scheduler-admission-controller /scheduler-admission-controller
ENTRYPOINT ["./scheduler-admission-controller"]
