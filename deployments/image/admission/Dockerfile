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

# Home location for all data and configs
ENV HOME=/opt/yunikorn

# Always run everything as yunikorn
RUN mkdir -p $HOME && \
    addgroup -S -g 4444 yunikorn && \
    adduser -S -h $HOME -G yunikorn -u 4444 yunikorn -s /bin/sh && \
    chown -R 4444:0 $HOME && \
    chmod -R g=u $HOME

COPY --chown=4444:0 scheduler-admission-controller $HOME/bin/

WORKDIR $HOME
USER yunikorn
ENTRYPOINT $HOME/bin/scheduler-admission-controller
