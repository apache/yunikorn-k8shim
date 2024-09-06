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

# Imagestage: use scratch base image
FROM scratch
COPY --chown=0:0 document/index.html /html/
COPY --chown=0:0 LICENSE NOTICE third-party-licenses.md web-test-server /
EXPOSE 9889
ENV DOCUMENT_ROOT /html
USER 4444:4444
ENTRYPOINT [ "/web-test-server" ]
