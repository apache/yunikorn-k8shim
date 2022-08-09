#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#limitations under the License.
#

# If you want to re-run the code-generator to generate code,
# Please make sure the directory structure must be the example.
# ex: github.com/apache/yunikorn-k8shim  

./scripts/generate-groups.sh "all" \
  github.com/apache/yunikorn-k8shim/pkg/client github.com/apache/yunikorn-k8shim/pkg/apis \
  "yunikorn.apache.org:v1alpha1" \
  --go-header-file "$(dirname "${BASH_SOURCE[@]}")/custom-boilerplate.go.txt" \
  --output-base "$(dirname "${BASH_SOURCE[@]}")/../../../.."

./scripts/generate-groups.sh "all" \
  github.com/apache/yunikorn-k8shim/pkg/sparkclient github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis \
  "sparkoperator.k8s.io:v1beta2" \
  --go-header-file "$(dirname "${BASH_SOURCE[@]}")"/custom-boilerplate.go.txt \
  --output-base "$(dirname "${BASH_SOURCE[@]}")/../../../.."
