<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

# TensorFlow training with YuniKorn
This doc gives a brief introduction about how to use [training-operator](https://github.com/kubeflow/training-operator)
to train TF models with YuniKorn scheduler on K8s, please read this [guide](https://yunikorn.apache.org/docs/user_guide/workloads/run_tf)
for more information.

## Setup
1. You need to set up YuniKorn scheduler on K8s cluster, please refer to [this doc](https://yunikorn.apache.org/docs/#install).
2. Install [training-operator](https://github.com/kubeflow/training-operator) that makes it easy to run distributed 
   or non-distributed ML jobs on K8s. You can install it with the following command.
```   
kubectl apply -k "github.com/kubeflow/training-operator/manifests/overlays/standalone?ref=v1.3.0"
```
3. Build a docker image  with the following command.
```
docker build -f Dockerfile -t kubeflow/tf-dist-mnist-test:1.0 .
```

## Run a TensorFlow job
You need to create a TFjob and configure it to use YuniKorn scheduler.
```
kubectl create -f tf-job-mnist.yaml
```

## Monitor your job
You can view the job info from YuniKorn UI. If you do not know how to access the YuniKorn UI, please read the
 [doc](https://yunikorn.apache.org/docs/#access-the-web-ui) here.
