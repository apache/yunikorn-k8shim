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

## Performance testing tools

For the [performance evaluation of YuniKorn with Kwok](https://yunikorn.apache.org/docs/next/performance/evaluate_perf_function_with_kwok), we've conducted four distinct performance tests: Throughput, Taint & Tolerations, Affinity & Anti-Affinity, and PriorityClass. This folder contains the tools used in these tests, enabling you to easily replicate the tests or fine-tune the performance evaluation process.

### Prerequisites
For this test, we're using `Kwok in a cluster` setup, which means you'll need to have a Kubernetes cluster ready beforehand.

### Introduction of tools
- kwok-setup.sh script allows for the swift establishment of Kwok within your cluster, with the capability to generate nodes based on the quantity you define.
  Example:
  ```
  $ ./kwok-setup.sh 3
  ...
  customresourcedefinition.apiextensions.k8s.io/attaches.kwok.x-k8s.io created
  customresourcedefinition.apiextensions.k8s.io/clusterattaches.kwok.x-k8s.io created
  customresourcedefinition.apiextensions.k8s.io/clusterexecs.kwok.x-k8s.io created
  ...
  node/kwok-node-0 created
  node/kwok-node-1 created
  node/kwok-node-2 created
  ```

- deploy-tool.sh is designed for throughput testing, allowing for rapid deployment creation and deletion while enabling you to set the desired number of replicas.
  Example:
  ```
  $ ./deploy-tool.sh 
  Usage: ./deploy-tool.sh [-d] [-i <interval>] <deployment_count> <replicas_count>

  Options:
    -d, --delete               Delete the specified number of deployments.
    -i, --interval <interval>  Set the interval between deployments in seconds.

  Arguments:
    <deployment_count>         Number of deployments to create or delete (required).
    <replicas_count>           Number of replicas for each deployment (required).

  $ ./deploy-tool.sh 3 10
  Deploying 3 deployments with 10 replicas each.
  deployment.apps/sleep-deployment-0 created
  deployment.apps/sleep-deployment-1 created
  deployment.apps/sleep-deployment-2 created

  $ ./deploy-tool.sh -d 3 10
  Deleting 3 deployments with 10 replicas each.
  deployment.apps "sleep-deployment-0" deleted
  deployment.apps "sleep-deployment-1" deleted
  deployment.apps "sleep-deployment-2" deleted
  ```

- toleration.sh offers two key functionalities: firstly, it allows for the tainting and untainting of nodes; secondly, it facilitates the creation of Taint & Toleration test cases using YAML templates.
  Example:
  ```
  $ ./toleration.sh
  Usage: ./toleration.sh <pod_count> <priorityClass_count>

  Commands:
    -t,   Taint kwok nodes with its index
    -u,   Untaint kwok nodes with its index
    -o,   Specifies the location of the output yaml file (default is ./output/priority.yaml)

  Arguments:
    <pod_count>     Number of pod to create (required).
    <node_count>    Number of kwok nodes (required).

  $ ./toleration.sh -t 10 3
  node/kwok-node-0 tainted
  node/kwok-node-1 tainted
  node/kwok-node-2 tainted

  $ ./toleration.sh -u 10 3
  node/kwok-node-0 untainted
  node/kwok-node-1 untainted
  node/kwok-node-2 untainted

  $ ./toleration.sh 10 3
  The file ./output/toleration.yaml does not exist.
  Create file ./output/toleration.yaml.
  Create 10 Pods and save them to a YAML file, each Pod with a randomly assigned toleration.
  $ cat ./output/toleration.yaml
  apiVersion: v1
  kind: Pod
  metadata:
    name: nginx-0
  ...
    tolerations:
    ...
    - key: key-0
      operator: "Exists"
  ```

- affinity.sh is designed to generate YAML-based test cases for both Affinity & Anti-Affinity. The script covers tests for node affinity and pod affinity, where node affinity is further categorized into required and preferred rules. For optimal testing, the specified `<pod_count>` must be divisible by 4.
  Example:
  ```
  $ ./affinity.sh
  Usage: ./affinity.sh <pod_count> <node_count>

  Options:
    -o,             Specifies the location of the output yaml file (default is ./output/affinity.yaml)

  Arguments:
    <pod_count>     Number of pod to create (required).
    <node_count>    Number of kwok nodes (required).

  $ ./affinity.sh 8 3
  The original content of the file located at ./output/affinity.yaml has been cleared.
  Create 2 pods, each with random node affinity using the required rule.
  Create 2 pods, each with random node affinity using the preferred rule.
  Create 4 pods, each with random node affinity using the preferred rule.
  $ cat output/affinity.yaml
  apiVersion: v1
  kind: Pod
  metadata:
    name: nginx-0
    ...
    affinity:
      nodeAffinity:
      ...
        - key: kubernetes.io/hostname
        operator: In/NotIn
  ```

- priority.sh specializes in creating PriorityClass test cases, also utilizing YAML for test case definition.
  Example:
  ```
  $ ./priority.sh
  Usage: ./priority.sh <pod_count> <priorityClass_count>

  Options:
    -o,                      Specifies the location of the output yaml file (default is ./output/priority.yaml)

  Arguments:
    <pod_count>              Number of pod to create (required).
    <priorityClass_count>    Number of priorityClass to create (required).

  $ ./priority.sh 10 10
  The file ./output/priority.yaml does not exist.
  The create ./output/priority.yaml.
  Create 10 PriorityClass and save then to a YAML file.
  Create 10 Pods and save them to a YAML file, with each Pod assigned a PriorityClass selected at random.

  $ cat output/priority.yaml
  apiVersion: scheduling.k8s.io/v1
  kind: PriorityClass
  metadata:
    name: priority-0
  value: 0
  ...
  ---
  apiVersion: v1
  kind: Pod
  metadata:
    name: nginx-9
  spec:
    ...
    priorityClassName: priority-4
  ```

