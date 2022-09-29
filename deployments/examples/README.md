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

# Examples

Explore sample yaml files under this package to submit applications and get them scheduled by yunikorn.

## nginx
A simple one pod deployment of a [nginx](./nginx/nginx.yaml) image.

## predicates
Several examples for affinity and anti affinity:

* [node affinity](./predicates/node-affinity-example.yaml).
* [node selector](./predicates/pod-nodeselector-example.yaml).
* [pod affinity](./predicates/pod-affinity-example.yaml).
* [pod anti affinity](./predicates/pod-anti-affinity-example.yaml).

## sleep
Two sample deployments using sleep jobs:
* [simple sleep](./sleep/sleeppods.yaml) launches 3 pods, each pod sleeps 30 seconds.
* [batch sleep](./sleep/batch-sleep-job.yaml) launch a job with 50 pods, each pod sleeps 300 seconds.

## spark
A sample application using Apache Spark.

Submit a spark job with the [run.sh](spark/cmd/run.sh) script, for full details see the [user guide](https://yunikorn.apache.org/docs/user_guide/workloads/run_spark) in the yunikorn-core.

Deployment files for the driver and executor: 
* [driver](spark/driver.yaml).
* [executor](spark/executor.yaml).

## Tensorflow job
A simple example that runs a [kubeflow/tensorflow](./tfjob/tf-job-mnist.yaml) job.
In this example it will run a distributed mnist model for e2e test, for full more detail see the [dist-mnist](https://github.com/kubeflow/training-operator/tree/master/examples/tensorflow/dist-mnist) section.

## gang
A sample application which implement gang in application level.
Start via the [gangDeploy.sh](./gang/cmd/gangDeploy.sh) script, for full more detail see the [gang](./gang/README.md) section.

Deployment file for gang-coordinator and gang-job:
* [gang-coordinator](./gang/gang-coordinator.yaml)
* [gang-job](./gang/gang-job.yaml)

## volumes
The volumes directory contains three examples:
1. [local volumes](#local-volume)
1. [NFS volume](#nfs-volume)
1. [EBS volumes](#ebs-volume)

All examples will generate an unending stream of data in a file called `dates.txt` on the mounted volume.
Run times should be limited or the disk usage must be monitored for longer runs. 

### local volume
* create the local volume and volume claim using the [local-pv.yaml](./volume/local-pv.yaml).
* create the pod that uses the volume via [pod-local.yaml](./volume/pod-local.yaml).

### NFS volume
1. start a NFS server which exports a local directory from the server instance [nfs-server.yaml](./volume/nfs-server.yaml).
1. get the IP address of the NFS server pod via 
   ```shell script
   kubectl get services | grep nfs-server | awk '{print $3}'
   ```
   Use the cluster IP address returned to update the client pod yaml for the next step.
1. replace nfs server the IP address in the [pod-nfs.yaml](./volume/pod-nfs.yaml) with the one retrieved above.
1. create the client pod that uses the exported nfs volume.

_NOTE_: The NFS server will not work on a Docker Desktop in a Mac OS-X environment. Even with kubernetes turned on. The image used by Docker Desktop does not support the kernel NFS driver used by the server.
Use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) as a workaround and follow the [environment setup](https://github.com/apache/yunikorn-core/blob/master/docs/setup/env-setup.md#installing-minikube) to add it to Docker Desktop.

### EBS volume
EBS volumes cannot be used outside of AWS. Therefor you must have an EKS cluster on AWS.
Pre-requisites and installation that is not described in this document:
* create a EKS cluster on AWS: [Creating an Amazon EKS Cluster](https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html).
* deploy the dashboard: [EKS dashboard tutorial](https://docs.aws.amazon.com/eks/latest/userguide/dashboard-tutorial.html).
  
Three example cases:
* bind an existing volume into the pod directly using the volume ID: [pod-ebs-direct.yaml](./volume/pod-ebs-direct.yaml).
    pre-requisite: create an EBS volume and retrieve the ID.
* bind an existing volume into the pod using a PV and PVC: [pod-ebs-exist.yaml](./volume/pod-ebs-exist.yaml).
    pre-requisite: add the PV on top of an existing EBS volume [ebs-pv.yaml](./volume/ebs-pv.yaml).
* create a new volume using dynamic provisioning via the storage class: [pod-ebs-dynamic.yaml](./volume/pod-ebs-dynamic.yaml).
    pre-requisite: add the storage class: [storage-class.yaml](./volume/storage-class.yaml). the cluster must also have `--enable-admission-plugins DefaultStorageClass` set.
    see [Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) in the kubernetes docs.
  
_NOTE_: dynamic provisioning can interfere with the existing volume examples given.

## namespace
The namespace example uses a placement rule and special queue configuration. The pods are a simple sleep pod which will be scheduled based on the namespace it runs in. The pod specification does not have a queue set.

* create the configmap as explained in the [user guide](https://yunikorn.apache.org/docs/user_guide/queue_config#queues) using the local [queues.yaml](./namespace/queues.yaml) file not the standard one.
* add the namespaces development and production using [namespaces.yaml](namespace/namespaces.yaml) file: `kubectl create -f namespaces.yaml`
* run the sleep pod in the development namespace which gets added to the `development` queue using the local [sleeppod_dev.yaml](namespace/sleeppod_dev.yaml): `kubectl create -f sleeppod_dev.yaml`.
  The pod spec does not specify a queue just a namespace but the application will be run in the `root.development` queue. 
* run the sleep pod in the production namespace which creates a new `production` queue using the local [sleeppod_prod.yaml](namespace/sleeppod_prod.yaml): `kubectl create -f namespaces.yaml`.
  The pod spec does not specify a queue just a namespace but the application will be run in the newly created `root.production` queue. This queue does not exist in the queue configuration. 

### placements
The placements' rules are described in [Yunikorn website](https://yunikorn.apache.org/docs/user_guide/placement_rules).
App placements rules in Yunikorn contains `Provided Rule`, `User Name Rule`, `Fixed Rule`, `Tag Rule`.
Every placement example includes a example yaml file and a config yaml file.
The sleep pod is described in the example file and the partition is provided in the config yaml file.
Before deploying the pods, the configuration field in yunikorn-release/helm/yunikorn/value.yaml should be replaced by the configuration field in the config.yaml.

* [Provided Rule](./placements/provided)
* [User Name Rule](./placements/username)
* [Fixed Rule](./placements/fixed)
* [Tag Rule](./placements/tag)
