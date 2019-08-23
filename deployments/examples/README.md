# Examples

Explore sample yaml files under this package to submit applications and get them scheduled by yunikorn.

## nignx
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

Start via the [run.sh](./spark/cmd/run.sh) script, for full details see the [user guide](https://github.com/cloudera/yunikorn-core/blob/master/docs/user-guide.md) in the yunikorn-core.

Deployment files for the driver and executor: 
* [driver](./spark/driver.yaml).
* [executor](./spark/executor.yaml).

## volumes
The volumes directory contains three cases:

Both cases will generate an unending stream of data in a file called `dates.txt` on the mounted volume. 
* local volume
  * create the local volume and volume claim using the [local-pv.yaml](./volume/local-pv.yaml). 
  * create the pod that uses the volume via [pod-local.yaml](./volume/pod-local.yaml).
* NFS volume
  * start a NFS server which exports a local directory from the server instance [nfs-server.yaml](./volume/nfs-server.yaml).
  * get the IP address of the NFS server pod via `kubectl get services`. Use the cluster IP address returned to update the client pod yaml for the next step.
  * create the pod that uses the exported nfs volume [pod-nfs.yaml](./volume/pod-nfs.yaml).

_NOTE_: The NFS server will not work on a Docker Desktop in a Mac OS-X environment with kubernetes turned on as it does not support the kernel NFS driver used by the server.
Use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) as a workaround.

* EBS volume with Kubernetes on AWS

  Pre-requisites that are not decribed:
  * create a EKS cluster on AWS: [Creating an Amazon EKS Cluster](https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html).
  * deploy the dashboard: [EKS dashboard tutorial](https://docs.aws.amazon.com/eks/latest/userguide/dashboard-tutorial.html).
  
  Three example cases:
  * Bind an existing volume into the pod using a PV and PVC: [pod-ebs-exist.yaml](./volume/pod-ebs-exist.yaml).
    <br>Pre-requisite: add the volume on top of an existing volume [ebs-pv.yaml](./volume/ebs-pv.yaml).
  * Bind an existing volume into the pod directly using the volume ID: [pod-ebs-direct.yaml](./volume/pod-ebs-direct.yaml).
  * Create a new volume using dynamic provisioning via the storage class: [pod-ebs-dynamic.yaml](./volume/pod-ebs-dynamic.yaml).
    <br>Pre-requisite: add the storage class: [storage-class.yaml](./volume/storage-class.yaml). the cluster must also have `--enable-admission-plugins DefaultStorageClass` set.
    <br>See [Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) in the kubernetes docs.
  
_NOTE_: dynamic provisioning can interfere with the existing volume examples given.

## namespace
The namespace example uses a placement rule and special queue configuration. The pod is a simple sleep pod which will be scheduled based on the namespace it runs in. The pod does not have a queue set.

* create the configmap as explained in the [user guide](https://github.com/cloudera/yunikorn-core/blob/master/docs/user-guide.md#create-the-configmap) using the local [queues.yaml](./namespace/queues.yaml) file not the standard one.
* add the development namespace using [development](./namespace/development.yaml) file: `kubectl create -f development.yaml`
* run the sleep pod using the local [sleeppod.yaml](./namespace/sleeppod.yaml). The pod spec does not specify a queue just a namespace but the application will be run in the `root.development` queue. 