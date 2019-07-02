# Examples

Explore sample yaml files under this package to submit applications and get them scheduled by yunikorn.

##nignx

##predicates

##sleep

##spark

##volumes
The volumes directory contains two cases:

Both cases will generate an unending stream of data in a file called `dates.txt` on the mounted volume. 
* local volume
  * create the local volume and volume claim using the [local-pv.yaml](./volumes/local-pv.yaml). 
  * create the pod that uses the volume via [pod-local.yaml](./volumes/pod-local.yaml).
* NFS volume
  * start a NFS server which exports a local directory from the server instance [nfs-server.yaml](./volumes/nfs-server.yaml).
  * get the IP address of the NFS server pod via `kubectl get services`. Use the cluster IP address returned to update the client pod yaml for the next step.
  * create the pod that uses the exported nfs volume [pod-nfs.yaml](./volumes/pod-nfs.yaml)

Note that the NFS server will not work on a Docker Desktop in a Mac OS-X environment with kubernetes turned on as it does not support the kernel NFS driver used by the server.
Use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) as a workaround.