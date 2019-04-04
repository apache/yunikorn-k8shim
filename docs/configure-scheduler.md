## Build docker image (without conf file)

Under project root, run command

```
make image2
```

this command will build image `yunikorn/scheduler-core:0.1.10` and push this image to [DockerHub](https://cloud.docker.com/u/yunikorn/repository/docker/yunikorn/scheduler-core).

_Note, push requires docker login using user `yunikorn`. You might need to comment out the push step from the Makefile or replace it with some other repository you have permission to._ 

## Create ConfigMap

This must be done before deploying the scheduler.

```
// 1) download configuration file
curl -o queues.yaml https://github.infra.cloudera.com/raw/yunikorn/k8s-shim/master/conf/queues.yaml

// 2) create configmap
kubectl create configmap yunikorn-configs --from-file=queues.yaml
configmap/yunikorn-configs created

// 3) check configmap
kubectl describe configmaps yunikorn-configs
```

**Note**, if name of the ConfigMap is something else other than `yunikorn-configs`, the updates to the ConfigMap will not be detected, therefore configuration hot-refresh will not work in this case.

## Attach ConfigMap Volume to Scheduler Pod

This is done in the scheduler yaml file, please look at [scheduler-configmap.yaml](../deployments/scheduler/scheduler-configmap.yaml)
for reference.


## Deploy the Scheduler

```
kubectl create -f deployments/scheduler/scheduler-configmap.yaml
```




