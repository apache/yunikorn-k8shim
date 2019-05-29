# YuniKorn Scheduler for Kubernetes (k8s-shim)

YuniKorn scheduler for kubernetes is a customized k8s scheduler, it can be dropped into k8s and takes care of pod scheduling.
This project contains the k8s shim layer code for k8s, it depends on `yunikorn-core` which encapsulates all the actual scheduling logic.
By default, it handles all pods scheduling if pod's spec has field `schedulerName: yunikorn`.

## Development Environment setup

Read [env-setup](./docs/env-setup.md) first to setup Docker, Kubernetes development environment.

### 1. Get source code
```
cd $GOPATH
mkdir -p src/github.infra.cloudera.com/yunikorn/
cd src/github.infra.cloudera.com/yunikorn/
git clone https://github.infra.cloudera.com/yunikorn/k8s-shim.git
```

### 2. Build and run it locally

#### Build binary on laptop

```
make build
```

this command will build a binary `k8s_yunikorn_scheduler` under project root. This binary is executable on local environment, as long as `kubectl` is properly configured.
Run `./k8s_yunikorn_scheduler -help` to see all options.

**Note**: it may take few minutes to run this command for the first time, because it needs to download all dependencies.

#### Alternatively, you can just run

```
make run
```

this will build the code, and run the binary with verbose logging.


### 3. Deploy to a k8s cluster

#### Build docker image for k8s-shim

```
make image
```

this command will build the image, tag it and push to a docker hub repo.
You may need to modify the image name and tag in `Makefile` if you want to push it somewhere else.

#### Create RBAC for the scheduler

```
kubectl create -f deployments/scheduler/yunikorn-rbac.yaml
```

#### Deploy the scheduler on k8s

```
kubectl create -f deployments/scheduler/scheduler-v0.2.0.yaml
```

#### Run sample jobs

All sample deployments can be found under `./deployments` directory.

```
// some nginx pods
kubectl create -f deployments/nigix/nginxjob.yaml

// some pods simply run sleep
kubectl create -f deployments/sleep/sleeppods.xml
```

`./deployments/spark` contains pod template files for Spark driver and executor, they can be used if you want to run Spark on k8s using this scheduler.

#### Access UI

```
// get name of yunikorn pod
kubectl get pods | grep yunikorn-scheduler | cut -d " " -f 1

// UI port
kubectl port-forward ${podName} 9889

// web service port
kubectl port-forward ${podName} 9080
```

Tutorial of running Spark with YuniKorn can be found [here](./docs/spark.md).