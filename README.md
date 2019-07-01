# YuniKorn Scheduler for Kubernetes (k8s-shim)

YuniKorn scheduler for kubernetes is a customized k8s scheduler, it can be dropped into k8s and takes care of pod scheduling.
This project contains the k8s shim layer code for k8s, it depends on `yunikorn-core` which encapsulates all the actual scheduling logic.
By default, it handles all pods scheduling if pod's spec has field `schedulerName: yunikorn`.

## Development Environment setup

Read [env-setup](./docs/env-setup.md) first to setup Docker, Kubernetes development environment.

### 1. Get source code
```
cd $GOPATH
mkdir -p src/github.com/cloudera/
cd src/github.com/cloudera/
git clone https://github.com/cloudera/k8s-shim.git
```

### 2. Build and run it locally
Prerequisite:
- Go 1.11+

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
kubectl create -f deployments/scheduler/scheduler-v0.3.5.yaml
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

## How do I contribute code?
You need to first sign and return an
[ICLA](https://github.com/cloudera/yunikorn-core/blob/master/CLAs/Cloudera%20ICLA_25APR2018.pdf)
and
[CCLA](https://github.com/cloudera/yunikorn-core/blob/master/CLAs/Cloudera%20CCLA_25APR2018.pdf)
before we can accept and redistribute your contribution. Once these are submitted you are
free to start contributing to k8s-shim. Submit these to CLA@cloudera.com.

### Find
We use Github issues to track bugs for this project. Find an issue that you would like to
work on (or file one if you have discovered a new issue!). If no-one is working on it,
assign it to yourself only if you intend to work on it shortly.

It’s a good idea to discuss your intended approach on the issue. You are much more
likely to have your patch reviewed and committed if you’ve already got buy-in from the
yunikorn community before you start.

### Fix
Now start coding! As you are writing your patch, please keep the following things in mind:

First, please include tests with your patch. If your patch adds a feature or fixes a bug
and does not include tests, it will generally not be accepted. If you are unsure how to
write tests for a particular component, please ask on the issue for guidance.

Second, please keep your patch narrowly targeted to the problem described by the issue.
It’s better for everyone if we maintain discipline about the scope of each patch. In
general, if you find a bug while working on a specific feature, file a issue for the bug,
check if you can assign it to yourself and fix it independently of the feature. This helps
us to differentiate between bug fixes and features and allows us to build stable
maintenance releases.

Finally, please write a good, clear commit message, with a short, descriptive title and
a message that is exactly long enough to explain what the problem was, and how it was
fixed.

Please create a pull request on github with your patch.
