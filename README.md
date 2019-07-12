# YuniKorn Scheduler for Kubernetes (yunikorn-k8shim)

YuniKorn scheduler shim for kubernetes is a customized k8s scheduler, it can be deployed in a K8s cluster and work as the scheduler.
This project contains the k8s shim layer code for k8s, it depends on `yunikorn-core` which encapsulates all the actual scheduling logic.
By default, it handles all pods scheduling if pod's spec has field `schedulerName: yunikorn`.

For detailed information on the components and how to build the overall scheduler please see the [yunikorn-core](https://github.com/cloudera/yunikorn-core).

## Development Environment setup

Read [env-setup](https://github.com/cloudera/yunikorn-core/blob/master/docs/env-setup.md) first to setup Docker, Kubernetes development environment.

## Build local steps
The dependencies in the project are managed using [go modules](https://blog.golang.org/using-go-modules).   

Prerequisite:
- Go 1.11+

### Build binary
The simplest way to get a local binary that can be run on a local Kubernetes environment is: 
```
make build
```

This command will build a binary `k8s_yunikorn_scheduler` under `_output/bin` dir. This binary is executable on local environment, as long as `kubectl` is properly configured.
Run `./k8s_yunikorn_scheduler -help` to see all options.

**Note**: it may take few minutes to run this command for the first time, because it needs to download all dependencies.

### Build run
If the local environment is up and running you can build and run the binary via: 
```
make run
```

This will build the code, and run the binary with verbose logging. It will set the configuration for the scheduler to the provided default configuration `queues.yaml`.

## Build image steps
Build docker image can be triggered by running one of the following two image targets:

Build an image that uses a build in configuration:
```
make image
```
or build an image that uses a config map:
```
make image_map
```
You *must* update the `IMAGE_TAG` variable in the `Makefile` to push to an accessible repository.

## Design documents
All design documents are located in a central location per component. The core component design documents also contains the design documents for cross component designs.
[List of design documents](docs/design/design-index.md) for the k8s-shim.

## How do I contribute code?

See how to contribute code in [this guide](docs/how-to-contribute.md).

