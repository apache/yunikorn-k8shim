# YuniKorn Scheduler for Kubernetes (yunikorn-k8shim)

YuniKorn scheduler shim for kubernetes is a customized k8s scheduler, it can be deployed in a K8s cluster and work as the scheduler.
This project contains the k8s shim layer code for k8s, it depends on `yunikorn-core` which encapsulates all the actual scheduling logic.
By default, it handles all pods scheduling if pod's spec has field `schedulerName: yunikorn`.

For detailed information on how to build the overall scheduler please see the [build document](https://github.com/apache/incubator-yunikorn-core/blob/master/docs/developer-guide.md) in the `yunikorn-core`.

## K8s-shim component build
This component build should only be used for development builds.
Prerequisites and build environment setup is described in the above mentioned build document.

### Build binary
The simplest way to get a local binary that can be run on a local Kubernetes environment is: 
```
make build
```
This command will build a binary `k8s_yunikorn_scheduler` under `_output/bin` dir. This binary is executable on local environment, as long as `kubectl` is properly configured.
Run `./k8s_yunikorn_scheduler -help` to see all options.
language: go

os:
  - linux

go:
  - "1.12"

git:
  depth: 1

services:
  - 'docker'

env:
  - GO111MODULE=on

install: true

jobs:
  include:
    - stage: unit test
      script:
        - curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.22.2
        - make common-check-license
        - make test
        - make lint
      after_success: bash <(curl -s https://codecov.io/bash)
    - stage: publish docker image
      deploy:
        provider: script
        script: make push
        on:
          branch: master
          condition: $TRAVIS_EVENT_TYPE = cron
    - stage: integration tests
      before_script:
        - curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && chmod +x kubectl && sudo mv kubectl /usr/local/bin/
        - go get sigs.k8s.io/kind
        - kind create cluster
        - export KUBECONFIG="$(kind get kubeconfig-path)"
      script: kubectl get nodes
**Note**: It may take few minutes to run this command for the first time, because it needs to download all dependencies.
In case you get an error relating to `checksum mismatch`, run `go clean -modcache` and then rerun `make build`.

### Build run
If the local kubernetes environment is up and running you can build and run the binary via: 
```
make run
```
This will build the code, and run the scheduler with verbose logging. 
It will set the configuration for the scheduler to the provided default configuration `queues.yaml` and uses the current setup for kubernetes.

### Build and run tests
Unit tests for the shim only can be run via:
```
make test
```
Any changes made to the shim code should not cause any existing tests to fail.

### Build image steps
Build docker image can be triggered by running following command.

```
make image
```

You can set `REGISTRY` and `VERSION` in the commandline to build docker image with a specified tag and version. For example,
```
make image REGISTRY=yunikorn VERSION=latest
```
This command will build a binary executable with version `latest` and the docker image tag is `yunikorn/yunikorn-scheduler-k8s:latest`.

You can run following command to retrieve the meta info for a docker image build, such as component revisions, date of the build, etc.

```
docker inspect --format='{{.Config.Labels}}' yunikorn/yunikorn-scheduler-k8s:latest
```

## Design documents
All design documents are located in a central location per component. The core component design documents also contains the design documents for cross component designs.
[List of design documents](docs/design/design-index.md) for the k8s-shim.

## How do I contribute code?

See how to contribute code in [this guide](docs/how-to-contribute.md).

