# YuniKorn QE 

This repository is the home for YuniKorn testing. This repo uses [ginkgo framework](https://onsi.github.io/ginkgo/) (BDD)  and [Gomega](https://github.com/onsi/gomega) matcher library to model the tests and are written in Go.

Below is the structure of the project.
* `test/e2e/CI` contains CI tests for YuniKorn
* `test/framework/cfg_manager` manages & maintains the test and cluster configuration
* `test/framework/testdata` contains all the test related data like configmaps, pod specs etc
* `test/framework/helpers` contains utility modules for k8s client, (de)serializers, rest api client and other common libraries.

## Pre-requisites
This project requires Go to be installed. On OS X with Homebrew you can just run `brew install go`.
OR follow this doc for deploying go https://golang.org/doc/install


## Understanding the Command Line Arguments
* `yk-namespace` - namespace under which YuniKorn is deployed. [Required]
* `kube-config` - path to kube config file, needed for k8s client [Required]
* `yk-host` - hostname of the yunikorn service, defaults to localhost.   
* `yk-port` - port number of the yunikorn service, defaults to 9080.
* `yk-scheme` - scheme of the yunikorn service, defaults to http.
* `timeout` -  timeout for the tests run

## Launching Tests

### Trigger through CLI
* Launching CI tests is as simple as below.
```console
$ kubectl config use-context <<cluster-under-test-context>>
$ ginkgo -r -v CI -- -yk-namespace "yunikorn-ns" -kube-config "$HOME/.kube/config"
```

* Launching all the tests can be done as..
```console
$ ginkgo -r -v -- -yk-namespace "yunikorn-ns" -kube-config "$HOME/.kube/config"

```

### Trigger through docker image
-- TODO --


