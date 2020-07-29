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

# End to End Testing in YuniKorn-K8shim

End-to-end (e2e) tests for YuniKorn-K8shim provide a mechanism to test end-to-end behavior of the system, and is the last signal to ensure end user operations match developer specifications. 

The primary objectives of the e2e tests are to ensure a consistent and reliable behavior of the yunikorn code base, and to catch hard-to-test bugs before users do, when unit and integration tests are insufficient.

The e2e tests are built atop of [Ginkgo](https://onsi.github.io/ginkgo/) and [Gomega](https://github.com/onsi/gomega). There are a host of features that this Behavior-Driven Development (BDD) testing framework provides, and it is recommended that the developer read the documentation prior to diving into the tests.

Below is the structure of the project.
* `test/e2e/` contains tests for YuniKorn Features like Scheduling, Predicates etc
* `test/e2e/framework/configManager` manages & maintains the test and cluster configuration
* `test/e2e/framework/helpers` contains utility modules for k8s client, (de)serializers, rest api client and other common libraries.
* `test/e2e/testdata` contains all the test related data like configmaps, pod specs etc

## Pre-requisites
This project requires Go to be installed. On OS X with Homebrew you can just run `brew install go`.
OR follow this doc for deploying go https://golang.org/doc/install

## Understanding the Command Line Arguments
* `yk-namespace` - namespace under which YuniKorn is deployed. [Required]
* `kube-config` - path to kube config file, needed for k8s client [Required]
* `yk-host` - hostname of the YuniKorn REST Server, defaults to localhost.   
* `yk-port` - port number of the YuniKorn REST Server, defaults to 9080.
* `yk-scheme` - scheme of the YuniKorn REST Server, defaults to http.
* `timeout` -  timeout for all tests, defaults to 24 hours

## Launching Tests

### Trigger through CLI
* Launching CI tests is as simple as below.
```console
$ kubectl config use-context <<cluster-under-test-context>>
$ ginkgo -r -v ci -timeout=2h -- -yk-namespace "yunikorn" -kube-config "$HOME/.kube/config"
```

* Launching all the tests can be done as..
```console
$ ginkgo -r -v -timeout=2h -- -yk-namespace "yunikorn" -kube-config "$HOME/.kube/config"

```
