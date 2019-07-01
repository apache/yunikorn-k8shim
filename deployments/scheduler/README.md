# Image Revisions

Image repo: 

* Yunikorn-core + k8s-shim: https://hub.docker.com/r/yunikorn/scheduler-core
* YuniKorn-web: https://hub.docker.com/r/yunikorn/scheduler-web

## [scheduler-v0.1.0.yaml](scheduler-v0.1.0.yaml)

Image Versions
* scheduler-core version: v0.1.0
* scheduler-web  version: v0.1.0

Very basic version used in first demo session.
* Deploys scheduler-core + scheduler-web
* Scheduler pod runs 3 containers, 1 for scheduler-core, 1 for scheduler-web, the other for kubectl (note, kubectl container is removed in further versions)
* Configuration file queues.yaml is built into the image
* Web UI has basic cluster/queue info

Capabilities:
* Hierarchical queues
* Calculate fair shares for queues and apps
* Schedule spark jobs
* Schedule applications as long as scheduler name is `yunikorn`
* Simple UI with cluster and queue info 

## v0.2.0

This was built on Apr 10th
* Deploys scheduler-core + scheduler-web
* Scheduler pod runs 2 containers, 1 for scheduler-core, 1 for scheduler-web
* Configuration file queues.yaml is built into the image
* Web UI has basic cluster/queue info
* A load balancer that helps to expose web UI link directly on K8s

deployment: [scheduler-v0.2.0.yaml](scheduler-v0.2.0.yaml)

this deployment only deploys both yunikorn-core + k8s-shim and yunikorn-web on a local K8s.

## v0.3.0

This was built on Apr 16th
* Deploys scheduler-core + scheduler-web
* Configuration loaded from config map (hot reload has some known issue)
* Web UI has basic cluster/queue/application info
* UI port is `9889`

deployment: [scheduler-v0.3.0.yaml](scheduler-v0.3.0.yaml)

## v0.3.5

This was built on May 22th
* Deploys scheduler-core + scheduler-web
* Configuration loaded from config map
* Configuration hot refresh works with config map
* Web UI has basic cluster/queue/application info
* UI port is `9889`

deployment: [scheduler-v0.3.0.yaml](scheduler-v0.3.0.yaml)

## v0.1.10

First version that supports loading configuration from ConfigMap.
* The docker image no longer ships configuration file, configuration will be loaded from ConfigMap
* Hot-refresh is not supported in this version
* the deployment 

deployment: [scheduler-v0.1.10.yaml](scheduler-v0.1.10.yaml)

this deployment only deploys yunikorn-core + k8s-shim on a local K8s. No UI container is configured. This can be used for testing.

## v0.1.11

This version supports configuration hot-refresh, when there is update in ConfigMap, it automatically triggers hot-refresh.

Revision: 

core: 6cf28a55186a374e5a48cb29e493fdb2145761ac
shim: f26ce24431445ccebc651527a00f2d51a2878433

deployment: [scheduler-v0.1.11.yaml](scheduler-v0.1.11.yaml)