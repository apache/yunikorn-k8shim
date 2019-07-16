# Deployment example revisions

## v0.3.5

This was built on May 22th

* Deploys scheduler-core + scheduler-web
* Configuration loaded from config map
* Configuration hot refresh works with config map
* Web UI has basic cluster/queue/application info
* UI port is `9889`

deployment: [scheduler-v0.3.5.yaml](scheduler-v0.3.5.yaml)

A new deployment file was added with this version. The deployment documentation was updated to create RBAC for the scheduler.
The RBAC requirement is not linked to a specific version of YuniKorn but depends on the kubernetes version used. 
  
deployment: [yunikorn-rbac.yaml](yunikorn-rbac.yaml)

## v0.3.0

This was built on April 16th

* Deploys scheduler-core + scheduler-web
* Configuration loaded from config map (hot reload has some known issue)
* Web UI has basic cluster/queue/application info
* UI port is `9889`

deployment: [scheduler-v0.3.0.yaml](scheduler-v0.3.0.yaml)

## v0.2.0

This was built on April 12th

This deployment only deploys both yunikorn-core + k8s-shim and yunikorn-web on a local K8s.

* Deploys scheduler-core + scheduler-web
* Scheduler pod runs 2 containers, 1 for scheduler-core, 1 for scheduler-web
* Configuration file queues.yaml is built into the image
* Web UI has basic cluster/queue info
* A load balancer that helps to expose web UI link directly on K8s

deployment: [scheduler-v0.2.0.yaml](scheduler-v0.2.0.yaml)

## v0.1.11

This was built on April 10th

This version supports configuration hot-refresh, when there is update in ConfigMap, it automatically triggers hot-refresh.

deployment: [scheduler-v0.1.11.yaml](scheduler-v0.1.11.yaml)

## v0.1.10

This was built on April 10th

This deployment only deploys yunikorn-core + k8s-shim on a local K8s. No UI container is configured. This can be used for testing.

First version that supports loading configuration from ConfigMap.
* The docker image no longer ships configuration file, configuration will be loaded from ConfigMap
* Hot-refresh is not supported in this version

deployment: [scheduler-v0.1.10.yaml](scheduler-v0.1.10.yaml)

## v0.1.0

This was built on April 10th

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

deployment: [scheduler-v0.1.0.yaml](scheduler-v0.1.0.yaml)