# Deployment examples

## scheduler

* Deploys scheduler-core + scheduler-web
* Scheduler pod runs 2 containers, 1 for scheduler-core, 1 for scheduler-web
* Configuration loaded from config map
* UI port is `9889`

deployment: [scheduler.yaml](scheduler.yaml)

## RBAC

A new deployment file was added with this version. The deployment documentation was updated to create RBAC for the scheduler.
The RBAC requirement is not linked to a specific version of YuniKorn but depends on the kubernetes version used. 
  
deployment: [yunikorn-rbac.yaml](yunikorn-rbac.yaml)

## loadbalancer

* Deploys scheduler-core + scheduler-web
* Scheduler pod runs 2 containers, 1 for scheduler-core, 1 for scheduler-web
* Configuration loaded from config map
* UI port is `9889`
* A load balancer that helps to expose web UI link directly on K8s

deployment: [scheduler-load.yaml](scheduler-load.yaml)

