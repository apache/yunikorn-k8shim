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

# Deployment examples

YuniKorn can be deployed in two different modes: standard and plugin.
Detailed description are here: [Deployment Modes](https://yunikorn.apache.org/docs/next/user_guide/deployment_modes)

## Scheduler Standard mode

Deploys scheduler-core + scheduler-web

* Scheduler pod runs 2 containers, 1 for scheduler-core, 1 for scheduler-web
* UI port is `9889`

Deployment: [scheduler.yaml](scheduler.yaml)

## Scheduler Plugin mode

Deployment: [plugin.yaml](plugin.yaml)

## Scheduler configuration

This deployment contains a default configuration for YuniKorn.
Detailed description are here: [Service Configuration](https://yunikorn.apache.org/docs/next/user_guide/service_config)

Deployment: [yunikorn-configs.yaml](yunikorn-configs.yaml)

## RBAC (Scheduler)

A new deployment file was added with this version. The deployment documentation was updated to create RBAC for the scheduler.
The RBAC requirement is not linked to a specific version of YuniKorn but depends on the kubernetes version used. 
  
Deployment: [yunikorn-rbac.yaml](yunikorn-rbac.yaml)

## Load Balancer

A load balancer that helps to expose web UI link directly on K8s

Deployment: [service.yaml](service.yaml)

## Admission Controller

Deploys an admission controller which will modify newly created pods to force them to be scheduled by YuniKorn. This
allows YuniKorn to take over the scheduling duties of the cluster without replacing the default scheduler explicitly.

* Deployment: [admission-controller-rbac.yaml](admission-controller-rbac.yaml)
  * Configures required roles and permissions to allow the admission controller to run.
* Deployment: [admission-controller-secrets.yaml](admission-controller-secrets.yaml) 
  * Deploys an empty secret which is used to store TLS certificates and keys.
* Deployment: [admission-controller.yaml](admission-controller.yaml)
  * Deploys the admission controller as a service. 

