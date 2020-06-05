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

