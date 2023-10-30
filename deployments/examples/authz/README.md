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

# Authorization use cases

Yunikorn offers a range of features, including advanced capabilities like hierarchical resource queues, access control lists, resource limits, preemption, priority, and placement rules for managing your cluster. This page presents a real-world scenario to demonstrate the practical application of these features.

The following will be included in this article：

- [Access control with ACL](./acl)
- [Placement of different users](./placement-rules)
- [Limit usable resources on a queue level](./resource-limits)
- [Preemption and priority scheduling with fencing](./priority)

## Prerequisites

Before configuring yunikorn-config, we need to create users using [Authentication](https://kubernetes.io/docs/reference/access-authn-authz/authentication/) and [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) from Kubernetes.

To create the necessary users for the examples, first navigate to the `k8s-api-access` directory:

```shell
cd k8s-api-access
```

Then use [./create-user.sh](./k8s-api-access/create-user.sh) to create a user.

After the user is created, the pod can be obtained by the following command to confirm the creation is successful：

```shell
kubectl --context=sue-context get pod
```

In our use cases, we frequently simulate different users deploying YAML files. To accomplish this, we utilize the `--context` command to select the appropriate user for each deployment:

```shell
kubectl --context=sue-context apply -f ./acl/nginx-1.yaml
```

When you are done testing, you can run [./remove-user.sh](./k8s-api-access/remove-user.sh) to delete all users.

## Access control with ACL

In the [yunikorn-configs.yaml](./acl/yunikorn-configs.yaml), we utilize `adminacl` to restrict access to the queue to only authorized users.


See the documentation on [User & Group Resolution](https://yunikorn.apache.org/docs/user_guide/usergroup_resolution) or [ACLs](https://yunikorn.apache.org/docs/user_guide/acls) for more information.

```yaml
queues:
  - name: root
    queues:
    - name: system
      adminacl: " admin"
    - name: tenants
      queues:
        - name: group-a
          adminacl: " group-a"
        - name: group-b
          adminacl: " group-b"
```

In the test case, users are given the option to specify the queue they want to use. The scheduler then checks if the user's application is permitted to be deployed to that queue. 

The following example illustrates this scenario, along with the expected test results:

| user, group          | Assign queue         | result  | YAML filename                 |
|----------------------|----------------------|---------|-------------------------------|
| sue, group-a         | root.tenants.group-a | created | [nginx-1](./acl/nginx-1.yaml) |
| sue, group-a         | root.tenants.group-b | blocked | [nginx-1](./acl/nginx-1.yaml) |
| kim, group-b         | root.tenants.group-a | blocked | [nginx-2](./acl/nginx-2.yaml) |
| kim, group-b         | root.tenants.group-b | created | [nginx-2](./acl/nginx-2.yaml) |
| anonymous, anonymous | root.tenants.group-a | blocked | [nginx-3](./acl/nginx-3.yaml) |
| anonymous, anonymous | root.tenants.group-b | blocked | [nginx-3](./acl/nginx-3.yaml) |

## Placement of different users

In [yunikorn-configs.yaml](./placement-rules/yunikorn-configs.yaml), we use `placementrules` to allow the scheduler to dynamically assign applications to a queue, and even create a new queue if needed.

See the documentation on [App Placement Rules](https://yunikorn.apache.org/docs/user_guide/placement_rules) for more information.

```yaml
placementrules:
  - name: provided
    create: true
    filter:
      type: allow
      users:
        - admin
      groups:
        - admin
    parent:
      name: fixed
      value: root.system
```

In the test case, the user doesn't need to specify the queue for their application. Instead, the scheduler will utilize the placement rules to assign the application to the appropriate queue. If needed, the scheduler will create new queues.

The following example illustrates this scenario, along with the expected test results:

| placement rule         | user, group  | provide queue             | namespace | Expected to be placed on  | YAML filename                                        |
|------------------------|--------------|---------------------------|-----------|---------------------------|------------------------------------------------------|
| provided               | admin, admin | root.system.high-priority |           | root.system.high-priority | [nginx-admin.yaml](./placement-rules/nginx-admin.yaml) |
| provided               | admin, admin | root.system.low-priority  |           | root.system.low-priority  | [nginx-admin.yaml](./placement-rules/nginx-admin.yaml) |
| username               | sue, group-a |                           |           | root.tenants.group-a.sue  | [nginx-sue.yaml](./placement-rules/nginx-sue.yaml)     |
| tag (value: namespace) | kim, group-b |                           | dev       | root.tenants.group-b.dev  | [nginx-kim.yaml](./placement-rules/nginx-kim.yaml)     |
| tag (value: namespace) | kim, group-b |                           | test      | root.tenants.group-b.test | [nginx-kim.yaml](./placement-rules/nginx-kim.yaml)     |

## Limit usable resources on a queue level

In [yunikorn-configs.yaml](./resource-limits/yunikorn-configs.yaml), we use `resources` to limit and reserve the amount of resources per queue.

See the documentation on [Partition and Queue Configuration #Resources](https://yunikorn.apache.org/docs/user_guide/queue_config#resources) for more information.

```yaml
queues:
- name: system
  adminacl: " admin"
  resources:
    guaranteed:
      {memory: 2G, vcore: 2}
    max:
      {memory: 6G, vcore: 6}
```

In the test case, users may request more resources than the queue allows, causing the scheduler to block applications that exceed the limits of each queue.

The following example illustrates this scenario, along with the expected test results:

| user, group  | Resource Limits for Destination Queues | request resources for each replicas | replica | result                                                   | YAML filename                                        |
|--------------|----------------------------------------|-------------------------------------|---------|----------------------------------------------------------|------------------------------------------------------|
| admin, admin | {memory: 6G, vcore: 6}                 | {memory: 512M, vcore: 250m}         | 1       | run all replica                                          | [nginx-admin.yaml](./resource-limits/nginx-admin.yaml) |
| sue, group-A | {memory: 2G, vcore: 4}                 | {memory: 512M, vcore: 500m}         | 5       | run 3 replica (4 replica will exceed the resource limit) | [nginx-sue.yaml](./resource-limits/nginx-sue.yaml)     |

## Preemption and priority scheduling with fencing

In [yunikorn-configs.yaml](./resource-limits/yunikorn-configs.yaml), we use `priority.offset` and `priority.policy` to configure the priority in a queue.

See the documentation on [App & Queue Priorities](https://yunikorn.apache.org/docs/user_guide/priorities) for more information.

```yaml
- name: tenants
  properties:
    priority.policy: "fence"
  queues:
    - name: group-a
      adminacl: " group-a"
      properties:
        priority.offset: "20"
```

In a resource-constrained environment, we will deploy applications to three queues simultaneously, each with a different priority. The scheduler will then deploy applications based on the priority of the queue.

In the following tests, we run the environment with a node resource limit of `{memory:16GB, vcore:16}`. Note that results will vary based on the environment, and you can modify the YAML file we provide to achieve similar results.

The following example illustrates this scenario, along with the expected test results:

### case 1 -

| queue                       | offset | # of deploy apps | # of apps accept by yunikorn | YAML filename                         |
|-----------------------------|--------|------------------|------------------------------|---------------------------------------|
| root.system.low-priority    | 1000   | 8                | 8                            | [system.yaml](./priority/system.yaml) |
| root.system.normal-priority | 0      | 8                | 5                            | [system.yaml](./priority/system.yaml) |
| root.system.high-priority   | -1000  | 8                | 0                            | [system.yaml](./priority/system.yaml) |

### case 2 - 

> **_NOTE:_** You will need to deploy all of the following YAML files simultaneously.

| queue                       | offset      | # of deploy apps | # of apps accept by yunikorn | YAML filename                                   |
|-----------------------------|-------------|------------------|------------------------------|-------------------------------------------------|
| root.system.normal-priority | 0 (global)  | 7                | 7                            | [nginx-admin.yaml](./priority/nginx-admin.yaml) |
| root.tenants.group-a        | 20 (fenced) | 7                | 6                            | [nginx-sue.yaml](./priority/nginx-sue.yaml)     |
| root.tenants.group-b        | 5 (fenced)  | 7                | 0                            | [nginx-kim.yaml](./priority/nginx-kim.yaml)     |
