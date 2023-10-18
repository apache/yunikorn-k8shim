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

# Placements examples
App placements rules in Yunikorn contains `Provided Rule`, `User Name Rule`, `Fixed Rule`, `Tag Rule`.
Every placement example includes a example yaml file and a config yaml file.
The sleep pod is described in the example file and the partition is provided in the config yaml file.

* [Provided Rule](./provided)
* [User Name Rule](./username)
* [Fixed Rule](./fixed)
* [Tag Rule](./tag)

## How to adopt the queues in `config.yaml`
Before deploying the pods, make sure that the data in the `yunikorn-configs` configmap is correct.
For example, `queues.yaml` in the `yunikorn-configs` configmap should be updated before starting fixed example.
`yunikorn-configs` configmap should contains following information.

> **_NOTE:_**
> `queues.yaml` should be __full__ queue config and then legal configuration would be updated to Yunikorn.

```
yunikornDefaults:
  queues.yaml: |
      partitions:
        - name: default
          placementrules:
            - name: tag
              value: namespace
              create: true
          queues:
            - name: root
              submitacl: '*'`
```

## Provided rule
```
placementrules:
  - name: provided
    create: true
    parent:
      name: user
      create: true
```
Application submit request by the user `developer`, queue in the application on submit: `my_special_queue`.
Result: `root.developer.my_special_queue`

## User name rule
```
placementrules:
  - name: user
    create: false # The queue won't be created when the queue doesn't exist.
```
Application submit request by the user `finance.test`, queue does exist:
Result: `root.finance_dot_test`

Application submit request by the user `developer`, queue does not exist:
Result: failed, next rule executed
Solution: The second cases could be submmitted and there are two optional choices.
1. `root.developer` is added to the queues.
2. The user name rule in the placementrules field could allowed to create the queue which doesn't exist. 

## Fixed rule
```
placementrules:
  - name: fixed
    value: last_resort
```
Application submit request by the user `developer`, queue in the application on submit: `my_special_queue`.
Result: `root.last_resort`

## Tag rule
```
placementrules:
  - name: tag
    value: namespace
    create: true
```
Application submit request for a kubernetes based application in the namespace `default` by the user developer, queue in the application on submit: `my_special_queue`.
Result: `root.default`

Application submit request for a kubernetes based application in the namespace `testing` by the user `developer`
Result: `root.testing`
