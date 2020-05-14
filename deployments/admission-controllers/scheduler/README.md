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

# Admission Controller to mutate and validate on-the-fly

This directory contains resources to create admission controller web-hooks:
- mutations: inject `schedulerName` and required `labels` to pod's spec/metadata before admitting it.
 This can be used to deploy on an existing Kubernetes cluster and route all pods to YuniKorn,
 which can be treated as an alternative way to replace default scheduler.
- validations: validate yunikorn configs (the config-map named `yunikorn-configs`) before admitting it.

## Steps

### Launch the admission controller

Create the admission controller web-hook, run following command

```shell script
./admission_util.sh create
```
this command does following tasks

- Create private key and certificate
- Sign and approve certificate with K8s `CertificateSigningRequest`
- Create a K8s `secret` which stores the private key and signed certificate
- Apply the secret to admission controller web-hook's deployment, insert the `caBundle`
- Create the admission controller web-hook and launch it on K8s

### Test the admission controller

#### Mutations

Launch a pod with following spec, note this spec did not specify `schedulerName`,
without the admission controller, it should be scheduled by default scheduler without any issue.

```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: sleep
    applicationId: "application-sleep-0001"
    queue: "root.sandbox"
  name: task0
spec:
  containers:
    - name: sleep-30s
      image: "alpine:latest"
      command: ["sleep", "30"]
      resources:
        requests:
          cpu: "100m"
          memory: "500M"
```

However, after the admission controller is started, this pod will keep at pending state (if yunikorn is not running).
Review the spec,

```shell script
kubectl get pod task0 -o yaml 
```

you'll see the `schedulerName` has been injected with value `yunikorn`.

#### Validations

YuniKorn loads its configuration from a configmap called yunikorn-configs, this admission controller adds a web-hook to
validate the update and create requests for this configmap.
It rejects invalid configuration which otherwise would have been rejected inside the scheduler update process.
Validations are currently limited to a syntax check only. The scheduler could still reject a configuration if it cannot
replace the current configuration due to conflicts.

For example, if yunikorn configs is updating with invalid node sort policy:
```
partitions:
  - name: default
    nodesortpolicy:
        type: invalid
    queues:
      - name: root
```

This update request should be denied and the client can receive an error as below:
```
error: configmaps "yunikorn-configs" could not be patched: admission webhook "admission-webhook.yunikorn.validate-conf"
 denied the request: undefined policy: invalid
```

### Stop and delete the admission controller

Use following command to cleanup all resources

```shell script
./admission_util.sh delete
```