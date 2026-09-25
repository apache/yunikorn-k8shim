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

# YuniKorn Queue Operator (opt-in)

The queue operator is an **optional** k8s-shim component that lets tenants
manage YuniKorn queues as native Kubernetes CRDs (`yunikorn.apache.org/v1alpha1`)
instead of hand-editing the monolithic `queues.yaml` field of the
`yunikorn-configs` ConfigMap.

It is complementary to (not a replacement for) the admission controller:

| Component            | Included in `make image` | Purpose |
| -------------------- | :----------------------: | ------- |
| Scheduler            | yes                      | Core pod scheduling.                                                     |
| Admission Controller | yes                      | Mutates pods, validates the ConfigMap.                                   |
| **Queue Operator**   | **no (opt-in)**          | Watches `Queue` CRs, materialises them into the `yunikorn-configs` CM.   |

Enable it only when you want CRD-driven, multi-tenant queue management as
described in [YUNIKORN-3192](https://issues.apache.org/jira/browse/YUNIKORN-3192).
If you do not deploy the operator, the shim's existing ConfigMap flow is
completely unchanged.

## What it does

Each `Queue` CR represents a top-level parent queue under `root`, along with
its entire hierarchy. The operator:

1. Watches every `Queue` CR across all namespaces.
2. Assembles them into a single YuniKorn `queues.yaml` payload (deduplicated
   by queue name, oldest CR wins on conflict).
3. Runs the assembled payload through YuniKorn's own scheduler-config
   validator (`configs.LoadSchedulerConfigFromByteArray`).
4. Writes / updates `configmaps/yunikorn-configs` in the target namespace
   with the rendered YAML under the `queues.yaml` key.
5. Records the outcome as CR `.status.conditions` (`Available` / `Degraded`).

A validating admission webhook (`vqueue.yunikorn.apache.org`) is also
built into the same binary and can be enabled with `--webhook-enabled` +
`--webhook-cert-path`. It rejects a `Queue` CR at admission time if the
resulting merged YuniKorn config would be invalid, so a bad CR never lands
in etcd.

## Build

```bash
# binary only
make queue_operator

# docker image
make qop_image
```

The image tag defaults to `apache/yunikorn:queue-operator-<arch>-<version>`.
Override with `QUEUE_OPERATOR_TAG=...` or `REGISTRY=...` / `VERSION=...`.

## Install

The checked-in manifests deploy the queue operator in the `yunikorn`
namespace. If you need another operator namespace, render the namespaced
resources and webhook Service reference for that namespace before applying.
Apply the manifests **in order**:

```bash
# 1. Custom Resource Definition
kubectl apply -f deployments/queue-operator/queue-crd.yaml

# 2. ServiceAccount / ClusterRole / ClusterRoleBinding / pre-created Lease
kubectl apply -n yunikorn -f deployments/queue-operator/queue-operator-rbac.yaml

# 3. If yunikorn-configs already exists, review and explicitly adopt it.
# kubectl annotate -n yunikorn configmap/yunikorn-configs \
#   yunikorn.apache.org/queue-operator-managed=true

# 4. Persistent CA Secret, webhook Service/configuration, and Deployment
kubectl apply -n yunikorn -f deployments/queue-operator/queue-operator.yaml
```

The webhook uses a persistent Secret containing two staggered CA key pairs.
The operator initializes an empty Secret once, publishes overlapping trust to
its named `ValidatingWebhookConfiguration`, and generates the serving key pair
into an in-memory `emptyDir` before the webhook listener starts. A non-empty
malformed Secret is never replaced; repair or restore it before restarting the
operator. The checked-in webhook uses `failurePolicy: Fail`, so Queue writes
remain blocked until the operator publishes trust and exposes a verified
endpoint. Each replica withholds readiness until its live TLS listener verifies
against the active CA. The operator also verifies the webhook through its
Service and repairs its named webhook if `failurePolicy` drifts.

Existing `yunikorn-configs` ConfigMaps are never modified implicitly. Back up
and inspect the ConfigMap before adding the adoption annotation shown above.
If the ConfigMap does not exist, the operator creates it with the annotation.
The checked-in RBAC scopes ConfigMap access to the `yunikorn` namespace; when
using another `TARGET_NAMESPACE`, create the equivalent Role and RoleBinding in
that namespace. `POD_NAMESPACE` is populated from the downward API and is used
only for the operator's Service, Secret, Lease, and serving identity.

Configuration knobs are exposed as environment variables on the Deployment:

| Env var           | Default             | Meaning                                                                   |
| ----------------- | ------------------- | ------------------------------------------------------------------------- |
| `TARGET_NAMESPACE`| `yunikorn`          | Namespace where the `yunikorn-configs` ConfigMap lives / will be created. |
| `PARTITION_NAME`  | `default`           | YuniKorn partition to graft queues under.                                 |
| `PLACEMENT_RULES` | *unset*             | Optional YAML list of YuniKorn placement rules for the partition.         |
| `NODE_SORT_POLICY`| *unset*             | Node sorting policy name, or a YAML object containing `type` and optional `resourceweights`. |
| `ROOT_PROPERTIES` | *unset*             | YAML string-to-string map applied to the generated `root` queue.          |

Optional settings are parsed independently. If one explicit value is
malformed, the operator logs and counts that dropped setting through
`queue_operator_settings_dropped_total{setting="..."}` while preserving valid
sibling settings. Examples:

```yaml
env:
  - name: NODE_SORT_POLICY
    value: |
      type: binpacking
      resourceweights:
        memory: 2
        vcore: 0.5
  - name: ROOT_PROPERTIES
    value: |
      application.sort.policy: fair
```

## Example `Queue` resource

```yaml
apiVersion: yunikorn.apache.org/v1alpha1
kind: Queue
metadata:
  name: team-a
  namespace: yunikorn
spec:
  queue:
    name: team-a
    parent: true
    submitACL: "*"
    resources:
      max:
        memory: "16G"
        vcore: "8"
    queues:
      - name: dev
        maxApplications: 20
      - name: prod
        maxApplications: 100
```

## Uninstall

```bash
# Remove fail-closed admission first, then its backend and state.
kubectl delete validatingwebhookconfiguration/yunikorn-queue-operator
kubectl delete -n yunikorn deployment/yunikorn-queue-operator poddisruptionbudget/yunikorn-queue-operator
kubectl delete -n yunikorn service/yunikorn-queue-operator-webhook secret/yunikorn-queue-operator-webhook-ca
kubectl delete -n yunikorn -f deployments/queue-operator/queue-operator-rbac.yaml
kubectl delete -f deployments/queue-operator/queue-crd.yaml
```

Deleting the CRD removes every `Queue` CR in the cluster; the operator will
have already written its final ConfigMap before shutdown.
