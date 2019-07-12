# Predicates Support

## Design

Predicates are a set of pre-registered functions in K8s, the scheduler invokes these functions to check if a pod
is eligible to be allocated onto a node. Common predicates are: node-selector, pod affinity/anti-affinity etc. To support
these predicates in YuniKorn, we don't intend to re-implement everything on our own, but to re-use the core predicates
code as much as possible.

YuniKorn-core is agnostic about underneath RMs, so the predicates functions are implemented in K8s-shim as a `SchedulerPlugin`.
SchedulerPlugin is a way to plug/extend scheduler capabilities. Shim can implement such plugin and register itself to
yunikorn-core, so plugged function can be invoked in the scheduler core. Find all supported plugins in
[types](https://github.com/cloudera/yunikorn-core/blob/master/pkg/plugins/types.go).

## Workflow

First, RM needs to register itself to yunikorn-core, it advertises what scheduler plugin interfaces are supported.
E.g a RM could implement `PredicatePlugin` interface and register itself to yunikorn-core. Then yunikorn-core will
call PredicatePlugin API to run predicates before making allocation decisions.


Following workflow demonstrates how allocation looks like when predicates are involved.

```
pending pods: A, B
shim sends requests to core, including A, B
core starts to schedule A, B
  partition -> queue -> app -> request
    schedule A (1)
      run predicates (3)
        generate predicates metadata (4)
        run predicate functions one by one with the metadata
        success
        proposal: A->N
    schedule B (2)
      run predicates (calling shim API)
        generate predicates metadata
        run predicate functions one by one with the metadata
        success
        proposal: B->N
commit the allocation proposal for A and notify k8s-shim
commit the allocation proposal for B and notify k8s-shim
shim binds pod A to N
shim binds pod B to N
```

(1) and (2) are running in parallel.

(3) yunikorn-core calls a `schedulerPlugin` API to run predicates, this API is implemented on k8s-shim side.

(4) K8s-shim generates metadata based on current scheduler cache, the metadata includes some intermittent states about nodes and pods.

## Predicates White-list

Intentionally, we only support a white-list of predicates. Majorly due to 2 reasons,
* Predicate functions are time-consuming, it has negative impact on scheduler performance. To support predicates that are only necessary can minimize the impact. This will be configurable via CLI options;
* The implementation depends heavily on K8s default scheduler code, though we reused some unit tests, the coverage is still a problem. We'll continue to improve the coverage when adding new predicates.

the white-list currently is defined in [DefaultSchedulerPolicy](https://github.com/cloudera/yunikorn-k8shim/blob/master/pkg/predicates/predictor.go).
