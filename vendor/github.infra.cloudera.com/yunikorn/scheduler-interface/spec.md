
<!--
About flow chart: use this website:
<!--- http://stable.ascii-flow.appspot.com/#Draw-->

# Scheduler Interface Spec

Authors:

## Objective

To define standard interface can be used by different resource management systems such as YARN/K8s.

### Goals in MVP

- Interface and implementation should be RM-agnostic.
- Interface can handle multiple RMs from multiple zones, and different policies can be configured in different zones. There will be less correlations between zones

Possible use cases:
- A large cluster needs multiple schedulers to achieve horizontally scalability.
- Multiple RMs need to run on the same cluster, grows and shrinks according to runtime resource usage and policies.

### Non-Goals in MVP

- Handle process-specific information: Scheduler Interface only handles decisions for scheduling instead of how containers will be launched.

## Design

Highlights:
- Scheduler should be as stateless as possible, should eliminate any local persistent storage for scheduling decisions.
- When RM restarts, recovers, etc. RM needs to re-sync state with scheduler.

### Architecture

## Scheduler Interface

There're two kinds of interfaces, one is grpc-based communication, another is api-based. Grpc based will be useful when scheduler has to be deployed in remote processes (For example, when we need to deploy scheduler support multiple clusters). Or wanna to have cross-lang communications (like between Java and Go).

Otherwise we highly suggest to use API-based interface to avoid addition costs in grpc serialization and de-serialization.

### RPC Interface

```protobuf
syntax = "proto3";
package si.v1;

import "google/protobuf/descriptor.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

option go_package = "si";

extend google.protobuf.FieldOptions {
  // Indicates that a field MAY contain information that is sensitive
  // and MUST be treated as such (e.g. not logged).
  bool si_secret = 1059;
}
```

There are three sets of RPCs:

* **Scheduler Service**: Scheduler can communicate with the Scheduler Service and do resource allocation/request update, etc.
* **Admin Service**: Admin can communicate with Scheduler Interface and get configuration updated.
* **Metrics Service**: Used to retrieve state of scheduler by users / RMs.

```protobuf
service Scheduler {
  // Register a new RM, if it is a reconnect from previous RM, cleanup
  // all in-memory data and resync with RM.
  rpc RegisterResourceManager (RegisterResourceManagerRequest)
    returns (RegisterResourceManagerResponse) { }

  // Update Scheduler status (including node status update, allocation request
  // updates, etc. And receive updates from scheduler for allocation changes,
  // any required status changes, etc.
  rpc Update (stream UpdateRequest)
    returns (stream UpdateResponse) { }
}

/*
service AdminService {
  // Include
  //   addQueueInfo.
  //   removeQueueInfo.
  //   updateQueueInfo.
  // Ref: org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo
  rpc UpdateConfig (UpdateConfigRequest)
    returns (UpdateConfigResponse) {}
}
*/
```

### API Interface

Here's a draft go-lang api.

```golang
package api

import "github.com/leftnoteasy/si-spec/lib/go/si"

type SchedulerApi interface {
    // Register a new RM, if it is a reconnect from previous RM, cleanup
    // all in-memory data and resync with RM.
    RegisterResourceManager(request *si.RegisterResourceManagerRequest, callback *ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)

    // Update Scheduler status (including node status update, allocation request
    // updates, etc.
    Update(request *si.UpdateRequest) error
}

// RM side needs to implement this API
type ResourceManagerCallback interface {
    RecvUpdateResponse(response *si.UpdateResponse) error
}

```

#### Why bi-directional grpc

The reason of using bi-directional streaming grpc is, according to performance benchmark: https://grpc.io/docs/guides/benchmarking.htmlping-pong latency is close to 0.5 ms. The same performance benchmark shows streaming QPS can be 4x of non-streaming RPC. Considering scheduler needs both throughput and better latency, we go with streaming API for scheduler related decisions.

### Communications between RM and Scheduler

Lifecycle of RM-Scheduler communication

```
Status of RM in scheduler:


                            Connection     timeout
    +-------+      +-------+ loss +-------+      +---------+
    |init   |+---->|Running|+---->|Paused |+---->| Stopped |
    +-------+      +----+--+      +-------+      +---------+
         RM register    |                             ^
         with scheduler |                             |
                        +-----------------------------+
                                   RM voluntarilly
                                     Shutdown
```

#### RM register with scheduler

When a new RM starts, fails, it will register with scheduler. In some cases, scheduler can ask RM to re-register because of connection issues or other internal issues.

```protobuf
message RegisterResourceManagerRequest {
  // An ID which can uniquely identify a RM **cluster**. (For example, if a RM cluster has multiple manager instances for HA purpose, they should use the same information when do registration).
  // If RM register with the same id, all previous scheduling state in memory will be cleaned up, and expect RM report full scheduling state after registration.
  string rmId = 1;

  // Version of RM scheduler interface client.
  string version = 2;

  // Policy group name:
  // This defines which policy to use. Policy should be statically configured. (Think about network security group concept of ec2).
  // Different RMs can refer to the same policyGroup if their static configuration is identical.
  string policyGroup = 3;
}

// Upon success, scheduler returns RegisterResourceManagerResponse to RM, otherwise RM receives exception.
message RegisterResourceManagerResponse {
  // Intentionally empty.
}
```

#### RM and scheduler updates.

Below is overview of how scheduler/RM keep connection and updates.

```protobuf
message UpdateRequest {
  // New allocation requests or replace existing allocation request (if allocation-id is same)
  repeated AllocationAsk asks = 1;

  // Allocations can be released.
  AllocationReleasesRequest releases = 2;

  // New node can be scheduled. If a node is notified to be "unscheduable", it needs to be part of this field as well.
  repeated NewNodeInfo newSchedulableNodes = 3;

  // Update nodes for existing schedulable nodes.
  // May include:
  // - Node resource changes. (Like grows/shrinks node resource)
  // - Node attribute changes. (Including node-partition concept like YARN, and concept like "local images".
  //
  // Should not include:
  // - Allocation-related changes with the node.
  // - Realtime Utilizations.
  repeated UpdateNodeInfo updatedNodes = 4;

  // UtilizationReports for allocation and nodes.
  repeated UtilizationReport utilizationReports = 5;
  
  // Id of RM, this will be used to identify which RM of the request comes from.
  string rmId = 6;
  
  // RM should explicitly add job when allocation request also explictly belongs to job.
  // This is optional if allocation request doesn't belong to a job. (Independent allocation)
  repeated AddJobRequest newJobs = 8;
  
  // RM can also remove jobs, all allocation/allocation requests associated with the job will be removed
  repeated RemoveJobRequest removeJobs = 9;
}

message UpdateResponse {
  // Scheduler can send action to RM.
  enum ActionFromScheduler {
    // Nothing needs to do
    NOACTION = 0;

    // Something is wrong, RM needs to stop the RM, and re-register with scheduler.
    RESYNC = 1;
  }

  // What RM needs to do, scheduler can send control code to RM when something goes wrong.
  // Don't use/expand this field for other general purposed actions. (Like kill a remote container process).
  ActionFromScheduler action = 1;

  // New allocations
  repeated Allocation newAllocations = 2;

  // Released allocations, this could be either ack from scheduler when RM asks to terminate some allocations. Or
  // it could be decision made by scheduler (such as preemption).
  repeated AllocationReleaseResponse releasedAllocations = 3;

  // Rejected allocation requests
  repeated RejectedAllocationAsk rejectedAllocations = 4;

  // Suggested node update.
  // This could include:
  // 1) Schedulable resources on each node. This can be used when we want to run
  //    two resource management systems side-by-side. For example, YARN/K8s running side by side.
  //    and update YARN NodeManager / Kubelet resource dynamically.
  // 2) Other recommendations.
  repeated NodeRecommendation nodeRecommendations = 5;
  
  // Rejected Jobs
  repeated RejectedJob rejectedJobs = 6;
  
  // Accepted Jobs
  repeated AcceptedJob acceptedJobs = 7;
  
  // Rejected Node Registrations
  repeated RejectedNode rejectedNodes = 8;
  
  // Accepted Node Registrations
  repeated AcceptedNode acceptedNodes = 9;
}

message RejectedJob {
  string jobId = 1; 
  string reason = 2;
}

message AcceptedJob {
  string jobId = 1;
}

message RejectedNode {
  string nodeId = 1;
  string reason = 2;
}

message AcceptedNode {
  string nodeId = 1;
}

```

#### Ask for more resources

Lifecycle of AllocationAsk:

```
                           Rejected by Scheduler
             +-------------------------------------------+
             |                                           |
             |                                           v
     +-------+---+ Asked  +-----------+Scheduler or,+-----------+
     |Initial    +------->|Pending    |+----+----+->|Rejected   |
     +-----------+By RM   +-+---------+ Asked by RM +-----------+
                                +
                                |
                                v
                          +-----------+
                          |Allocated  |
                          +-----------+
```

Lifecycle of Allocations:

```
         +--Allocated by
         v    Scheduler
 +-----------+        +------------+
 |Allocated  |+------ |Completed   |
 +---+-------+ Stoppe +------------+
     |         by RM
     |                +------------+
     +--------------->|Preempted   |
     +  Preempted by  +------------+
     |    Scheduler
     |
     |
     |                +------------+
     +--------------->|Expired     |
         Timeout      +------------+
        (Part of Allocation
           ask)
```

Common fields for allocation:

```protobuf
message Priority {
  oneof priority {
    // Priority of each ask, higher is more important.
    // How to deal with Priority is handled by each scheduler implementation.
    int32 priorityValue = 1;

    // PriorityClass is used for app owners to set named priorities. This is a portable way for
    // app owners have a consistent way to setup priority across clusters
    string priorityClassName = 2;
  }
}

// A sparse map of resource to Quantity.
message Resource {
  map<string, Quantity> resources = 1;
}

// Quantity includes a single int64 value
message Quantity {
  int64 value = 1;
}
```

Allocation ask:

```protobuf
message AllocationAsk {
  // Allocation key is used by both of scheduler and RM to track container allocation.
  // It doesn't have to be same as RM's internal allocation id (such as Pod name of K8s or ContainerId of YARN).
  // If multiple allocation from a same AllocationAsk returned to RM, they will share the same allocationKey.
  // If ALlocationAsk with the same allocationKey exists, old one will be replaced by the new one.
  string allocationKey = 1;

  // How much resource per allocation?
  Resource resourceAsk = 2;

  // Tags of Allocation
  map<string, string> tags = 3;

  // Priority of ask
  Priority priority = 4;

  // Placement constraint defines how app wanna to be placed in the cluster.
  // if not set, no placement constraint will be applied.
  PlacementConstraint placementConstraint = 6;

  // Maximum number of allocations
  int32 maxAllocations = 7;

  // Who owns the allocation
  UserGroupInformation ugi = 8;

  // Place this allocation to specified queue.
  // If queue not specified, scheduler will place the allocation to a queue
  // according to policy
  // When queue is specified and allocation cannot be allocated in asked queue
  // AllocationAsk will be rejected.
  string queueName = 9;

  // Execution timeout: How long this allocation will be terminated (by scheduler)
  // once allocated by scheduler, 0 or negative value means never expire.
  int64 executionTimeoutMilliSeconds = 10;
  
  // Which job this allocation ask belongs to, if it is not specified. The allocation
  // won't belong to any job.
  string jobId = 11;
  
  // Which partition requested by this ask
  // One ask can only request one node partition
  string PartitionName = 12;
  
  // TODO.
  // wangda: even if I don't like it, it seems we have to support old-school style
  // YARN resource requests, otherwise it gonna be very hard to support legacy apps
  // running on YARN.
  // Maybe a simple approach is to add a flag to indicate it is an old-school resource
  // request from YARN.
  // Check: YARN code: placement.LocalityAppPlacementAllocator for more details. 
}

message UserGroupInformation {
  string user = 1;
  repeated string groups = 2;
}

message AddJobRequest {
  string jobId = 1;
  string queueName = 2;
  string partitionName = 3;
}

message RemoveJobRequest {
  string jobId = 1;
  string partitionName = 2;
}
```

PlacementConstraint: (Reference to works have been done in YARN-6592). And here is design doc:
https://issues.apache.org/jira/secure/attachment/12867869/YARN-6592-Rich-Placement-Constraints-Design-V1.pdf

```protobuf
// PlacementConstraint could have simplePlacementConstraint or
// CompositePlacementConstraint. One of them will be set.
message PlacementConstraint {
  oneof constraint {
    SimplePlacementConstraint simpleConstraint = 1;

    // This protocol can extended to support complex constraints
    // To make an easier scheduler implementation and avoid confusing user.
    // Protocol related to CompositePlacementConstraints will be
    // commented and only for your references.
    // CompositePlacementConstraint compositeConstraint = 2;
  }
}

// Simple placement constraint represent constraint for affinity/anti-affinity
// to node attribute or allocation tags.
// When both of NodeAffinityConstraints and AllocationAffinityConstraints
// specified, both will be checked and verified while scheduling.
message SimplePlacementConstraint {
  // Constraint
  NodeAffinityConstraints nodeAffinityConstraint = 1;
  AllocationAffinityConstraints allocationAffinityAttribute = 2;
}

// Affinity to node, multiple AffinityTargetExpression will be specified,
// They will be connected by AND.
message NodeAffinityConstraints {
  repeated AffinityTargetExpression targetExpressions = 2;
}

// Affinity to allocations (containers).
// Affinity is single-direction, which means if RM wants to do mutual affinity/
// anti-affinity between allocations, same constraints need to be added
// to all allocation asks.
message AllocationAffinityConstraints {
  // Scope: scope is key of node attribute, which determines if >1 allocations
  // in the same group or not.
  // When allocations on node(s) which have same node attribute value
  // for given node attribute key == scope. They're in the same group.
  //
  // e.g. when user wants to do anti-affinity between allocation on node
  // basis, scope can be set to "hostname", max-cardinality = 1;
  string scope = 1;
  repeated AffinityTargetExpression tragetExpressions = 2;
  int32 minCardinality = 3;
  int32 maxCardinality = 4;

  // Is this a required (hard) or preferred (soft) request.
  bool required = 5;
}

message AffinityTargetExpression {
  // Following 4 operators can be specified, by default is "IN".
  // When EXIST/NOT_EXISTS specified, scheduler only check if given targetKey
  // appears on node attribute or allocation tag.
  enum AffinityTargetOperator {
    IN = 0;
    NOT_IN = 1;
    EXIST = 2;
    NOT_EXIST = 3;
  }

  AffinityTargetExpression targetOperator = 1;
  string targetKey = 2;
  repeated string targetValues = 3;
}
```

As mentioned above, we can support Composite Placement Constraint in the future. To ease initial implementation of scheduler, following code is commented out on purpose which is for your reference only.

```protobuf
/*
Following protocol is commented on-purpose, currently for your references only

message CompositePlacementConstraintProto {
  enum CompositeType {
    // All children constraints have to be satisfied.
    AND = 0;
    // One of the children constraints has to be satisfied.
    OR = 1;
    // Attempt to satisfy the first child constraint for delays[0] units (e.g.,
    // millisec or heartbeats). If this fails, try to satisfy the second child
    // constraint for delays[1] units and so on.
    DELAYED_OR = 2;
  }

  CompositeType compositeType = 1;
  repeated PlacementConstraintProto childConstraints = 2;
  repeated TimedPlacementConstraintProto timedChildConstraints = 3;
}


message TimedPlacementConstraintProto {
  enum DelayUnit {
    MILLISECONDS = 0;
    OPPORTUNITIES = 1;
  }

  required PlacementConstraintProto placementConstraint = 1;
  required int64 schedulingDelay = 2;
  DelayUnit delayUnit = 3 [ default = MILLISECONDS ];
}
*/
```

#### Release previously allocated resources

```protobuf
message AllocationReleasesRequest {
  repeated AllocationReleaseRequest allocationsToRelease = 1;
  repeated AllocationAskReleaseRequest allocationAsksToRelease = 2;
}

// Release allocation
message AllocationReleaseRequest {
  // optional, when this is set, only release allocation by given uuid.
  string uuid = 1;
  
  // when this is set, filter allocations by job id.
  // empty value will filter allocations don't belong to job. 
  // when job id is set and uuid not set, release all allocations under the job id.
  string jobId = 2;
  
  // Which partition to release, required.
  string partitionName = 3;
  
  // For human-readable
  string message = 4;
}

// Release allocation
message AllocationAskReleaseRequest {
  // optional, when this is set, only release allocation ask by specified
  string allocationkey = 1;
  
  // when this is set, filter allocation key by job id.
  // empty value will filter allocations don't belong to job. 
  // when job id is set and allocationKey not set, release all allocations key under the job id.
  string jobId = 2;
  
  // Which partition to release, required.
  string partitionName = 3;
  
  // For human-readable
  string message = 4;
}



```

#### Schedulable nodes registration and updates

State transition of node:

```


   +-----------+      +--------+        +-------+
   |SCHEDULABLE|+---->|DRAINING|+------>|REMOVED|
   +-----------+      +--------+        +-------+
         ^     ASKED       +     ASKED BY
         |      BY RM      |     RM TO REMOVE
         |     TO DRAIN    |
         |                 |
         |                 |
         |                 |
         +-----------------+
             RM ASK TO SCHEDULABLE
                   AGAIN
```

See protocol below:

```protobuf
message NewNodeInfo {
  // Id of node, it should be identical if same node daemon restarted.
  string nodeId = 1;

  // node attributes
  map<string, string> attributes = 2;

  // Schedulable Resource, scheduler may overcommit resource of node depends on available information
  // (such as utilization, priority of each container, etc.)
  Resource schedulableResource = 3;
  
  // Allocated resources, this will be added when node registered to RM
  // (recovery) 
  repeated Allocation existingAllocations = 4;
}

message UpdateNodeInfo {
  // Action from RM
  enum ActionFromRM {
    // Do not allocate new allocations on the node.
    DRAIN_NODE = 0;

    // Decomission node, it will immediately stop allocations on the same
    // node and move the node from schedulable lists.
    DECOMISSION = 1;

    // From Draining state to SCHEDULABLE state.
    // If node is not in draining state, error will be thrown
    DRAIN_TO_SCHEDULABLE = 2;
  }

  // Id of node
  string nodeId = 1;

  // New attributes of node, which will replace previously reported attribute.
  map<string, string> attributes = 2;

  // new schedulable resource, scheduler may preempt allocations on the
  // node or schedule more allocations accordingly.
  Resource schedulableResource = 3;

  ActionFromRM action = 4;
}
```

#### Utilization report

```protobuf
message UtilizationReport {
  // it could be either node id or allocation uuid.
  string id = 1;

  // Actual used resource
  Resource actualUsedResource = 2;
}
```

#### Feedback from Scheduler

Following is feedback from scheduler to RM:

Allocation is allocated allocations from scheduler.

```protobuf
message Allocation {
  // AllocationKey from AllocationAsk
  string allocationKey = 1;

  // Allocation tags from AllocationAsk
  map<string, string> allocationTags = 2;

  // uuid of the allocation
  string uuid = 3;

  // Resource for each allocation
  Resource resourcePerAlloc = 5;

  // Priority of ask
  Priority priority = 6;

  // Queue which the allocation belongs to
  string queueName = 7;

  // Node which the allocation belongs to
  string nodeId = 8;
  
  // Id of Job
  string jobId = 9;
  
  // Partition of the allocation 
  string partition = 10;
}
```

When allocation ask rejected by scheduler, information will be shared by scheduler.

```protobuf

message RejectedAllocationAsk {
  string allocationKey = 1;
  string reason = 2;
}
```

Scheduler can notify suggestions to RM about node. This can be either human-readable or actions can be taken.

```protobuf

message NodeRecommendation {
  Resource recommendedSchedulableResource = 1;

  // Any other human-readable message
  string message = 2;
}
```

For released allocations

```protobuf

// When allocation released, either by RM or preempted by scheduler. It will be sent back to RM.
message AllocationReleaseResponse {

  enum TerminationType {
    // STOPPED by ResourceManager.
    STOPPED_BY_RM = 0;

    // TIMEOUT (when timeout of allocation set)
    TIMEOUT = 1;

    // PREEMPTED BY SCHEDULER
    PREEMPTED_BY_SCHEDULER = 2;
  }

  // UUID of allocation.
  string allocationUUID = 1;

  // Why terminated
  TerminationType terminationType = 2;

  // Message (for human readble)
  string message = 3;
}
```

### Following are constant of spec

Scheduler Interface reserved all attribute in si.io namespace.

Known attribute names for nodes and applications.

```go

// Constants for node attribtues
const (
    ARCH="si.io/arch"
    HOSTNAME="si.io/hostname"
    RACKNAME="si.io/rackname"
    OS="si.io/os"
    INSTANCE_TYPE="si.io/instance-type"
    FAILURE_DOMAIN_ZONE="si.io/zone"
    FAILURE_DOMAIN_REGION="si.io/region"
    LOCAL_IMAGES="si.io/local-images"
    NODE_PARTITION="si.io/node-partition"
)

// Constants for allocation attribtues
const (
    JOB_ID="si.io/job-id"
    CONTAINER_IMAGE="si.io/container-image"
    CONTAINER_PORTS="si.io/container-ports"
)
```