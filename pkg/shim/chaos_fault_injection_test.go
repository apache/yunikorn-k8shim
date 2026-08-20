/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package shim

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

// Fault injection tests for the scheduling stack. They run the real stack through the MockScheduler
// harness (real Context, real Application/Task state machines, real dispatcher and an embedded core)
// while the mock kube client fails a seeded random fraction of the Bind calls, and assert the end
// state of the shim cache and of the core once everything has settled.
//
// Two scenarios:
//   - TestChaosBindFailure: bind failures only, the cluster is static
//   - TestChaosNodeFlap: bind failures plus nodes that are removed and added back through the
//     informer while the scheduling burst is running
//
// The injected faults are the bind failure and the node flap. Everything else models what a real
// cluster does:
//   - a successful bind is followed by the informer reporting the pod assigned and then running
//   - a pod whose bind failed keeps showing up unassigned and pending in the informer resyncs, the
//     shim writes no pod status on a bind failure
//   - a bound pod keeps showing up assigned to its node, including while its node object is removed:
//     the pod object keeps its nodeName, which is what the apiserver reports
//
// The oracles are end state invariants, they hold under any interleaving of the scheduling loop, the
// dispatcher and the informer:
//   - O1 no phantom assignment: a pod that never bound is not assigned to a node in the cache
//   - O2 bound pods are consistent: a pod that bound is assigned to the node of its last bind
//   - O3 no wedge: no pod is left assumed once the cluster is idle
//   - O4 no dangling assignment: the node a pod is assigned to is in the cache
//   - O5 no shim only placement: the core holds an allocation, on the same node, for every pod the
//     shim has placed on a node
//
// O1 is the YUNIKORN-3355 oracle: an allocation in the cache for a pod that never ran is exactly the
// phantom allocation that leaks node capacity for the lifetime of the pod. O5 is the divergence
// oracle: a pod the shim holds on a node without a matching core allocation is capacity the core will
// hand out a second time. O4 and O5 can only be violated by the node flap, so they are checked in the
// flap scenario.

const chaosConfigData = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 400000000
                vcore: 40000
              max:
                memory: 1000000000
                vcore: 100000
`

const (
	chaosAppCount  = 3
	chaosPodCount  = 150
	chaosNodeCount = 4
	// chaosFailProbability is the fraction of the Bind calls that are failed. High enough that every
	// run has a large sample of failed binds, low enough to keep the bound path exercised.
	chaosFailProbability = 0.3
	// chaosSettleTimeout is the ceiling for the cluster to go idle, not the expected runtime
	chaosSettleTimeout = 60 * time.Second
	// chaosStableFor is how long the cluster has to stay idle before it counts as settled. The first
	// idle instant is not enough: a task whose bind failed is dropped from the application before the
	// informer re-creates it, so the cluster looks idle in the gap.
	chaosStableFor    = 3 * time.Second
	chaosPollInterval = 100 * time.Millisecond
	// chaosResyncInterval is how often the informer reports every live pod again
	chaosResyncInterval = 250 * time.Millisecond
	// the flap schedule is tuned to overlap the scheduling burst: the first bind lands about two
	// seconds after the pods are submitted and the burst runs for a few seconds after that
	chaosNodeFlaps    = 8
	chaosFlapInterval = 700 * time.Millisecond
	chaosFlapDownTime = 600 * time.Millisecond
)

// chaosSeeds are the seeds used for every run: the failure sequences must be stable over time and
// over -count so a failure can be replayed
var chaosSeeds = []int64{1, 42, 20260816}

// chaosConfig is a single fault injection scenario
type chaosConfig struct {
	seed     int64
	failProb float64
	// nodeFlaps is the number of remove/re-add cycles, 0 keeps the cluster static. Flapping needs the
	// nodes to be delivered through the informer so that the Context node handlers run.
	nodeFlaps int
}

func (c chaosConfig) flapping() bool {
	return c.nodeFlaps > 0
}

// chaosRecord is what the injected bind function did for a single pod. It is the ground truth the
// oracles are checked against: the shim must never claim a placement the kube client never accepted.
type chaosRecord struct {
	attempts      int
	successes     int
	lastBoundNode string // the node of the last successful bind, empty if none succeeded
}

// chaosBoundPod is the notification sent to the cluster simulator for a successful bind
type chaosBoundPod struct {
	uid  string
	node string
}

// chaosBinder is the fault injecting bind function and the record of what it did
type chaosBinder struct {
	lock     locking.Mutex
	rng      *rand.Rand
	failProb float64
	records  map[string]*chaosRecord
	bound    chan chaosBoundPod
}

func newChaosBinder(cfg chaosConfig) *chaosBinder {
	return &chaosBinder{
		rng:      rand.New(rand.NewSource(cfg.seed)), //nolint:gosec
		failProb: cfg.failProb,
		records:  make(map[string]*chaosRecord),
		// generously sized so that bind never blocks on the simulator: a pod can be bound more than
		// once over a run
		bound: make(chan chaosBoundPod, 8*chaosPodCount),
	}
}

// bind is installed as the mock kube client bind function. It is called from the task goroutine while
// the kube client mock holds its own lock, so it must not call back into the mock: the successful
// binds are handed to the cluster simulator over a channel that cannot fill up.
func (b *chaosBinder) bind(pod *v1.Pod, hostID string) error {
	uid := string(pod.UID)
	b.lock.Lock()
	defer b.lock.Unlock()
	record, ok := b.records[uid]
	if !ok {
		record = &chaosRecord{}
		b.records[uid] = record
	}
	record.attempts++
	if b.rng.Float64() < b.failProb {
		return fmt.Errorf("injected bind failure: pod %s on node %s", pod.Name, hostID)
	}
	record.successes++
	record.lastBoundNode = hostID
	b.bound <- chaosBoundPod{uid: uid, node: hostID}
	return nil
}

// snapshot returns a copy of the records collected so far
func (b *chaosBinder) snapshot() map[string]chaosRecord {
	b.lock.Lock()
	defer b.lock.Unlock()
	records := make(map[string]chaosRecord, len(b.records))
	for uid, record := range b.records {
		records[uid] = *record
	}
	return records
}

// TestChaosBindFailure fails a fraction of the binds on an otherwise static cluster. Oracles O1, O2
// and O3.
func TestChaosBindFailure(t *testing.T) {
	for _, seed := range chaosSeeds {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runChaosScenario(t, chaosConfig{seed: seed, failProb: chaosFailProbability})
		})
	}
}

// TestChaosNodeFlap adds node removals and re-adds through the informer on top of the bind failures.
// Oracles O1 to O4 and O5.
func TestChaosNodeFlap(t *testing.T) {
	for _, seed := range chaosSeeds {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runChaosScenario(t, chaosConfig{seed: seed, failProb: chaosFailProbability, nodeFlaps: chaosNodeFlaps})
		})
	}
}

func runChaosScenario(t *testing.T, cfg chaosConfig) {
	cluster := MockScheduler{}
	cluster.init()
	binder := newChaosBinder(cfg)
	// install the fault injection before the cluster runs, no bind can be in flight yet
	cluster.apiProvider.MockBindFn(binder.bind)
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	assert.NilError(t, cluster.updateConfig(chaosConfigData, nil), "update config failed")

	nodes := make([]*v1.Node, 0, chaosNodeCount)
	for i := 0; i < chaosNodeCount; i++ {
		nodeName := fmt.Sprintf("chaos.host.%02d", i)
		if !cfg.flapping() {
			assert.NilError(t, cluster.addNode(nodeName, nil, 100000000, 10000, 200), "add node failed")
			continue
		}
		// the flap has to go through the Context node handlers, so the node is delivered by the
		// informer rather than pushed straight into the cache and the core
		node := chaosNode(nodeName)
		nodes = append(nodes, node)
		cluster.AddNode(node.DeepCopy())
	}
	if cfg.flapping() {
		waitForChaosNodes(t, &cluster, nodes)
	}

	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1000000).
		AddResource(siCommon.CPU, 100).
		Build()

	// the pristine pod templates: the pods handed to the informer are copies, the shim is free to hold
	// on to them and, when the assignment tracking is broken, to modify them
	pods := make([]*v1.Pod, 0, chaosPodCount)
	for i := 0; i < chaosPodCount; i++ {
		appID := fmt.Sprintf("chaosapp-%d-%d", cfg.seed, i%chaosAppCount)
		pods = append(pods, createTestPod("root.a", appID, fmt.Sprintf("chaostask-%d-%03d", cfg.seed, i), taskResource))
	}

	stop := make(chan struct{})
	flapDone := make(chan struct{})
	var simulators sync.WaitGroup
	var stopOnce sync.Once
	// the simulators must be stopped before the cluster: they deliver their events over the mock
	// informer and that only drains while the cluster runs
	stopSimulators := func() {
		stopOnce.Do(func() {
			close(stop)
			simulators.Wait()
		})
	}
	defer stopSimulators()

	// the cluster simulator owns the apiserver view of every pod. A single goroutine so that the
	// events it delivers are ordered.
	simulators.Add(1)
	go func() {
		defer simulators.Done()
		live := make(map[string]*v1.Pod, len(pods))
		for _, pod := range pods {
			live[string(pod.UID)] = pod.DeepCopy()
		}
		ticker := time.NewTicker(chaosResyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case bound := <-binder.bound:
				current := live[bound.uid]
				assigned := current.DeepCopy()
				assigned.Spec.NodeName = bound.node
				cluster.UpdatePod(current.DeepCopy(), assigned.DeepCopy())
				running := assigned.DeepCopy()
				running.Status.Phase = v1.PodRunning
				cluster.UpdatePod(assigned.DeepCopy(), running.DeepCopy())
				live[bound.uid] = running
			case <-ticker.C:
				// a pod that never bound is reported unassigned and pending, a bound pod is reported
				// on its node even while that node object is gone
				for _, pod := range live {
					cluster.UpdatePod(pod.DeepCopy(), pod.DeepCopy())
				}
			}
		}
	}()

	// node flapper: remove a node and add it back a moment later, several times over the run. Every
	// node is present again once it is done.
	simulators.Add(1)
	go func() {
		defer simulators.Done()
		defer close(flapDone)
		if !cfg.flapping() {
			return
		}
		rng := rand.New(rand.NewSource(cfg.seed + 104729)) //nolint:gosec
		for i := 0; i < cfg.nodeFlaps; i++ {
			select {
			case <-stop:
				return
			case <-time.After(chaosFlapInterval):
			}
			node := nodes[rng.Intn(len(nodes))]
			cluster.DeleteNode(node.DeepCopy())
			select {
			case <-stop:
				// leave the node out, the run is being torn down
				return
			case <-time.After(chaosFlapDownTime):
			}
			cluster.AddNode(node.DeepCopy())
		}
	}()

	for _, pod := range pods {
		cluster.AddPod(pod.DeepCopy())
	}

	start := time.Now()
	waitForChaosFlaps(t, cfg, flapDone)
	settled := waitForChaosSettle(t, &cluster, binder, pods)
	settle := time.Since(start)
	// the oracles read the cache and the core, so nothing may move under them any more
	stopSimulators()
	checkChaosOracles(t, &cluster, cfg, binder, pods, settled, settle)
}

// checkChaosOracles is the end state pass: it walks every pod once, checks the oracles that apply to
// the scenario against what the kube client mock recorded and reports what the run exercised
func checkChaosOracles(t *testing.T, cluster *MockScheduler, cfg chaosConfig, binder *chaosBinder,
	pods []*v1.Pod, settled bool, settle time.Duration) {
	records := binder.snapshot()
	schedulerCache := cluster.context.GetSchedulerCache()
	coreAllocations := chaosCoreAllocations(cluster)
	oracles := newChaosOracles()
	bound, neverBound, unattempted, attempts, bindFailures, maxAttempts := 0, 0, 0, 0, 0, 0
	for _, pod := range pods {
		uid := string(pod.UID)
		record := records[uid]
		cachedNode := ""
		if cached := schedulerCache.GetPod(uid); cached != nil {
			cachedNode = cached.Spec.NodeName
		}
		attempts += record.attempts
		bindFailures += record.attempts - record.successes
		if record.attempts > maxAttempts {
			maxAttempts = record.attempts
		}
		dump := chaosPodDump(cluster, pod, record)

		// O3 no wedge: nothing is left assumed once the cluster is idle
		oracles.check("O3", !schedulerCache.IsAssumedPod(uid), dump)

		if record.successes > 0 {
			bound++
			// O2 bound pods are consistent: the cache holds the pod on the node it was bound to
			oracles.check("O2", schedulerCache.GetPod(uid) != nil && cachedNode == record.lastBoundNode, dump)
		} else {
			if record.attempts == 0 {
				unattempted++
			} else {
				neverBound++
			}
			// O1 no phantom assignment: a pod that never bound must not be assigned to a node. An
			// assignment for a pod that never ran is the YUNIKORN-3355 phantom allocation.
			oracles.check("O1", cachedNode == "", dump)
		}

		// O4 and O5 can only be broken by a node that went away, so they are the flap oracles
		if !cfg.flapping() || cachedNode == "" {
			continue
		}
		// O4 no dangling assignment: the node a pod is assigned to is in the cache
		oracles.check("O4", schedulerCache.GetNode(cachedNode) != nil, dump)
		// O5 the core knows about the placement the shim is holding, on the same node. A pod the shim
		// has on a node without a core allocation is capacity the core can hand out twice.
		coreNode, inCore := coreAllocations[uid]
		oracles.check("O5", inCore && coreNode == cachedNode, fmt.Sprintf("core node %q vs %s", coreNode, dump))
	}
	oracles.report(t, len(pods))
	if t.Failed() && cfg.flapping() {
		t.Logf("shim placement against core placement per node:\n%s", strings.Join(chaosNodeSummary(cluster, pods), "\n"))
	}

	assert.Assert(t, bound > 0, "no bind succeeded, the run did not exercise the bound path")
	assert.Assert(t, bindFailures > 0, "no bind failed, the run did not exercise the fault injection")
	t.Logf("seed %d p(bind fail)=%.1f flaps=%d settled=%v: %d pods, %d bound, %d never bound, %d never attempted, "+
		"%d bind attempts (%d failed, max %d for a single pod), settle wait %v",
		cfg.seed, cfg.failProb, cfg.nodeFlaps, settled, len(pods), bound, neverBound, unattempted, attempts,
		bindFailures, maxAttempts, settle.Round(time.Millisecond))
}

// chaosOracles collects the pods that break each oracle. A fault injection run breaks an invariant
// systematically or not at all, so a violation is collected and reported once with its count instead
// of failing the test at the first pod: the count and the spread over the pods are the diagnosis.
type chaosOracles struct {
	violations map[string][]string
}

func newChaosOracles() *chaosOracles {
	return &chaosOracles{violations: make(map[string][]string)}
}

func (o *chaosOracles) check(oracle string, holds bool, dump string) {
	if !holds {
		o.violations[oracle] = append(o.violations[oracle], dump)
	}
}

// report fails the test once per broken oracle, with the number of pods that broke it and the first
// few of them in full
func (o *chaosOracles) report(t *testing.T, podCount int) {
	t.Helper()
	for _, oracle := range []string{"O1", "O2", "O3", "O4", "O5"} {
		dumps := o.violations[oracle]
		if len(dumps) == 0 {
			continue
		}
		sort.Strings(dumps)
		shown := dumps
		if len(shown) > 5 {
			shown = shown[:5]
		}
		t.Errorf("%s violated by %d of the %d pods, first %d:\n  %s",
			oracle, len(dumps), podCount, len(shown), strings.Join(shown, "\n  "))
	}
}

// chaosCoreAllocations returns the allocation key to node mapping the scheduler core holds for the
// partition. The allocation key of a task is the pod UID.
func chaosCoreAllocations(cluster *MockScheduler) map[string]string {
	allocations := make(map[string]string)
	partition := cluster.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName)
	if partition == nil {
		return allocations
	}
	for _, app := range partition.GetApplications() {
		for _, alloc := range app.GetAllAllocations() {
			allocations[alloc.GetAllocationKey()] = alloc.GetNodeID()
		}
	}
	return allocations
}

// chaosNodeSummary compares, per node, what the shim cache holds against what the core holds. A node
// the shim has pods on while the core has none is capacity the core will hand out again.
func chaosNodeSummary(cluster *MockScheduler, pods []*v1.Pod) []string {
	schedulerCache := cluster.context.GetSchedulerCache()
	shimPods := make(map[string]int)
	for _, pod := range pods {
		if cached := schedulerCache.GetPod(string(pod.UID)); cached != nil && cached.Spec.NodeName != "" {
			shimPods[cached.Spec.NodeName]++
		}
	}
	summary := make([]string, 0, len(shimPods))
	for nodeName, count := range shimPods {
		coreAllocations, coreResource := 0, "<node not in core>"
		if coreNode := cluster.coreContext.Scheduler.GetClusterContext().GetNode(nodeName, partitionName); coreNode != nil {
			coreAllocations = len(coreNode.GetYunikornAllocations())
			coreResource = coreNode.GetAllocatedResource().String()
		}
		summary = append(summary, fmt.Sprintf("  node %s: shim pods %d, core allocations %d, core allocated %s",
			nodeName, count, coreAllocations, coreResource))
	}
	sort.Strings(summary)
	return summary
}

// chaosNode builds a node object that is delivered through the informer so that the Context node
// handlers run for it
func chaosNode(name string) *v1.Node {
	zero := resource.Scale(0)
	capacity := v1.ResourceList{
		v1.ResourceMemory: *resource.NewScaledQuantity(100000000, zero),
		v1.ResourceCPU:    *resource.NewMilliQuantity(10000, resource.DecimalSI),
		v1.ResourcePods:   *resource.NewScaledQuantity(200, zero),
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID("UUID-" + name),
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
			Allocatable: capacity,
			Capacity:    capacity,
		},
	}
}

// waitForChaosNodes waits for the nodes delivered by the informer to be registered in the core, pods
// submitted before that are simply unschedulable
func waitForChaosNodes(t *testing.T, cluster *MockScheduler, nodes []*v1.Node) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if cluster.GetActiveNodeCountInCore(partitionName) == len(nodes) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d nodes registered in the core",
				cluster.GetActiveNodeCountInCore(partitionName), len(nodes))
		}
		time.Sleep(chaosPollInterval)
	}
}

// waitForChaosFlaps waits for the flap schedule to finish so that every node is back before the
// cluster is expected to settle
func waitForChaosFlaps(t *testing.T, cfg chaosConfig, flapDone chan struct{}) {
	t.Helper()
	if !cfg.flapping() {
		return
	}
	budget := time.Duration(cfg.nodeFlaps)*(chaosFlapInterval+chaosFlapDownTime) + chaosSettleTimeout
	select {
	case <-flapDone:
	case <-time.After(budget):
		t.Fatalf("node flap schedule did not finish within %v", budget)
	}
}

// waitForChaosSettle waits for the cluster to go idle: no pod is assumed and every task has reached a
// state it does not leave on its own. The condition has to hold continuously for chaosStableFor: a
// cluster that keeps churning never settles. A cluster that does not settle in time fails the test
// but the run continues, the oracles then name the invariant the cluster is stuck on.
func waitForChaosSettle(t *testing.T, cluster *MockScheduler, binder *chaosBinder, pods []*v1.Pod) bool {
	t.Helper()
	deadline := time.Now().Add(chaosSettleTimeout)
	idleSince := time.Time{}
	var lastPending []string
	for {
		pending := pendingChaosPods(cluster, binder, pods)
		if len(pending) == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) >= chaosStableFor {
				return true
			}
		} else {
			idleSince = time.Time{}
			lastPending = pending
		}
		if time.Now().After(deadline) {
			shown := lastPending
			if len(shown) > 5 {
				shown = shown[:5]
			}
			t.Errorf("cluster did not settle within %v (stable window %v), %d pods were still in flight at the last check, first %d:\n%s",
				chaosSettleTimeout, chaosStableFor, len(lastPending), len(shown), strings.Join(shown, "\n"))
			return false
		}
		time.Sleep(chaosPollInterval)
	}
}

// pendingChaosPods returns a description of every pod that is not settled yet: assumed, or with a task
// that can still move on its own. The description is the dump used when the settle times out.
func pendingChaosPods(cluster *MockScheduler, binder *chaosBinder, pods []*v1.Pod) []string {
	records := binder.snapshot()
	schedulerCache := cluster.context.GetSchedulerCache()
	pending := make([]string, 0)
	for _, pod := range pods {
		if !schedulerCache.IsAssumedPod(string(pod.UID)) && isChaosSettledState(chaosTaskState(cluster, pod)) {
			continue
		}
		pending = append(pending, "  "+chaosPodDump(cluster, pod, records[string(pod.UID)]))
	}
	sort.Strings(pending)
	return pending
}

// chaosPodDump is the full state of a pod: what the kube client mock did for it and what the shim
// thinks of it. It is used both for the settle timeout dump and for the oracle failures.
func chaosPodDump(cluster *MockScheduler, pod *v1.Pod, record chaosRecord) string {
	uid := string(pod.UID)
	schedulerCache := cluster.context.GetSchedulerCache()
	cachedNode := "<not cached>"
	nodeKnown := false
	if cached := schedulerCache.GetPod(uid); cached != nil {
		cachedNode = fmt.Sprintf("%q", cached.Spec.NodeName)
		nodeKnown = cached.Spec.NodeName != "" && schedulerCache.GetNode(cached.Spec.NodeName) != nil
	}
	return fmt.Sprintf("pod %s (uid %s): task state %q, assumed %v, orphaned %v, cache node %s, cache node known %v, "+
		"bind attempts %d, bind successes %d, last bound node %q",
		pod.Name, uid, chaosTaskState(cluster, pod), schedulerCache.IsAssumedPod(uid),
		schedulerCache.IsPodOrphaned(uid), cachedNode, nodeKnown, record.attempts, record.successes,
		record.lastBoundNode)
}

// chaosTaskState returns the state of the task of a pod, or an empty string if the shim does not know
// about it. A task the shim never created or has already dropped cannot move any more.
func chaosTaskState(cluster *MockScheduler, pod *v1.Pod) string {
	app := cluster.context.GetApplication(pod.Labels[constants.LabelApplicationID])
	if app == nil {
		return ""
	}
	task := app.GetTask(string(pod.UID))
	if task == nil {
		return ""
	}
	return task.GetTaskState()
}

// isChaosSettledState reports whether a task in this state is done moving without further input. An
// unknown task, a bound task and a terminated task are all settled, everything else means the
// scheduling of that pod is still in progress.
func isChaosSettledState(state string) bool {
	if state == "" || state == cache.TaskStates().Bound {
		return true
	}
	for _, terminated := range cache.TaskStates().Terminated {
		if state == terminated {
			return true
		}
	}
	return false
}
