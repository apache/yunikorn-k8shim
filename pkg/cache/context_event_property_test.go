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

package cache

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// This is a randomized property test for the informer event handlers of the context. It generates a
// logical history: the versions a set of pods and nodes went through in the API server. That single
// history is then delivered to the context in a number of different, all equally legal, informer
// orderings. The oracle is metamorphic, there is no reference model: whatever the informer does with
// the events, the context must end up in the same state for every ordering of the same history.
//
// The orderings the informer is allowed to produce, and which are generated here:
//   - per object the events are delivered in version order, events of different objects interleave
//     in any way
//   - versions can be skipped: the informer collapses updates it has not delivered yet
//   - the create can be collapsed into an update: the shim sees an update for a pod it has never
//     seen a create for, the trigger of YUNIKORN-3317
//   - any delivered version can be delivered again as an update with itself: an informer resync
//   - the delete can carry a stale object wrapped in a DeletedFinalStateUnknown: the informer lost
//     the watch and only knows the last object it has seen
//
// The generated histories are constrained so that all orderings are required to converge, these are
// properties of the objects, not of the shim:
//   - a pod is created unassigned and pending, it can then be assigned to a node, start running and
//     terminate, in that order. The assignment and the resources of a pod never change.
//   - a node which any pod is assigned to is never deleted: a node deletion tells the core to
//     release the allocations of that node, so a history with a node deletion in it genuinely has
//     an end state which depends on whether a pod was delivered before or after the deletion.
//
// A failure is replayable: the report holds the seed, the generated history, the full event trace of
// the reference ordering and of every ordering which diverged from it, plus the state which differs.
const (
	orderingPodCount  = 6
	orderingNodeCount = 3
	// orderingCount is the number of orderings of the same history that are run and compared
	orderingCount = 8
	// orderingMaxVersions is the number of versions an object can go through, creation included
	orderingMaxVersions = 4
	// orderingReportLimit is the number of diverging orderings a failure report holds
	orderingReportLimit = 3
	// orderingSeedEnv holds an extra seed to run, for exploratory runs only: the fixed seeds always run
	orderingSeedEnv = "YUNIKORN_EVENT_FUZZ_SEED"
)

// orderingSeeds are the seeds used for every run: histories must be stable over time and over -count
var orderingSeeds = []int64{1, 7, 42, 1234, 20260817}

// the ways a single event of an object is delivered
const (
	deliverAdd = iota
	deliverUpdate
	deliverDelete
)

// orderingPod is the logical history of a single pod: the versions it went through in the API
// server, oldest first, and whether it was deleted at the end
type orderingPod struct {
	uid      string
	foreign  bool
	deleted  bool
	versions []*v1.Pod
}

// orderingNode is the logical history of a single node, see orderingPod
type orderingNode struct {
	name     string
	deleted  bool
	versions []*v1.Node
}

// orderingHistory is the logical truth all orderings deliver
type orderingHistory struct {
	pods  []*orderingPod
	nodes []*orderingNode
}

// orderingEvent is a single informer event: the delivery of one version of one object
type orderingEvent struct {
	node   bool // the event belongs to a node, to a pod otherwise
	object int  // index into the pod or node list of the history
	kind   int
	oldVer int // the version delivered as the old object of an update, -1 if there is none
	newVer int // the version delivered as the object of the event
	stale  bool
}

type orderingFuzzer struct {
	t       *testing.T
	seed    int64
	rng     *rand.Rand
	history *orderingHistory
}

func newOrderingFuzzer(t *testing.T, seed int64) *orderingFuzzer {
	t.Helper()
	fuzzer := &orderingFuzzer{
		t:    t,
		seed: seed,
		rng:  rand.New(rand.NewSource(seed)), //nolint:gosec
	}
	fuzzer.history = fuzzer.newHistory()
	return fuzzer
}

func orderingPodUID(idx int) string {
	return fmt.Sprintf("ordering-pod-uid-%04d", idx)
}

func orderingNodeName(idx int) string {
	return fmt.Sprintf("ordering-node-%04d", idx)
}

// newPodObject returns the object of a single version of a pod: the informer hands out a new object
// for every event, the version is stamped on it as the resource version
func (f *orderingFuzzer) newPodObject(idx, version int, foreign bool, nodeName string, phase v1.PodPhase) *v1.Pod {
	requests := make(map[v1.ResourceName]resource.Quantity)
	requests[v1.ResourceMemory] = resource.MustParse("1G")
	requests[v1.ResourceCPU] = resource.MustParse("500m")
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:            fmt.Sprintf("ordering-pod-%04d", idx),
			Namespace:       "default",
			UID:             types.UID(orderingPodUID(idx)),
			ResourceVersion: strconv.Itoa(version),
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Name:      "test-container",
					Resources: v1.ResourceRequirements{Requests: requests},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
	if !foreign {
		// a pod is only managed by YuniKorn when it asks for the scheduler by name
		pod.Spec.SchedulerName = constants.SchedulerName
		pod.Labels = map[string]string{constants.LabelApplicationID: fmt.Sprintf("ordering-app-%04d", idx%2)}
	}
	return pod
}

func (f *orderingFuzzer) newNodeObject(idx, version int, memory int64) *v1.Node {
	allocatable := make(map[v1.ResourceName]resource.Quantity)
	allocatable[v1.ResourceMemory] = *resource.NewQuantity(memory, resource.DecimalSI)
	allocatable[v1.ResourceCPU] = *resource.NewQuantity(10, resource.DecimalSI)
	return &v1.Node{
		TypeMeta: apis.TypeMeta{
			Kind:       "Node",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:            orderingNodeName(idx),
			Namespace:       "default",
			UID:             types.UID(fmt.Sprintf("ordering-node-uid-%04d", idx)),
			ResourceVersion: strconv.Itoa(version),
		},
		Status: v1.NodeStatus{
			Allocatable: allocatable,
		},
	}
}

// newHistory generates the logical truth: the versions every object went through. The nodes are
// generated first as a pod can only be assigned to a node which is never deleted, see the comment
// on top of the file.
func (f *orderingFuzzer) newHistory() *orderingHistory {
	history := &orderingHistory{
		pods:  make([]*orderingPod, 0, orderingPodCount),
		nodes: make([]*orderingNode, 0, orderingNodeCount),
	}
	hosts := make([]string, 0, orderingNodeCount)
	for idx := 0; idx < orderingNodeCount; idx++ {
		node := &orderingNode{
			name: orderingNodeName(idx),
			// the first node is always kept, the pods need a node they can be assigned to
			deleted: idx > 0 && f.rng.Intn(4) == 0,
		}
		for version := 0; version < 1+f.rng.Intn(orderingMaxVersions); version++ {
			// the capacity of the node changes over its lifetime, nothing else does
			node.versions = append(node.versions, f.newNodeObject(idx, version, int64(version+10)*1000*1000*1000))
		}
		if !node.deleted {
			hosts = append(hosts, node.name)
		}
		history.nodes = append(history.nodes, node)
	}

	for idx := 0; idx < orderingPodCount; idx++ {
		pod := &orderingPod{
			uid:     orderingPodUID(idx),
			foreign: f.rng.Intn(2) == 0,
			deleted: f.rng.Intn(4) == 0,
		}
		host := hosts[f.rng.Intn(len(hosts))]
		// the life of a pod: created unassigned and pending, assigned, running, terminated. The
		// history stops at a random point, every version is one step of that progression.
		nodeName := ""
		phase := v1.PodPending
		steps := f.rng.Intn(orderingMaxVersions)
		for version := 0; version <= steps; version++ {
			switch version {
			case 0:
			case 1:
				nodeName = host
			case 2:
				phase = v1.PodRunning
			default:
				phase = v1.PodSucceeded
			}
			pod.versions = append(pod.versions, f.newPodObject(idx, version, pod.foreign, nodeName, phase))
		}
		history.pods = append(history.pods, pod)
	}
	return history
}

// describe returns the generated history in a form which can be read in a failure report
func (h *orderingHistory) describe() []string {
	lines := make([]string, 0, len(h.nodes)+len(h.pods))
	for idx, node := range h.nodes {
		versions := make([]string, 0, len(node.versions))
		for _, object := range node.versions {
			versions = append(versions, orderingResourceString(common.GetNodeResource(&object.Status)))
		}
		lines = append(lines, fmt.Sprintf("node-%d %s deleted=%t versions=[%s]",
			idx, node.name, node.deleted, strings.Join(versions, " ")))
	}
	for idx, pod := range h.pods {
		versions := make([]string, 0, len(pod.versions))
		for _, object := range pod.versions {
			versions = append(versions, fmt.Sprintf("%s@%q", object.Status.Phase, object.Spec.NodeName))
		}
		lines = append(lines, fmt.Sprintf("pod-%d %s foreign=%t deleted=%t versions=[%s]",
			idx, pod.uid, pod.foreign, pod.deleted, strings.Join(versions, " ")))
	}
	return lines
}

// deliveries returns the events a single object produces in one ordering. The canonical ordering is
// the one an informer produces when nothing is collapsed, resynced or lost, it is the reference all
// other orderings are compared against.
func (f *orderingFuzzer) deliveries(node bool, object, versions int, deleted, canonical bool) []orderingEvent {
	events := make([]orderingEvent, 0, versions+1)
	emit := func(kind, oldVer, newVer int, stale bool) {
		events = append(events, orderingEvent{node: node, object: object, kind: kind, oldVer: oldVer, newVer: newVer, stale: stale})
	}

	// pick the versions which are delivered, the last one always is: the versions in between can be
	// collapsed by the informer into the update which follows them
	delivered := make([]int, 0, versions)
	for version := 0; version < versions-1; version++ {
		if canonical || f.rng.Intn(2) != 0 {
			delivered = append(delivered, version)
		}
	}
	delivered = append(delivered, versions-1)

	for idx, version := range delivered {
		switch {
		case idx > 0:
			emit(deliverUpdate, delivered[idx-1], version, false)
		case canonical || version == 0 || f.rng.Intn(2) == 0:
			emit(deliverAdd, -1, version, false)
		default:
			// the create was collapsed into an update: the shim is given an old object it has never
			// seen a create for (YUNIKORN-3317)
			emit(deliverUpdate, version-1, version, false)
		}
		if !canonical && f.rng.Intn(4) == 0 {
			// an informer resync delivers the version which is already known as an update with itself
			emit(deliverUpdate, version, version, false)
		}
	}

	if deleted {
		last := versions - 1
		if !canonical && last > 0 && f.rng.Intn(3) == 0 {
			// the informer lost the watch: the delete carries the last object it has seen
			emit(deliverDelete, -1, last-1, true)
		} else {
			emit(deliverDelete, -1, last, false)
		}
	}
	return events
}

// ordering returns one legal delivery ordering of the whole history. The canonical ordering hands
// over the nodes before the pods, exactly like the shim initialization does.
func (f *orderingFuzzer) ordering(canonical bool) []orderingEvent {
	streams := make([][]orderingEvent, 0, len(f.history.nodes)+len(f.history.pods))
	for idx, node := range f.history.nodes {
		streams = append(streams, f.deliveries(true, idx, len(node.versions), node.deleted, canonical))
	}
	for idx, pod := range f.history.pods {
		streams = append(streams, f.deliveries(false, idx, len(pod.versions), pod.deleted, canonical))
	}

	total := 0
	for _, stream := range streams {
		total += len(stream)
	}
	events := make([]orderingEvent, 0, total)
	for len(events) < total {
		if canonical {
			// no interleaving: the objects are handed over one after the other
			for _, stream := range streams {
				events = append(events, stream...)
			}
			break
		}
		// pick the next event from a random object which still has events to deliver
		idx := f.rng.Intn(len(streams))
		if len(streams[idx]) == 0 {
			continue
		}
		events = append(events, streams[idx][0])
		streams[idx] = streams[idx][1:]
	}
	return events
}

// describe returns the event in a form which can be read in a failure report
func (f *orderingFuzzer) describe(event orderingEvent) string {
	name := fmt.Sprintf("pod-%d", event.object)
	if event.node {
		name = fmt.Sprintf("node-%d", event.object)
	}
	switch event.kind {
	case deliverAdd:
		return fmt.Sprintf("%s add v%d", name, event.newVer)
	case deliverUpdate:
		if event.oldVer == event.newVer {
			return fmt.Sprintf("%s resync v%d", name, event.newVer)
		}
		return fmt.Sprintf("%s update v%d->v%d", name, event.oldVer, event.newVer)
	default:
		if event.stale {
			return fmt.Sprintf("%s delete v%d (stale)", name, event.newVer)
		}
		return fmt.Sprintf("%s delete v%d", name, event.newVer)
	}
}

// orderingRun holds the context one ordering is delivered to and the state of the core which is
// visible from that ordering
type orderingRun struct {
	ctx *Context
	// allocations are the foreign allocations the core holds: the pod UID mapped to the node the
	// allocation was made on. A release removes the allocation again.
	allocations map[string]string
}

func newOrderingRun(t *testing.T) *orderingRun {
	t.Helper()
	apiProvider := client.NewMockedAPIProvider(false)
	run := &orderingRun{
		ctx:         NewContext(apiProvider),
		allocations: make(map[string]string),
	}
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		for _, alloc := range request.Allocations {
			if alloc.AllocationTags[siCommon.Foreign] != "" {
				run.allocations[alloc.AllocationKey] = alloc.NodeID
			}
		}
		if request.Releases != nil {
			for _, release := range request.Releases.AllocationsToRelease {
				// a foreign allocation is not owned by an application
				if release.ApplicationID == "" {
					delete(run.allocations, release.AllocationKey)
				}
			}
		}
		return nil
	})
	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			if node.Action == si.NodeInfo_CREATE_DRAIN {
				// the core accepts every node, the registration waits for this event
				dispatcher.Dispatch(CachedSchedulerNodeEvent{
					NodeID: node.NodeID,
					Event:  NodeAccepted,
				})
			}
		}
		return nil
	})
	return run
}

// deliver hands a single event to the informer handlers of the context. Every event carries a copy
// of the object: the informer never hands out the same object twice.
func (r *orderingRun) deliver(history *orderingHistory, event orderingEvent) {
	if event.node {
		node := history.nodes[event.object]
		object := node.versions[event.newVer].DeepCopy()
		switch event.kind {
		case deliverAdd:
			r.ctx.addNode(object)
		case deliverUpdate:
			r.ctx.updateNode(node.versions[event.oldVer].DeepCopy(), object)
		default:
			if event.stale {
				r.ctx.deleteNode(cache.DeletedFinalStateUnknown{Key: object.Name, Obj: object})
			} else {
				r.ctx.deleteNode(object)
			}
		}
		return
	}

	pod := history.pods[event.object]
	object := pod.versions[event.newVer].DeepCopy()
	switch event.kind {
	case deliverAdd:
		r.ctx.AddPod(object)
	case deliverUpdate:
		r.ctx.UpdatePod(pod.versions[event.oldVer].DeepCopy(), object)
	default:
		if event.stale {
			r.ctx.DeletePod(cache.DeletedFinalStateUnknown{Key: object.Namespace + "/" + object.Name, Obj: object})
		} else {
			r.ctx.DeletePod(object)
		}
	}
}

// snapshot returns the end state of one ordering: the pods and nodes the scheduler cache holds and
// the foreign allocations the core holds. The lines are keyed by object so that the snapshots of two
// orderings can be compared line by line.
func (r *orderingRun) snapshot(history *orderingHistory) []string {
	lines := make([]string, 0, 2*len(history.pods)+len(history.nodes))
	for _, pod := range history.pods {
		cached := r.ctx.schedulerCache.GetPod(pod.uid)
		if cached == nil {
			lines = append(lines, fmt.Sprintf("pod %s cached=false", pod.uid))
		} else {
			lines = append(lines, fmt.Sprintf("pod %s cached=true node=%q phase=%s orphaned=%t",
				pod.uid, cached.Spec.NodeName, cached.Status.Phase, r.ctx.schedulerCache.IsPodOrphaned(pod.uid)))
		}
	}
	for _, node := range history.nodes {
		nodeInfo := r.ctx.schedulerCache.GetNode(node.name)
		if nodeInfo == nil {
			lines = append(lines, fmt.Sprintf("node %s present=false", node.name))
			continue
		}
		lines = append(lines, fmt.Sprintf("node %s present=true capacity=%s pods=%d",
			node.name, orderingResourceString(common.GetNodeResource(&nodeInfo.Node().Status)), len(nodeInfo.Pods))) //nolint:staticcheck
	}
	for _, pod := range history.pods {
		if !pod.foreign {
			continue
		}
		node, ok := r.allocations[pod.uid]
		lines = append(lines, fmt.Sprintf("foreign %s allocated=%t node=%q", pod.uid, ok, node))
	}
	return lines
}

// orderingResourceString returns the resource in a stable form: the report of a replay must be
// identical
func orderingResourceString(res *si.Resource) string {
	if res == nil {
		return "<nil>"
	}
	names := make([]string, 0, len(res.Resources))
	for name := range res.Resources {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, fmt.Sprintf("%s=%d", name, res.Resources[name].GetValue()))
	}
	return strings.Join(values, ",")
}

// run delivers every ordering of the generated history to its own context and verifies that all of
// them converge to the same state
func (f *orderingFuzzer) run() {
	reference := f.ordering(true)
	referenceState := f.deliverOrdering(reference)

	divergences := make([]string, 0)
	diverged := 0
	for idx := 1; idx < orderingCount; idx++ {
		ordering := f.ordering(false)
		state := f.deliverOrdering(ordering)
		differences := orderingDifferences(referenceState, state)
		if len(differences) == 0 {
			continue
		}
		diverged++
		if diverged > orderingReportLimit {
			continue
		}
		divergences = append(divergences, fmt.Sprintf("ordering %d:\n    %s\n  differences:\n    %s",
			idx, strings.Join(f.describeAll(ordering), "\n    "), strings.Join(differences, "\n    ")))
	}
	if diverged == 0 {
		return
	}

	f.t.Fatalf("informer event orderings of the same history did not converge\nseed: %d\ndiverged: %d of %d orderings\nhistory:\n  %s\nreference ordering:\n    %s\n  end state:\n    %s\n  %s",
		f.seed, diverged, orderingCount-1,
		strings.Join(f.history.describe(), "\n  "),
		strings.Join(f.describeAll(reference), "\n    "),
		strings.Join(referenceState, "\n    "),
		strings.Join(divergences, "\n  "))
}

// deliverOrdering runs a single ordering against a context of its own and returns its end state
func (f *orderingFuzzer) deliverOrdering(ordering []orderingEvent) []string {
	run := newOrderingRun(f.t)
	for _, event := range ordering {
		run.deliver(f.history, event)
	}
	return run.snapshot(f.history)
}

func (f *orderingFuzzer) describeAll(ordering []orderingEvent) []string {
	described := make([]string, 0, len(ordering))
	for _, event := range ordering {
		described = append(described, f.describe(event))
	}
	return described
}

// orderingDifferences returns the state which differs between two orderings, the first difference
// first. Both snapshots hold a line for every generated object so they can be compared line by line.
func orderingDifferences(reference, other []string) []string {
	differences := make([]string, 0)
	for idx := range reference {
		if reference[idx] == other[idx] {
			continue
		}
		differences = append(differences, fmt.Sprintf("reference: %s\n    ordering:  %s", reference[idx], other[idx]))
	}
	return differences
}

func TestContextEventOrdering(t *testing.T) {
	for _, seed := range orderingSeedList(t) {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			dispatcher.Start()
			defer dispatcher.UnregisterAllEventHandlers()
			defer dispatcher.Stop()
			newOrderingFuzzer(t, seed).run()
		})
	}
}

// TestContextEventOrderingDeterminism verifies that a seed always generates the same history and the
// same orderings: without that a failure report cannot be replayed
func TestContextEventOrderingDeterminism(t *testing.T) {
	first := newOrderingFuzzer(t, orderingSeeds[0])
	second := newOrderingFuzzer(t, orderingSeeds[0])
	assert.DeepEqual(t, first.history.describe(), second.history.describe())
	for idx := 0; idx < orderingCount; idx++ {
		assert.DeepEqual(t, first.describeAll(first.ordering(idx == 0)), second.describeAll(second.ordering(idx == 0)))
	}
}

// orderingSeedList returns the seeds to run: the fixed list, plus the seed of the seed environment
// variable when set for an exploratory run
func orderingSeedList(t *testing.T) []int64 {
	seeds := make([]int64, len(orderingSeeds))
	copy(seeds, orderingSeeds)
	value, ok := os.LookupEnv(orderingSeedEnv)
	if !ok {
		return seeds
	}
	seed, err := strconv.ParseInt(value, 10, 64)
	assert.NilError(t, err, "invalid %s value %q", orderingSeedEnv, value)
	return append(seeds, seed)
}
