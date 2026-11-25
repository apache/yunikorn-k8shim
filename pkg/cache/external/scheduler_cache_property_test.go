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

package external

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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/client"
)

// This is a randomized property test for the scheduler cache. It drives random sequences of the
// operations the cache sees in production (informer add/update/delete, assume and forget from the
// scheduling cycle, node add and remove) against a trivial reference model and asserts a set of
// invariants after every single operation. The sequences are generated from a fixed set of seeds
// so a failure can be replayed: the failure report contains the seed, the step number and the tail
// of the operation history.

const (
	fuzzPodCount    = 8
	fuzzNodeCount   = 4
	fuzzSteps       = 5000
	fuzzHistoryTail = 20
	// fuzzSeedEnv holds an extra seed to run, for exploratory runs only: the fixed seeds always run
	fuzzSeedEnv = "YUNIKORN_CACHE_FUZZ_SEED"
	// fuzzSharedClaim is referenced by every pod which has a volume, so that a node can hold more
	// than one reference to the same claim
	fuzzSharedClaim = "fuzz-pvc-shared"
	// fuzzAffinityLabel is set on the pods which carry affinity and is the one their affinity terms
	// select on, so that the terms are the ones a real workload would use
	fuzzAffinityLabel = "fuzz-affinity"
)

// fuzzSeeds are the seeds used for every run: sequences must be stable over time and over -count
var fuzzSeeds = []int64{1, 7, 42, 1234, 20260816}

// the operations that are generated, see applyPodOp and applyNodeOp for their meaning
const (
	opUpdatePodPending = iota
	opUpdatePodAssigned
	opUpdatePodRunning
	opUpdatePodTerminated
	opRemovePod
	opAssumePod
	opForgetPod
	opUpdateNode
	opRemoveNode
	opReadNodesInfo
)

// the shapes a pod of the pool can have. The shape is derived from the index of the pod and is
// therefore stable over its whole lifetime: the volumes and the affinity of a real pod are
// immutable, an update which changes them is not something the cache can ever see.
const (
	shapePlain = iota
	shapeWithPVC
	shapeWithAffinity
	shapeWithAntiAffinity
	shapeCount
)

// weights of the generated operations: assume and forget are weighted up as the invariants that
// are the hardest to keep are the ones around the assumed state
var fuzzOpWeights = []struct {
	op     int
	weight int
}{
	{opUpdatePodPending, 20},
	{opUpdatePodAssigned, 8},
	{opUpdatePodRunning, 10},
	{opUpdatePodTerminated, 3},
	{opRemovePod, 8},
	{opAssumePod, 18},
	{opForgetPod, 18},
	{opUpdateNode, 8},
	{opRemoveNode, 7},
	{opReadNodesInfo, 8},
}

// podState is the reference model of a single pod: what the cache is expected to hold for it
type podState struct {
	present  bool        // the pod is in the cache
	nodeName string      // the Spec.NodeName of the pod held by the cache
	assigned string      // the node the cache has the pod assigned to, empty if none
	orphan   bool        // the pod is assigned to a node the cache does not know
	assumed  bool        // the pod is assumed
	allBound bool        // all volumes of the assumed pod are bound
	phase    v1.PodPhase // the phase of the pod held by the cache
	bound    bool        // the cluster confirmed the assignment, it is not an assumption
}

// cacheModel predicts the state of the cache for the operations that are generated. It is
// deliberately a flat map of pods and nodes: the invariants are checked against this model, not
// against a second copy of the cache implementation.
type cacheModel struct {
	pods  map[string]*podState
	nodes map[string]bool
}

func newCacheModel() *cacheModel {
	return &cacheModel{
		pods:  make(map[string]*podState),
		nodes: make(map[string]bool),
	}
}

func (m *cacheModel) podKeys() []string {
	keys := make([]string, 0, len(m.pods))
	for key := range m.pods {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// nodeNames returns the nodes the model knows about, sorted so that a node can be picked from the
// list without depending on map iteration order
func (m *cacheModel) nodeNames() []string {
	names := make([]string, 0, len(m.nodes))
	for name := range m.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// update models an update of a pod in the cache: an informer event when fromCluster is set, an
// internal re-add of the pod otherwise (assume, forget and the adoption of an orphan)
func (m *cacheModel) update(key, nodeName string, phase v1.PodPhase, fromCluster bool) {
	pod, ok := m.pods[key]
	if !ok {
		pod = &podState{}
		m.pods[key] = pod
	}

	node := nodeName
	if pod.present && pod.assumed && pod.assigned != "" && node == "" {
		// the update does not carry an assignment but the pod is assumed on a node: the assumed
		// assignment is kept until the result of the bind shows up
		node = pod.assigned
	}
	pod.assigned = ""
	pod.orphan = false

	terminated := phase == v1.PodFailed || phase == v1.PodSucceeded
	if phase == v1.PodRunning || terminated {
		// the bind has completed, the pod is not assumed anymore
		pod.assumed = false
		pod.allBound = false
	}
	if terminated {
		// a terminated pod is dropped from the cache completely
		delete(m.pods, key)
		return
	}

	if node != "" {
		if m.nodes[node] {
			pod.assigned = node
		} else {
			// the node is not known to the cache: the pod has to wait for it to show up
			pod.orphan = true
		}
	}
	if fromCluster {
		// only the cluster can confirm an assignment, an assumed node name is our own
		pod.bound = nodeName != ""
	}
	pod.present = true
	pod.nodeName = node
	pod.phase = phase
}

func (m *cacheModel) assume(key, nodeName string, allBound bool) {
	phase := v1.PodPending
	if pod, ok := m.pods[key]; ok {
		phase = pod.phase
	}
	m.update(key, nodeName, phase, false)
	pod := m.pods[key]
	pod.assumed = true
	pod.allBound = allBound
}

func (m *cacheModel) forget(key, nodeName string, phase v1.PodPhase) {
	pod, ok := m.pods[key]
	if !ok {
		return
	}
	// a pod that is no longer assumed has been bound already: its assignment is real and is kept
	revert := pod.assumed && nodeName != ""
	pod.assumed = false
	pod.allBound = false
	if revert {
		nodeName = ""
	}
	m.update(key, nodeName, phase, false)
}

func (m *cacheModel) removePod(key string) {
	delete(m.pods, key)
}

func (m *cacheModel) updateNode(name string) {
	if m.nodes[name] {
		// an update of a known node does not change any pod state
		return
	}
	m.nodes[name] = true
	// the new node adopts the orphans waiting for it
	for _, key := range m.podKeys() {
		if pod := m.pods[key]; pod.orphan && pod.nodeName == name {
			m.update(key, pod.nodeName, pod.phase, false)
		}
	}
}

func (m *cacheModel) removeNode(name string) {
	if !m.nodes[name] {
		return
	}
	delete(m.nodes, name)
	// the pods the cluster placed on the node become orphans, the cached pod keeps its node name.
	// A pod that is still assumed was never bound to the node: its assignment is reverted, the pod
	// is pending again.
	for _, key := range m.podKeys() {
		pod := m.pods[key]
		if pod.assigned != name {
			continue
		}
		revert := pod.assumed
		pod.assigned = ""
		pod.assumed = false
		pod.allBound = false
		if revert {
			pod.nodeName = ""
			pod.bound = false
			continue
		}
		pod.orphan = true
	}
}

// argSnapshot holds an object passed to the cache together with a copy taken before the call: the
// cache must never modify an object it is given, they can be owned by the informer cache
type argSnapshot struct {
	desc   string
	passed interface{}
	before interface{}
}

type cacheFuzzer struct {
	t       *testing.T
	seed    int64
	rng     *rand.Rand
	cache   *SchedulerCache
	model   *cacheModel
	history []string
	args    []argSnapshot
	step    int
}

func newCacheFuzzer(t *testing.T, seed int64) *cacheFuzzer {
	t.Helper()
	return &cacheFuzzer{
		t:     t,
		seed:  seed,
		rng:   rand.New(rand.NewSource(seed)), //nolint:gosec
		cache: NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs()),
		model: newCacheModel(),
	}
}

func fuzzPodKey(idx int) string {
	return fmt.Sprintf("fuzz-pod-uid-%04d", idx)
}

func fuzzNodeName(idx int) string {
	return fmt.Sprintf("fuzz-node-%04d", idx)
}

// newPod returns a new pod object for the given pod of the pool: every operation passes a fresh
// object to the cache, exactly like the informer does. The volumes and the affinity of the pod only
// depend on its index, they are the same for every object generated for it: a pod which changes
// shape over its lifetime is not something the cache can ever see and would only exercise code
// which is unreachable in production.
func (f *cacheFuzzer) newPod(idx int, nodeName string, phase v1.PodPhase) *v1.Pod {
	requests := make(map[v1.ResourceName]resource.Quantity)
	requests[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	requests[v1.ResourceName("cpu")] = *resource.NewQuantity(1, resource.DecimalSI)
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        fmt.Sprintf("fuzz-pod-%04d", idx),
			Namespace:   "default",
			UID:         types.UID(fuzzPodKey(idx)),
			Annotations: map[string]string{"step": strconv.Itoa(f.step)},
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

	switch idx % shapeCount {
	case shapePlain:
		// no volumes and no affinity
	case shapeWithPVC:
		// a claim of its own and one shared with the other pods which have volumes, so that a node
		// can hold a reference count above one. The volume which is not a claim must not be counted.
		pod.Spec.Volumes = []v1.Volume{
			{
				Name:         "own",
				VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: fmt.Sprintf("fuzz-pvc-%04d", idx)}},
			},
			{
				Name:         "shared",
				VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: fuzzSharedClaim}},
			},
			{
				Name:         "scratch",
				VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
			},
		}
	case shapeWithAffinity:
		pod.Labels = map[string]string{fuzzAffinityLabel: "true"}
		pod.Spec.Affinity = &v1.Affinity{
			PodAffinity: &v1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{fuzzAffinityTerm()},
			},
		}
	case shapeWithAntiAffinity:
		pod.Labels = map[string]string{fuzzAffinityLabel: "true"}
		pod.Spec.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{fuzzAffinityTerm()},
			},
		}
	}
	return pod
}

// fuzzAffinityTerm returns the required term the pods which carry affinity use: it selects the pods
// of the pool which carry affinity themselves, per node
func fuzzAffinityTerm() v1.PodAffinityTerm {
	return v1.PodAffinityTerm{
		LabelSelector: &apis.LabelSelector{
			MatchLabels: map[string]string{fuzzAffinityLabel: "true"},
		},
		TopologyKey: "kubernetes.io/hostname",
	}
}

func (f *cacheFuzzer) newNode(idx int) *v1.Node {
	allocatable := make(map[v1.ResourceName]resource.Quantity)
	allocatable[v1.ResourceName("memory")] = *resource.NewQuantity(100*1024*1000*1000, resource.DecimalSI)
	allocatable[v1.ResourceName("cpu")] = *resource.NewQuantity(100, resource.DecimalSI)
	return &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      fuzzNodeName(idx),
			Namespace: "default",
			UID:       types.UID(fmt.Sprintf("fuzz-node-uid-%04d", idx)),
		},
		Status: v1.NodeStatus{
			Allocatable: allocatable,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
}

func (f *cacheFuzzer) record(format string, args ...interface{}) {
	f.history = append(f.history, fmt.Sprintf("step %d: %s", f.step, fmt.Sprintf(format, args...)))
}

// track registers an object passed to the cache for the no-caller-mutation check
func (f *cacheFuzzer) track(desc string, passed, before interface{}) {
	f.args = append(f.args, argSnapshot{desc: desc, passed: passed, before: before})
}

// pickOp returns the next operation to run based on the configured weights
func (f *cacheFuzzer) pickOp() int {
	total := 0
	for _, entry := range fuzzOpWeights {
		total += entry.weight
	}
	pick := f.rng.Intn(total)
	for _, entry := range fuzzOpWeights {
		if pick < entry.weight {
			return entry.op
		}
		pick -= entry.weight
	}
	return opUpdatePodPending
}

// nodeForUpdate returns the node an informer update for the given pod carries: a pod which is
// assumed can only be reported on the node it was assumed on, for any other pod any node of the
// pool can show up, including one the cache does not know yet (the recovery case)
func (f *cacheFuzzer) nodeForUpdate(key string) string {
	if pod, ok := f.model.pods[key]; ok && pod.nodeName != "" {
		return pod.nodeName
	}
	return fuzzNodeName(f.rng.Intn(fuzzNodeCount))
}

func (f *cacheFuzzer) applyPodOp(op int) bool {
	idx := f.rng.Intn(fuzzPodCount)
	key := fuzzPodKey(idx)
	switch op {
	case opUpdatePodPending:
		// an informer add or update of a pod without an assignment, the pod may or may not be in
		// the cache already: the cache has a single entry point for both
		f.updatePod(f.newPod(idx, "", v1.PodPending), "")
	case opUpdatePodAssigned:
		// an informer add or update of a pod the cluster reports as assigned to a node
		node := f.nodeForUpdate(key)
		f.updatePod(f.newPod(idx, node, v1.PodPending), node)
	case opUpdatePodRunning:
		// the bind became visible: the pod is running on the node it was assigned to
		node := f.nodeForUpdate(key)
		f.updatePod(f.newPod(idx, node, v1.PodRunning), node)
	case opUpdatePodTerminated:
		phase := v1.PodSucceeded
		if f.rng.Intn(2) == 0 {
			phase = v1.PodFailed
		}
		node := ""
		if pod, ok := f.model.pods[key]; ok {
			node = pod.nodeName
		}
		f.updatePod(f.newPod(idx, node, phase), node)
	case opRemovePod:
		node := ""
		phase := v1.PodPending
		if pod, ok := f.model.pods[key]; ok {
			node = pod.nodeName
			phase = pod.phase
		}
		pod := f.newPod(idx, node, phase)
		before := pod.DeepCopy()
		f.cache.RemovePod(pod)
		f.model.removePod(key)
		f.track("pod", pod, before)
		f.record("RemovePod(%s)", key)
	case opAssumePod:
		return f.assumePod(key)
	case opForgetPod:
		return f.forgetPod(key)
	}
	return true
}

// updatePod runs an informer driven update of a pod and models it
func (f *cacheFuzzer) updatePod(pod *v1.Pod, node string) {
	before := pod.DeepCopy()
	f.cache.UpdatePod(pod)
	f.model.update(string(pod.UID), node, pod.Status.Phase, true)
	f.track("pod", pod, before)
	f.record("UpdatePod(%s, node=%q, phase=%s)", pod.UID, node, pod.Status.Phase)
}

// assumePod mirrors Context.AssumePod: the pod must be in the cache, the node must be known and
// the node name is stamped on a copy of the cached pod
func (f *cacheFuzzer) assumePod(key string) bool {
	cached := f.cache.GetPod(key)
	if cached == nil || cached.Status.Phase != v1.PodPending {
		return false
	}
	nodes := f.model.nodeNames()
	if len(nodes) == 0 {
		return false
	}
	node := nodes[f.rng.Intn(len(nodes))]
	allBound := f.rng.Intn(2) == 0
	pod := cached.DeepCopy()
	pod.Spec.NodeName = node
	before := pod.DeepCopy()
	f.cache.AssumePod(pod, allBound)
	f.model.assume(key, node, allBound)
	f.track("pod", pod, before)
	f.record("AssumePod(%s, node=%q, allBound=%t)", key, node, allBound)
	return true
}

// forgetPod mirrors Context.ForgetPod: the pod is only forgotten when it is in the cache and the
// object handed to the cache is the cached one
func (f *cacheFuzzer) forgetPod(key string) bool {
	cached := f.cache.GetPod(key)
	if cached == nil {
		return false
	}
	modelPod, ok := f.model.pods[key]
	if !ok {
		return false
	}
	node, phase := modelPod.nodeName, modelPod.phase
	before := cached.DeepCopy()
	f.cache.ForgetPod(cached)
	f.model.forget(key, node, phase)
	f.track("pod", cached, before)
	f.record("ForgetPod(%s, node=%q)", key, node)
	return true
}

func (f *cacheFuzzer) applyNodeOp(op int) bool {
	idx := f.rng.Intn(fuzzNodeCount)
	node := f.newNode(idx)
	before := node.DeepCopy()
	switch op {
	case opUpdateNode:
		f.cache.UpdateNode(node)
		f.model.updateNode(node.Name)
		f.record("UpdateNode(%s)", node.Name)
	case opRemoveNode:
		f.cache.RemoveNode(node)
		f.model.removeNode(node.Name)
		f.record("RemoveNode(%s)", node.Name)
	}
	f.track("node", node, before)
	return true
}

// readNodesInfo mirrors a read of the predicate shared lister: the lists the cache derives from the
// node map are only materialized by their getter, without a read they stay nil forever
func (f *cacheFuzzer) readNodesInfo() bool {
	f.cache.LockForReads()
	defer f.cache.UnlockForReads()
	switch f.rng.Intn(3) {
	case 0:
		f.cache.GetNodesInfo()
		f.record("GetNodesInfo()")
	case 1:
		f.cache.GetNodesInfoPodsWithAffinity()
		f.record("GetNodesInfoPodsWithAffinity()")
	default:
		f.cache.GetNodesInfoPodsWithReqAntiAffinity()
		f.record("GetNodesInfoPodsWithReqAntiAffinity()")
	}
	return true
}

func (f *cacheFuzzer) applyOp(op int) bool {
	switch op {
	case opUpdateNode, opRemoveNode:
		return f.applyNodeOp(op)
	case opReadNodesInfo:
		return f.readNodesInfo()
	}
	return f.applyPodOp(op)
}

// run generates and runs the operation sequence, checking all invariants after every operation
func (f *cacheFuzzer) run(steps int) {
	for f.step = 1; f.step <= steps; f.step++ {
		f.args = f.args[:0]
		if !f.applyOp(f.pickOp()) {
			// the preconditions of the operation are not met, nothing was run
			continue
		}
		f.check()
	}
}

// check runs all invariants and fails the test with a replayable report if any of them is violated
func (f *cacheFuzzer) check() {
	violations := make([]string, 0)
	violations = append(violations, f.checkAssumedVisibility()...)
	violations = append(violations, f.checkForgetForgets()...)
	violations = append(violations, f.checkAssignmentCoherence()...)
	violations = append(violations, f.checkNoCallerMutation()...)
	violations = append(violations, f.checkContainment()...)
	violations = append(violations, f.checkModelAgreement()...)
	violations = append(violations, f.checkNodeInfoAccounting()...)
	violations = append(violations, f.checkOrphanCoherence()...)
	violations = append(violations, f.checkDerivedNodesInfo()...)
	violations = append(violations, f.checkPVCRefCounts()...)
	if len(violations) == 0 {
		return
	}

	history := f.history
	if len(history) > fuzzHistoryTail {
		history = history[len(history)-fuzzHistoryTail:]
	}
	f.t.Fatalf("scheduler cache invariant violated\nseed: %d\nstep: %d\nviolations:\n  %s\nlast %d operations:\n  %s",
		f.seed, f.step, strings.Join(violations, "\n  "), len(history), strings.Join(history, "\n  "))
}

// checkAssumedVisibility verifies I1: a pod which is assumed is visible on the node it is assumed
// on, an informer update must never strip the assumed assignment
func (f *cacheFuzzer) checkAssumedVisibility() []string {
	violations := make([]string, 0)
	for _, key := range sortedKeys(f.cache.assumedPods) {
		pod := f.cache.GetPod(key)
		if pod == nil {
			violations = append(violations, fmt.Sprintf("I1 assumed-visibility: assumed pod %s is not in the cache", key))
			continue
		}
		modelPod, ok := f.model.pods[key]
		if !ok || !modelPod.assumed {
			// the disagreement itself is reported by I6
			continue
		}
		if pod.Spec.NodeName == "" || pod.Spec.NodeName != modelPod.nodeName {
			violations = append(violations, fmt.Sprintf("I1 assumed-visibility: assumed pod %s is on node %q, expected %q",
				key, pod.Spec.NodeName, modelPod.nodeName))
		}
		if node := f.cache.assignedPods[key]; node != modelPod.nodeName {
			violations = append(violations, fmt.Sprintf("I1 assumed-visibility: assumed pod %s is assigned to %q, expected %q",
				key, node, modelPod.nodeName))
		}
	}
	return violations
}

// checkForgetForgets verifies I2: a pod which is not assumed and was never confirmed on a node by
// the cluster must not be assigned to one. This is the invariant broken by YUNIKORN-3355: a
// forgotten pod which keeps its assignment is recovered as an existing allocation later on. The
// removal of the node a pod is assumed on is the same class of failure: the assignment the shim
// made must not survive it.
func (f *cacheFuzzer) checkForgetForgets() []string {
	violations := make([]string, 0)
	for _, key := range f.model.podKeys() {
		modelPod := f.model.pods[key]
		if modelPod.assumed || modelPod.bound {
			continue
		}
		if pod := f.cache.GetPod(key); pod != nil && pod.Spec.NodeName != "" {
			violations = append(violations, fmt.Sprintf("I2 forget-forgets: unbound pod %s is on node %q", key, pod.Spec.NodeName))
		}
		if node, ok := f.cache.assignedPods[key]; ok {
			violations = append(violations, fmt.Sprintf("I2 forget-forgets: unbound pod %s is assigned to node %q", key, node))
		}
		if node := f.nodeHoldingPod(key); node != "" {
			violations = append(violations, fmt.Sprintf("I2 forget-forgets: unbound pod %s is on the node info of %q", key, node))
		}
	}
	return violations
}

// checkAssignmentCoherence verifies I3: the assigned pods and the node infos hold the same view
func (f *cacheFuzzer) checkAssignmentCoherence() []string {
	violations := make([]string, 0)
	for _, key := range sortedKeys(f.cache.assignedPods) {
		node := f.cache.assignedPods[key]
		nodeInfo, ok := f.cache.nodesMap[node]
		if !ok {
			violations = append(violations, fmt.Sprintf("I3 assignment-coherence: pod %s is assigned to unknown node %q", key, node))
			continue
		}
		if !nodeInfoHasPod(nodeInfo, key) {
			violations = append(violations, fmt.Sprintf("I3 assignment-coherence: pod %s is assigned to node %q but not on its node info", key, node))
		}
	}
	for _, node := range sortedKeys(f.cache.nodesMap) {
		// nolint:staticcheck
		for _, fwkPod := range f.cache.nodesMap[node].Pods {
			key := string(fwkPod.GetPod().UID)
			if assigned := f.cache.assignedPods[key]; assigned != node {
				violations = append(violations, fmt.Sprintf("I3 assignment-coherence: pod %s is on the node info of %q but assigned to %q",
					key, node, assigned))
			}
		}
	}
	return violations
}

// checkNoCallerMutation verifies I4: the cache never modifies an object it is given, they can be
// owned by the informer cache and shared with other threads
func (f *cacheFuzzer) checkNoCallerMutation() []string {
	violations := make([]string, 0)
	for _, arg := range f.args {
		if !apiequality.Semantic.DeepEqual(arg.before, arg.passed) {
			violations = append(violations, fmt.Sprintf("I4 no-caller-mutation: the %s passed to the cache was modified\n    before: %s\n    after:  %s",
				arg.desc, mutationDetail(arg.before), mutationDetail(arg.passed)))
		}
	}
	return violations
}

// mutationDetail describes the fields of an object the cache could modify, the full object is too
// large to be of any use in a failure report
func mutationDetail(obj interface{}) string {
	switch object := obj.(type) {
	case *v1.Pod:
		return fmt.Sprintf("uid=%s nodeName=%q phase=%s labels=%v annotations=%v",
			object.UID, object.Spec.NodeName, object.Status.Phase, object.Labels, object.Annotations)
	case *v1.Node:
		return fmt.Sprintf("name=%s unschedulable=%t labels=%v annotations=%v",
			object.Name, object.Spec.Unschedulable, object.Labels, object.Annotations)
	default:
		return fmt.Sprintf("%v", obj)
	}
}

// checkContainment verifies I5: the scheduling state of the cache only covers cached pods
func (f *cacheFuzzer) checkContainment() []string {
	violations := make([]string, 0)
	for _, key := range sortedKeys(f.cache.assumedPods) {
		if _, ok := f.cache.podsMap[key]; !ok {
			violations = append(violations, fmt.Sprintf("I5 containment: assumed pod %s is not in the pods map", key))
		}
	}
	for _, key := range sortedKeys(f.cache.assignedPods) {
		if _, ok := f.cache.podsMap[key]; !ok {
			violations = append(violations, fmt.Sprintf("I5 containment: assigned pod %s is not in the pods map", key))
		}
	}
	for _, key := range sortedKeys(f.cache.orphanedPods) {
		if _, ok := f.cache.podsMap[key]; !ok {
			violations = append(violations, fmt.Sprintf("I5 containment: orphaned pod %s is not in the pods map", key))
		}
	}
	return violations
}

// checkModelAgreement verifies I6: the cache holds what the reference model predicts
func (f *cacheFuzzer) checkModelAgreement() []string {
	violations := make([]string, 0)
	keys := make(map[string]bool)
	for key := range f.model.pods {
		keys[key] = true
	}
	for key := range f.cache.podsMap {
		keys[key] = true
	}
	for _, key := range sortedKeys(keys) {
		modelPod, ok := f.model.pods[key]
		if !ok {
			modelPod = &podState{}
		}
		pod := f.cache.GetPod(key)
		if (pod != nil) != modelPod.present {
			violations = append(violations, fmt.Sprintf("I6 model-agreement: pod %s in cache is %t, expected %t", key, pod != nil, modelPod.present))
			continue
		}
		if pod == nil {
			continue
		}
		if pod.Spec.NodeName != modelPod.nodeName {
			violations = append(violations, fmt.Sprintf("I6 model-agreement: pod %s is on node %q, expected %q", key, pod.Spec.NodeName, modelPod.nodeName))
		}
		if node := f.cache.assignedPods[key]; node != modelPod.assigned {
			violations = append(violations, fmt.Sprintf("I6 model-agreement: pod %s is assigned to %q, expected %q", key, node, modelPod.assigned))
		}
		if assumed := f.cache.IsAssumedPod(key); assumed != modelPod.assumed {
			violations = append(violations, fmt.Sprintf("I6 model-agreement: pod %s assumed is %t, expected %t", key, assumed, modelPod.assumed))
		}
		if modelPod.assumed && f.cache.ArePodVolumesAllBound(key) != modelPod.allBound {
			violations = append(violations, fmt.Sprintf("I6 model-agreement: pod %s allBound is %t, expected %t",
				key, f.cache.ArePodVolumesAllBound(key), modelPod.allBound))
		}
		if orphan := f.cache.IsPodOrphaned(key); orphan != modelPod.orphan {
			violations = append(violations, fmt.Sprintf("I6 model-agreement: pod %s orphaned is %t, expected %t", key, orphan, modelPod.orphan))
		}
	}
	return violations
}

// checkNodeInfoAccounting verifies I7: a pod is on a node info at most once and the aggregates the
// node info maintains incrementally are the ones a node info built from scratch out of the same
// pods holds. The reference is the framework itself, the check is not a second implementation of
// its accounting: a pod which is added twice or removed without being subtracted shows up here.
func (f *cacheFuzzer) checkNodeInfoAccounting() []string {
	violations := make([]string, 0)
	for _, name := range sortedKeys(f.cache.nodesMap) {
		nodeInfo := f.cache.nodesMap[name]
		seen := make(map[string]bool)
		// nolint:staticcheck
		pods := make([]*v1.Pod, 0, len(nodeInfo.Pods))
		// nolint:staticcheck
		for _, fwkPod := range nodeInfo.Pods {
			pod := fwkPod.GetPod()
			key := string(pod.UID)
			if seen[key] {
				violations = append(violations, fmt.Sprintf("I7 node-info-accounting: pod %s is on the node info of %q more than once", key, name))
			}
			seen[key] = true
			pods = append(pods, pod)
		}

		expected := framework.NewNodeInfo(pods...)
		if !sameResource(nodeInfo.Requested, expected.Requested) {
			violations = append(violations, fmt.Sprintf("I7 node-info-accounting: node %q requested is %s, expected %s",
				name, resourceDetail(nodeInfo.Requested), resourceDetail(expected.Requested)))
		}
		if !sameResource(nodeInfo.NonZeroRequested, expected.NonZeroRequested) {
			violations = append(violations, fmt.Sprintf("I7 node-info-accounting: node %q non-zero requested is %s, expected %s",
				name, resourceDetail(nodeInfo.NonZeroRequested), resourceDetail(expected.NonZeroRequested)))
		}
		// nolint:staticcheck
		if got, want := podKeysOf(nodeInfo.PodsWithAffinity), podKeysOf(expected.PodsWithAffinity); !apiequality.Semantic.DeepEqual(got, want) {
			violations = append(violations, fmt.Sprintf("I7 node-info-accounting: node %q holds pods with affinity %v, expected %v", name, got, want))
		}
		// nolint:staticcheck
		if got, want := podKeysOf(nodeInfo.PodsWithRequiredAntiAffinity), podKeysOf(expected.PodsWithRequiredAntiAffinity); !apiequality.Semantic.DeepEqual(got, want) {
			violations = append(violations, fmt.Sprintf("I7 node-info-accounting: node %q holds pods with required anti-affinity %v, expected %v", name, got, want))
		}
	}
	return violations
}

// checkOrphanCoherence verifies I8: an orphan is a pod waiting for the node the cluster placed it
// on to show up. The node must be one the cache does not have, a known node adopts its orphans, and
// an orphan is by definition not assigned to a node.
func (f *cacheFuzzer) checkOrphanCoherence() []string {
	violations := make([]string, 0)
	for _, key := range sortedKeys(f.cache.orphanedPods) {
		pod := f.cache.orphanedPods[key]
		if pod.Spec.NodeName == "" {
			violations = append(violations, fmt.Sprintf("I8 orphan-coherence: orphaned pod %s is not on a node", key))
		} else if _, ok := f.cache.nodesMap[pod.Spec.NodeName]; ok {
			violations = append(violations, fmt.Sprintf("I8 orphan-coherence: orphaned pod %s waits for node %q which the cache has", key, pod.Spec.NodeName))
		}
		if node, ok := f.cache.assignedPods[key]; ok {
			violations = append(violations, fmt.Sprintf("I8 orphan-coherence: orphaned pod %s is assigned to node %q", key, node))
		}
	}
	return violations
}

// checkDerivedNodesInfo verifies I9: the node lists the cache derives from the node map are either
// invalidated or up to date. They are materialized by their getter and dropped by every mutation
// which can change them: a mutation which forgets to drop one leaves the predicates with a stale
// list.
func (f *cacheFuzzer) checkDerivedNodesInfo() []string {
	violations := make([]string, 0)
	violations = append(violations, f.checkDerivedNodeList("nodesInfo", f.cache.nodesInfo,
		func(nodeInfo *framework.NodeInfo) bool { return true })...)
	violations = append(violations, f.checkDerivedNodeList("nodesInfoPodsWithAffinity", f.cache.nodesInfoPodsWithAffinity,
		// nolint:staticcheck
		func(nodeInfo *framework.NodeInfo) bool { return len(nodeInfo.PodsWithAffinity) > 0 })...)
	violations = append(violations, f.checkDerivedNodeList("nodesInfoPodsWithReqAntiAffinity", f.cache.nodesInfoPodsWithReqAntiAffinity,
		// nolint:staticcheck
		func(nodeInfo *framework.NodeInfo) bool { return len(nodeInfo.PodsWithRequiredAntiAffinity) > 0 })...)
	return violations
}

// checkDerivedNodeList compares one derived list against the nodes of the node map which the getter
// of the list selects. The list is not ordered, the getter builds it from the map, so the check is
// on the membership and on the identity of the node infos, not on their order.
func (f *cacheFuzzer) checkDerivedNodeList(name string, cached []fwk.NodeInfo, include func(*framework.NodeInfo) bool) []string {
	violations := make([]string, 0)
	if cached == nil {
		// not materialized: the next read rebuilds it from the node map
		return violations
	}
	expected := make(map[string]*framework.NodeInfo)
	for _, node := range sortedKeys(f.cache.nodesMap) {
		if nodeInfo := f.cache.nodesMap[node]; include(nodeInfo) {
			expected[node] = nodeInfo
		}
	}
	seen := make(map[string]bool)
	for _, entry := range cached {
		node := entry.Node().Name
		if seen[node] {
			violations = append(violations, fmt.Sprintf("I9 derived-cache: %s holds node %q more than once", name, node))
			continue
		}
		seen[node] = true
		nodeInfo, ok := expected[node]
		if !ok {
			violations = append(violations, fmt.Sprintf("I9 derived-cache: %s holds node %q which does not belong in it", name, node))
			continue
		}
		if entry != fwk.NodeInfo(nodeInfo) {
			violations = append(violations, fmt.Sprintf("I9 derived-cache: %s holds a stale node info for node %q", name, node))
		}
	}
	for _, node := range sortedKeys(expected) {
		if !seen[node] {
			violations = append(violations, fmt.Sprintf("I9 derived-cache: %s is missing node %q", name, node))
		}
	}
	return violations
}

// checkPVCRefCounts verifies I10: the claims a node info counts are the ones its pods reference.
// The reference counts drive the predicate which decides if a claim is in use, a count which is not
// dropped with its pod keeps a claim busy forever.
func (f *cacheFuzzer) checkPVCRefCounts() []string {
	violations := make([]string, 0)
	for _, name := range sortedKeys(f.cache.nodesMap) {
		nodeInfo := f.cache.nodesMap[name]
		expected := make(map[string]int)
		// nolint:staticcheck
		for _, fwkPod := range nodeInfo.Pods {
			pod := fwkPod.GetPod()
			for _, volume := range pod.Spec.Volumes {
				if volume.PersistentVolumeClaim == nil {
					continue
				}
				expected[framework.GetNamespacedName(pod.Namespace, volume.PersistentVolumeClaim.ClaimName)]++
			}
		}
		if !apiequality.Semantic.DeepEqual(nodeInfo.PVCRefCounts, expected) {
			violations = append(violations, fmt.Sprintf("I10 pvc-ref-counts: node %q counts %v, expected %v", name, nodeInfo.PVCRefCounts, expected))
		}
	}
	return violations
}

// sameResource compares the fields of two resources which the pods of the pool can contribute to
func sameResource(got, want *framework.Resource) bool {
	return got.MilliCPU == want.MilliCPU && got.Memory == want.Memory &&
		got.EphemeralStorage == want.EphemeralStorage && got.AllowedPodNumber == want.AllowedPodNumber &&
		apiequality.Semantic.DeepEqual(got.ScalarResources, want.ScalarResources)
}

func resourceDetail(res *framework.Resource) string {
	return fmt.Sprintf("cpu=%d memory=%d storage=%d pods=%d scalar=%v",
		res.MilliCPU, res.Memory, res.EphemeralStorage, res.AllowedPodNumber, res.ScalarResources)
}

// podKeysOf returns the UIDs of the given pods in a stable order
func podKeysOf(pods []fwk.PodInfo) []string {
	keys := make([]string, 0, len(pods))
	for _, fwkPod := range pods {
		keys = append(keys, string(fwkPod.GetPod().UID))
	}
	sort.Strings(keys)
	return keys
}

// nodeHoldingPod returns the name of the node which holds the given pod on its node info
func (f *cacheFuzzer) nodeHoldingPod(key string) string {
	for _, node := range sortedKeys(f.cache.nodesMap) {
		if nodeInfoHasPod(f.cache.nodesMap[node], key) {
			return node
		}
	}
	return ""
}

func nodeInfoHasPod(nodeInfo *framework.NodeInfo, key string) bool {
	// nolint:staticcheck
	for _, fwkPod := range nodeInfo.Pods {
		if string(fwkPod.GetPod().UID) == key {
			return true
		}
	}
	return false
}

// sortedKeys returns the keys of the given map in a stable order so that the failure report of a
// replay is identical
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestSchedulerCacheProperty(t *testing.T) {
	for _, seed := range fuzzSeedList(t) {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			newCacheFuzzer(t, seed).run(fuzzSteps)
		})
	}
}

// TestSchedulerCachePropertyDeterminism verifies that a seed always generates the same sequence:
// without that a failure report cannot be replayed
func TestSchedulerCachePropertyDeterminism(t *testing.T) {
	first := newCacheFuzzer(t, fuzzSeeds[0])
	first.run(500)
	second := newCacheFuzzer(t, fuzzSeeds[0])
	second.run(500)
	assert.DeepEqual(t, first.history, second.history)
}

// fuzzSeedList returns the seeds to run: the fixed list, plus the seed of the seed environment
// variable when set for an exploratory run
func fuzzSeedList(t *testing.T) []int64 {
	seeds := make([]int64, len(fuzzSeeds))
	copy(seeds, fuzzSeeds)
	value, ok := os.LookupEnv(fuzzSeedEnv)
	if !ok {
		return seeds
	}
	seed, err := strconv.ParseInt(value, 10, 64)
	assert.NilError(t, err, "invalid %s value %q", fuzzSeedEnv, value)
	return append(seeds, seed)
}
