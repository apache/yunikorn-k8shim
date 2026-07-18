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
	"math"
	"math/big"
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

const (
	appID  = "app01"
	app2ID = "app02"
)

//nolint:funlen
func TestGetTaskGroupFromAnnotation(t *testing.T) {
	// correct json
	testGroup := `
	[
		{
			"name": "test-group-1",
			"minMember": 10,
			"minResource": {
				"cpu": 1,
				"memory": "2Gi"
			},
			"nodeSelector": {
				"test": "testnode",
				"locate": "west"
			},
			"tolerations": [
				{
					"key": "key",
					"operator": "Equal",
					"value": "value",
					"effect": "NoSchedule"
				}
			]
		},
		{
			"name": "test-group-2",
			"minMember": 5,
			"minResource": {
				"cpu": 2,
				"memory": "4Gi"
			}
		}
	]`
	testGroup2 := `
	[
		{
			"name": "test-group-3",
			"minMember": 3,
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// Error json
	testGroupErr := `
	[
		{
			"name": "test-group-err-1",
			"minMember": "ERR",
			"minResource": {
				"cpu": "ERR",
				"memory": "ERR"
			},
		}
	]`
	// without name
	testGroupErr2 := `
	[
		{
			"minMember": 3,
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// without minMember
	testGroupErr3 := `
	[
		{
			"name": "test-group-err-3",
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// without minResource
	testGroupErr4 := `
	[
		{
			"name": "test-group-err-4",
			"minMember": 3
		}
	]`
	// negative minMember without minResource
	testGroupErr5 := `
	[
		{
			"name": "test-group-err-5",
			"minMember": -100
		}
	]`
	// negative minMember with minResource
	testGroupErr6 := `
	[
		{
			"name": "test-group-err-6",
			"minMember": -100,
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// Insert task group info to pod annotation
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-err",
			Namespace: "test",
			UID:       "test-pod-UID-err",
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	// Empty case
	taskGroupEmpty, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupEmpty == nil)
	assert.Assert(t, err == nil)
	// Error case
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr}
	taskGroupErr, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr2}
	taskGroupErr2, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr2 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr3}
	taskGroupErr3, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr3 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr4}
	taskGroupErr4, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr4 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr5}
	taskGroupErr5, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr5 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr6}
	taskGroupErr6, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr6 == nil)
	assert.Assert(t, err != nil)
	// Correct case
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroup}
	taskGroups, err := GetTaskGroupsFromAnnotation(pod)
	assert.NilError(t, err)
	// Group value check
	assert.Equal(t, taskGroups[0].Name, "test-group-1")
	assert.Equal(t, taskGroups[0].MinMember, int32(10))
	assert.Equal(t, taskGroups[0].MinResource["cpu"], resource.MustParse("1"))
	assert.Equal(t, taskGroups[0].MinResource["memory"], resource.MustParse("2Gi"))
	assert.Equal(t, taskGroups[1].Name, "test-group-2")
	assert.Equal(t, taskGroups[1].MinMember, int32(5))
	assert.Equal(t, taskGroups[1].MinResource["cpu"], resource.MustParse("2"))
	assert.Equal(t, taskGroups[1].MinResource["memory"], resource.MustParse("4Gi"))
	// NodeSelector check
	assert.Equal(t, taskGroups[0].NodeSelector["test"], "testnode")
	assert.Equal(t, taskGroups[0].NodeSelector["locate"], "west")
	// Toleration check
	var tolerations []v1.Toleration
	toleration := v1.Toleration{
		Key:      "key",
		Operator: "Equal",
		Value:    "value",
		Effect:   "NoSchedule",
	}
	tolerations = append(tolerations, toleration)
	assert.DeepEqual(t, taskGroups[0].Tolerations, tolerations)

	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroup2}
	taskGroups2, err := GetTaskGroupsFromAnnotation(pod)
	assert.NilError(t, err)
	assert.Equal(t, taskGroups2[0].Name, "test-group-3")
	assert.Equal(t, taskGroups2[0].MinMember, int32(3))
	assert.Equal(t, taskGroups2[0].MinResource["cpu"], resource.MustParse("2"))
	assert.Equal(t, taskGroups2[0].MinResource["memory"], resource.MustParse("1Gi"))
}

func TestGetTaskGroupsFromAnnotationValidatesResources(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod-validate", Namespace: "test", UID: "test-pod-UID-validate"},
	}
	rejected := []struct {
		name string
		anno string
	}{
		{"negative cpu", `[{"name":"g","minMember":1,"minResource":{"cpu":"-1"}}]`},
		{"cpu MilliValue overflow", `[{"name":"g","minMember":1,"minResource":{"cpu":"9223372036854776"}}]`},
		{"memory Value overflow", `[{"name":"g","minMember":1,"minResource":{"memory":"9223372036854775808"}}]`},
		{"minMember times minResource overflow", `[{"name":"g","minMember":2000000000,"minResource":{"cpu":"5000000"}}]`},
		{"aggregate overflow across taskGroups", `[{"name":"a","minMember":1,"minResource":{"memory":"5E"}},{"name":"b","minMember":1,"minResource":{"memory":"5E"}}]`},
		{"cpu and vcore canonical aggregate overflow", `[{"name":"a","minMember":1,"minResource":{"cpu":"5P"}},{"name":"b","minMember":1,"minResource":{"vcore":"5E"}}]`},
		{"cpu and vcore collide within a group", `[{"name":"g","minMember":1,"minResource":{"cpu":"1","vcore":"1"}}]`},
		{"explicit pods collides with implicit pods", `[{"name":"a","minMember":2147483647,"minResource":{}},{"name":"b","minMember":1,"minResource":{"pods":"9223372036854775807"}}]`},
	}
	for _, tc := range rejected {
		pod.Annotations = map[string]string{constants.AnnotationTaskGroups: tc.anno}
		tg, err := GetTaskGroupsFromAnnotation(pod)
		assert.Assert(t, tg == nil, tc.name)
		assert.Assert(t, err != nil, tc.name)
	}
	// A task group with representable, non-negative resources is accepted.
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: `[{"name":"g","minMember":2,"minResource":{"cpu":"1","memory":"1Gi"}}]`}
	tg, err := GetTaskGroupsFromAnnotation(pod)
	assert.NilError(t, err)
	assert.Equal(t, len(tg), 1)
}

// TestValidatedTaskGroupsNeverProduceNegativePlaceholderAsk is the caller-level
// invariant: whenever GetTaskGroupsFromAnnotation accepts an annotation, the
// placeholder ask that setTaskGroups builds (common.Add over GetTGResource) must be
// non-negative on every resource, including after cpu->vcore canonicalization and
// the implicit pods count. It exercises the cpu/vcore/pods key overlap that a
// raw-key aggregate check would miss.
func TestValidatedTaskGroupsNeverProduceNegativePlaceholderAsk(t *testing.T) {
	strs := []string{"0", "1", "500m", "1.5", "2", "1Gi", "5E", "5P",
		"9223372036854775807", "9223372036854775808", "18446744073709551616"}
	keys := []string{"cpu", "vcore", "memory", "pods", "nvidia.com/gpu"}
	members := []int32{1, 2, 2000000000, math.MaxInt32}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", UID: "u"}}
	accepted := 0
	for _, k1 := range keys {
		for _, k2 := range keys {
			for _, s := range strs {
				for _, m := range members {
					anno := fmt.Sprintf(
						`[{"name":"a","minMember":%d,"minResource":{%q:%q}},{"name":"b","minMember":1,"minResource":{%q:%q}}]`,
						m, k1, s, k2, s)
					pod.Annotations = map[string]string{constants.AnnotationTaskGroups: anno}
					tg, err := GetTaskGroupsFromAnnotation(pod)
					if err != nil {
						continue // rejected: fine
					}
					accepted++
					// Replicate setTaskGroups exactly.
					ask := common.NewResourceBuilder().Build()
					for _, g := range tg {
						ask = common.Add(ask, common.GetTGResource(g.MinResource, int64(g.MinMember)))
					}
					// big.Int oracle: GetTGResource/common.Add use unchecked int64, so an
					// accepted annotation whose ask overflowed would carry a wrong (possibly
					// still non-negative) Value. Recompute the exact ask in big.Int and require
					// an exact per-key match, which catches an overflow that wraps to any value
					// and any drift between the validator's canonicalization and GetTGResource's.
					want := map[string]*big.Int{}
					addWant := func(k string, v *big.Int) {
						if want[k] == nil {
							want[k] = new(big.Int)
						}
						want[k].Add(want[k], v)
					}
					for _, g := range tg {
						m := big.NewInt(int64(g.MinMember))
						addWant("pods", m)
						for resName, q := range g.MinResource {
							canonical, acc := resName, q.Value()
							if resName == v1.ResourceCPU.String() {
								canonical, acc = siCommon.CPU, q.MilliValue()
							}
							addWant(canonical, new(big.Int).Mul(m, big.NewInt(acc)))
						}
					}
					for name, q := range ask.Resources {
						if w := want[name]; w == nil || w.Cmp(big.NewInt(q.Value)) != 0 {
							t.Fatalf("placeholder ask %s=%d does not match exact value %v (overflow) for annotation %s", name, q.Value, want[name], anno)
						}
					}
				}
			}
		}
	}
	t.Logf("accepted %d combos, all placeholder asks match the exact value", accepted)
	assert.Assert(t, accepted > 0, "no combos accepted; the validator may be over-rejecting")
}
