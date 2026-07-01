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

package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

func u64Ptr(v uint64) *uint64 { return &v }

// =============================================================================
// ToYuniKornQueueConfig: every CR field must be propagated.
// =============================================================================

func TestToYuniKornQueueConfig_Minimal(t *testing.T) {
	got := ToYuniKornQueueConfig(QueueConfig{Name: "team-a"})
	if got.Name != "team-a" {
		t.Errorf("name: got %q", got.Name)
	}
	if got.Parent {
		t.Errorf("parent default must be false, got true")
	}
}

func TestToYuniKornQueueConfig_AllFieldsCopied(t *testing.T) {
	in := QueueConfig{
		Name:   "team-a",
		Parent: boolPtr(true),
		Resources: &Resources{
			Guaranteed: map[string]string{"memory": "1G", "vcore": "1"},
			Max:        map[string]string{"memory": "8G", "vcore": "4"},
		},
		MaxApplications: u64Ptr(100),
		Properties: map[string]string{
			"application.sort.policy": "fair",
			"priority.offset":         "10",
		},
		AdminACL:  "admin",
		SubmitACL: "*",
		Limits: []Limit{{
			Limit:           "u",
			Users:           []string{"alice"},
			Groups:          []string{"infra"},
			MaxResources:    map[string]string{"memory": "2G"},
			MaxApplications: u64Ptr(20),
		}},
		Queues: []QueueConfig{{Name: "dev"}, {Name: "stg"}},
		ChildTemplate: &ChildTemplate{
			MaxApplications: u64Ptr(50),
			Properties:      map[string]string{"x": "y"},
			Resources: &Resources{
				Max: map[string]string{"memory": "4G"},
			},
		},
	}

	got := ToYuniKornQueueConfig(in)

	if got.Name != "team-a" {
		t.Errorf("name: %q", got.Name)
	}
	if !got.Parent {
		t.Error("parent: expected true")
	}
	if !reflect.DeepEqual(got.Resources, &queueconfig.Resources{
		Guaranteed: map[string]string{"memory": "1G", "vcore": "1"},
		Max:        map[string]string{"memory": "8G", "vcore": "4"},
	}) {
		t.Errorf("resources mismatch: %+v", got.Resources)
	}
	if got.MaxApplications != 100 {
		t.Errorf("maxApplications: %d", got.MaxApplications)
	}
	if got.Properties["application.sort.policy"] != "fair" {
		t.Errorf("properties not copied: %+v", got.Properties)
	}
	if got.AdminACL != "admin" || got.SubmitACL != "*" {
		t.Errorf("acl: admin=%q submit=%q", got.AdminACL, got.SubmitACL)
	}
	if len(got.Limits) != 1 || got.Limits[0].MaxApplications != 20 {
		t.Errorf("limits: %+v", got.Limits)
	}
	if len(got.Queues) != 2 {
		t.Errorf("child queues: %d", len(got.Queues))
	}
	if got.ChildTemplate == nil || got.ChildTemplate.MaxApplications != 50 {
		t.Errorf("childTemplate: %+v", got.ChildTemplate)
	}
	if got.ChildTemplate.Resources == nil || got.ChildTemplate.Resources.Max["memory"] != "4G" {
		t.Errorf("childTemplate.resources: %+v", got.ChildTemplate)
	}
}

func TestToYuniKornQueueConfig_NilOptionalsAreOmitted(t *testing.T) {
	got := ToYuniKornQueueConfig(QueueConfig{Name: "team-a"})
	if got.Resources != nil {
		t.Error("Resources should be nil")
	}
	if got.ChildTemplate != nil {
		t.Error("ChildTemplate should be nil")
	}
	if got.Properties != nil {
		t.Errorf("Properties should be nil-equivalent, got %+v", got.Properties)
	}
}

func TestToYuniKornQueueConfig_RecursesIntoChildren(t *testing.T) {
	in := QueueConfig{
		Name: "team-a", Parent: boolPtr(true),
		Queues: []QueueConfig{{
			Name: "dev", Parent: boolPtr(true),
			Queues: []QueueConfig{{Name: "leaf"}},
		}},
	}
	got := ToYuniKornQueueConfig(in)
	if got.Queues[0].Queues[0].Name != "leaf" {
		t.Errorf("recursion broken: %+v", got)
	}
}

// =============================================================================
// L1QueuesFromCRs
// =============================================================================

func TestL1QueuesFromCRs_PreservesOrderAndConverts(t *testing.T) {
	crs := []Queue{
		{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}, Spec: QueueSpec{Queue: QueueConfig{Name: "team-a"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "team-b"}, Spec: QueueSpec{Queue: QueueConfig{Name: "team-b"}}},
	}
	got := L1QueuesFromCRs(crs)
	if len(got) != 2 {
		t.Fatalf("len: got %d want 2", len(got))
	}
	if got[0].Name != "team-a" || got[1].Name != "team-b" {
		t.Errorf("order: %v %v", got[0].Name, got[1].Name)
	}
}

// =============================================================================
// SortByNamespaceName
// =============================================================================

func TestSortByNamespaceName(t *testing.T) {
	in := []Queue{
		{ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "zns"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "zeta", Namespace: "ans"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "ans"}},
	}
	out := SortByNamespaceName(in)

	want := []string{"ans/alpha", "ans/zeta", "zns/alpha"}
	got := []string{
		out[0].Namespace + "/" + out[0].Name,
		out[1].Namespace + "/" + out[1].Name,
		out[2].Namespace + "/" + out[2].Name,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order: got %v want %v", got, want)
	}

	if in[0].Namespace != "zns" {
		t.Errorf("input must not be mutated: %+v", in)
	}
}
