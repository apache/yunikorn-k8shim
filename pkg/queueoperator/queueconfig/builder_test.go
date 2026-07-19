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

package queueconfig

import (
	"strings"
	"testing"
)

// =============================================================================
// BuildMerged
// =============================================================================

func TestBuildMerged_EmptyInputProducesValidEmptyHierarchy(t *testing.T) {
	cfg := BuildMerged(nil, BuildOptions{PartitionName: "default"})
	if len(cfg.Partitions) != 1 || cfg.Partitions[0].Name != "default" {
		t.Fatalf("partition wrong: %+v", cfg)
	}
	root := cfg.Partitions[0].Queues[0]
	if root.Name != "root" || !root.Parent || root.SubmitACL != "*" {
		t.Errorf("root not wired correctly: %+v", root)
	}
	if len(root.Queues) != 0 {
		t.Errorf("expected zero L1 queues, got %d", len(root.Queues))
	}
	expectValid(t, cfg)
}

func TestBuildMerged_GraftsEachChildAsL1(t *testing.T) {
	cfg := BuildMerged(
		[]QueueConfig{{Name: "team-a"}, {Name: "team-b"}},
		BuildOptions{PartitionName: "default"},
	)
	root := cfg.Partitions[0].Queues[0]
	if len(root.Queues) != 2 {
		t.Fatalf("expected 2 L1 queues, got %d", len(root.Queues))
	}
	if root.Queues[0].Name != "team-a" || root.Queues[1].Name != "team-b" {
		t.Errorf("ordering broken: %v %v", root.Queues[0].Name, root.Queues[1].Name)
	}
	expectValid(t, cfg)
}

func TestBuildMerged_PreservesCallerOrder(t *testing.T) {
	cfg := BuildMerged(
		[]QueueConfig{{Name: "z-team"}, {Name: "a-team"}},
		BuildOptions{PartitionName: "default"},
	)
	if cfg.Partitions[0].Queues[0].Queues[0].Name != "z-team" {
		t.Errorf("caller order not preserved")
	}
}

func TestBuildMerged_PassesPlacementRulesThrough(t *testing.T) {
	cfg := BuildMerged(nil, BuildOptions{
		PartitionName:  "default",
		PlacementRules: []PlacementRule{{Name: "tag", Value: "namespace", Create: true}},
	})
	if len(cfg.Partitions[0].PlacementRules) != 1 {
		t.Errorf("placement rules dropped: %+v", cfg.Partitions[0].PlacementRules)
	}
	expectValid(t, cfg)
}

func TestBuildMerged_DuplicatesSurfaceAsValidationError(t *testing.T) {
	// BuildMerged must NOT silently dedupe — duplicates must surface as a
	// validation error so the operator can mark the offender Degraded.
	cfg := BuildMerged(
		[]QueueConfig{{Name: "team-a"}, {Name: "team-a"}},
		BuildOptions{PartitionName: "default"},
	)
	_, _, err := Validate(cfg)
	if err == nil {
		t.Fatal("expected duplicate to be rejected by validator")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate error, got: %v", err)
	}
}

// =============================================================================
// MarshalYAML
// =============================================================================

func TestMarshalYAML_Nil(t *testing.T) {
	_, err := MarshalYAML(nil)
	if err == nil {
		t.Error("expected error on nil")
	}
}

func TestMarshalYAML_RoundTripParseable(t *testing.T) {
	yamlBytes, err := MarshalYAML(BuildMerged(
		[]QueueConfig{{Name: "team-a"}},
		BuildOptions{PartitionName: "default"},
	))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(yamlBytes), "team-a") {
		t.Errorf("yaml missing queue: %s", yamlBytes)
	}
	if _, err := ValidateBytes(yamlBytes); err != nil {
		t.Errorf("round-trip validation failed: %v", err)
	}
}
