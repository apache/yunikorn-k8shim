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
	"errors"
	"testing"
)

func clearOptionalSettings(t *testing.T) {
	t.Helper()
	for _, name := range []string{PartitionNameEnvVar, PlacementRulesEnvVar, NodeSortPolicyEnvVar, RootPropertiesEnvVar} {
		t.Setenv(name, "")
	}
}

func TestLoadOptionsFromEnv_DefaultsWhenUnset(t *testing.T) {
	clearOptionalSettings(t)

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if opts.PartitionName != DefaultPartitionName {
		t.Errorf("partition: got %q want %q", opts.PartitionName, DefaultPartitionName)
	}
	if opts.PlacementRules != nil {
		t.Errorf("placement rules should be nil when env unset, got %+v", opts.PlacementRules)
	}
}

func TestLoadOptionsFromEnv_PartitionOverride(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(PartitionNameEnvVar, "my-partition")

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if opts.PartitionName != "my-partition" {
		t.Errorf("partition: %q", opts.PartitionName)
	}
}

func TestLoadOptionsFromEnv_ValidPlacementRules(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(PlacementRulesEnvVar, `
- name: tag
  value: namespace
  create: true
- name: fixed
  value: root.fallback
  create: false
`)
	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if len(opts.PlacementRules) != 2 {
		t.Fatalf("want 2 rules, got %d", len(opts.PlacementRules))
	}
	if opts.PlacementRules[0].Name != "tag" || !opts.PlacementRules[0].Create {
		t.Errorf("rule[0]: %+v", opts.PlacementRules[0])
	}
}

func TestLoadOptionsFromEnv_MalformedPlacementRulesReturnsErrButValidPartition(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(PartitionNameEnvVar, "p")
	t.Setenv(PlacementRulesEnvVar, "::not-yaml::")

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 1 {
		t.Fatal("expected parse error")
	}
	if errs[0].Setting != PlacementRulesEnvVar {
		t.Errorf("error setting = %q, want %q", errs[0].Setting, PlacementRulesEnvVar)
	}
	var optionErr OptionError
	if !errors.As(errs[0], &optionErr) {
		t.Errorf("error is not an OptionError: %T", errs[0])
	}
	// Even on parse failure the partition name must remain usable so callers
	// can softfail and continue.
	if opts.PartitionName != "p" {
		t.Errorf("partition should still be set on parse error: %q", opts.PartitionName)
	}
	if opts.PlacementRules != nil {
		t.Errorf("placement rules should be nil on parse error: %+v", opts.PlacementRules)
	}
}

func TestLoadOptionsFromEnv_NodeSortPolicyName(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(NodeSortPolicyEnvVar, "binpacking")

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if opts.NodeSortPolicy == nil || opts.NodeSortPolicy.Type != "binpacking" {
		t.Fatalf("node sort policy = %+v, want binpacking", opts.NodeSortPolicy)
	}
}

func TestLoadOptionsFromEnv_NodeSortPolicyObject(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(NodeSortPolicyEnvVar, `type: fair
resourceweights:
  memory: 2
  vcore: 0.5`)

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if opts.NodeSortPolicy == nil || opts.NodeSortPolicy.Type != "fair" {
		t.Fatalf("node sort policy = %+v, want fair", opts.NodeSortPolicy)
	}
	if got := opts.NodeSortPolicy.ResourceWeights["memory"]; got != 2 {
		t.Errorf("memory weight = %v, want 2", got)
	}
	if got := opts.NodeSortPolicy.ResourceWeights["vcore"]; got != 0.5 {
		t.Errorf("vcore weight = %v, want 0.5", got)
	}
}

func TestLoadOptionsFromEnv_RejectsUnknownStructuredFields(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(PlacementRulesEnvVar, "- name: tag\n  unexpected: true")
	t.Setenv(NodeSortPolicyEnvVar, "type: fair\nresourceweight: {}")

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 2 {
		t.Fatalf("errors = %v, want 2", errs)
	}
	if errs[0].Setting != PlacementRulesEnvVar || errs[1].Setting != NodeSortPolicyEnvVar {
		t.Errorf("error settings = %q, %q", errs[0].Setting, errs[1].Setting)
	}
	if opts.PlacementRules != nil || opts.NodeSortPolicy != nil {
		t.Errorf("settings with unknown fields should be dropped: rules=%v policy=%v", opts.PlacementRules, opts.NodeSortPolicy)
	}
}

func TestLoadOptionsFromEnv_RejectsMultipleYAMLDocuments(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(NodeSortPolicyEnvVar, "type: fair\n---\ntype: binpacking")

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 1 || errs[0].Setting != NodeSortPolicyEnvVar {
		t.Fatalf("errors = %v, want one %s error", errs, NodeSortPolicyEnvVar)
	}
	if opts.NodeSortPolicy != nil {
		t.Errorf("multi-document setting should be dropped: %+v", opts.NodeSortPolicy)
	}
}

func TestLoadOptionsFromEnv_RootProperties(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(RootPropertiesEnvVar, `application.sort.policy: fair
preemption.delay: 30s`)

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if opts.RootProperties["application.sort.policy"] != "fair" || opts.RootProperties["preemption.delay"] != "30s" {
		t.Errorf("root properties = %v", opts.RootProperties)
	}
}

func TestLoadOptionsFromEnv_ParsesSettingsIndependently(t *testing.T) {
	clearOptionalSettings(t)
	t.Setenv(PlacementRulesEnvVar, "not: [valid")
	t.Setenv(NodeSortPolicyEnvVar, "binpacking")
	t.Setenv(RootPropertiesEnvVar, "not: [valid")

	opts, errs := LoadOptionsFromEnv()
	if len(errs) != 2 {
		t.Fatalf("errors = %v, want 2", errs)
	}
	if errs[0].Setting != PlacementRulesEnvVar || errs[1].Setting != RootPropertiesEnvVar {
		t.Errorf("error settings = %q, %q", errs[0].Setting, errs[1].Setting)
	}
	if opts.NodeSortPolicy == nil || opts.NodeSortPolicy.Type != "binpacking" {
		t.Errorf("valid sibling setting was discarded: %+v", opts.NodeSortPolicy)
	}
	if opts.PlacementRules != nil || opts.RootProperties != nil {
		t.Errorf("malformed settings should be nil: rules=%v properties=%v", opts.PlacementRules, opts.RootProperties)
	}
}
