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
	"testing"
)

func TestLoadOptionsFromEnv_DefaultsWhenUnset(t *testing.T) {
	t.Setenv(PartitionNameEnvVar, "")
	t.Setenv(PlacementRulesEnvVar, "")

	opts, err := LoadOptionsFromEnv()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if opts.PartitionName != DefaultPartitionName {
		t.Errorf("partition: got %q want %q", opts.PartitionName, DefaultPartitionName)
	}
	if opts.PlacementRules != nil {
		t.Errorf("placement rules should be nil when env unset, got %+v", opts.PlacementRules)
	}
}

func TestLoadOptionsFromEnv_PartitionOverride(t *testing.T) {
	t.Setenv(PartitionNameEnvVar, "my-partition")
	t.Setenv(PlacementRulesEnvVar, "")

	opts, err := LoadOptionsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if opts.PartitionName != "my-partition" {
		t.Errorf("partition: %q", opts.PartitionName)
	}
}

func TestLoadOptionsFromEnv_ValidPlacementRules(t *testing.T) {
	t.Setenv(PartitionNameEnvVar, "")
	t.Setenv(PlacementRulesEnvVar, `
- name: tag
  value: namespace
  create: true
- name: fixed
  value: root.fallback
  create: false
`)
	opts, err := LoadOptionsFromEnv()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(opts.PlacementRules) != 2 {
		t.Fatalf("want 2 rules, got %d", len(opts.PlacementRules))
	}
	if opts.PlacementRules[0].Name != "tag" || !opts.PlacementRules[0].Create {
		t.Errorf("rule[0]: %+v", opts.PlacementRules[0])
	}
}

func TestLoadOptionsFromEnv_MalformedPlacementRulesReturnsErrButValidPartition(t *testing.T) {
	t.Setenv(PartitionNameEnvVar, "p")
	t.Setenv(PlacementRulesEnvVar, "::not-yaml::")

	opts, err := LoadOptionsFromEnv()
	if err == nil {
		t.Fatal("expected parse error")
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
