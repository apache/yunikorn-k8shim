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

// Package queueconfig builds and validates the YuniKorn scheduler
// configuration that the queue-operator writes to the YuniKorn ConfigMap.
//
// It is the single source of truth for translating Queue CRs into the YuniKorn
// on-disk format and for validating the result against the exact rules used by
// the YuniKorn scheduler itself (via github.com/apache/yunikorn-core).
package queueconfig

import (
	"fmt"

	"go.yaml.in/yaml/v3"
)

// RootQueueName is the name of the implicit root queue under which every
// Queue CR is grafted as an L1 child. It must remain "root" because YuniKorn
// hard-codes that name.
const RootQueueName = "root"

// SchedulerConfig is the top-level YuniKorn scheduler configuration.
//
// The yaml tags below intentionally use lowercase (no separator) to match the
// on-disk format that YuniKorn parses with strict-field decoding.
type SchedulerConfig struct {
	Partitions []PartitionConfig `yaml:"partitions"`
}

// PartitionConfig is a single YuniKorn partition. queue-operator emits exactly
// one partition (typically "default").
type PartitionConfig struct {
	Name           string          `yaml:"name"`
	PlacementRules []PlacementRule `yaml:"placementrules,omitempty"`
	Queues         []QueueConfig   `yaml:"queues"`
}

// PlacementRule maps to YuniKorn's PlacementRule type.
type PlacementRule struct {
	Name   string           `yaml:"name"`
	Value  string           `yaml:"value,omitempty"`
	Create bool             `yaml:"create"`
	Parent *PlacementRule   `yaml:"parent,omitempty"`
	Filter *PlacementFilter `yaml:"filter,omitempty"`
}

// PlacementFilter maps to YuniKorn's Filter type for placement rules.
type PlacementFilter struct {
	Type   string   `yaml:"type,omitempty"`
	Users  []string `yaml:"users,omitempty"`
	Groups []string `yaml:"groups,omitempty"`
}

// QueueConfig mirrors github.com/apache/yunikorn-core/pkg/common/configs.QueueConfig.
type QueueConfig struct {
	Name            string            `yaml:"name"`
	Parent          bool              `yaml:"parent"`
	Resources       *Resources        `yaml:"resources,omitempty"`
	MaxApplications uint64            `yaml:"maxapplications,omitempty"`
	Properties      map[string]string `yaml:"properties,omitempty"`
	AdminACL        string            `yaml:"adminacl,omitempty"`
	SubmitACL       string            `yaml:"submitacl,omitempty"`
	Queues          []QueueConfig     `yaml:"queues,omitempty"`
	Limits          []Limit           `yaml:"limits,omitempty"`
	ChildTemplate   *ChildTemplate    `yaml:"childtemplate,omitempty"`
}

// Resources mirrors YuniKorn's Resources struct.
type Resources struct {
	Guaranteed map[string]string `yaml:"guaranteed,omitempty"`
	Max        map[string]string `yaml:"max,omitempty"`
}

// Limit mirrors YuniKorn's Limit struct.
type Limit struct {
	Limit           string            `yaml:"limit"`
	Users           []string          `yaml:"users,omitempty"`
	Groups          []string          `yaml:"groups,omitempty"`
	MaxResources    map[string]string `yaml:"maxresources,omitempty"`
	MaxApplications uint64            `yaml:"maxapplications,omitempty"`
}

// ChildTemplate mirrors YuniKorn's ChildTemplate struct.
type ChildTemplate struct {
	MaxApplications uint64            `yaml:"maxapplications,omitempty"`
	Properties      map[string]string `yaml:"properties,omitempty"`
	Resources       *Resources        `yaml:"resources,omitempty"`
}

// BuildOptions controls how the merged scheduler config is assembled.
type BuildOptions struct {
	// PartitionName is the YuniKorn partition the queues are grafted into.
	PartitionName string
	// PlacementRules, if non-empty, are placed at the partition level.
	PlacementRules []PlacementRule
}

// BuildMerged grafts the supplied L1 queues under the implicit root queue and
// wraps them in a single-partition SchedulerConfig.
//
// The caller is responsible for ordering the children deterministically; the
// reconciler sorts by creationTimestamp (oldest wins on duplicates), the
// webhook sorts by namespace/name. We do NOT silently dedupe — duplicate-name
// detection is the responsibility of the validator (YuniKorn's checkQueues
// surfaces it as a validation error). Hiding a duplicate here would mask a
// misconfiguration.
//
// Structural invariants this function deliberately hardcodes
// -----------------------------------------------------------
// Several YuniKorn validation rules become unreachable through any Queue CR
// because we fix the shape of the parts of the config that could trip them.
// Anyone changing this function should preserve these invariants, or
// proactively wire fresh tests + an admission-time field for the relaxed
// rule before relaxing it:
//
//   - Root has no `Resources` set → V15 (root must not have resource limits)
//     can never fire from a CR. Exposing root resources would re-enable it.
//   - Root has no `Limits` set → V17 (partition.limits ↔ root.limits
//     equivalence) is vacuous, AND the limit-hierarchy walk
//     (checkLimitResource / checkLimitMaxApplications, rules V47–V56)
//     starts with an empty parent map. Sibling L1 queues therefore cannot
//     constrain each other's per-user/group caps — they're independent
//     budget pools by construction. The inter-CR tests in validator_test.go
//     pin this behaviour.
//   - We emit exactly one Partition and never populate Partition.Limits or
//     Partition.NodeSortPolicy → V3 (duplicate partition name),
//     V13/V14/V16 (root structure), V57/V58 (node-sorting policy) are all
//     unreachable from a CR. They remain proven via unit tests against the
//     wrapped validator, but no Queue CR can express them today.
func BuildMerged(l1Children []QueueConfig, opts BuildOptions) *SchedulerConfig {
	root := QueueConfig{
		Name:      RootQueueName,
		Parent:    true,
		SubmitACL: "*",
		Queues:    l1Children,
	}

	return &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name:           opts.PartitionName,
			PlacementRules: opts.PlacementRules,
			Queues:         []QueueConfig{root},
		}},
	}
}

// MarshalYAML serialises a SchedulerConfig to the canonical YuniKorn YAML
// format that the YuniKorn scheduler reads.
func MarshalYAML(cfg *SchedulerConfig) ([]byte, error) {
	if cfg == nil {
		return nil, fmt.Errorf("scheduler config is nil")
	}
	return yaml.Marshal(cfg)
}
