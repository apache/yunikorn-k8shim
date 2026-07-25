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

// This file is the line-coverage test suite for the YuniKorn config validator
// wrapper. There is one test case for every error path in
// yunikorn-core/pkg/common/configs/configvalidator.go, plus the happy paths.
//
// When YuniKorn adds a new validation rule we should also add a case here to
// prove the wrapper surfaces it (in addition to the upstream tests, which we
// already get for free by virtue of importing the package).

package queueconfig

import (
	"errors"
	"strings"
	"testing"
)

// helper: build a SchedulerConfig with a single partition and the supplied
// children grafted under root.
func cfg(rootChildren ...QueueConfig) *SchedulerConfig {
	return &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			Queues: []QueueConfig{{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    rootChildren,
			}},
		}},
	}
}

// helper: build a SchedulerConfig allowing the caller to set the full root
// queue (e.g. with resources, which must be rejected).
func cfgWithRoot(root QueueConfig) *SchedulerConfig {
	return &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name:   "default",
			Queues: []QueueConfig{root},
		}},
	}
}

// expectInvalid asserts Validate returned a *ValidationError whose underlying
// message contains substr.
func expectInvalid(t *testing.T, cfg *SchedulerConfig, stage, substr string) {
	t.Helper()
	_, _, err := Validate(cfg)
	if err == nil {
		t.Fatalf("expected validation error containing %q, got nil", substr)
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected errors.Is(ErrInvalidConfig)=true, got %T", err)
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if stage != "" && ve.Stage != stage {
		t.Errorf("expected stage=%q, got %q", stage, ve.Stage)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Errorf("expected error to contain %q, got: %v", substr, err)
	}
}

func expectValid(t *testing.T, cfg *SchedulerConfig) {
	t.Helper()
	_, parsed, err := Validate(cfg)
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
	if parsed == nil {
		t.Fatal("expected non-nil parsed config on success")
	}
}

// =============================================================================
// happy paths
// =============================================================================

func TestValidate_HappyPath_EmptyHierarchy(t *testing.T) {
	expectValid(t, cfg())
}

func TestValidate_HappyPath_SingleLeaf(t *testing.T) {
	expectValid(t, cfg(QueueConfig{Name: "team-a"}))
}

func TestValidate_HappyPath_NestedHierarchy(t *testing.T) {
	expectValid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Queues: []QueueConfig{
			{Name: "dev"}, {Name: "stg"}, {Name: "prod"},
		},
	}))
}

func TestValidate_HappyPath_WithResources(t *testing.T) {
	expectValid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Resources: &Resources{
			Guaranteed: map[string]string{"memory": "4G", "vcore": "2"},
			Max:        map[string]string{"memory": "8G", "vcore": "4"},
		},
		Queues: []QueueConfig{{
			Name: "dev",
			Resources: &Resources{
				Guaranteed: map[string]string{"memory": "1G", "vcore": "1"},
				Max:        map[string]string{"memory": "4G", "vcore": "2"},
			},
		}},
	}))
}

// =============================================================================
// configs.SchedulerConfig top-level: nil and empty
// =============================================================================

func TestValidate_NilConfig(t *testing.T) {
	_, _, err := Validate(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Stage != "render" {
		t.Errorf("expected render-stage ValidationError, got %v", err)
	}
}

// =============================================================================
// checkQueuesStructure
// =============================================================================

func TestValidate_RootMustNotHaveResourceLimits(t *testing.T) {
	expectInvalid(t, cfgWithRoot(QueueConfig{
		Name: "root", Parent: true,
		Resources: &Resources{Max: map[string]string{"memory": "100G"}},
	}), "validate", "root queue must not have resource limits set")
}

// When the partition has multiple top-level queues (none named root), YuniKorn
// auto-inserts a root and re-grafts. That's a happy path on the parser side,
// but the inserted root has no resource limits to begin with so it should pass.
func TestValidate_AutoInsertsRootWhenMultipleTopLevelQueues(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name:   "default",
			Queues: []QueueConfig{{Name: "team-a"}, {Name: "team-b"}},
		}},
	}
	expectValid(t, cfg)
}

func TestValidate_AutoInsertsRootWhenSingleNonRoot(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name:   "default",
			Queues: []QueueConfig{{Name: "team-a"}},
		}},
	}
	expectValid(t, cfg)
}

// =============================================================================
// checkQueues / IsQueueNameValid / QueueNameRegExp
// =============================================================================

func TestValidate_RejectsInvalidQueueName_TooLong(t *testing.T) {
	long := strings.Repeat("a", 65)
	expectInvalid(t, cfg(QueueConfig{Name: long}), "validate", "invalid queue name")
}

func TestValidate_RejectsInvalidQueueName_DotInName(t *testing.T) {
	// dots are illegal in queue names — the YuniKorn separator is "."
	expectInvalid(t, cfg(QueueConfig{Name: "team.a"}), "validate", "invalid queue name")
}

func TestValidate_RejectsInvalidQueueName_Space(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{Name: "team a"}), "validate", "invalid queue name")
}

func TestValidate_AcceptsValidQueueNameWithSpecialChars(t *testing.T) {
	// YuniKorn QueueNameRegExp allows _ : # / @ -
	expectValid(t, cfg(QueueConfig{Name: "team_a"}))
	expectValid(t, cfg(QueueConfig{Name: "team-a"}))
	expectValid(t, cfg(QueueConfig{Name: "team@svc"}))
}

func TestValidate_RejectsDuplicateChildNames(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Queues: []QueueConfig{{Name: "dev"}, {Name: "dev"}},
	}), "validate", "duplicate child name")
}

// Yunikorn's checkQueues lower-cases names for the dup map: "Dev" and "dev"
// must collide.
func TestValidate_RejectsDuplicateChildNamesCaseInsensitive(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Queues: []QueueConfig{{Name: "dev"}, {Name: "Dev"}},
	}), "validate", "duplicate child name")
}

// =============================================================================
// checkACL
// =============================================================================

func TestValidate_RejectsACLWithMoreThanTwoFields(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name:     "team-a",
		AdminACL: "userA userB userC userD",
	}), "validate", "multiple spaces found in ACL")
}

func TestValidate_AcceptsWildcardACL(t *testing.T) {
	expectValid(t, cfg(QueueConfig{
		Name:      "team-a",
		AdminACL:  "*",
		SubmitACL: "*",
	}))
}

func TestValidate_AcceptsEmptyACL(t *testing.T) {
	expectValid(t, cfg(QueueConfig{Name: "team-a"}))
}

// =============================================================================
// checkResourceConfig + checkQueueResource
// =============================================================================

func TestValidate_GuaranteedExceedsMax(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Resources: &Resources{
			Guaranteed: map[string]string{"memory": "10G"},
			Max:        map[string]string{"memory": "1G"},
		},
	}), "validate", "guaranteed resource")
}

func TestValidate_ChildMaxExceedsParentMax(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Resources: &Resources{Max: map[string]string{"memory": "4G"}},
		Queues: []QueueConfig{{
			Name:      "dev",
			Resources: &Resources{Max: map[string]string{"memory": "8G"}},
		}},
	}), "validate", "max resource of parent")
}

func TestValidate_SumOfChildGuaranteedExceedsParentGuaranteed(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Resources: &Resources{
			Guaranteed: map[string]string{"memory": "4G"},
			Max:        map[string]string{"memory": "100G"},
		},
		Queues: []QueueConfig{
			{Name: "dev", Resources: &Resources{Guaranteed: map[string]string{"memory": "3G"}}},
			{Name: "stg", Resources: &Resources{Guaranteed: map[string]string{"memory": "3G"}}},
		},
	}), "validate", "guaranteed resource of parent")
}

func TestValidate_SumOfChildGuaranteedExceedsParentMax(t *testing.T) {
	// Parent has no guaranteed but a max smaller than sum of children's guaranteed.
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Resources: &Resources{Max: map[string]string{"memory": "4G"}},
		Queues: []QueueConfig{
			{Name: "dev", Resources: &Resources{Guaranteed: map[string]string{"memory": "3G"}}},
			{Name: "stg", Resources: &Resources{Guaranteed: map[string]string{"memory": "3G"}}},
		},
	}), "validate", "max resource")
}

func TestValidate_RejectsInvalidResourceQuantity(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Resources: &Resources{
			Max: map[string]string{"memory": "not-a-number"},
		},
	}), "validate", "")
}

// =============================================================================
// checkLimits / checkLimit
// =============================================================================

func TestValidate_LimitWithoutUsersOrGroups(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name:   "team-a",
		Limits: []Limit{{Limit: "no-target", MaxApplications: 10}},
	}), "validate", "empty user and group lists")
}

func TestValidate_LimitWithInvalidUserName(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{{
			Limit: "bad-user", Users: []string{"not a valid name"},
			MaxApplications: 10,
		}},
	}), "validate", "invalid limit user name")
}

func TestValidate_LimitWithInvalidGroupName(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{{
			Limit: "bad-group", Groups: []string{"bad@group#name"},
			MaxApplications: 10,
		}},
	}), "validate", "invalid limit group name")
}

func TestValidate_LimitDuplicateUserName(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{
			{Limit: "u1", Users: []string{"alice"}, MaxApplications: 10},
			{Limit: "u2", Users: []string{"alice"}, MaxApplications: 10},
		},
	}), "validate", "duplicated user name")
}

// V28: duplicate group name across the same queue's Limits list.
// Symmetric to TestValidate_LimitDuplicateUserName but for groups.
func TestValidate_LimitDuplicateGroupName(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{
			{Limit: "g1", Groups: []string{"infra"}, MaxApplications: 10},
			{Limit: "g2", Groups: []string{"infra"}, MaxApplications: 10},
		},
	}), "validate", "duplicated group name")
}

// V29: wildcard group "*" must be the last entry inside a single Limit's
// Groups list. A specific group following "*" is rejected.
// Symmetric to TestValidate_LimitWildcardUserNotLast.
// We trigger this within a SINGLE limit's Groups list (rather than across
// two limits) because a leading wildcard-only limit would hit V30
// ("only one group is the wildcard") and short-circuit before V29.
func TestValidate_LimitWildcardGroupNotLast(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{{
			Limit: "g-mixed", Groups: []string{"*", "infra"},
			MaxApplications: 10,
		}},
	}), "validate", "should not set no wildcard group")
}

func TestValidate_LimitWildcardUserNotLast(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{
			{Limit: "u-wild", Users: []string{"*"}, MaxApplications: 10},
			{Limit: "u-named", Users: []string{"alice"}, MaxApplications: 10},
		},
	}), "validate", "should not set no wildcard user")
}

func TestValidate_LimitGroupWildcardOnly(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{{
			Limit: "g-wild", Groups: []string{"*"}, MaxApplications: 10,
		}},
	}), "validate", "should not specify only one group limit")
}

func TestValidate_LimitMaxResourcesZero(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{{
			Limit: "zero-res", Users: []string{"alice"},
			MaxResources: map[string]string{"memory": "0"},
		}},
	}), "validate", "MaxResources should be greater than zero")
}

func TestValidate_LimitWithNoResourceAndNoMaxApps(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Limits: []Limit{{
			Limit: "empty", Users: []string{"alice"},
		}},
	}), "validate", "all resource limits are null")
}

func TestValidate_LimitMaxAppsExceedsQueueMaxApps(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name:            "team-a",
		MaxApplications: 5,
		Limits: []Limit{{
			Limit: "u", Users: []string{"alice"}, MaxApplications: 100,
		}},
	}), "validate", "exceed current the queue MaxApplications")
}

func TestValidate_LimitMaxResourceExceedsQueueMaxResource(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a",
		Resources: &Resources{
			Max: map[string]string{"memory": "1G"},
		},
		Limits: []Limit{{
			Limit: "u", Users: []string{"alice"},
			MaxResources: map[string]string{"memory": "10G"},
		}},
	}), "validate", "exeecd current the queue MaxResources")
}

// =============================================================================
// checkQueueMaxApplications
// =============================================================================

func TestValidate_ParentMaxAppsLowerThanChild(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true, MaxApplications: 5,
		Queues: []QueueConfig{{Name: "dev", MaxApplications: 100}},
	}), "validate", "parent maxApplications must be larger")
}

func TestValidate_ParentMaxAppsDefinedButChildZero(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true, MaxApplications: 100,
		Queues: []QueueConfig{{Name: "dev"}},
	}), "validate", "maxApplications is either undefined or zero")
}

// =============================================================================
// checkLimitResource (parent vs child user/group hierarchy)
// =============================================================================

func TestValidate_ChildUserLimitExceedsParent(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{{
			Limit: "u-parent", Users: []string{"alice"},
			MaxResources: map[string]string{"memory": "2G"},
		}},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "u-child", Users: []string{"alice"},
				MaxResources: map[string]string{"memory": "10G"},
			}},
		}},
	}), "validate", "greater than immediate or ancestor parent maximum resource")
}

func TestValidate_ChildGroupLimitExceedsParent(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{{
			Limit: "g-parent", Groups: []string{"infra"},
			MaxResources: map[string]string{"memory": "2G"},
		}, {Limit: "g-anchor", Groups: []string{"other"}, MaxResources: map[string]string{"memory": "1G"}}},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "g-child", Groups: []string{"infra"},
				MaxResources: map[string]string{"memory": "10G"},
			}, {Limit: "g-anchor2", Groups: []string{"other"}, MaxResources: map[string]string{"memory": "1G"}}},
		}},
	}), "validate", "greater than immediate or ancestor parent maximum resource")
}

// V48: a child queue limit on a specific user must not exceed an ancestor's
// wildcard-user limit. There is no direct user-limit on "alice" at the
// parent, but the parent declares Users: ["*"] with a cap of 2G — that cap
// must apply transitively to any specific user in the child.
func TestValidate_ChildUserLimitExceedsWildcardAncestor(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{{
			Limit: "u-wild-parent", Users: []string{"*"},
			MaxResources: map[string]string{"memory": "2G"},
		}},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "u-child", Users: []string{"alice"},
				MaxResources: map[string]string{"memory": "10G"},
			}},
		}},
	}), "validate", "greater than wildcard maximum resource")
}

// V50: a child queue limit on a specific group must not exceed an ancestor's
// wildcard-group limit. We need an anchor non-wildcard group at the parent
// level to avoid tripping V30 ("only one group is the wildcard"); the child
// only needs its violating limit.
func TestValidate_ChildGroupLimitExceedsWildcardAncestor(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{
			{Limit: "g-anchor-parent", Groups: []string{"other"},
				MaxResources: map[string]string{"memory": "1G"}},
			{Limit: "g-wild-parent", Groups: []string{"*"},
				MaxResources: map[string]string{"memory": "2G"}},
		},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "g-child", Groups: []string{"infra"},
				MaxResources: map[string]string{"memory": "10G"},
			}},
		}},
	}), "validate", "greater than wildcard maximum resource")
}

// =============================================================================
// checkLimitMaxApplications (parent vs child user/group hierarchy)
// =============================================================================

func TestValidate_ChildUserMaxAppsExceedsParent(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{{
			Limit: "u-parent", Users: []string{"alice"}, MaxApplications: 5,
		}},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "u-child", Users: []string{"alice"}, MaxApplications: 50,
			}},
		}},
	}), "validate", "greater than immediate or ancestor parent max applications")
}

// V54: a child queue limit's user MaxApplications must not exceed an ancestor's
// wildcard-user MaxApplications. Symmetric to V53 but with the parent's cap
// declared on Users: ["*"] rather than on a named user.
func TestValidate_ChildUserMaxAppsExceedsWildcardAncestor(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{{
			Limit: "u-wild-parent", Users: []string{"*"}, MaxApplications: 5,
		}},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "u-child", Users: []string{"alice"}, MaxApplications: 50,
			}},
		}},
	}), "validate", "greater than wildcard max applications")
}

// V55: a child queue limit's group MaxApplications must not exceed the same
// group's MaxApplications at the parent. The group-MaxApps counterpart of
// V49 (which checks group MaxResources).
func TestValidate_ChildGroupMaxAppsExceedsParent(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{{
			Limit: "g-parent", Groups: []string{"infra"}, MaxApplications: 5,
		}},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "g-child", Groups: []string{"infra"}, MaxApplications: 50,
			}},
		}},
	}), "validate", "greater than immediate or ancestor parent max applications")
}

// V56: a child queue limit on a specific group must not exceed an ancestor's
// wildcard-group MaxApplications. Mirrors V50 in the MaxApplications plane.
// As with V50, the parent needs an anchor non-wildcard group limit so the
// wildcard limit doesn't get rejected by V30 in isolation.
func TestValidate_ChildGroupMaxAppsExceedsWildcardAncestor(t *testing.T) {
	expectInvalid(t, cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Limits: []Limit{
			{Limit: "g-anchor-parent", Groups: []string{"other"}, MaxApplications: 100},
			{Limit: "g-wild-parent", Groups: []string{"*"}, MaxApplications: 5},
		},
		Queues: []QueueConfig{{
			Name: "dev",
			Limits: []Limit{{
				Limit: "g-child", Groups: []string{"infra"}, MaxApplications: 50,
			}},
		}},
	}), "validate", "greater than wildcard max applications")
}

// =============================================================================
// checkPlacementRules (validated via top-level partition placement_rules)
// =============================================================================

func TestValidate_PlacementRule_InvalidName(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			PlacementRules: []PlacementRule{
				{Name: "9bad-rule-name"},
			},
			Queues: []QueueConfig{{Name: "root", Parent: true, SubmitACL: "*"}},
		}},
	}
	expectInvalid(t, cfg, "validate", "invalid rule name")
}

func TestValidate_PlacementRule_InvalidFilterType(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			PlacementRules: []PlacementRule{
				{Name: "fixed", Value: "team-a", Create: true,
					Filter: &PlacementFilter{Type: "bogus"}},
			},
			Queues: []QueueConfig{{Name: "root", Parent: true, SubmitACL: "*",
				Queues: []QueueConfig{{Name: "team-a"}}}},
		}},
	}
	expectInvalid(t, cfg, "validate", "invalid rule filter type")
}

func TestValidate_PlacementRule_FixedRuleReferencesNonLeaf(t *testing.T) {
	// fixed rule pointing at a queue marked as parent (not a leaf) should fail.
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			PlacementRules: []PlacementRule{
				{Name: "fixed", Value: "team-a", Create: false},
			},
			Queues: []QueueConfig{{Name: "root", Parent: true, SubmitACL: "*",
				Queues: []QueueConfig{{Name: "team-a", Parent: true,
					Queues: []QueueConfig{{Name: "dev"}}}}}},
		}},
	}
	expectInvalid(t, cfg, "validate", "not a leaf")
}

func TestValidate_PlacementRule_FixedRuleReferencesNonExistingQueue(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			PlacementRules: []PlacementRule{
				{Name: "fixed", Value: "root.does-not-exist", Create: false},
			},
			Queues: []QueueConfig{{Name: "root", Parent: true, SubmitACL: "*"}},
		}},
	}
	expectInvalid(t, cfg, "validate", "non-existing queues")
}

func TestValidate_PlacementRule_HappyPath(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			PlacementRules: []PlacementRule{
				{Name: "tag", Value: "namespace", Create: true},
			},
			Queues: []QueueConfig{{Name: "root", Parent: true, SubmitACL: "*"}},
		}},
	}
	expectValid(t, cfg)
}

// =============================================================================
// YAML parse stage (strict-field decoding)
// =============================================================================

func TestValidateBytes_RejectsUnknownField(t *testing.T) {
	// "thisisnotafield" is not a known YuniKorn key — strict decoding should reject.
	bad := []byte(`partitions:
  - name: default
    queues:
      - name: root
        thisisnotafield: 42
`)
	_, err := ValidateBytes(bad)
	if err == nil {
		t.Fatal("expected error for unknown YAML field")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Stage != "parse" {
		t.Errorf("expected stage=parse, got %q (err=%v)", ve.Stage, err)
	}
}

func TestValidateBytes_RejectsMalformedYAML(t *testing.T) {
	bad := []byte("partitions: [unclosed")
	_, err := ValidateBytes(bad)
	if err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}

func TestValidateBytes_AcceptsDefault(t *testing.T) {
	// Round-trip: marshal a default valid config, then re-validate the bytes.
	yamlBytes, _, err := Validate(cfg(QueueConfig{Name: "team-a"}))
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := ValidateBytes(yamlBytes); err != nil {
		t.Errorf("expected default to round-trip, got: %v", err)
	}
}

// =============================================================================
// ValidationError ergonomics
// =============================================================================

func TestValidationError_IncludesYAMLOnSemanticFailure(t *testing.T) {
	_, _, err := Validate(cfg(QueueConfig{
		Name: "team-a", Parent: true,
		Queues: []QueueConfig{{Name: "dev"}, {Name: "dev"}},
	}))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.YAML == "" {
		t.Error("expected ValidationError.YAML to be populated for debug")
	}
	if !strings.Contains(ve.YAML, "team-a") {
		t.Errorf("expected YAML to contain rendered queue name, got: %s", ve.YAML)
	}
}

func TestValidationError_UnwrapReturnsUnderlying(t *testing.T) {
	_, _, err := Validate(cfg(QueueConfig{
		Name: strings.Repeat("a", 65),
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Unwrap(err) == nil {
		t.Error("expected non-nil underlying error from Unwrap")
	}
}

// =============================================================================
// Inter-CR merged-config propagation invariants
// -----------------------------------------------------------------------------
// In production every Queue CR becomes one L1 child of an *implicit*,
// limit-free root queue (see queueconfig.BuildMerged). YuniKorn's user/group
// limit-hierarchy rules (V47–V56) propagate ONLY downward along an ancestor
// chain — so siblings under our limit-free root cannot constrain each other.
//
// These tests pin that property so we never silently regress in either
// direction:
//
//  1. Two unrelated CRs with their own user/group limits must both validate;
//     CR-A's "alice 2G" must not be conflated with CR-B's "alice 100G".
//  2. The walk still fires the moment one of those CRs internally violates
//     a parent→child limit rule (proves the merged validator is actually
//     traversing each subtree, not bailing early or unifying siblings).
// =============================================================================

func TestValidate_InterCR_SiblingLimitsAreIndependent(t *testing.T) {
	// Two "L1 children under root" each carry the same user name ("alice")
	// with very different caps. Under our limit-free root they're siblings,
	// so neither should constrain the other.
	expectValid(t, cfg(
		QueueConfig{
			Name: "team-a", Parent: true,
			Limits: []Limit{{
				Limit: "u-a", Users: []string{"alice"},
				MaxResources: map[string]string{"memory": "2G"}, MaxApplications: 5,
			}},
		},
		QueueConfig{
			Name: "team-b", Parent: true,
			Limits: []Limit{{
				Limit: "u-b", Users: []string{"alice"},
				MaxResources: map[string]string{"memory": "100G"}, MaxApplications: 500,
			}},
		},
	))
}

func TestValidate_InterCR_SiblingGroupLimitsAreIndependent(t *testing.T) {
	// Same shape but for groups, with anchor non-wildcard members so V30
	// doesn't fire while still proving wildcards on either sibling don't
	// bleed across the root boundary.
	expectValid(t, cfg(
		QueueConfig{
			Name: "team-a", Parent: true,
			Limits: []Limit{
				{Limit: "g-anchor", Groups: []string{"other"},
					MaxResources: map[string]string{"memory": "1G"}},
				{Limit: "g-wild", Groups: []string{"*"},
					MaxResources: map[string]string{"memory": "2G"}},
			},
		},
		QueueConfig{
			Name: "team-b", Parent: true,
			Limits: []Limit{{
				Limit: "g-b", Groups: []string{"infra"},
				MaxResources: map[string]string{"memory": "100G"},
			}},
		},
	))
}

// Proves the merged-config validator still WALKS each L1 subtree by firing on
// a per-subtree violation while a sibling subtree is well-formed. A wrong
// implementation that "unified" sibling limits, or that bailed at the first
// CR, would either accept the bad CR or reject the good one — neither is
// what we want.
func TestValidate_InterCR_PerSubtreeViolationStillFires(t *testing.T) {
	expectInvalid(t, cfg(
		QueueConfig{
			Name: "team-good", Parent: true,
			Limits: []Limit{{
				Limit: "u", Users: []string{"alice"},
				MaxResources: map[string]string{"memory": "10G"}, MaxApplications: 50,
			}},
		},
		QueueConfig{
			Name: "team-bad", Parent: true,
			Limits: []Limit{{
				Limit: "u-parent", Users: []string{"alice"}, MaxApplications: 5,
			}},
			Queues: []QueueConfig{{
				Name: "dev",
				Limits: []Limit{{
					Limit: "u-child", Users: []string{"alice"}, MaxApplications: 50,
				}},
			}},
		},
	), "validate", "greater than immediate or ancestor parent max applications")
}
