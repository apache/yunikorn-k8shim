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

// This is a smoke probe — kept intentionally tiny — that confirms the wired-up
// yunikorn-core validator actually rejects a config we know to be invalid. It
// guards against silently importing the wrong package or losing the dependency
// in a future go.mod cleanup.

package queueconfig

import (
	"errors"
	"strings"
	"testing"
)

// TestProbe_RejectsDuplicateChildQueueNames is a single representative check
// that the YuniKorn validator we wrap actually fires on a known-bad config.
// The exhaustive rule-by-rule coverage lives in validator_test.go.
func TestProbe_RejectsDuplicateChildQueueNames(t *testing.T) {
	cfg := &SchedulerConfig{
		Partitions: []PartitionConfig{{
			Name: "default",
			Queues: []QueueConfig{{
				Name:   "root",
				Parent: true,
				Queues: []QueueConfig{
					{Name: "team-a"},
					{Name: "team-a"},
				},
			}},
		}},
	}

	_, _, err := Validate(cfg)
	if err == nil {
		t.Fatal("expected validation error for duplicate child queue names, got nil")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected error to satisfy errors.Is(ErrInvalidConfig), got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected error to mention 'duplicate', got: %v", err)
	}
}
