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
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

// Environment variable names. Kept here so both the controller and the webhook
// read from the same source of truth — there must be ONE answer to "what
// partition will the queue go into" and "what placement rules are in effect"
// at admission time and at reconcile time.
const (
	PartitionNameEnvVar  = "PARTITION_NAME"
	PlacementRulesEnvVar = "PLACEMENT_RULES"
	DefaultPartitionName = "default"
)

// LoadOptionsFromEnv builds a BuildOptions from the well-known env vars the
// operator deployment sets. It is intentionally permissive: a malformed
// PLACEMENT_RULES YAML is logged and ignored (returned as an error second
// value) so that startup is never blocked on a typo in an optional field.
//
// Callers MUST set BuildOptions.PartitionName even on error — the returned
// options always contain a usable PartitionName (defaulted if env unset).
func LoadOptionsFromEnv() (BuildOptions, error) {
	opts := BuildOptions{
		PartitionName: os.Getenv(PartitionNameEnvVar),
	}
	if opts.PartitionName == "" {
		opts.PartitionName = DefaultPartitionName
	}

	raw := os.Getenv(PlacementRulesEnvVar)
	if raw == "" {
		return opts, nil
	}

	var rules []PlacementRule
	if err := yaml.Unmarshal([]byte(raw), &rules); err != nil {
		return opts, fmt.Errorf("parse %s: %w", PlacementRulesEnvVar, err)
	}
	opts.PlacementRules = rules
	return opts, nil
}
