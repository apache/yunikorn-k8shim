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
	"io"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Environment variable names. Kept here so both the controller and the webhook
// read from the same source of truth — there must be ONE answer to "what
// partition will the queue go into" and "what placement rules are in effect"
// at admission time and at reconcile time.
const (
	PartitionNameEnvVar  = "PARTITION_NAME"
	PlacementRulesEnvVar = "PLACEMENT_RULES"
	NodeSortPolicyEnvVar = "NODE_SORT_POLICY"
	RootPropertiesEnvVar = "ROOT_PROPERTIES"
	DefaultPartitionName = "default"
)

// OptionError identifies one optional operator setting that could not be
// parsed. Setting is an environment variable name from the bounded set above.
type OptionError struct {
	Setting string
	Err     error
}

func (e OptionError) Error() string {
	return "parse " + e.Setting + ": " + e.Err.Error()
}

func (e OptionError) Unwrap() error { return e.Err }

// LoadOptionsFromEnv builds a BuildOptions from the well-known env vars the
// operator deployment sets. Optional fields are parsed independently so one
// malformed setting does not discard valid sibling settings.
//
// The returned options always contain a usable PartitionName (defaulted if
// unset), and errors identify the dropped optional setting.
func LoadOptionsFromEnv() (BuildOptions, []OptionError) {
	opts := BuildOptions{
		PartitionName: os.Getenv(PartitionNameEnvVar),
	}
	var errs []OptionError
	if opts.PartitionName == "" {
		opts.PartitionName = DefaultPartitionName
	}

	if raw := os.Getenv(PlacementRulesEnvVar); raw != "" {
		var rules []PlacementRule
		if err := decodeStrict(raw, &rules); err != nil {
			errs = append(errs, OptionError{Setting: PlacementRulesEnvVar, Err: err})
		} else {
			opts.PlacementRules = rules
		}
	}

	if raw := os.Getenv(NodeSortPolicyEnvVar); raw != "" {
		policy, err := parseNodeSortPolicy(raw)
		if err != nil {
			errs = append(errs, OptionError{Setting: NodeSortPolicyEnvVar, Err: err})
		} else {
			opts.NodeSortPolicy = policy
		}
	}

	if raw := os.Getenv(RootPropertiesEnvVar); raw != "" {
		properties := make(map[string]string)
		if err := decodeStrict(raw, &properties); err != nil {
			errs = append(errs, OptionError{Setting: RootPropertiesEnvVar, Err: err})
		} else {
			opts.RootProperties = properties
		}
	}

	return opts, errs
}

func parseNodeSortPolicy(raw string) (*NodeSortingPolicy, error) {
	var policyName string
	if err := yaml.Unmarshal([]byte(raw), &policyName); err == nil && strings.TrimSpace(policyName) != "" {
		return &NodeSortingPolicy{Type: strings.TrimSpace(policyName)}, nil
	}

	var policy NodeSortingPolicy
	if err := decodeStrict(raw, &policy); err != nil {
		return nil, err
	}
	if strings.TrimSpace(policy.Type) == "" {
		return nil, &yaml.TypeError{Errors: []string{"type must be non-empty"}}
	}
	policy.Type = strings.TrimSpace(policy.Type)
	return &policy, nil
}

func decodeStrict(raw string, value any) error {
	decoder := yaml.NewDecoder(strings.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return &yaml.TypeError{Errors: []string{"multiple YAML documents are not allowed"}}
		}
		return err
	}
	return nil
}
