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

package common

import (
	"errors"
	"strings"

	"gopkg.in/yaml.v2"
)

// Converts partitionsWrapper to YAML string
func (pw *PartitionsWrapper) ToYAML() (string, error) {
	d, err := yaml.Marshal(pw)
	if err != nil {
		return "", err
	}

	return string(d), nil
}

func CreateBasicConfigMap() *PartitionsWrapper {
	p1 := PartitionConfigType{
		Name: "default",
		PlacementRules: []*PlacementRuleConfigType{{
			Name:   "tag",
			Value:  "namespace",
			Create: true,
		}},
		Queues: []*QueueConfigType{{
			Name:      "root",
			SubmitACL: "*",
		}},
	}
	partitions := PartitionsWrapper{
		Partitions: []*PartitionConfigType{&p1},
	}

	return &partitions
}

// Returns pointer to corresponding queue.
// Expects queuePath to be slice of queue names in order.
func findQueue(subQs []*QueueConfigType, queuePath []string) (*QueueConfigType, error) {
	for _, subQ := range subQs {
		if subQ.Name == queuePath[0] {
			if len(queuePath) == 1 { // path is single queue
				return subQ, nil
			}
			return findQueue(subQ.Queues, queuePath[1:]) // recursive call to check children
		}
	}
	return nil, errors.New("queue not in path")
}

// Expects queuePath to use periods as delimiters. ie "root.queueA.child"
func (pw *PartitionsWrapper) SetSchedulingPolicy(partition string, queuePathStr string, policy string) error {
	path := strings.Split(queuePathStr, ".")
	for _, p := range pw.Partitions {
		if p.Name == partition { // find correct partition
			q, err := findQueue(p.Queues, path)
			if err != nil {
				return err
			}
			q.Properties = map[string]string{"application.sort.policy": policy}
			return nil
		}
	}
	return errors.New("partition not found")
}

func (pw *PartitionsWrapper) AddQueue(partition string, parentPathStr string, newQ *QueueConfigType) error {
	parentPath := strings.Split(parentPathStr, ".")
	for _, p := range pw.Partitions {
		if p.Name == partition {
			parentQ, err := findQueue(p.Queues, parentPath)
			if err != nil {
				return err
			}
			parentQ.Queues = append(parentQ.Queues, newQ)
			return nil
		}
	}
	return errors.New("partition not found")
}

/* -- Data structures below -- */

// Holds all partitions
type PartitionsWrapper struct {
	Partitions []*PartitionConfigType
}

type PartitionConfigType struct {
	Name   string
	Queues []*QueueConfigType `yaml:",omitempty"`

	//Optional args
	PlacementRules []*PlacementRuleConfigType `yaml:",omitempty"`
	Limits         LimitsConfigType           `yaml:",omitempty"`
	Preemption     bool                       `yaml:",omitempty"`
}

type QueueConfigType struct {
	Name       string
	Parent     bool               `yaml:",omitempty"`
	Queues     []*QueueConfigType `yaml:",omitempty"`
	Properties map[string]string  `yaml:",omitempty"`
	AdminACL   string             `yaml:",omitempty"`
	SubmitACL  string             `yaml:",omitempty"`
	Limits     *LimitsConfigType  `yaml:",omitempty"`

	// Add structs
	Resources string `yaml:",omitempty"`
}

type PlacementRuleConfigType struct {
	Name   string
	Create bool   `yaml:",omitempty"`
	Value  string `yaml:",omitempty"`

	// Add structs
	Parent string `yaml:",omitempty"`
	Filter string `yaml:",omitempty"`
}

type LimitsConfigType struct {
	Limit           string         `yaml:",omitempty"`
	Users           []string       `yaml:",omitempty"`
	Groups          []string       `yaml:",omitempty"`
	MaxApplications int            `yaml:",omitempty"`
	MaxResources    map[string]int `yaml:",omitempty"`
}
