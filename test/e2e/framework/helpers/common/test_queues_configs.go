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
	"strconv"
	"strings"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/configs"

	"gopkg.in/yaml.v2"
)

// Converts partitionsWrapper to YAML string
func ToYAML(sc *configs.SchedulerConfig) (string, error) {
	d, err := yaml.Marshal(sc)
	if err != nil {
		return "", err
	}

	return string(d), nil
}

func CreateBasicConfigMap() *configs.SchedulerConfig {
	p1 := configs.PartitionConfig{
		Name: "default",
		PlacementRules: []configs.PlacementRule{{
			Name:   "tag",
			Value:  "namespace",
			Create: true,
		}},
		Queues: []configs.QueueConfig{{
			Name:      "root",
			SubmitACL: "*",
		}},
	}
	partitions := configs.SchedulerConfig{
		Partitions: []configs.PartitionConfig{p1},
	}

	return &partitions
}

// Returns pointer to corresponding queue.
// Expects queuePath to be slice of queue names in order.
func getQueue(subQs []configs.QueueConfig, queuePath []string) (*configs.QueueConfig, error) {
	for i, subQ := range subQs {
		if subQ.Name == queuePath[0] {
			if len(queuePath) == 1 { // path is single queue
				return &subQs[i], nil
			}
			return getQueue(subQ.Queues, queuePath[1:]) // recursive call to check children
		}
	}
	return nil, errors.New("queue not in path")
}

func getPartition(sc *configs.SchedulerConfig, partition string) (*configs.PartitionConfig, error) {
	for i, p := range sc.Partitions {
		if p.Name == partition {
			return &sc.Partitions[i], nil
		}
	}
	return nil, errors.New("partition does not exist")
}

func SetSchedulingPolicy(sc *configs.SchedulerConfig, partition string, queuePathStr string, policy string) error {
	path := strings.Split(queuePathStr, ".")
	for _, p := range sc.Partitions {
		if p.Name == partition { // find correct partition
			q, err := getQueue(p.Queues, path)
			if err != nil {
				return err
			}
			if q.Properties == nil {
				q.Properties = map[string]string{}
			}
			q.Properties["application.sort.policy"] = policy
			return nil
		}
	}
	return errors.New("partition not found")
}

func SetQueueTimestamp(sc *configs.SchedulerConfig, partition string, queuePathStr string) (string, error) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	path := strings.Split(queuePathStr, ".")
	for _, p := range sc.Partitions {
		if p.Name == partition { // find correct partition
			q, err := getQueue(p.Queues, path)
			if err != nil {
				return "", err
			}
			if q.Properties == nil {
				q.Properties = map[string]string{}
			}
			q.Properties["timestamp"] = ts
			return ts, nil
		}
	}
	return "", errors.New("partition not found")
}

func AddQueue(sc *configs.SchedulerConfig, partition string, parentPathStr string, newQ configs.QueueConfig) error {
	parentPath := strings.Split(parentPathStr, ".")
	for _, p := range sc.Partitions {
		if p.Name == partition {
			parentQ, err := getQueue(p.Queues, parentPath)
			if err != nil {
				return err
			}
			parentQ.Queues = append(parentQ.Queues, newQ)
			return nil
		}
	}
	return errors.New("partition not found")
}

func SetNodeSortPolicy(sc *configs.SchedulerConfig, partition string, policy configs.NodeSortingPolicy) error {
	p, err := getPartition(sc, partition)
	if err != nil {
		return err
	}
	p.NodeSortPolicy = policy
	return nil
}
