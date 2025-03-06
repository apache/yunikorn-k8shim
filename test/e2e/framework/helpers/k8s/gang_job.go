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

package k8s

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

type TestJobConfig struct {
	JobName     string
	Parallelism int32
	Completions int32
}

func InitTestJob(jobName string, parallelism, completions int32, pod *v1.Pod) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
		},
		Spec: batchv1.JobSpec{
			Parallelism: &parallelism,
			Completions: &completions,
			Template: v1.PodTemplateSpec{
				Spec:       pod.Spec,
				ObjectMeta: pod.ObjectMeta,
			},
		},
	}

	return job
}

func getGangSchedulingAnnotations(placeholderTimeout int,
	schedulingStyle string,
	taskGroupName string,
	taskGroups []*cache.TaskGroup) map[string]string {
	annotations := make(map[string]string)
	var schedulingParams string

	if placeholderTimeout != 0 {
		schedulingParams = "placeholderTimeoutInSeconds=" + strconv.Itoa(placeholderTimeout)
	}

	if schedulingStyle != "" {
		if schedulingParams != "" {
			schedulingParams += " gangSchedulingStyle=" + schedulingStyle
		} else {
			schedulingParams = "gangSchedulingStyle" + schedulingStyle
		}
	}

	if schedulingParams != "" {
		annotations[constants.DomainYuniKorn+"schedulingPolicyParameters"] = schedulingParams
	}

	annotations[constants.DomainYuniKorn+"task-group-name"] = taskGroupName
	taskGroupJSON, err := json.Marshal(taskGroups)
	if err != nil {
		panic("Unable to marshal taskGroups")
	}
	annotations[constants.DomainYuniKorn+"task-groups"] = string(taskGroupJSON)

	return annotations
}

func DecoratePodForGangScheduling(
	placeholderTimeout int,
	schedulingStyle string,
	taskGroupName string,
	taskGroups []*cache.TaskGroup,
	pod *v1.Pod) *v1.Pod {
	gangSchedulingAnnotations := getGangSchedulingAnnotations(placeholderTimeout, schedulingStyle, taskGroupName, taskGroups)
	pod.Annotations = utils.MergeMaps(pod.Annotations, gangSchedulingAnnotations)

	return pod
}

func InitTaskGroups(conf SleepPodConfig, mainTaskGroupName, secondTaskGroupName string, parallelism int) ([]*cache.TaskGroup, error) {
	if parallelism < math.MinInt32 || parallelism > math.MaxInt32 {
		return nil, fmt.Errorf("parallelism value %d is out of int32 range", parallelism)
	}
	tg1 := &cache.TaskGroup{
		MinMember: int32(parallelism),
		Name:      mainTaskGroupName,
		MinResource: map[string]resource.Quantity{
			"cpu":    resource.MustParse(strconv.FormatInt(conf.CPU, 10) + "m"),
			"memory": resource.MustParse(strconv.FormatInt(conf.Mem, 10) + "M"),
		},
	}

	// create TG2 more with more members than needed, also make sure that
	// placeholders will stay in Pending state
	if parallelism < math.MinInt32 || parallelism > math.MaxInt32-1 {
		return nil, fmt.Errorf("parallelism+1 value %d is out of int32 range", parallelism+1)
	}
	tg2 := &cache.TaskGroup{
		MinMember: int32(parallelism + 1), // nolint: gosec
		Name:      secondTaskGroupName,
		MinResource: map[string]resource.Quantity{
			"cpu":    resource.MustParse(strconv.FormatInt(conf.CPU, 10) + "m"),
			"memory": resource.MustParse(strconv.FormatInt(conf.Mem, 10) + "M"),
		},
		NodeSelector: map[string]string{
			"kubernetes.io/hostname": "nonexistingnode",
		},
	}

	tGroups := make([]*cache.TaskGroup, 2)
	tGroups[0] = tg1
	tGroups[1] = tg2

	return tGroups, nil
}

func InitTaskGroup(conf SleepPodConfig, taskGroupName string, parallelism int32) []*cache.TaskGroup {
	tg1 := &cache.TaskGroup{
		MinMember: parallelism,
		Name:      taskGroupName,
		MinResource: map[string]resource.Quantity{
			"cpu":    resource.MustParse(strconv.FormatInt(conf.CPU, 10) + "m"),
			"memory": resource.MustParse(strconv.FormatInt(conf.Mem, 10) + "M"),
		},
	}

	tGroups := make([]*cache.TaskGroup, 1)
	tGroups[0] = tg1

	return tGroups
}
