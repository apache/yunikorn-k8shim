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

package cache

import (
	"encoding/json"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

func GetTaskGroupsFromAnnotation(pod *v1.Pod) ([]TaskGroup, error) {
	taskGroupInfo := utils.GetPodAnnotationValue(pod, constants.AnnotationTaskGroups)
	if taskGroupInfo == "" {
		return nil, nil
	}

	taskGroups := []TaskGroup{}
	err := json.Unmarshal([]byte(taskGroupInfo), &taskGroups)
	if err != nil {
		return nil, err
	}
	// json.Unmarshal won't return error if name or MinMember is empty, but will return error if MinResource is empty or error format.
	totals := make(map[string]int64)
	for _, taskGroup := range taskGroups {
		if taskGroup.Name == "" {
			return nil, fmt.Errorf("can't get taskGroup Name from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinResource == nil {
			return nil, fmt.Errorf("can't get taskGroup MinResource from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember == int32(0) {
			return nil, fmt.Errorf("can't get taskGroup MinMember from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember < int32(0) {
			return nil, fmt.Errorf("minMember cannot be negative, %s",
				taskGroupInfo)
		}
		if err := validateTaskGroupResources(taskGroup, totals); err != nil {
			return nil, err
		}
	}
	return taskGroups, nil
}

// validateTaskGroupResources folds one task group's placeholder-ask contribution
// into totals (the canonical-key aggregate across all groups) and rejects an ask
// that cannot be a non-negative int64: a negative minResource, an int64 overflow
// (the accessor, the minMember product, or the cross-group aggregate), or a
// same-canonical-key collision. It mirrors GetTGResource, which emits minMember
// "pods" plus minMember*minResource per resource ("cpu" canonicalizes to
// siCommon.CPU, "vcore"); a collision (cpu with vcore, or an explicit "pods") is
// rejected because GetTGResource would otherwise let one silently overwrite the
// other (cpu vs vcore by map-iteration order, an explicit "pods" over the implicit count).
func validateTaskGroupResources(taskGroup TaskGroup, totals map[string]int64) error {
	members := int64(taskGroup.MinMember)
	// seen holds the canonical keys this task group has already contributed, so a
	// same-group collision is rejected instead of silently overwritten. The implicit
	// "pods" count claims the "pods" key up front.
	seen := map[string]bool{"pods": true}
	if totals["pods"] > math.MaxInt64-members {
		return fmt.Errorf("aggregate placeholder request for \"pods\" overflows int64 across taskGroups")
	}
	totals["pods"] += members

	for resName, quantity := range taskGroup.MinResource {
		if quantity.Sign() < 0 {
			return fmt.Errorf("minResource %q in taskGroup %q cannot be negative", resName, taskGroup.Name)
		}
		cpu := resName == v1.ResourceCPU.String()
		canonical := resName
		milliConvert := int64(1)
		if cpu {
			canonical = siCommon.CPU
			milliConvert = 1000
		}
		if seen[canonical] {
			return fmt.Errorf("minResource %q in taskGroup %q collides with another resource under canonical key %q", resName, taskGroup.Name, canonical)
		}
		seen[canonical] = true
		// Reject before the int64 accessor or the minMember product can overflow:
		// members*value (in milli-units for cpu) must stay within int64.
		if quantity.CmpInt64(math.MaxInt64/(members*milliConvert)) > 0 {
			return fmt.Errorf("minResource %q in taskGroup %q overflows int64 when scaled by minMember %d", resName, taskGroup.Name, members)
		}
		value := quantity.Value()
		if cpu {
			value = quantity.MilliValue()
		}
		contribution := members * value
		if totals[canonical] > math.MaxInt64-contribution {
			return fmt.Errorf("aggregate placeholder request for %q overflows int64 across taskGroups", canonical)
		}
		totals[canonical] += contribution
	}
	return nil
}
