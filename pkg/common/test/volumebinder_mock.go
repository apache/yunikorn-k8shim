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

package test

import (
	"context"
	"errors"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

var _ volumebinding.SchedulerVolumeBinder = &VolumeBinderMock{}

type VolumeBinderMock struct {
	volumeClaimError    error
	findPodVolumesError error
	assumeVolumeError   error
	bindError           error
	conflictReasons     volumebinding.ConflictReasons

	podVolumeClaim *volumebinding.PodVolumeClaims
	podVolumes     *volumebinding.PodVolumes
	allBound       bool
}

func NewVolumeBinderMock() *VolumeBinderMock {
	return &VolumeBinderMock{
		allBound: true,
	}
}

func (v *VolumeBinderMock) GetPodVolumeClaims(_ klog.Logger, _ *v1.Pod) (podVolumeClaims *volumebinding.PodVolumeClaims, err error) {
	if v.volumeClaimError != nil {
		return nil, v.volumeClaimError
	}

	return v.podVolumeClaim, nil
}

func (v *VolumeBinderMock) GetEligibleNodes(_ klog.Logger, _ []*v1.PersistentVolumeClaim) (eligibleNodes sets.Set[string]) {
	return nil
}

func (v *VolumeBinderMock) FindPodVolumes(_ klog.Logger, _ *v1.Pod, _ *volumebinding.PodVolumeClaims, _ *v1.Node) (podVolumes *volumebinding.PodVolumes, reasons volumebinding.ConflictReasons, err error) {
	if v.findPodVolumesError != nil {
		return nil, nil, v.findPodVolumesError
	}

	if len(v.conflictReasons) > 0 {
		return nil, v.conflictReasons, nil
	}

	return v.podVolumes, nil, nil
}

func (v *VolumeBinderMock) AssumePodVolumes(_ klog.Logger, _ *v1.Pod, _ string, _ *volumebinding.PodVolumes) (allFullyBound bool, err error) {
	if v.assumeVolumeError != nil {
		return false, v.assumeVolumeError
	}

	return v.allBound, nil
}

func (v *VolumeBinderMock) RevertAssumedPodVolumes(_ *volumebinding.PodVolumes) {
}

func (v *VolumeBinderMock) BindPodVolumes(_ context.Context, _ *v1.Pod, _ *volumebinding.PodVolumes) error {
	return v.bindError
}

func (v *VolumeBinderMock) EnableVolumeClaimsError(message string) {
	v.volumeClaimError = errors.New(message)
}

func (v *VolumeBinderMock) EnableFindPodVolumesError(message string) {
	v.findPodVolumesError = errors.New(message)
}

func (v *VolumeBinderMock) SetConflictReasons(reasons ...string) {
	var conflicts []volumebinding.ConflictReason
	for _, r := range reasons {
		conflicts = append(conflicts, volumebinding.ConflictReason(r))
	}
	v.conflictReasons = conflicts
}

func (v *VolumeBinderMock) SetAssumePodVolumesError(message string) {
	v.assumeVolumeError = errors.New(message)
}
