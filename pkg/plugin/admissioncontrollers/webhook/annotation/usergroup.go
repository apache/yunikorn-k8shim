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

package annotation

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"

	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1Beta "k8s.io/api/batch/v1beta1"

	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type UserGroupAnnotationHandler struct {
	TrustControllers bool
	SystemUsers      []*regexp.Regexp
	ExternalUsers    []*regexp.Regexp
	ExternalGroups   []*regexp.Regexp
}

type Extractor func(*admissionv1.AdmissionRequest) (map[string]string, error)

var (
	Deployment  = reflect.TypeOf(appsv1.Deployment{}).Name()
	DaemonSet   = reflect.TypeOf(appsv1.DaemonSet{}).Name()
	StatefulSet = reflect.TypeOf(appsv1.StatefulSet{}).Name()
	ReplicaSet  = reflect.TypeOf(appsv1.ReplicaSet{}).Name()
	Job         = reflect.TypeOf(batchv1.Job{}).Name()
	CronJob     = reflect.TypeOf(batchv1Beta.CronJob{}).Name()

	extractors = map[string]Extractor{
		Deployment:  fromDeployment,
		DaemonSet:   fromDaemonSet,
		StatefulSet: fromStatefulSet,
		ReplicaSet:  fromReplicaSet,
		Job:         fromJob,
		CronJob:     fromCronJob,
	}
)

func (u *UserGroupAnnotationHandler) IsAnnotationAllowed(userName string, groups []string) bool {
	if u.TrustControllers {
		for _, sysUser := range u.SystemUsers {
			if sysUser.MatchString(userName) {
				log.Logger().Debug("Request submitted from a system user, bypassing",
					zap.String("userName", userName))
				return true
			}
		}
	}

	for _, allowedUser := range u.ExternalUsers {
		if allowedUser.MatchString(userName) {
			log.Logger().Debug("Request submitted from an allowed external user",
				zap.String("userName", userName))
			return true
		}
	}

	for _, allowedGroup := range u.ExternalGroups {
		for _, group := range groups {
			if allowedGroup.MatchString(group) {
				log.Logger().Debug("Request submitted from an allowed external group",
					zap.String("userName", userName),
					zap.String("group", group))
				return true
			}
		}
	}

	return false
}

func (u *UserGroupAnnotationHandler) IsAnnotationValid(userInfoAnnotation string) error {
	var userGroups si.UserGroupInformation
	err := json.Unmarshal([]byte(userInfoAnnotation), &userGroups)
	if err != nil {
		return err
	}

	log.Logger().Debug("Successfully validated user info annotation", zap.String("externally provided user", userGroups.User),
		zap.String("externally provided groups", strings.Join(userGroups.Groups, ",")))

	return nil
}

func (u *UserGroupAnnotationHandler) GetAnnotationsFromRequestKind(kind string, req *admissionv1.AdmissionRequest) (map[string]string, bool, error) {
	extractFn, ok := extractors[kind]
	if !ok {
		return nil, false, nil
	}
	result, err := extractFn(req)
	return result, true, err
}

func fromDeployment(req *admissionv1.AdmissionRequest) (map[string]string, error) {
	var deployment appsv1.Deployment
	err := json.Unmarshal(req.Object.Raw, &deployment)
	if err != nil {
		return nil, err
	}

	return deployment.Spec.Template.Annotations, nil
}

func fromDaemonSet(req *admissionv1.AdmissionRequest) (map[string]string, error) {
	var daemonSet appsv1.DaemonSet
	err := json.Unmarshal(req.Object.Raw, &daemonSet)
	if err != nil {
		return nil, err
	}

	return daemonSet.Spec.Template.Annotations, nil
}

func fromStatefulSet(req *admissionv1.AdmissionRequest) (map[string]string, error) {
	var statefulSet appsv1.StatefulSet
	err := json.Unmarshal(req.Object.Raw, &statefulSet)
	if err != nil {
		return nil, err
	}

	return statefulSet.Spec.Template.Annotations, nil
}

func fromReplicaSet(req *admissionv1.AdmissionRequest) (map[string]string, error) {
	var replicaSet appsv1.ReplicaSet
	err := json.Unmarshal(req.Object.Raw, &replicaSet)
	if err != nil {
		return nil, err
	}

	return replicaSet.Spec.Template.Annotations, nil
}

func fromJob(req *admissionv1.AdmissionRequest) (map[string]string, error) {
	var job batchv1.Job
	err := json.Unmarshal(req.Object.Raw, &job)
	if err != nil {
		return nil, err
	}

	return job.Spec.Template.Annotations, nil
}

func fromCronJob(req *admissionv1.AdmissionRequest) (map[string]string, error) {
	var cronJob batchv1Beta.CronJob
	err := json.Unmarshal(req.Object.Raw, &cronJob)
	if err != nil {
		return nil, err
	}

	return cronJob.Spec.JobTemplate.Spec.Template.Annotations, nil
}
