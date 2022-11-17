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
	"strings"

	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1Beta "k8s.io/api/batch/v1beta1"

	"github.com/apache/yunikorn-k8shim/pkg/plugin/admissioncontrollers/webhook/conf"

	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/admissioncontrollers/webhook/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type UserGroupAnnotationHandler struct {
	conf *conf.AdmissionControllerConf
}

func NewUserGroupAnnotationHandler(conf *conf.AdmissionControllerConf) *UserGroupAnnotationHandler {
	return &UserGroupAnnotationHandler{
		conf: conf,
	}
}

type extractor func(*admissionv1.AdmissionRequest) (map[string]string, string, error)

var (
	Deployment  = reflect.TypeOf(appsv1.Deployment{}).Name()
	DaemonSet   = reflect.TypeOf(appsv1.DaemonSet{}).Name()
	StatefulSet = reflect.TypeOf(appsv1.StatefulSet{}).Name()
	ReplicaSet  = reflect.TypeOf(appsv1.ReplicaSet{}).Name()
	Job         = reflect.TypeOf(batchv1.Job{}).Name()
	CronJob     = reflect.TypeOf(batchv1Beta.CronJob{}).Name()

	extractors = map[string]extractor{
		Deployment:  fromDeployment,
		DaemonSet:   fromDaemonSet,
		StatefulSet: fromStatefulSet,
		ReplicaSet:  fromReplicaSet,
		Job:         fromJob,
		CronJob:     fromCronJob,
	}
)

const (
	defaultPodAnnotationsPath = "/spec/template/metadata/annotations"
	cronJobPodAnnotationsPath = "/spec/jobtemplate/spec/template/metadata/annotations"
)

func (u *UserGroupAnnotationHandler) IsAnnotationAllowed(userName string, groups []string) bool {
	if u.conf.GetTrustControllers() {
		for _, sysUser := range u.conf.GetSystemUsers() {
			if sysUser.MatchString(userName) {
				log.Logger().Debug("Request submitted from a system user, bypassing",
					zap.String("userName", userName))
				return true
			}
		}
	}

	for _, allowedUser := range u.conf.GetExternalUsers() {
		if allowedUser.MatchString(userName) {
			log.Logger().Debug("Request submitted from an allowed external user",
				zap.String("userName", userName))
			return true
		}
	}

	for _, allowedGroup := range u.conf.GetExternalGroups() {
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

func (u *UserGroupAnnotationHandler) GetAnnotationsFromRequestKind(req *admissionv1.AdmissionRequest) (map[string]string, bool, error) {
	extractFn, ok := extractors[req.Kind.Kind]
	if !ok {
		return nil, false, nil
	}
	annotations, _, err := extractFn(req)
	return annotations, true, err
}

func (u *UserGroupAnnotationHandler) GetPatchForWorkload(req *admissionv1.AdmissionRequest, user string, groups []string) ([]common.PatchOperation, error) {
	extractFn, ok := extractors[req.Kind.Kind]
	if !ok {
		return nil, nil
	}
	annotations, path, err := extractFn(req)
	if err != nil {
		return nil, err
	}

	patchOp, patchErr := u.getPatchOperation(annotations, path, user, groups)
	if patchErr != nil {
		return nil, patchErr
	}

	patch := make([]common.PatchOperation, 1)
	patch[0] = *patchOp

	return patch, nil
}

func (u *UserGroupAnnotationHandler) GetPatchForPod(annotations map[string]string, user string, groups []string) (*common.PatchOperation, error) {
	patchOp, err := u.getPatchOperation(annotations, "/metadata/annotations", user, groups)
	if err != nil {
		return nil, err
	}
	return patchOp, nil
}

func (u *UserGroupAnnotationHandler) getPatchOperation(annotations map[string]string, path, user string, groups []string) (*common.PatchOperation, error) {
	newAnnotations := make(map[string]string)
	for k, v := range annotations {
		newAnnotations[k] = v
	}

	var userGroups si.UserGroupInformation
	userGroups.User = user
	userGroups.Groups = groups
	jsonBytes, err := json.Marshal(userGroups)
	if err != nil {
		return nil, err
	}

	newAnnotations[common.UserInfoAnnotation] = string(jsonBytes)

	return &common.PatchOperation{
		Op:    "add",
		Path:  path,
		Value: newAnnotations,
	}, nil
}

func fromDeployment(req *admissionv1.AdmissionRequest) (map[string]string, string, error) {
	var deployment appsv1.Deployment
	err := json.Unmarshal(req.Object.Raw, &deployment)
	if err != nil {
		return nil, "", err
	}

	return deployment.Spec.Template.Annotations, defaultPodAnnotationsPath, nil
}

func fromDaemonSet(req *admissionv1.AdmissionRequest) (map[string]string, string, error) {
	var daemonSet appsv1.DaemonSet
	err := json.Unmarshal(req.Object.Raw, &daemonSet)
	if err != nil {
		return nil, "", err
	}

	return daemonSet.Spec.Template.Annotations, defaultPodAnnotationsPath, nil
}

func fromStatefulSet(req *admissionv1.AdmissionRequest) (map[string]string, string, error) {
	var statefulSet appsv1.StatefulSet
	err := json.Unmarshal(req.Object.Raw, &statefulSet)
	if err != nil {
		return nil, "", err
	}

	return statefulSet.Spec.Template.Annotations, defaultPodAnnotationsPath, nil
}

func fromReplicaSet(req *admissionv1.AdmissionRequest) (map[string]string, string, error) {
	var replicaSet appsv1.ReplicaSet
	err := json.Unmarshal(req.Object.Raw, &replicaSet)
	if err != nil {
		return nil, "", err
	}

	return replicaSet.Spec.Template.Annotations, defaultPodAnnotationsPath, nil
}

func fromJob(req *admissionv1.AdmissionRequest) (map[string]string, string, error) {
	var job batchv1.Job
	err := json.Unmarshal(req.Object.Raw, &job)
	if err != nil {
		return nil, "", err
	}

	return job.Spec.Template.Annotations, defaultPodAnnotationsPath, nil
}

func fromCronJob(req *admissionv1.AdmissionRequest) (map[string]string, string, error) {
	var cronJob batchv1Beta.CronJob
	err := json.Unmarshal(req.Object.Raw, &cronJob)
	if err != nil {
		return nil, "", err
	}

	return cronJob.Spec.JobTemplate.Spec.Template.Annotations, cronJobPodAnnotationsPath, nil
}
