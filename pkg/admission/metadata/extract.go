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

package metadata

import (
	"encoding/json"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1Beta "k8s.io/api/batch/v1beta1"
)

type extractResult struct {
	annotations map[string]string
	labels      map[string]string
	path        string
}

type extractor func(*admissionv1.AdmissionRequest) (*extractResult, error)

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

func fromDeployment(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	var deployment appsv1.Deployment
	err := json.Unmarshal(req.Object.Raw, &deployment)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: deployment.Spec.Template.Annotations,
		labels:      deployment.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromDaemonSet(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	var daemonSet appsv1.DaemonSet
	err := json.Unmarshal(req.Object.Raw, &daemonSet)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: daemonSet.Spec.Template.Annotations,
		labels:      daemonSet.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromStatefulSet(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	var statefulSet appsv1.StatefulSet
	err := json.Unmarshal(req.Object.Raw, &statefulSet)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: statefulSet.Spec.Template.Annotations,
		labels:      statefulSet.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromReplicaSet(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	var replicaSet appsv1.ReplicaSet
	err := json.Unmarshal(req.Object.Raw, &replicaSet)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: replicaSet.Spec.Template.Annotations,
		labels:      replicaSet.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromJob(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	var job batchv1.Job
	err := json.Unmarshal(req.Object.Raw, &job)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: job.Spec.Template.Annotations,
		labels:      job.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromCronJob(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	var cronJob batchv1Beta.CronJob
	err := json.Unmarshal(req.Object.Raw, &cronJob)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: cronJob.Spec.JobTemplate.Spec.Template.Annotations,
		labels:      cronJob.Spec.JobTemplate.Spec.Template.Labels,
		path:        cronJobPodAnnotationsPath,
	}, nil
}
