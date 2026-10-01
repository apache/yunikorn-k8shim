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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/apache/yunikorn-k8shim/pkg/admission/common"
)

type extractResult struct {
	annotations map[string]string
	labels      map[string]string
	path        string
}

type extractorFn func(obj runtime.RawExtension) (*extractResult, error)

var (
	Deployment  = reflect.TypeFor[appsv1.Deployment]().Name()
	DaemonSet   = reflect.TypeFor[appsv1.DaemonSet]().Name()
	StatefulSet = reflect.TypeFor[appsv1.StatefulSet]().Name()
	ReplicaSet  = reflect.TypeFor[appsv1.ReplicaSet]().Name()
	Job         = reflect.TypeFor[batchv1.Job]().Name()
	CronJob     = reflect.TypeFor[batchv1.CronJob]().Name()
	Pod         = reflect.TypeFor[corev1.Pod]().Name()

	extractors = map[string]extractorFn{
		Deployment:  fromDeployment,
		DaemonSet:   fromDaemonSet,
		StatefulSet: fromStatefulSet,
		ReplicaSet:  fromReplicaSet,
		Job:         fromJob,
		CronJob:     fromCronJob,
		Pod:         fromPod,
	}
)

func getResultFromRequest(req *admissionv1.AdmissionRequest) (*extractResult, error) {
	return extractFromReq(req, false)
}

// extractFromReq loads the labels and annotations from the object, can handle create and update requests.
// Supports all kinds.
func extractFromReq(req *admissionv1.AdmissionRequest, old bool) (*extractResult, error) {
	extractFn, ok := extractors[req.Kind.Kind]
	if !ok {
		return nil, common.ErrorUnsupportedKind
	}
	if old {
		return extractFn(req.OldObject)
	}
	return extractFn(req.Object)
}

func fromDeployment(obj runtime.RawExtension) (*extractResult, error) {
	var deployment appsv1.Deployment
	err := json.Unmarshal(obj.Raw, &deployment)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: deployment.Spec.Template.Annotations,
		labels:      deployment.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromDaemonSet(obj runtime.RawExtension) (*extractResult, error) {
	var daemonSet appsv1.DaemonSet
	err := json.Unmarshal(obj.Raw, &daemonSet)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: daemonSet.Spec.Template.Annotations,
		labels:      daemonSet.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromStatefulSet(obj runtime.RawExtension) (*extractResult, error) {
	var statefulSet appsv1.StatefulSet
	err := json.Unmarshal(obj.Raw, &statefulSet)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: statefulSet.Spec.Template.Annotations,
		labels:      statefulSet.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromReplicaSet(obj runtime.RawExtension) (*extractResult, error) {
	var replicaSet appsv1.ReplicaSet
	err := json.Unmarshal(obj.Raw, &replicaSet)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: replicaSet.Spec.Template.Annotations,
		labels:      replicaSet.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromJob(obj runtime.RawExtension) (*extractResult, error) {
	var job batchv1.Job
	err := json.Unmarshal(obj.Raw, &job)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: job.Spec.Template.Annotations,
		labels:      job.Spec.Template.Labels,
		path:        defaultPodAnnotationsPath,
	}, nil
}

func fromCronJob(obj runtime.RawExtension) (*extractResult, error) {
	var cronJob batchv1.CronJob
	err := json.Unmarshal(obj.Raw, &cronJob)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: cronJob.Spec.JobTemplate.Spec.Template.Annotations,
		labels:      cronJob.Spec.JobTemplate.Spec.Template.Labels,
		path:        cronJobPodAnnotationsPath,
	}, nil
}

func fromPod(obj runtime.RawExtension) (*extractResult, error) {
	var pod corev1.Pod
	err := json.Unmarshal(obj.Raw, &pod)
	if err != nil {
		return nil, err
	}

	return &extractResult{
		annotations: pod.Annotations,
		labels:      pod.Labels,
		path:        PodAnnotationsPath,
	}, nil
}
