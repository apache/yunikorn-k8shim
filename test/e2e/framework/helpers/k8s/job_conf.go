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
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type JobConfig struct {
	Parallelism int32
	Name        string
	Namespace   string
	PodConfig   TestPodConfig
}

func InitJobConfig(conf JobConfig) (*batchv1.Job, error) {
	pod, err := InitTestPod(conf.PodConfig)
	if err != nil {
		return nil, err
	}
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      conf.Name,
			Namespace: conf.Namespace,
		},
		Spec: batchv1.JobSpec{
			Completions: &conf.Parallelism,
			Parallelism: &conf.Parallelism,
			Template: v1.PodTemplateSpec{
				ObjectMeta: pod.ObjectMeta,
				Spec:       pod.Spec,
			},
		},
	}

	job.Spec.Template.Spec.RestartPolicy = v1.RestartPolicyNever // Job only supports "OnFailure" or "Never"

	return &job, nil
}
