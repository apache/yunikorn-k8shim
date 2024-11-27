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

package admission_controller_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

const appName = "sleep"

var suiteName string
var kubeClient k8s.KubeCtl
var ns string
var bypassNs = "kube-system"
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)
var one = int32(1)
var preemptPolicyNever = v1.PreemptNever
var preemptPolicyPreemptLower = v1.PreemptLowerPriority

var testPod = v1.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Name:   "sleepjob",
		Labels: map[string]string{"app": appName},
	},
	Spec: v1.PodSpec{
		Containers: []v1.Container{
			{
				Name:    "sleepjob",
				Image:   "alpine:latest",
				Command: []string{"sleep", "30"},
			},
		},
	},
}

var testDeployment = appsv1.Deployment{
	Spec: appsv1.DeploymentSpec{
		Replicas: &one,
		Selector: &metav1.LabelSelector{
			MatchLabels: testPod.Labels,
		},
		Template: getPodSpec(v1.RestartPolicyAlways),
	},
	ObjectMeta: testPod.ObjectMeta,
}

var testJob = batchv1.Job{
	Spec: batchv1.JobSpec{
		Parallelism: &one,
		Completions: &one,
		Template:    getPodSpec(v1.RestartPolicyNever),
	},
	ObjectMeta: testPod.ObjectMeta,
}

var testStatefulSet = appsv1.StatefulSet{
	Spec: appsv1.StatefulSetSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: testPod.Labels,
		},
		Template: getPodSpec(v1.RestartPolicyAlways),
	},
	ObjectMeta: testPod.ObjectMeta,
}

var testReplicaSet = appsv1.ReplicaSet{
	Spec: appsv1.ReplicaSetSpec{
		Replicas: &one,
		Selector: &metav1.LabelSelector{
			MatchLabels: testPod.Labels,
		},
		Template: getPodSpec(v1.RestartPolicyAlways),
	},
	ObjectMeta: testPod.ObjectMeta,
}

var testDaemonSet = appsv1.DaemonSet{
	Spec: appsv1.DaemonSetSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: testPod.Labels,
		},
		Template: getPodSpec(v1.RestartPolicyAlways),
	},
	ObjectMeta: testPod.ObjectMeta,
}

var testCronJob = batchv1.CronJob{
	Spec: batchv1.CronJobSpec{
		Schedule: "* * * * *",
		JobTemplate: batchv1.JobTemplateSpec{
			Spec: batchv1.JobSpec{
				Parallelism: &one,
				Completions: &one,
				Template:    getPodSpec(v1.RestartPolicyNever),
			},
			ObjectMeta: testPod.ObjectMeta,
		},
	},
	ObjectMeta: testPod.ObjectMeta,
}

var testPreemptPriorityClass = schedulingv1.PriorityClass{
	ObjectMeta: metav1.ObjectMeta{
		Name:        "yk-test-preempt",
		Annotations: map[string]string{constants.AnnotationAllowPreemption: constants.True},
	},
	Value:            2000,
	PreemptionPolicy: &preemptPolicyPreemptLower,
}

var testNonPreemptPriorityClass = schedulingv1.PriorityClass{
	ObjectMeta: metav1.ObjectMeta{
		Name:        "yk-test-non-preempt",
		Annotations: map[string]string{constants.AnnotationAllowPreemption: constants.False},
	},
	Value:            1000,
	PreemptionPolicy: &preemptPolicyNever,
}

var testNonYkPriorityClass = schedulingv1.PriorityClass{
	ObjectMeta: metav1.ObjectMeta{
		Name: "yk-test-non-yk",
	},
	Value:            1500,
	PreemptionPolicy: &preemptPolicyPreemptLower,
}

func getPodSpec(restartPolicy v1.RestartPolicy) v1.PodTemplateSpec {
	p := testPod.DeepCopy()
	p.Spec.RestartPolicy = restartPolicy
	return v1.PodTemplateSpec{
		ObjectMeta: p.ObjectMeta,
		Spec:       p.Spec,
	}
}

func TestAdmissionController(t *testing.T) {
	ReportAfterSuite("TestAdmissionController", func(report Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-admission_controller_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	RegisterFailHandler(Fail)
	RunSpecs(t, "Admission Controller Suite")
}

var _ = BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	restClient = yunikorn.RClient{}

	kubeClient = k8s.KubeCtl{}
	Expect(kubeClient.SetClient()).To(Succeed())

	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")

	By("Port-forward the scheduler pod")
	err := kubeClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(HaveOccurred())

	By(fmt.Sprintf("Creating priority class %s", testPreemptPriorityClass.Name))
	_, err = kubeClient.CreatePriorityClass(&testPreemptPriorityClass)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Creating priority class %s", testNonPreemptPriorityClass.Name))
	_, err = kubeClient.CreatePriorityClass(&testNonPreemptPriorityClass)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Creating priority class %s", testNonYkPriorityClass.Name))
	_, err = kubeClient.CreatePriorityClass(&testNonYkPriorityClass)
	Ω(err).ShouldNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	kubeClient = k8s.KubeCtl{}
	Expect(kubeClient.SetClient()).To(Succeed())

	By(fmt.Sprintf("Removing priority class %s", testNonYkPriorityClass.Name))
	err := kubeClient.DeletePriorityClass(testNonYkPriorityClass.Name)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Removing priority class %s", testNonPreemptPriorityClass.Name))
	err = kubeClient.DeletePriorityClass(testNonPreemptPriorityClass.Name)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Removing priority class %s", testPreemptPriorityClass.Name))
	err = kubeClient.DeletePriorityClass(testPreemptPriorityClass.Name)
	Ω(err).ShouldNot(HaveOccurred())

	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})
