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

package client

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

const (
	noOfInformers = 9 // total number of active informers
)

func TestWaitForSync(t *testing.T) {
	clients := getTestClients()
	test.SyncDone.Store(false)
	go func() {
		time.Sleep(500 * time.Millisecond)
		test.SyncDone.Store(true)
	}()

	start := time.Now()
	clients.WaitForSync()
	diff := time.Since(start)
	assert.Equal(t, int64(1000), diff.Truncate(time.Second).Milliseconds(), "WaitForSync() didn't block for 1 second")
}

func TestRun(t *testing.T) {
	stopped := false
	clients := getTestClients()
	test.RunningInformers.Store(0)
	stop := make(chan struct{})
	defer func() {
		if !stopped {
			close(stop)
		}
	}()

	clients.Run(stop)
	err := common.WaitForCondition(func() bool {
		return test.RunningInformers.Load() == noOfInformers
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "number of running informers: expected %d got %d", noOfInformers, test.RunningInformers.Load())

	close(stop)
	stopped = true
	err = common.WaitForCondition(func() bool {
		return test.RunningInformers.Load() == 0
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "no. of informers still running: %d", test.RunningInformers.Load())
}

func TestNewClients(t *testing.T) {
	watcherStarted := make(chan struct{})
	client := fake.NewSimpleClientset()
	client.PrependWatchReactor("*", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := client.Tracker().Watch(gvr, ns)
		if err != nil {
			return false, nil, err
		}
		close(watcherStarted)
		return true, watch, nil
	})

	informerFactory := informers.NewSharedInformerFactory(client, 0)
	emptySchedulerConf := conf.SchedulerConf{}
	mockKube := NewKubeClientMock(false)

	clt := NewClients(nil, informerFactory, &emptySchedulerConf, mockKube)
	informers := clt.getInformers()
	assert.Equal(t, len(informers), len(informerTypes))
}

func getTestClients() *Clients {
	hasInformers := []hasInformer{}

	podInformer := save(test.NewMockedPodInformer(), &hasInformers)
	nodeInformer := save(test.NewMockedNodeInformer(), &hasInformers)
	configMapInformer := save(test.NewMockedConfigMapInformer(), &hasInformers)
	storageInformer := save(NewMockedStorageClassInformer(), &hasInformers)
	pvInformer := save(NewMockedPersistentVolumeInformer(), &hasInformers)
	pvcInformer := save(NewMockedPersistentVolumeClaimInformer(), &hasInformers)
	priorityClassInformer := save(test.NewMockPriorityClassInformer(), &hasInformers)
	namespaceInformer := save(test.NewMockNamespaceInformer(false), &hasInformers)
	csiNodeInformer := save(NewMockedCSINodeInformer(), &hasInformers)

	return &Clients{
		hasInformers:          hasInformers,
		PodInformer:           podInformer,
		NodeInformer:          nodeInformer,
		ConfigMapInformer:     configMapInformer,
		PVInformer:            pvInformer,
		PVCInformer:           pvcInformer,
		NamespaceInformer:     namespaceInformer,
		StorageInformer:       storageInformer,
		CSINodeInformer:       csiNodeInformer,
		PriorityClassInformer: priorityClassInformer,
	}
}
