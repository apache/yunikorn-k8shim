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

	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

const (
	noOfInformers = 16 // total number of active informers
)

func TestWaitForSync(t *testing.T) {
	clients := getClients()
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
	clients := getClients()
	test.RunningInformers.Store(0)
	stop := make(chan struct{})
	defer func() {
		if !stopped {
			close(stop)
		}
	}()

	clients.Run(stop)
	err := utils.WaitForCondition(func() bool {
		return test.RunningInformers.Load() == noOfInformers
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "number of running informers: expected %d got %d", noOfInformers, test.RunningInformers.Load())

	close(stop)
	stopped = true
	err = utils.WaitForCondition(func() bool {
		return test.RunningInformers.Load() == 0
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "no. of informers still running: %d", test.RunningInformers.Load())
}

func getClients() *Clients {
	return &Clients{
		ConfigMapInformer:             test.NewMockedConfigMapInformer(),
		CSIDriverInformer:             NewMockedCSIDriverInformer(),
		CSINodeInformer:               NewMockedCSINodeInformer(),
		CSIStorageCapacityInformer:    NewMockedCSIStorageCapacityInformer(),
		NamespaceInformer:             test.NewMockNamespaceInformer(false),
		NodeInformer:                  test.NewMockedNodeInformer(),
		PodInformer:                   test.NewMockedPodInformer(),
		PriorityClassInformer:         test.NewMockPriorityClassInformer(),
		PVCInformer:                   NewMockedPersistentVolumeClaimInformer(),
		PVInformer:                    NewMockedPersistentVolumeInformer(),
		ReplicaSetInformer:            NewMockedReplicaSetInformer(),
		ReplicationControllerInformer: NewMockedReplicationControllerInformer(),
		ServiceInformer:               NewMockedServiceInformer(),
		StatefulSetInformer:           NewMockedStatefulSetInformer(),
		StorageClassInformer:          NewMockedStorageClassInformer(),
		VolumeAttachmentInformer:      test.NewMockVolumeAttachmentInformer(),
	}
}
