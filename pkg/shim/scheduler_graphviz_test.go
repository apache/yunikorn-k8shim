//go:build graphviz
// +build graphviz

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

package shim

import (
	"fmt"
	"os"
	"testing"

	"github.com/looplab/fsm"
	"gotest.tools/assert"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestSchedulerFsmGraph(t *testing.T) {
	var callback api.ResourceManagerCallback

	mockedAMProtocol := cache.NewMockedAMProtocol()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.GetAPIs().SchedulerAPI = test.NewSchedulerAPIMock().RegisterFunction(
		func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, fmt.Errorf("some error")
		})

	ctx := cache.NewContext(mockedAPIProvider)
	shim := newShimSchedulerInternal(ctx, mockedAPIProvider,
		appmgmt.NewAMService(mockedAMProtocol, mockedAPIProvider), callback)

	graph := fsm.Visualize(shim.stateMachine)

	err := os.MkdirAll("../../_output/fsm", 0755)
	assert.NilError(t, err, "Creating output dir failed")
	os.WriteFile("../../_output/fsm/k8shim-scheduler-state.dot", []byte(graph), 0644)
	assert.NilError(t, err, "Writing graph failed")
}
