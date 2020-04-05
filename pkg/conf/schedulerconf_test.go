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
package conf

import (
	"fmt"
	"sync"
	"testing"

	"gotest.tools/assert"
)

func TestDefaultValues(t *testing.T) {
	conf := GetSchedulerConf()
	assert.Equal(t, conf.ClusterID, DefaultClusterID)
	assert.Equal(t, conf.PolicyGroup, DefaultPolicyGroup)
	assert.Equal(t, conf.ClusterVersion, DefaultClusterVersion)
	assert.Equal(t, conf.SchedulerName, DefaultSchedulerName)
	assert.Equal(t, conf.LoggingLevel, DefaultLoggingLevel)
	assert.Equal(t, conf.LogEncoding, DefaultLogEncoding)
	assert.Equal(t, conf.EventChannelCapacity, DefaultEventChannelCapacity)
	assert.Equal(t, conf.DispatchTimeout, DefaultDispatchTimeout)
	assert.Equal(t, conf.KubeQPS, DefaultKubeQPS)
	assert.Equal(t, conf.KubeBurst, DefaultKubeBurst)
	assert.Equal(t, conf.Predicates, "")
}

// run some concurrent write/read
// if the Get and Set function is not thread safe,
// this will give data race warnings when running with -race flag
func TestConcurrency(t *testing.T) {
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		for i := 0; i < 10; i++ {
			fmt.Println(GetSchedulerConf().ClusterID)
			fmt.Println(GetSchedulerConf().ClusterVersion)
			fmt.Println(GetSchedulerConf().SchedulerName)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < 10; i++ {
			Set(&SchedulerConf{
				ClusterID:            "test-id",
				ClusterVersion:       "0.1",
				SchedulerName:        "yk",
			})
		}
		wg.Done()
	}()

	wg.Wait()
}
