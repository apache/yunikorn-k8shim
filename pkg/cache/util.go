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
package cache

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

func GetAllPods(context *Context, appId string) ([]*v1.Pod, error) {
	app, ok := context.applications[appId]
	if !ok {
		return nil, fmt.Errorf("could not find application %s", appId)
	}
	pods := make([]*v1.Pod, 0)
	for _, task := range app.taskMap {
		pods = append(pods, task.GetTaskPod())
	}
	return pods, nil
}
