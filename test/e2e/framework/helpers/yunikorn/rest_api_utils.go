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

package yunikorn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
)

type RClient struct {
	BaseURL   *url.URL
	UserAgent string

	httpClient *http.Client
}

func (c *RClient) newRequest(method, path string, body interface{}) (*http.Request, error) {
	rel := &url.URL{Path: path}
	if c.BaseURL == nil {
		c.BaseURL = &url.URL{Host: GetYKHost(),
			Scheme: GetYKScheme()}
	}
	u := c.BaseURL.ResolveReference(rel)
	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}
	if len(c.UserAgent) == 0 {
		c.UserAgent = "Golang_Spider_Bot/3.0"
	}
	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	return req, nil
}
func (c *RClient) do(req *http.Request, v interface{}) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(v)
	return resp, err
}

func (c *RClient) GetPartitions() (*dao.PartitionDAOInfo, error) {
	req, err := c.newRequest("GET", configmanager.QueuesPath, nil)
	if err != nil {
		return nil, err
	}
	partitions := new(dao.PartitionDAOInfo)
	_, err = c.do(req, partitions)
	return partitions, err
}

func (c *RClient) GetQueues() (map[string]interface{}, error) {
	req, err := c.newRequest("GET", configmanager.QueuesPath, nil)
	if err != nil {
		return nil, err
	}
	var queues map[string]interface{}
	_, err = c.do(req, &queues)
	return queues, err
}

func (c *RClient) GetSpecificQueueInfo(queueName string) (map[string]interface{}, error) {
	queues, err := c.GetQueues()
	if err != nil {
		return nil, err
	}
	//root queue
	var rootQ = queues["queues"].(map[string]interface{})
	var allSubQueues = rootQ["queues"].([]interface{})
	for _, s := range allSubQueues {
		var subQ, success = s.(map[string]interface{})
		if !success {
			return nil, errors.New("map typecast failed")
		}
		if subQ["queuename"] == queueName {
			return subQ, nil
		}
	}
	return nil, fmt.Errorf("QueueInfo not found: %s", queueName)
}

func (c *RClient) GetApps() ([]map[string]interface{}, error) {
	req, err := c.newRequest("GET", configmanager.AppsPath, nil)
	if err != nil {
		return nil, err
	}
	var apps []map[string]interface{}
	_, err = c.do(req, &apps)
	return apps, err
}

func (c *RClient) GetAppInfo(appID string) (map[string]interface{}, error) {
	apps, err := c.GetApps()
	if err != nil {
		return nil, err
	}
	for _, appInfo := range apps {
		for key, element := range appInfo {
			if key == "applicationID" && element == appID {
				return appInfo, nil
			}
		}
	}
	return nil, fmt.Errorf("AppInfo not found: %s", appID)
}

func (c *RClient) GetAppsFromSpecificQueue(queueName string) ([]map[string]interface{}, error) {
	apps, err := c.GetApps()
	if err != nil {
		return nil, err
	}
	var appsOfQueue []map[string]interface{}
	for _, appInfo := range apps {
		for key, element := range appInfo {
			if key == "queueName" && element == queueName {
				appsOfQueue = append(appsOfQueue, appInfo)
			}
		}
	}
	return appsOfQueue, nil
}

func (c *RClient) isAppInDesiredState(appID string, state string) wait.ConditionFunc {
	return func() (bool, error) {
		appInfo, err := c.GetAppInfo(appID)
		if err != nil {
			return false, nil //returning nil here for wait & loop
		}

		switch appInfo["applicationState"] {
		case state:
			return true, nil
		case States().Application.Rejected:
			return false, fmt.Errorf(fmt.Sprintf("App not in desired state: %s", state))
		}
		return false, nil
	}
}

func (c *RClient) WaitForAppStateTransition(appID string, state string, timeout int) error {
	return wait.PollImmediate(time.Second, time.Duration(timeout)*time.Second, c.isAppInDesiredState(appID, state))
}

func (c *RClient) AreAllExecPodsAllotted(appID string, execPodCount int) wait.ConditionFunc {
	return func() (bool, error) {
		appInfo, err := c.GetAppInfo(appID)
		if err != nil {
			return false, err
		}

		if appInfo["allocations"] == nil {
			return false, fmt.Errorf(fmt.Sprintf("Allocations are not yet complete for appID: %s", appID))
		}
		if len(appInfo["allocations"].([]interface{})) >= execPodCount {
			return true, nil
		}
		return false, nil
	}
}

func isRootSched(policy string) wait.ConditionFunc {
	return func() (bool, error) {
		restClient := RClient{}
		pInfo, err := restClient.GetPartitions()
		if err != nil {
			return false, err
		}
		if pInfo == nil {
			return false, errors.New("no response from rest client")
		}

		rootInfo := pInfo.Queues
		if policy == "default" {
			return len(rootInfo.Properties) == 0, nil
		} else if rootInfo.Properties["application.sort.policy"] == policy {
			return true, nil
		}

		return false, nil
	}
}

func WaitForSchedPolicy(policy string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, isRootSched(policy))
}
