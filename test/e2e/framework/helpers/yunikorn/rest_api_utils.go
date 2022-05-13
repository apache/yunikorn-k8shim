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
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
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

func (c *RClient) GetQueues(partition string) (*dao.PartitionQueueDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.QueuesPath, partition), nil)
	if err != nil {
		return nil, err
	}
	var queues *dao.PartitionQueueDAOInfo
	_, err = c.do(req, &queues)
	return queues, err
}

func (c *RClient) GetSpecificQueueInfo(partition string, queueName string) (*dao.PartitionQueueDAOInfo, error) {
	queues, err := c.GetQueues(partition)
	if err != nil {
		return nil, err
	}
	var allSubQueues = queues.Children
	for _, subQ := range allSubQueues {
		if subQ.QueueName == queueName {
			return &subQ, nil
		}
	}
	return nil, fmt.Errorf("QueueInfo not found: %s", queueName)
}

func (c *RClient) GetHealthCheck() (dao.SchedulerHealthDAOInfo, error) {
	req, err := c.newRequest("GET", configmanager.HealthCheckPath, nil)
	if err != nil {
		return dao.SchedulerHealthDAOInfo{}, err
	}
	healthCheck := dao.SchedulerHealthDAOInfo{}
	_, err = c.do(req, &healthCheck)
	return healthCheck, err
}

func (c *RClient) GetApps(partition string, queueName string) ([]map[string]interface{}, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.AppsPath, partition, queueName), nil)
	if err != nil {
		return nil, err
	}
	var apps []map[string]interface{}
	_, err = c.do(req, &apps)
	return apps, err
}

func (c *RClient) GetAppInfo(partition string, queueName string, appID string) (map[string]interface{}, error) {
	apps, err := c.GetApps(partition, queueName)
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

func (c *RClient) GetAppsFromSpecificQueue(partition string, queueName string) ([]map[string]interface{}, error) {
	apps, err := c.GetApps(partition, queueName)
	if err != nil {
		return nil, err
	}
	var appsOfQueue []map[string]interface{}
	for _, appInfo := range apps {
		appsOfQueue = append(appsOfQueue, appInfo)
	}
	return appsOfQueue, nil
}

func (c *RClient) isAppInDesiredState(partition string, queueName string, appID string, state string) wait.ConditionFunc {
	return func() (bool, error) {
		appInfo, err := c.GetAppInfo(partition, queueName, appID)
		if err != nil {
			return false, nil // returning nil here for wait & loop
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

func (c *RClient) WaitForAppStateTransition(partition string, queueName string, appID string, state string, timeout int) error {
	return wait.PollImmediate(time.Second, time.Duration(timeout)*time.Second, c.isAppInDesiredState(partition, queueName, appID, state))
}

func (c *RClient) AreAllExecPodsAllotted(partition string, queueName string, appID string, execPodCount int) wait.ConditionFunc {
	return func() (bool, error) {
		appInfo, err := c.GetAppInfo(partition, queueName, appID)
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

func (c *RClient) ValidateSchedulerConfig(cm v1.ConfigMap) (*dao.ValidateConfResponse, error) {
	validateConfResponse := new(dao.ValidateConfResponse)
	req, err := c.newRequest("POST", configmanager.ValidateConfPath, &cm)
	if err != nil {
		return validateConfResponse, err
	}
	_, err = c.do(req, validateConfResponse)
	return validateConfResponse, err
}

func isRootSched(policy string) wait.ConditionFunc {
	return func() (bool, error) {
		restClient := RClient{}
		qInfo, err := restClient.GetQueues("default")
		if err != nil {
			return false, err
		}
		if qInfo == nil {
			return false, errors.New("no response from rest client")
		}

		if policy == "default" {
			return len(qInfo.Properties) == 0, nil
		} else if qInfo.Properties["application.sort.policy"] == policy {
			return true, nil
		}

		return false, nil
	}
}

func WaitForSchedPolicy(policy string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, isRootSched(policy))
}

func GetFailedHealthChecks() (string, error) {
	restClient := RClient{}
	var failCheck string
	healthCheck, err := restClient.GetHealthCheck()
	if err != nil {
		return "", fmt.Errorf("Failed to get scheduler health check from API")
	}
	if !healthCheck.Healthy {
		for _, check := range healthCheck.HealthChecks {
			if !check.Succeeded {
				failCheck += fmt.Sprintln("Name:", check.Name, "Message:", check.DiagnosisMessage)
			}
		}
	}
	return failCheck, nil
}

func (c *RClient) GetQueues2(partition string) (*dao.PartitionQueueDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.QueuesPath, partition), nil)
	if err != nil {
		return nil, err
	}
	var queues *dao.PartitionQueueDAOInfo
	_, err = c.do(req, &queues)
	return queues, err
}

func (c *RClient) GetQueue2(partition string, queueName string) (*dao.PartitionQueueDAOInfo, error) {
	queues, err := c.GetQueues2(partition)
	if err != nil {
		return nil, err
	}
	if queueName == "root" {
		return queues, nil
	}

	var allSubQueues = queues.Children
	for _, subQ := range allSubQueues {
		if subQ.QueueName == queueName {
			return &subQ, nil
		}
	}
	return nil, fmt.Errorf("QueueInfo not found: %s", queueName)
}

// Returns pointer to corresponding queue.
// Expects queuePath to be slice of queue names in order.
func getQueueHelper(subQs []dao.QueueDAOInfo, queuePath []string) (*dao.QueueDAOInfo, error) {
	for i, subQ := range subQs {
		if subQ.QueueName == queuePath[0] {
			if len(queuePath) == 1 { // path is single queue
				return &subQs[i], nil
			}
			return getQueueHelper(subQ.ChildQueues, queuePath[1:])
		}
	}
	return nil, errors.New("queue not in path")
}

// GetQueue returns pointer to corresponding queue.
// Expects queuePath to use periods as delimiters. ie "root.queueA.child"
func GetQueue(partition *dao.PartitionDAOInfo, queuePathStr string) (*dao.QueueDAOInfo, error) {
	if partition.Queues.QueueName == "" {
		return &dao.QueueDAOInfo{}, errors.New("partition has no queues")
	}

	path := strings.Split(queuePathStr, ".")
	if len(path) == 1 && path[0] == "root" {
		return &partition.Queues, nil
	}

	return getQueueHelper(partition.Queues.ChildQueues, path[1:])
}

// ConditionFunc returns true if queue timestamp property equals ts
// Expects queuePath to use periods as delimiters. ie "root.queueA.child"
func compareQueueTS(queuePathStr string, ts string) wait.ConditionFunc {
	return func() (bool, error) {
		restClient := RClient{}
		//pInfo, err := restClient.GetPartitions()
		//if err != nil {
		//	return false, err
		//}
		//if pInfo == nil {
		//	return false, errors.New("no response from rest client")
		//}
		//
		//qInfo, qErr := GetQueue(pInfo, queuePathStr)
		//if qErr != nil {
		//	return false, err
		//}
		qInfo, err := restClient.GetQueue2("default", "root")
		if err != nil {
			return false, err
		}

		return qInfo.Properties["timestamp"] == ts, nil
	}
}

// Expects queuePath to use periods as delimiters. ie "root.queueA.child"
func WaitForQueueTS(queuePathStr string, ts string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, compareQueueTS(queuePathStr, ts))
}
