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
package helpers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/cfg_manager"
	"io"
	"net/http"
	"net/url"
	"path"
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

func (c *RClient) GetQueues() (map[string]interface{}, error) {
	req, err := c.newRequest("GET", path.Join(GetYKUrl(), cfg_manager.QueuesPath), nil)
	if err != nil {
		return nil, err
	}
	var queues map[string]interface{}
	_, err = c.do(req, &queues)
	return queues, err
}

func (c *RClient) GetApps() ([]map[string]interface{}, error) {
	req, err := c.newRequest("GET", cfg_manager.AppsPath, nil)
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
	return nil, errors.New(fmt.Sprintf("AppInfo not found: %s", appID))
}
