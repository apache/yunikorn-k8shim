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

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	jobName := os.Getenv("jobName")
	serviceName := os.Getenv("serviceName")
	memberAmount, err := strconv.Atoi(os.Getenv("memberAmount"))
	if err != nil {
		memberAmount = 10
	}
	runtimeMin, err := strconv.Atoi(os.Getenv("runtimeMin"))
	if err != nil {
		runtimeMin = 1
	}
	serviceName = strings.ToUpper(serviceName) + "_SERVICE_HOST"
	serviceIP := os.Getenv(serviceName)
	addRequest(serviceIP, jobName)
	// Check if we satisfy gang minMember or not every 2 second
	for {
		number := checkRequest(serviceIP, jobName)
		if number >= memberAmount {
			fmt.Println("satisfy gang minMember.")
			fmt.Println("start to run job.")
			time.Sleep(time.Duration(runtimeMin) * time.Minute) // means the application start running job
			break
		}
		time.Sleep(2 * time.Second)
	}
}

func addRequest(ip string, jobName string) {
	resp, err := http.Get("http://" + ip + ":8863" + "/ws/v1/add/" + jobName)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Get request body fail.")
	}
	fmt.Println(string(body))
}

func checkRequest(ip string, jobName string) int {
	resp, err := http.Get("http://" + ip + ":8863" + "/ws/v1/check/" + jobName)
	var value int
	if err != nil {
		fmt.Println("Check jobMember fail.")
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&value)
	if err != nil {
		fmt.Println("Decode fail.")
	}
	return value
}
