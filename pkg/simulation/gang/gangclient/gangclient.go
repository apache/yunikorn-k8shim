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
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	jobID := os.Getenv("JOB_ID")
	if jobID == "" {
		err := errors.New("can't get the jobID from env")
		log.Fatal(err)
	}
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		err := errors.New("can't get the service name from env")
		log.Fatal(err)
	}
	memberAmount, err := strconv.Atoi(os.Getenv("MEMBER_AMOUNT"))
	if err != nil {
		err = errors.New("can't get the member amount from env")
		log.Fatal(err)
	}
	taskExecutionSeconds, err := strconv.Atoi(os.Getenv("TASK_EXECUTION_SECONDS"))
	if err != nil {
		err = errors.New("can't get the task execution seconds from env")
		log.Fatal(err)
	}
	serviceName = strings.ToUpper(serviceName) + "_SERVICE_HOST"
	serviceIP := os.Getenv(serviceName)
	err = addRequest(serviceIP, jobID)
	if err != nil {
		log.Fatal(err)
	}
	// Check if we satisfy gang minMember or not every 2 second
	for {
		number, err := checkRequest(serviceIP, jobID)
		if err != nil {
			log.Fatal(err)
		}
		if number >= memberAmount {
			log.Printf("satisfy gang minMember.")
			log.Printf("start to run task.")
			log.Printf("Task will run for %d min and %d sec.", taskExecutionSeconds/60, taskExecutionSeconds-((taskExecutionSeconds/60)*60))
			time.Sleep(time.Duration(taskExecutionSeconds) * time.Second)
			break
		}
		log.Printf("The ready member and expected member: %d/%d", number, memberAmount)
		time.Sleep(2 * time.Second)
	}
}

func addRequest(ip string, jobID string) error {
	_, err := http.Get("http://" + ip + ":8863" + "/ws/v1/add/" + jobID)
	if err != nil {
		return err
	}
	return nil
}

func checkRequest(ip string, jobID string) (int, error) {
	resp, err := http.Get("http://" + ip + ":8863" + "/ws/v1/check/" + jobID)
	var value int
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&value)
	if err != nil {
		return -1, err
	}
	return value, nil
}
