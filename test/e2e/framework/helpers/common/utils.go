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

package common

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func GetAbsPath(p string) (string, error) {
	path, err := filepath.Abs(p)
	return path, err
}

// GetTestName returns the test Name in a single string without spaces or /
func GetTestName() string {
	testDesc := ginkgo.CurrentGinkgoTestDescription()
	name := strings.Replace(testDesc.FullTestText, " ", "_", -1)
	name = strings.Trim(name, "*")
	return strings.Replace(name, "/", "-", -1)
}

// ReportDirectoryPath determines the directory path.
func ReportDirectoryPath() string {
	prefix := ""
	testName := GetTestName()
	return filepath.Join(configmanager.TestResultsPath, prefix, testName)
}

// CreateReportDirectory creates and returns the directory path
// If the directory cannot be created it'll return an error
func CreateReportDirectory() (string, error) {
	testPath := ReportDirectoryPath()
	if _, err := os.Stat(testPath); err == nil {
		return testPath, nil
	}
	err := os.MkdirAll(testPath, os.ModePerm)
	return testPath, err
}

// CreateLogFile creates the ReportDirectory if it is not present, writes the
// given testdata to the given filename.
func CreateLogFile(filename string, data []byte) error {
	path, err := CreateReportDirectory()
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "ReportDirectory cannot be created: %v\n", err)
		return err
	}

	finalPath := filepath.Join(path, filename)
	err = ioutil.WriteFile(finalPath, data, configmanager.LogPerm)
	return err
}

func GetFileContents(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	return data, err
}

func GetUUID() string {
	return uuid.NewString()
}

func RandSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func SliceExists(slice interface{}, item interface{}) (bool, error) {
	s := reflect.ValueOf(slice)

	if s.Kind() != reflect.Slice {
		return false, errors.New("SliceExists() given a non-slice type")
	}

	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == item {
			return true, nil
		}
	}
	return false, errors.New("slice does not exists")
}

// Takes 2 restClient responses with queue information, and checks if they are the same by
// 		1. Validates queue name is same
//		2. Validates qA and qB have same number of sub-queues
// 		3a. If same queue name and 0 sub-queues, return true
//		3b. Otherwise, call CompareQueueMap on each child pairing of qA and qB
func CompareQueueMap(a map[string]interface{}, b map[string]interface{}) (bool, error) {
	// Validate queue name
	if a["queuename"] != b["queuename"] {
		return false, fmt.Errorf("%s is not equal to %s", a["queuename"], b["queuename"])
	}

	aChildren, err := GetSubQueues(a)
	if err != nil {
		return false, err
	}
	bChildren, err := GetSubQueues(b)
	if err != nil {
		return false, err
	}

	if len(aChildren) == 0 && len(bChildren) == 0 {
		return true, nil
	}
	if len(aChildren) != len(bChildren) {
		return false, fmt.Errorf("queue A has %d sub-queues, but queue B has %d", len(aChildren), len(bChildren))
	}

	// Fix order of children
	aOrder, bOrder := make(map[string]int), make(map[string]int)
	for i := 0; i < len(aChildren); i++ {
		aChildName, ok := aChildren[i]["queuename"].(string)
		if !ok {
			return false, fmt.Errorf("casting error")
		}
		bChildName, ok := bChildren[i]["queuename"].(string)
		if !ok {
			return false, fmt.Errorf("casting error")
		}
		aOrder[aChildName], bOrder[bChildName] = i, i
	}

	// Verify children
	res := true
	for childName, aIdx := range aOrder {
		var curRes bool
		if bIdx, contains := bOrder[childName]; contains {
			aChild := aChildren[aIdx]
			bChild := bChildren[bIdx]
			curRes, err = CompareQueueMap(aChild, bChild)
			if err != nil {
				return false, err
			}
			res = res && curRes
		} else {
			return false, fmt.Errorf("queue B does not contain %s", childName)
		}
	}
	return res, err
}

// Returns a list of the children Q's of input. If no children, returns nil
func GetSubQueues(q map[string]interface{}) ([]map[string]interface{}, error) {
	qs, ok := q["queues"]
	if !ok {
		return nil, fmt.Errorf("Invalid arguments")
	}

	if qs == nil {
		return nil, nil
	}

	var subQs []map[string]interface{}
	subQResp, ok := qs.([]interface{})
	if !ok {
		return nil, fmt.Errorf("casting error")
	}
	for _, qResp := range subQResp {
		subQ, ok := qResp.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("casting error")
		}
		subQs = append(subQs, subQ)
	}

	return subQs, nil
}
