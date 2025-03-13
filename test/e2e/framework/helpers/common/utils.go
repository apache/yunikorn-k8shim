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
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

var MasterTaints = map[string]struct{}{
	"node-role.kubernetes.io/control-plane": {},
	"node-role.kubernetes.io/master":        {},
}

var ComputeNodeLabels = map[string]struct{}{
	"compute": {},
}

var RoleNodeLabel = "role"

func GetAbsPath(p string) (string, error) {
	path, err := filepath.Abs(p)
	return path, err
}

// GetTestName returns the test Name in a single string without spaces or /
func GetTestName() string {
	//nolint
	testReport := ginkgo.CurrentSpecReport()
	name := strings.ReplaceAll(testReport.FullText(), " ", "_")
	name = strings.Trim(name, "*")
	return strings.ReplaceAll(name, "/", "-")
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

// CreateDirectoryForJunitReports and returns the directory path
// If the directory cannot be created it'll return an error
func CreateJUnitReportDir() error {
	dir := configmanager.YuniKornTestConfig.LogDir
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	err := os.MkdirAll(dir, os.ModePerm)
	return err
}

func GetFileContents(filename string) ([]byte, error) {
	data, err := os.ReadFile(filename)
	return data, err
}

func GetUUID() string {
	return uuid.NewString()
}

func RandSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		//nolint:gosec
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
//  1. Validates queue name is same
//  2. Validates qA and qB have same number of sub-queues
//     3a. If same queue name and 0 sub-queues, return true
//     3b. Otherwise, call CompareQueueMap on each child pairing of qA and qB
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
		return nil, fmt.Errorf("invalid arguments")
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

func RunShellCmdForeground(cmdStr string) (string, error) {
	if len(cmdStr) == 0 {
		return "", fmt.Errorf("not enough arguments")
	}

	cmdSplit := strings.Fields(cmdStr)

	execPath, err := exec.LookPath(cmdSplit[0])
	if err != nil {
		return "", err
	}

	errStream := new(bytes.Buffer)
	stdOutStream := new(bytes.Buffer)
	cmd := &exec.Cmd{
		Path:   execPath,
		Args:   cmdSplit,
		Stdout: stdOutStream,
		Stderr: errStream,
	}

	if err := cmd.Run(); err != nil {
		return "", err
	}

	if errStr := errStream.String(); len(errStr) > 0 {
		return stdOutStream.String(), errors.New(errStr)
	}

	return stdOutStream.String(), nil
}

func CreateLogFile(suiteName string, specName string, logType string, extension string) (*os.File, error) {
	filePath, err := getLogFilePath(suiteName, specName, logType, extension)
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(filePath)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(ginkgo.GinkgoWriter, "Created log file: %s\n", filePath)
	return file, nil
}

func getLogFilePath(suiteName string, specName string, logType string, extension string) (string, error) {
	gitRoot, runErr := RunShellCmdForeground("git rev-parse --show-toplevel")
	if runErr != nil {
		return "", runErr
	}
	gitRoot = strings.TrimSpace(gitRoot)
	suiteName = replaceInvalidFileChars(strings.TrimSpace(suiteName))
	specName = replaceInvalidFileChars(strings.TrimSpace(specName))

	dumpLogFilePath := filepath.Join(gitRoot, configmanager.LogPath, suiteName, fmt.Sprintf("%s_%s.%s", specName, logType, extension))
	return dumpLogFilePath, nil
}

func replaceInvalidFileChars(str string) string {
	// some charaters are not allowed in upload-artifact : https://github.com/actions/upload-artifact/issues/333
	invalidChars := []string{"\"", ":", "<", ">", "|", "*", "?", "\r", "\n"}
	for _, char := range invalidChars {
		str = strings.ReplaceAll(str, char, "_")
	}
	return str
}

func GetSuiteName(testFilePath string) string {
	dir := filepath.Dir(testFilePath)
	suiteName := filepath.Base(dir)
	return suiteName
}
