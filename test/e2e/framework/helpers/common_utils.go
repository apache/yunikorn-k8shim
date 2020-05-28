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
	"fmt"
	"github.com/google/uuid"
	"github.com/onsi/ginkgo"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"yunikorn-qe/framework/cfg_manager"
)

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
	return filepath.Join(cfg_manager.TestResultsPath, prefix, testName)
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
	err = ioutil.WriteFile(finalPath, data, cfg_manager.LogPerm)
	return err
}

func check(err error) {
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "ERROR: %v\n", err)
	}
}

func GetFileContents(filename string) []byte{
	data, err := ioutil.ReadFile(filename)
	check(err)
	return data
}

func GetYKUrl() string{
	return fmt.Sprintf("%s://%s:%s",
		cfg_manager.YuniKornTestConfig.YkScheme,
		cfg_manager.YuniKornTestConfig.YkHost,
		cfg_manager.YuniKornTestConfig.YkPort,
		)
}

func GetYKHost() string{
	return fmt.Sprintf("%s:%s",
		cfg_manager.YuniKornTestConfig.YkHost,
		cfg_manager.YuniKornTestConfig.YkPort,
	)
}

func GetYKScheme() string{
	return cfg_manager.YuniKornTestConfig.YkScheme
}

func GetUUID() string{
	return uuid.New().String()
}
