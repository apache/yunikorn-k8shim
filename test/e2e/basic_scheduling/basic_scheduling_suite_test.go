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

package basicscheduling_test

import (
	"path/filepath"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/onsi/ginkgo/reporters"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

var oldConfigMap *v1.ConfigMap

func TestBasicScheduling(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	junitReporter := reporters.NewJUnitReporter(filepath.Join(configmanager.YuniKornTestConfig.LogDir, "basic_scheduling_junit.xml"))
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "TestBasicScheduling", []ginkgo.Reporter{junitReporter})
}

var By = ginkgo.By

var Ω = gomega.Ω
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
var BeEquivalentTo = gomega.BeEquivalentTo
