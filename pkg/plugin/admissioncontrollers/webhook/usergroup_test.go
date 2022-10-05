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
	"regexp"
	"testing"

	"gotest.tools/assert"
)

const (
	userName       = "test"
	testAnnotation = "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}"
)

var (
	groups = []string{"system:authenticated", "devs"}
)

func TestValidateAnnotation(t *testing.T) {
	ac := getDefaultAdmissionController()

	err := ac.isAnnotationValid("xyzxyz")
	assert.ErrorContains(t, err, "invalid character 'x'")

	err = ac.isAnnotationValid(testAnnotation)
	assert.NilError(t, err)
}

func TestBypassControllers(t *testing.T) {
	ac := getDefaultAdmissionController()
	allowed := ac.isAnnotationAllowed("system:serviceaccount:kube-system:job-controller", groups)
	assert.Assert(t, allowed)
}

func TestExternalUsers(t *testing.T) {
	ac := getDefaultAdmissionController()
	extUsersRegexps := make([]*regexp.Regexp, 1)
	extUsersRegexps[0] = regexp.MustCompile("yunikorn")
	ac.externalUsers = extUsersRegexps

	allowed := ac.isAnnotationAllowed("yunikorn", groups)
	assert.Assert(t, allowed)
}

func TestExternalGroups(t *testing.T) {
	ac := getDefaultAdmissionController()
	extGroupsRegexps := make([]*regexp.Regexp, 1)
	extGroupsRegexps[0] = regexp.MustCompile("devs")
	ac.externalGroups = extGroupsRegexps

	allowed := ac.isAnnotationAllowed(userName, groups)
	assert.Assert(t, allowed)
}

func TestExternalAuthenticationDenied(t *testing.T) {
	ac := getDefaultAdmissionController()
	allowed := ac.isAnnotationAllowed("yunikorn", groups)
	assert.Assert(t, !allowed)
}

func getDefaultAdmissionController() *admissionController {
	sysUsersRegexps := make([]*regexp.Regexp, 1)
	sysUsersRegexps[0] = regexp.MustCompile(defaultSystemUsers)
	extUsersRegexps := make([]*regexp.Regexp, 0)
	extGroupsRegexps := make([]*regexp.Regexp, 0)

	ac := admissionController{
		configName:               "test",
		schedulerValidateConfURL: "/test",
		bypassAuth:               false,
		bypassControllers:        true,
		systemUsers:              sysUsersRegexps,
		externalGroups:           extUsersRegexps,
		externalUsers:            extGroupsRegexps,
	}

	return &ac
}
