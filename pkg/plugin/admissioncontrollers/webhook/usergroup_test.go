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
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
)

const testAnnotation = "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}"

func TestUnmarshalAnnotationFails(t *testing.T) {
	ac := getDefaultAdmissionController()
	req := getAdmissionRequest()

	err := ac.validateExternalUserInfo(req, "xyzxyz")
	assert.ErrorContains(t, err, "invalid character 'x'")
}

func TestBypassControllers(t *testing.T) {
	ac := getDefaultAdmissionController()
	req := getAdmissionRequest()
	req.UserInfo.Username = "system:serviceaccount:kube-system:job-controller"

	err := ac.validateExternalUserInfo(req, testAnnotation)
	assert.NilError(t, err)
}

func TestExternalUsers(t *testing.T) {
	ac := getDefaultAdmissionController()
	req := getAdmissionRequest()
	req.UserInfo.Username = "yunikorn"
	extUsersRegexps := make([]*regexp.Regexp, 1)
	extUsersRegexps[0] = regexp.MustCompile("yunikorn")
	ac.externalUsers = extUsersRegexps

	err := ac.validateExternalUserInfo(req, testAnnotation)
	assert.NilError(t, err)
}

func TestExternalGroups(t *testing.T) {
	ac := getDefaultAdmissionController()
	req := getAdmissionRequest()
	extGroupsRegexps := make([]*regexp.Regexp, 1)
	extGroupsRegexps[0] = regexp.MustCompile("devs")
	ac.externalGroups = extGroupsRegexps

	err := ac.validateExternalUserInfo(req, testAnnotation)
	assert.NilError(t, err)
}

func TestExternalAuthenticationDenied(t *testing.T) {
	ac := getDefaultAdmissionController()
	req := getAdmissionRequest()
	req.UserInfo.Username = "yunikorn"

	err := ac.validateExternalUserInfo(req, testAnnotation)
	assert.ErrorContains(t, err, "user yunikorn with groups [system:authenticated,devs] is not allowed to set user annotation")
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

func getAdmissionRequest() *admissionv1.AdmissionRequest {
	req := &admissionv1.AdmissionRequest{
		UserInfo: authv1.UserInfo{
			Username: "test",
			UID:      "uid-1",
			Groups:   []string{"system:authenticated", "devs"},
			Extra:    nil,
		},
	}

	return req
}
