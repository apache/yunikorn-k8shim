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

package client

import (
	"os"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
	"k8s.io/client-go/kubernetes"
)

const testKubeConfig = `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://localhost:6443
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user: {}
`

func TestUserAgentWithVersion(t *testing.T) {
	testCases := []struct {
		name     string
		concern  string
		version  string
		expected string
	}{
		{"no version", userAgentWrites, "", "yunikorn-scheduler/writes"},
		{"version appended", userAgentWrites, "1.7.0", "yunikorn-scheduler/writes (1.7.0)"},
		{"admission controller", UserAgentAdmissionController, "1.7.0", "yunikorn-admission-controller (1.7.0)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, userAgentWithVersion(tc.concern, tc.version), "unexpected user agent")
		})
	}
}

func TestRateLimitPolicy(t *testing.T) {
	testCases := []struct {
		name          string
		concern       string
		qps           int
		burst         int
		expectedQPS   float32
		expectedBurst int
		expectedLimit string
	}{
		{"unlimited", userAgentInformers, 0, 0, -1, 0, "unlimited"},
		{"negative is unlimited", userAgentWrites, -1, -1, -1, 0, "unlimited"},
		{"burst without qps is ignored", userAgentBootstrap, 0, 100, -1, 0, "unlimited"},
		{"limited", userAgentEvents, 100, 200, 100, 200, "100 qps / 200 burst"},
		{"burst defaults to qps", UserAgentAdmissionController, 100, 0, 100, 100, "100 qps / 100 burst"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qps, burst, rateLimit := rateLimitPolicy(tc.concern, tc.qps, tc.burst)
			assert.Equal(t, tc.expectedQPS, qps, "unexpected QPS")
			assert.Equal(t, tc.expectedBurst, burst, "unexpected burst")
			assert.Equal(t, tc.expectedLimit, rateLimit, "unexpected rate limit description")
		})
	}
}

func TestNewRestConfig(t *testing.T) {
	kc := writeKubeConfig(t)

	testCases := []struct {
		name    string
		concern string
		qps     int
		burst   int
	}{
		{"unlimited", userAgentInformers, 0, 0},
		{"limited", userAgentEvents, 100, 200},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectedQPS, expectedBurst, _ := rateLimitPolicy(tc.concern, tc.qps, tc.burst)
			config := newRestConfig(kc, tc.qps, tc.burst, tc.concern)
			assert.Equal(t, UserAgent(tc.concern), config.UserAgent, "user agent not set")
			// creating the limiter is always left to client-go: a negative QPS makes it
			// create none, a QPS of 0 would silently fall back to the client-go defaults
			assert.Assert(t, config.RateLimiter == nil, "rate limiter must be left to client-go")
			assert.Equal(t, expectedQPS, config.QPS, "QPS not taken from the rate limit policy")
			assert.Equal(t, expectedBurst, config.Burst, "burst not taken from the rate limit policy")
		})
	}
}

// client-go must accept every configuration we generate, it rejects a QPS which is set
// without a burst and a rejected configuration is fatal at startup
func TestNewClientSetAcceptsRestConfig(t *testing.T) {
	kc := writeKubeConfig(t)

	testCases := []struct {
		name  string
		qps   int
		burst int
	}{
		{"unlimited", 0, 0},
		{"qps without burst", 100, 0},
		{"qps and burst", 100, 200},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := kubernetes.NewForConfig(newRestConfig(kc, tc.qps, tc.burst, userAgentWrites))
			assert.NilError(t, err, "clientset creation failed")
		})
	}
}

// writeKubeConfig creates a kubeconfig file for a non existing cluster and returns its path
func writeKubeConfig(t *testing.T) string {
	t.Helper()
	kc := filepath.Join(t.TempDir(), "kubeconfig")
	err := os.WriteFile(kc, []byte(testKubeConfig), 0600)
	assert.NilError(t, err, "could not write kubeconfig")
	return kc
}
