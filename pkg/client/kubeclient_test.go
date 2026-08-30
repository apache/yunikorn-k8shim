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

func TestClientRateLimit(t *testing.T) {
	testCases := []struct {
		name          string
		qps           int
		burst         int
		expectedQPS   float32
		expectedBurst int
	}{
		{"unset is no limiter", -1, -1, -1, 0},
		{"zero leaves the client-go defaults", 0, 0, 0, 0},
		{"burst defaults to qps", 100, 0, 100, 100},
		{"negative burst defaults to qps", 100, -1, 100, 100},
		{"qps and burst", 100, 50, 100, 50},
		{"burst is kept without a qps", -1, 50, -1, 50},
		{"burst is kept on the client-go defaults", 0, 50, 0, 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qps, burst := clientRateLimit(tc.qps, tc.burst)
			assert.Equal(t, tc.expectedQPS, qps, "unexpected QPS")
			assert.Equal(t, tc.expectedBurst, burst, "unexpected burst")
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
		{"unset", userAgentScheduler, -1, -1},
		{"client-go defaults", userAgentBootstrap, 0, 0},
		{"limited", userAgentEvents, 100, 200},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectedQPS, expectedBurst := clientRateLimit(tc.qps, tc.burst)
			config := newRestConfig(kc, tc.qps, tc.burst, tc.concern)
			assert.Equal(t, tc.concern, config.UserAgent, "user agent not set to the concern")
			// creating the limiter is always left to client-go: a negative QPS makes it
			// create none, a QPS of 0 falls back to the client-go defaults
			assert.Assert(t, config.RateLimiter == nil, "rate limiter must be left to client-go")
			assert.Equal(t, expectedQPS, config.QPS, "QPS not normalised")
			assert.Equal(t, expectedBurst, config.Burst, "burst not normalised")
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
			_, err := kubernetes.NewForConfig(newRestConfig(kc, tc.qps, tc.burst, userAgentScheduler))
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
