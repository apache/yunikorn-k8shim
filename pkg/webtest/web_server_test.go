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

package webtest

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestWebServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/v1/test", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})
	proxiedServer := http.Server{
		Addr:              ":40001",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := proxiedServer.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				assert.NilError(t, err, "failed to start server")
			}
		}
	}()
	defer func() {
		if err := proxiedServer.Close(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				assert.NilError(t, err, "failed to stop server")
			}
		}
	}()

	server, err := NewWebServer("testdata", "127.0.0.1:40002", "http://127.0.0.1:40001")
	assert.NilError(t, err, "failed to create server")
	server.Start()
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// validate local content
	res, err := http.Get("http://127.0.0.1:40002/test.txt")
	assert.NilError(t, err, "failed to read from server")
	body, err := io.ReadAll(res.Body)
	assert.NilError(t, err, "failed to read body")
	str := string(body)
	assert.Check(t, strings.Contains(str, "TESTING"), "test string not found")

	// validate proxied content
	res, err = http.Get("http://127.0.0.1:40002/ws/v1/test")
	assert.NilError(t, err, "failed to read from server")
	body, err = io.ReadAll(res.Body)
	assert.NilError(t, err, "failed to read body")
	str = string(body)
	assert.Check(t, strings.Contains(str, "OK"), "test string not found")
}
