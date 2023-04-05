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
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/yunikorn-k8shim/pkg/webtest"
)

var (
	version string
	date    string
)

func main() {
	documentRoot := envOrDefault("DOCUMENT_ROOT", "document")
	listenAddress := envOrDefault("LISTEN_ADDRESS", ":9889")
	proxyUrl := envOrDefault("PROXY_URL", "http://127.0.0.1:9080")
	log.Default().Printf("Starting yunikorn-web version: %s, buildDate: %s, docRoot: %s, listenAddress: %s, proxyUrl: %s",
		version, date, documentRoot, listenAddress, proxyUrl)
	server, err := webtest.NewWebServer(documentRoot, listenAddress, proxyUrl)
	if err != nil {
		log.Fatal(err)
	}
	server.Start()

	done := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		close(done)
	}()
	<-done
	server.Stop()
}

func envOrDefault(envVar, defValue string) string {
	if result, ok := os.LookupEnv(envVar); ok {
		return result
	}
	return defValue
}
