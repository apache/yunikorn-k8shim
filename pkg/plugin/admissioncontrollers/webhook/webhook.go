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
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

const (
	HTTPPort                          = 9089
	tlsDir                            = `/run/secrets/tls`
	tlsCertFile                       = `cert.pem`
	tlsKeyFile                        = `key.pem`
	policyGroupEnvVarName             = "POLICY_GROUP"
	schedulerServiceAddressEnvVarName = "SCHEDULER_SERVICE_ADDRESS"
	schedulerValidateConfURLPattern   = "http://%s/ws/v1/validate-conf"

	// legal URLs
	mutateURL       = "/mutate"
	validateConfURL = "/validate-conf"
)

func main() {
	certPath := filepath.Join(tlsDir, tlsCertFile)
	keyPath := filepath.Join(tlsDir, tlsKeyFile)
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.Logger().Fatal("Failed to load key pair", zap.Error(err))
	}
	policyGroup := os.Getenv(policyGroupEnvVarName)
	if policyGroup == "" {
		policyGroup = conf.DefaultPolicyGroup
	}
	schedulerServiceAddress := os.Getenv(schedulerServiceAddressEnvVarName)

	webHook := admissionController{
		configName:               fmt.Sprintf("%s.yaml", policyGroup),
		schedulerValidateConfURL: fmt.Sprintf(schedulerValidateConfURLPattern, schedulerServiceAddress),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(mutateURL, webHook.serve)
	mux.HandleFunc(validateConfURL, webHook.serve)
	server := &http.Server{
		Addr:      fmt.Sprintf(":%v", HTTPPort),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}},
		Handler:   mux,
	}

	go func() {
		if err = server.ListenAndServeTLS("", ""); err != nil {
			log.Logger().Fatal("failed to start admission controller", zap.Error(err))
		}
	}()

	log.Logger().Info("the admission controller started",
		zap.Int("port", HTTPPort),
		zap.Strings("listeningOn", []string{mutateURL, validateConfURL}))

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	log.Logger().Info("shutting down the admission controller...")
	err = server.Shutdown(context.Background())
	if err != nil {
		log.Logger().Warn("failed to stop the admission controller",
			zap.Error(err))
	}
}
