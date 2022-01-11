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
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

const (
	HTTPPort                              = 9089
	policyGroupEnvVarName                 = "POLICY_GROUP"
	schedulerServiceAddressEnvVarName     = "SCHEDULER_SERVICE_ADDRESS"
	schedulerValidateConfURLPattern       = "http://%s/ws/v1/validate-conf"
	admissionControllerNamespace          = "ADMISSION_CONTROLLER_NAMESPACE"
	admissionControllerService            = "ADMISSION_CONTROLLER_SERVICE"
	admissionControllerNamespaceBlacklist = "ADMISSION_CONTROLLER_NAMESPACE_BLACKLIST"
	defaultNamespaceBlacklist             = "^kube-system$"

	// legal URLs
	mutateURL       = "/mutate"
	validateConfURL = "/validate-conf"
	healthURL       = "/health"
)

func main() {
	namespace := os.Getenv(admissionControllerNamespace)
	serviceName := os.Getenv(admissionControllerService)
	namespaceBlacklist, ok := os.LookupEnv(admissionControllerNamespaceBlacklist)
	if !ok {
		namespaceBlacklist = defaultNamespaceBlacklist
	}

	wm, err := NewWebhookManager(namespace, serviceName)
	if err != nil {
		log.Logger().Fatal("Failed to initialize webhook manager", zap.Error(err))
	}

	err = wm.LoadCACertificates()
	if err != nil {
		log.Logger().Fatal("Failed to initialize CA certificates", zap.Error(err))
	}

	pair, err := wm.GenerateServerCertificate()
	if err != nil {
		log.Logger().Fatal("Unable to generate server certificate", zap.Error(err))
	}

	err = wm.InstallWebhooks()
	if err != nil {
		log.Logger().Fatal("Unable to install webhooks for admission controller", zap.Error(err))
	}

	policyGroup := os.Getenv(policyGroupEnvVarName)
	if policyGroup == "" {
		policyGroup = conf.DefaultPolicyGroup
	}
	schedulerServiceAddress := os.Getenv(schedulerServiceAddressEnvVarName)

	webHook, err := initAdmissionController(
		fmt.Sprintf("%s.yaml", policyGroup),
		fmt.Sprintf(schedulerValidateConfURLPattern, schedulerServiceAddress),
		namespaceBlacklist)
	if err != nil {
		log.Logger().Fatal("failed to configure admission controller", zap.Error(err))
	}

	mux := http.NewServeMux()
	mux.HandleFunc(healthURL, webHook.health)
	mux.HandleFunc(mutateURL, webHook.serve)
	mux.HandleFunc(validateConfURL, webHook.serve)
	server := &http.Server{
		Addr: fmt.Sprintf(":%v", HTTPPort),
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{*pair}},
		Handler: mux,
	}

	go func() {
		if err = server.ListenAndServeTLS("", ""); err != nil {
			log.Logger().Fatal("failed to start admission controller", zap.Error(err))
		}
	}()

	log.Logger().Info("the admission controller started",
		zap.Int("port", HTTPPort),
		zap.Strings("listeningOn", []string{healthURL, mutateURL, validateConfURL}))

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
