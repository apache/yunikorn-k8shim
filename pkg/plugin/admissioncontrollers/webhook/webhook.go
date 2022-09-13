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
	"strconv"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

const (
	HTTPPort                             = 9089
	policyGroupEnvVarName                = "POLICY_GROUP"
	schedulerServiceAddressEnvVarName    = "SCHEDULER_SERVICE_ADDRESS"
	schedulerValidateConfURLPattern      = "http://%s/ws/v1/validate-conf"
	admissionControllerNamespace         = "ADMISSION_CONTROLLER_NAMESPACE"
	admissionControllerService           = "ADMISSION_CONTROLLER_SERVICE"
	admissionControllerProcessNamespaces = "ADMISSION_CONTROLLER_PROCESS_NAMESPACES"
	admissionControllerBypassNamespaces  = "ADMISSION_CONTROLLER_BYPASS_NAMESPACES"
	admissionControllerLabelNamespaces   = "ADMISSION_CONTROLLER_LABEL_NAMESPACES"
	admissionControllerNoLabelNamespaces = "ADMISSION_CONTROLLER_NO_LABEL_NAMESPACES"

	// user & group resolution settings
	admissionControllerBypassAuth        = "ADMISSION_CONTROLLER_BYPASS_AUTH"
	admissionControllerBypassControllers = "ADMISSION_CONTROLLER_BYPASS_CONTROLLERS"
	admissionControllerSystemUsers       = "ADMISSION_CONTROLLER_SYSTEM_USERS"
	admissionControllerExternalUsers     = "ADMISSION_CONTROLLER_EXTERNAL_USERS"
	admissionControllerExternalGroups    = "ADMISSION_CONTROLLER_EXTERNAL_GROUPS"

	defaultBypassNamespaces  = "^kube-system$"
	defaultBypassAuth        = false
	defaultBypassControllers = true
	defaultSystemUsers       = "system:serviceaccount:kube-system:*"

	// legal URLs
	mutateURL       = "/mutate"
	validateConfURL = "/validate-conf"
	healthURL       = "/health"
)

type WebHook struct {
	ac     *admissionController
	port   int
	server *http.Server
	sync.Mutex
}

func main() {
	namespace := os.Getenv(admissionControllerNamespace)
	serviceName := os.Getenv(admissionControllerService)
	processNamespaces, ok := os.LookupEnv(admissionControllerProcessNamespaces)
	if !ok {
		processNamespaces = ""
	}
	bypassNamespaces, ok := os.LookupEnv(admissionControllerBypassNamespaces)
	if !ok {
		bypassNamespaces = defaultBypassNamespaces
	}
	labelNamespaces, ok := os.LookupEnv(admissionControllerLabelNamespaces)
	if !ok {
		labelNamespaces = ""
	}
	noLabelNamespaces, ok := os.LookupEnv(admissionControllerNoLabelNamespaces)
	if !ok {
		noLabelNamespaces = ""
	}

	bypassAuth := defaultBypassAuth
	bypassAuthEnv, ok := os.LookupEnv(admissionControllerBypassAuth)
	if ok {
		parsed, err := strconv.ParseBool(bypassAuthEnv)
		if err != nil {
			log.Logger().Warn("Unable to parse value, using default",
				zap.String("env var", admissionControllerBypassAuth),
				zap.Bool("default", defaultBypassAuth))
		} else {
			bypassAuth = parsed
		}
	}
	systemUsers, ok := os.LookupEnv(admissionControllerSystemUsers)
	if !ok {
		systemUsers = defaultSystemUsers
	}
	externalUsers, ok := os.LookupEnv(admissionControllerExternalUsers)
	if !ok {
		externalUsers = ""
	}
	externalGroups, ok := os.LookupEnv(admissionControllerExternalGroups)
	if !ok {
		externalUsers = ""
	}

	bypassControllers := defaultBypassControllers
	bypassControllersEnv, ok := os.LookupEnv(admissionControllerBypassControllers)
	if ok {
		parsed, err := strconv.ParseBool(bypassControllersEnv)
		if err != nil {
			log.Logger().Warn("Unable to parse value, using default",
				zap.String("env var", admissionControllerBypassControllers),
				zap.Bool("default", defaultBypassControllers))
		} else {
			bypassAuth = parsed
		}
	}

	wm, err := NewWebhookManager(namespace, serviceName)
	if err != nil {
		log.Logger().Fatal("Failed to initialize webhook manager", zap.Error(err))
	}

	policyGroup := os.Getenv(policyGroupEnvVarName)
	if policyGroup == "" {
		policyGroup = conf.DefaultPolicyGroup
	}
	schedulerServiceAddress := os.Getenv(schedulerServiceAddressEnvVarName)

	ac, err := initAdmissionController(
		fmt.Sprintf("%s.yaml", policyGroup),
		fmt.Sprintf(schedulerValidateConfURLPattern, schedulerServiceAddress),
		processNamespaces, bypassNamespaces, labelNamespaces, noLabelNamespaces,
		bypassAuth, bypassControllers, systemUsers, externalUsers, externalGroups)
	if err != nil {
		log.Logger().Fatal("failed to configure admission controller", zap.Error(err))
	}

	webhook := CreateWebhook(ac, HTTPPort)

	certs := UpdateWebhookConfiguration(wm)
	webhook.Startup(certs)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	WaitForCertExpiration(wm, signalChan)

	for {
		switch <-signalChan {
		case syscall.SIGUSR1: // reload certificates
			certs := UpdateWebhookConfiguration(wm)
			webhook.Shutdown()
			webhook.Startup(certs)
			WaitForCertExpiration(wm, signalChan)
		default: // terminate
			webhook.Shutdown()
			os.Exit(0)
		}
	}
}

func WaitForCertExpiration(wm WebhookManager, ch chan os.Signal) {
	go func() {
		wm.WaitForCertificateExpiration()
		ch <- syscall.SIGUSR1
	}()
}

func UpdateWebhookConfiguration(wm WebhookManager) *tls.Certificate {
	err := wm.LoadCACertificates()
	if err != nil {
		log.Logger().Fatal("Failed to initialize CA certificates", zap.Error(err))
	}

	certs, err := wm.GenerateServerCertificate()
	if err != nil {
		log.Logger().Fatal("Unable to generate server certificate", zap.Error(err))
	}

	err = wm.InstallWebhooks()
	if err != nil {
		log.Logger().Fatal("Unable to install webhooks for admission controller", zap.Error(err))
	}

	return certs
}

func CreateWebhook(ac *admissionController, port int) *WebHook {
	return &WebHook{
		ac:   ac,
		port: port,
	}
}

func (wh *WebHook) Startup(certs *tls.Certificate) {
	wh.Lock()
	defer wh.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc(healthURL, wh.ac.health)
	mux.HandleFunc(mutateURL, wh.ac.serve)
	mux.HandleFunc(validateConfURL, wh.ac.serve)

	wh.server = &http.Server{
		Addr: fmt.Sprintf(":%v", wh.port),
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{*certs}},
		Handler: mux,
	}

	go func() {
		if err := wh.server.ListenAndServeTLS("", ""); err != nil {
			if err == http.ErrServerClosed {
				log.Logger().Info("existing server closed")
			} else {
				log.Logger().Fatal("failed to start admission controller", zap.Error(err))
			}
		}
	}()

	log.Logger().Info("the admission controller started",
		zap.Int("port", HTTPPort),
		zap.Strings("listeningOn", []string{healthURL, mutateURL, validateConfURL}))
}

func (wh *WebHook) Shutdown() {
	wh.Lock()
	defer wh.Unlock()

	if wh.server != nil {
		log.Logger().Info("shutting down the admission controller...")
		err := wh.server.Shutdown(context.Background())
		if err != nil {
			log.Logger().Fatal("failed to stop the admission controller", zap.Error(err))
		}
		wh.server = nil
	}
}
