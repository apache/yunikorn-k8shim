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
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/admission"
	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	schedulerconf "github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

const (
	HTTPPort        = 9089
	healthURL       = "/health"
	mutateURL       = "/mutate"
	validateConfURL = "/validate-conf"
)

type WebHook struct {
	ac     *admission.AdmissionController
	port   int
	server *http.Server
	sync.Mutex
}

func main() {
	configMaps, err := client.LoadBootstrapConfigMaps(schedulerconf.GetSchedulerNamespace())
	if err != nil {
		log.Logger().Fatal("Failed to load initial configmaps", zap.Error(err))
		return
	}
	amConf := conf.NewAdmissionControllerConf(configMaps)

	kubeClient := client.NewKubeClient(amConf.GetKubeConfig())
	amConf.StartInformers(kubeClient)

	wm, err := admission.NewWebhookManager(amConf)

	if err != nil {
		log.Logger().Fatal("Failed to initialize webhook manager", zap.Error(err))
	}

	ac := admission.InitAdmissionController(amConf)

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
			amConf.StopInformers()
			webhook.Shutdown()
			os.Exit(0)
		}
	}
}

func WaitForCertExpiration(wm admission.WebhookManager, ch chan os.Signal) {
	go func() {
		wm.WaitForCertificateExpiration()
		ch <- syscall.SIGUSR1
	}()
}

func UpdateWebhookConfiguration(wm admission.WebhookManager) *tls.Certificate {
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

func CreateWebhook(ac *admission.AdmissionController, port int) *WebHook {
	return &WebHook{
		ac:   ac,
		port: port,
	}
}

func (wh *WebHook) Startup(certs *tls.Certificate) {
	wh.Lock()
	defer wh.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc(healthURL, wh.ac.Health)
	mux.HandleFunc(mutateURL, wh.ac.Serve)
	mux.HandleFunc(validateConfURL, wh.ac.Serve)

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
