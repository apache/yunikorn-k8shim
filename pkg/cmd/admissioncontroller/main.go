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
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/admission"
	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
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
	locking.Mutex
}

func main() {
	configMaps, err := client.LoadBootstrapConfigMaps()
	if err != nil {
		log.Log(log.Admission).Fatal("Failed to load initial configmaps", zap.Error(err))
		return
	}

	amConf := conf.NewAdmissionControllerConf(configMaps)
	kubeClient := client.NewKubeClient(amConf.GetKubeConfig())

	informers := admission.NewInformers(kubeClient, amConf.GetNamespace())

	if hadlerErr := amConf.RegisterHandlers(informers.ConfigMap); hadlerErr != nil {
		log.Log(log.Admission).Fatal("Failed to register handlers", zap.Error(hadlerErr))
		return
	}
	pcCache, pcErr := admission.NewPriorityClassCache(informers.PriorityClass)
	if pcErr != nil {
		log.Log(log.Admission).Fatal("Failed to create new priority calss cache", zap.Error(pcErr))
		return
	}
	nsCache, nsErr := admission.NewNamespaceCache(informers.Namespace)
	if nsErr != nil {
		log.Log(log.Admission).Fatal("Failed to create namespace cache", zap.Error(nsErr))
		return
	}
	informers.Start()

	wm, err := admission.NewWebhookManager(amConf)
	if err != nil {
		log.Log(log.Admission).Fatal("Failed to initialize webhook manager", zap.Error(err))
	}

	ac := admission.InitAdmissionController(amConf, pcCache, nsCache)

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
			informers.Stop()
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
		log.Log(log.Admission).Fatal("Failed to initialize CA certificates", zap.Error(err))
	}

	certs, err := wm.GenerateServerCertificate()
	if err != nil {
		log.Log(log.Admission).Fatal("Unable to generate server certificate", zap.Error(err))
	}

	err = wm.InstallWebhooks()
	if err != nil {
		log.Log(log.Admission).Fatal("Unable to install webhooks for admission controller", zap.Error(err))
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
			MinVersion:   tls.VersionTLS12,           // No SSL, TLS 1.0 or TLS 1.1 support
			NextProtos:   []string{"h2", "http/1.1"}, // prefer HTTP/2 over HTTP/1.1
			CipherSuites: wh.getCipherSuites(),       // limit cipher suite to secure ones
			Certificates: []tls.Certificate{*certs},
		},
		Handler: mux,
	}

	go func() {
		if err := wh.server.ListenAndServeTLS("", ""); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				log.Log(log.Admission).Info("existing server closed")
			} else {
				log.Log(log.Admission).Fatal("failed to start admission controller", zap.Error(err))
			}
		}
	}()

	log.Log(log.Admission).Info("the admission controller started",
		zap.Int("port", HTTPPort),
		zap.Strings("listeningOn", []string{healthURL, mutateURL, validateConfURL}))
}

func (wh *WebHook) Shutdown() {
	wh.Lock()
	defer wh.Unlock()

	if wh.server != nil {
		log.Log(log.Admission).Info("shutting down the admission controller...")
		err := wh.server.Shutdown(context.Background())
		if err != nil {
			log.Log(log.Admission).Fatal("failed to stop the admission controller", zap.Error(err))
		}
		wh.server = nil
	}
}

// getCipherSuites returns the IDs of the currently considered secure ciphers.
// Order of choice is defined in the cipherSuitesPreferenceOrder
func (wh *WebHook) getCipherSuites() []uint16 {
	var ids []uint16
	for _, cs := range tls.CipherSuites() {
		ids = append(ids, cs.ID)
	}
	return ids
}
