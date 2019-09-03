/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

const (
	HttpPort    = 8443
	tlsDir      = `/run/secrets/tls`
	tlsCertFile = `cert.pem`
	tlsKeyFile  = `key.pem`
)

func main() {
	certPath := filepath.Join(tlsDir, tlsCertFile)
	keyPath := filepath.Join(tlsDir, tlsKeyFile)
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.Logger.Fatal("Failed to load key pair", zap.Error(err))
	}

	webHook := admissionController{}
	mux := http.NewServeMux()
	mux.HandleFunc("/mutate", webHook.serve)
	server := &http.Server{
		Addr: fmt.Sprintf(":%v", HttpPort),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}},
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServeTLS("", ""); err != nil {
			log.Logger.Fatal("failed to start admission controller", zap.Error(err))
		}
	}()

	log.Logger.Info("the admission controller started",
		zap.Int("port", HttpPort),
		zap.String("listeningOn", "/mutate"))

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	log.Logger.Info("shutting down the admission controller...")
	err = server.Shutdown(context.Background()); if err != nil {
		log.Logger.Warn("failed to stop the admission controller",
			zap.Error(err))
	}
}
