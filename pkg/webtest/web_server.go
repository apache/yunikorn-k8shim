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
	ctx "context"
	"errors"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type WebServer struct {
	server *http.Server
}

func NewWebServer(documentRoot, listenAddress, proxyUrl string) (*WebServer, error) {
	origin, err := url.Parse(proxyUrl)
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(origin)
	fs := http.FileServer(http.Dir(documentRoot))
	mux := http.NewServeMux()
	mux.Handle("/", fs)
	mux.Handle("/ws/", proxy)
	server := &http.Server{
		Addr:              listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return &WebServer{
		server: server,
	}, nil
}

func (ws *WebServer) Start() {
	go func() {
		if err := ws.server.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				log.Fatal(err)
			}
		}
	}()
}

func (ws *WebServer) Stop() {
	ws.server.Close()
	if err := ws.server.Shutdown(ctx.Background()); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
}
