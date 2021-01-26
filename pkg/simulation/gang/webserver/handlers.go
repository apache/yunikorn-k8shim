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
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func writeHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
}

func setTaskReady(w http.ResponseWriter, r *http.Request) {
	writeHeader(w)
	lock.Lock()
	defer lock.Unlock()
	vars := mux.Vars(r)
	jobID := vars["jobID"]
	jobMember[jobID]++
	if err := json.NewEncoder(w).Encode(jobMember); err != nil {
		log.Printf("Add task %s fail.", jobID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func checkJobReady(w http.ResponseWriter, r *http.Request) {
	writeHeader(w)
	vars := mux.Vars(r)
	jobID := vars["jobID"]
	if err := json.NewEncoder(w).Encode(jobMember[jobID]); err != nil {
		log.Printf("Check job %s Member fail", jobID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
