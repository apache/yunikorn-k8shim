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

package locking

import (
	"sync"

	godeadlock "github.com/sasha-s/go-deadlock"

	corelocking "github.com/apache/yunikorn-core/pkg/locking"
)

var once sync.Once

func init() {
	once.Do(func() {
		// call into core locking package to ensure that all locks are globally configured
		corelocking.IsTrackingEnabled()
	})
}

// Mutex, and RWMutex below it, declare themselves lock primitives to the checklocks analysis.
// Without the declaration the analysis recognises a lock by its type name only, so the
// forwarders in forwarders.go read as ordinary methods that take a lock and return without
// releasing it and every one of them needs a "+checklocksignore" to silence that; see
// forwarders.go for what those ignores cost. Whether a type behaves as a Mutex or an RWMutex
// is taken from the type itself: it has an RLock method or it does not.
//
// +checklockslocktype
type Mutex struct {
	mu godeadlock.Mutex
}

// +checklockslocktype
type RWMutex struct {
	mu godeadlock.RWMutex
}
