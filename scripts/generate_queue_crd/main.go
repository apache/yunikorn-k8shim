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
	"bytes"
	"fmt"
	"os"
)

const (
	recursivePlaceholder = `                    items: {}`
	recursiveSchema      = `                    items:
                      type: object
                      x-kubernetes-preserve-unknown-fields: true`
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: generate_queue_crd <generated-crd>")
		os.Exit(2)
	}

	path := os.Args[1]
	data, err := os.ReadFile(path) //nolint:gosec // The path is a Makefile-controlled generated artifact.
	if err != nil {
		fmt.Fprintf(os.Stderr, "read generated CRD: %v\n", err)
		os.Exit(1)
	}

	if count := bytes.Count(data, []byte(recursivePlaceholder)); count != 1 {
		fmt.Fprintf(os.Stderr, "expected one recursive schema placeholder, found %d\n", count) //nolint:gosec // Plain CLI text, not HTML.
		os.Exit(1)
	}

	data = bytes.Replace(data, []byte(recursivePlaceholder), []byte(recursiveSchema), 1)
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec // This is a public generated manifest.
		fmt.Fprintf(os.Stderr, "write generated CRD: %v\n", err)
		os.Exit(1)
	}
}
