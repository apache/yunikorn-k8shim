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

package ginkgo_writer

import (
	"fmt"
	"io"
)

// GinkgoWriter implements a GinkgoWriterInterface and io.Writer
type Writer struct {
	outWriter io.Writer
}

func NewGinkgoWriter(outWriter io.Writer) *Writer {
	return &Writer{
		outWriter: outWriter,
	}
}

func (w *Writer) Write(b []byte) (n int, err error) {
	return w.outWriter.Write(b)
}

// GinkgoWriterInterface
func (w *Writer) TeeTo(writer io.Writer) {
	// Not implemented
}

func (w *Writer) ClearTeeWriters() {
	// Not implemented
}

func (w *Writer) Print(a ...interface{}) {
	fmt.Fprint(w, a...)
}

func (w *Writer) Printf(format string, a ...interface{}) {
	fmt.Fprintf(w, format, a...)
}

func (w *Writer) Println(a ...interface{}) {
	fmt.Fprintln(w, a...)
}
