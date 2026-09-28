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

package metadata

import (
	"errors"

	admissionv1 "k8s.io/api/admission/v1"

	"github.com/apache/yunikorn-k8shim/pkg/admission/common"
)

type LabelExtractor struct{}

// GetLabelsFromRequest loads the labels from the workload object, can handle create and update requests.
func (l *LabelExtractor) GetLabelsFromRequest(req *admissionv1.AdmissionRequest, old bool) (map[string]string, bool, error) {
	result, err := extractFromReq(req, old)
	if errors.Is(err, common.ErrorUnsupportedKind) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	return result.labels, true, err
}
