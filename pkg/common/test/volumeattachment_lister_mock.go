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

package test

import (
	"fmt"

	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	listersv1 "k8s.io/client-go/listers/storage/v1"
)

type MockVolumeAttachmentLister struct {
	volumeAttachments map[string]*storagev1.VolumeAttachment
}

func NewMockVolumeAttachmentLister() listersv1.VolumeAttachmentLister {
	return &MockVolumeAttachmentLister{
		volumeAttachments: make(map[string]*storagev1.VolumeAttachment),
	}
}

func (nsl *MockVolumeAttachmentLister) List(labels.Selector) (ret []*storagev1.VolumeAttachment, err error) {
	ret = make([]*storagev1.VolumeAttachment, 0)
	for _, pc := range nsl.volumeAttachments {
		ret = append(ret, pc)
	}
	return ret, nil
}

func (l *MockVolumeAttachmentLister) Add(va *storagev1.VolumeAttachment) {
	l.volumeAttachments[va.Name] = va
}

func (l *MockVolumeAttachmentLister) Get(name string) (*storagev1.VolumeAttachment, error) {
	ns, ok := l.volumeAttachments[name]
	if !ok {
		return nil, fmt.Errorf("volumeAttachment is not found")
	}
	return ns, nil
}
