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

package k8s

import (
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/resource"
)

type PvConfig struct {
	Name         string
	Labels       map[string]string
	Capacity     string
	AccessModes  []v1.PersistentVolumeAccessMode
	Type         string
	Path         string
	NodeAffinity *v1.VolumeNodeAffinity
	StorageClass string
}

const (
	LocalTypePv string = "Local"
)

func InitPersistentVolume(conf PvConfig) (*v1.PersistentVolume, error) {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: conf.Name,
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse(conf.Capacity),
			},
			AccessModes:                   conf.AccessModes,
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain,
			StorageClassName:              conf.StorageClass,
		},
	}
	if conf.Type == LocalTypePv {
		pv.Spec.PersistentVolumeSource = v1.PersistentVolumeSource{
			Local: &v1.LocalVolumeSource{
				Path: conf.Path,
			},
		}
		if conf.NodeAffinity == nil {
			// Create fake condition which won't exclude anything
			pv.Spec.NodeAffinity = &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "fakeKey",
									Operator: v1.NodeSelectorOpNotIn,
									Values:   []string{"fakeValue"},
								},
							},
						},
					},
				},
			}
		} else {
			pv.Spec.NodeAffinity = conf.NodeAffinity
		}
	}
	return pv, nil
}

type ScConfig struct {
	Name        string
	Provisioner string
	Parameters  map[string]string
}

func InitStorageClass(conf ScConfig) (*storagev1.StorageClass, error) {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: conf.Name,
		},
		Provisioner: conf.Provisioner,
		Parameters:  conf.Parameters,
	}
	return sc, nil
}

type PvcConfig struct {
	Name             string
	Capacity         string
	VolumeName       string
	StorageClassName string
}

func InitPersistentVolumeClaim(conf PvcConfig) (*v1.PersistentVolumeClaim, error) {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: conf.Name,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(conf.Capacity),
				},
			},
		},
	}
	if conf.VolumeName != "" {
		pvc.Spec.VolumeName = conf.VolumeName
	}
	if conf.StorageClassName != "" {
		pvc.Spec.StorageClassName = &conf.StorageClassName
	}
	return pvc, nil
}
