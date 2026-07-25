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

// This file lives in the api/v1alpha1 package (rather than internal/queueconfig)
// to avoid an import cycle: the webhook in this same package needs to call
// queueconfig.BuildMerged, while these helpers must know about the v1alpha1
// Queue CR types. Keeping the adapter here means queueconfig stays a pure
// "YuniKorn-shape in, YAML + validation out" package.

package v1alpha1

import (
	"sort"

	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

// ToYuniKornQueueConfig converts a single Queue CR's QueueConfig into the
// YuniKorn-shaped queueconfig.QueueConfig used in the merged scheduler config.
//
// All optional pointer fields collapse to their zero value when nil so the
// resulting struct can be safely YAML-marshalled with omitempty semantics.
func ToYuniKornQueueConfig(in QueueConfig) queueconfig.QueueConfig {
	out := queueconfig.QueueConfig{Name: in.Name}

	if in.Parent != nil {
		out.Parent = *in.Parent
	}
	if in.Resources != nil {
		out.Resources = &queueconfig.Resources{
			Guaranteed: in.Resources.Guaranteed,
			Max:        in.Resources.Max,
		}
	}
	if in.MaxApplications != nil {
		out.MaxApplications = *in.MaxApplications
	}
	if len(in.Properties) > 0 {
		out.Properties = in.Properties
	}
	if in.AdminACL != "" {
		out.AdminACL = in.AdminACL
	}
	if in.SubmitACL != "" {
		out.SubmitACL = in.SubmitACL
	}
	if len(in.Queues) > 0 {
		out.Queues = make([]queueconfig.QueueConfig, 0, len(in.Queues))
		for _, c := range in.Queues {
			out.Queues = append(out.Queues, ToYuniKornQueueConfig(c))
		}
	}
	if len(in.Limits) > 0 {
		out.Limits = make([]queueconfig.Limit, 0, len(in.Limits))
		for _, l := range in.Limits {
			yl := queueconfig.Limit{
				Limit:        l.Limit,
				Users:        l.Users,
				Groups:       l.Groups,
				MaxResources: l.MaxResources,
			}
			if l.MaxApplications != nil {
				yl.MaxApplications = *l.MaxApplications
			}
			out.Limits = append(out.Limits, yl)
		}
	}
	if in.ChildTemplate != nil {
		ct := &queueconfig.ChildTemplate{Properties: in.ChildTemplate.Properties}
		if in.ChildTemplate.MaxApplications != nil {
			ct.MaxApplications = *in.ChildTemplate.MaxApplications
		}
		if in.ChildTemplate.Resources != nil {
			ct.Resources = &queueconfig.Resources{
				Guaranteed: in.ChildTemplate.Resources.Guaranteed,
				Max:        in.ChildTemplate.Resources.Max,
			}
		}
		out.ChildTemplate = ct
	}

	return out
}

// L1QueuesFromCRs converts a slice of Queue CRs into the YuniKorn-shaped L1
// children list used by queueconfig.BuildMerged. Caller-supplied ordering is
// preserved.
func L1QueuesFromCRs(crs []Queue) []queueconfig.QueueConfig {
	out := make([]queueconfig.QueueConfig, 0, len(crs))
	for i := range crs {
		out = append(out, ToYuniKornQueueConfig(crs[i].Spec.Queue))
	}
	return out
}

// SortByNamespaceName returns a copy of the slice ordered by (namespace, name).
//
// The webhook uses this so admission decisions are deterministic and
// independent of API-server pagination/list order: any two webhook calls that
// see the same set of CRs validate against an identically-ordered merged
// config.
func SortByNamespaceName(in []Queue) []Queue {
	out := make([]Queue, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}
