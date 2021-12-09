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
// Code generated by informer-gen. DO NOT EDIT.

package v1beta2

import (
	"context"
	time "time"

	sparkoperatork8siov1beta2 "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	versioned "github.com/apache/incubator-yunikorn-k8shim/pkg/sparkclient/clientset/versioned"
	internalinterfaces "github.com/apache/incubator-yunikorn-k8shim/pkg/sparkclient/informers/externalversions/internalinterfaces"
	v1beta2 "github.com/apache/incubator-yunikorn-k8shim/pkg/sparkclient/listers/sparkoperator.k8s.io/v1beta2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// SparkApplicationInformer provides access to a shared informer and lister for
// SparkApplications.
type SparkApplicationInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1beta2.SparkApplicationLister
}

type sparkApplicationInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewSparkApplicationInformer constructs a new informer for SparkApplication type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewSparkApplicationInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredSparkApplicationInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredSparkApplicationInformer constructs a new informer for SparkApplication type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredSparkApplicationInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.SparkoperatorV1beta2().SparkApplications(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.SparkoperatorV1beta2().SparkApplications(namespace).Watch(context.TODO(), options)
			},
		},
		&sparkoperatork8siov1beta2.SparkApplication{},
		resyncPeriod,
		indexers,
	)
}

func (f *sparkApplicationInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredSparkApplicationInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *sparkApplicationInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&sparkoperatork8siov1beta2.SparkApplication{}, f.defaultInformer)
}

func (f *sparkApplicationInformer) Lister() v1beta2.SparkApplicationLister {
	return v1beta2.NewSparkApplicationLister(f.Informer().GetIndexer())
}
