package test

import (
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type MockedNodeInformer struct {
	nodeLister v1.NodeLister
}

func NewMockedNodeInformer() *MockedNodeInformer {
	return &MockedNodeInformer{
		nodeLister: NewNodeListerMock(),
	}
}

func (m *MockedNodeInformer) Informer() cache.SharedIndexInformer {
	return nil
}

func (m *MockedNodeInformer) Lister() v1.NodeLister {
	return m.nodeLister
}

func (m *MockedNodeInformer) SetLister(lister v1.NodeLister) {
	m.nodeLister = lister
}

