package test

import (
	"fmt"

	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
)

// CSINodeListerMock declares a storagev1.CSINode type for testing.
type CSINodeListerMock []storagev1.CSINode

// Get returns a fake CSINode object.
func (n CSINodeListerMock) Get(name string) (*storagev1.CSINode, error) {
	for _, cn := range n {
		if cn.Name == name {
			return &cn, nil
		}
	}
	return nil, errors.NewNotFound(storagev1.Resource("csinodes"), name)
}

// List lists all CSINodes in the indexer.
func (n CSINodeListerMock) List(selector labels.Selector) (ret []*storagev1.CSINode, err error) {
	return nil, fmt.Errorf("not implemented")
}
