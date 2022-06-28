package simple_preemptor_test

import (
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestSimplePreemptor(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "SimplePreemptor Suite")
}

var Ω = gomega.Ω
