package pod_resource_scaling

import (
	"testing"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestPodScaling(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Pod Resource Scaling Suite")
}

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred
var dev string
