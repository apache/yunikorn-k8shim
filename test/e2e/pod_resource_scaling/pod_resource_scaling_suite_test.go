package pod_resource_scaling

import (
	"path/filepath"
	"testing"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestPodResourceScaling(t *testing.T) {
	ginkgo.ReportAfterSuite("TestPodResourceScaling", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-pod_resource_scaling_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Pod Resource Scaling Suite")
}

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred
var Equal = gomega.Equal
var By = ginkgo.By
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
