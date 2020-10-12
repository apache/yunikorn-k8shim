package predicates_test

import (
	"path/filepath"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/ginkgo/reporters"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

var k = k8s.KubeCtl{}
var oldConfigMap *v1.ConfigMap
var annotation string

var _ = BeforeSuite(func() {
	annotation = "ann-" + common.RandSeq(10)
	By("Enable basic scheduling config over config maps")
	Ω(k.SetClient()).To(BeNil())
	var c, err = k.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.DefaultYuniKornConfigMap)
	Ω(err).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())

	oldConfigMap = c.DeepCopy()
	Ω(c).Should(BeEquivalentTo(oldConfigMap))
	configStr, err2 := common.CreateBasicConfigMap().ToYAML()
	Ω(err2).NotTo(HaveOccurred())

	c.Data[configmanager.DefaultPolicyGroup] = configStr
	var d, err3 = k.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(d).NotTo(BeNil())
	// Updating scheduler pod annotation to trigger force refresh of configmaps
	// https://jira.cloudera.com/browse/COMPX-4042
	Ω(k.UpdateYunikornSchedulerPodAnnotation(annotation)).NotTo(HaveOccurred())
	err = yunikorn.WaitForSchedPolicy("default", 2*time.Minute)
	Ω(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("Restoring the old config maps")
	var c, err = k.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.DefaultYuniKornConfigMap)
	Ω(err).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())
	c.Data = oldConfigMap.Data
	var e, err3 = k.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(e).NotTo(BeNil())
	// Updating scheduler pod annotation to trigger force refresh of configmaps
	// https://jira.cloudera.com/browse/COMPX-4042
	Ω(k.RemoveYunikornSchedulerPodAnnotation(annotation)).NotTo(HaveOccurred())
})

func TestPredicates(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(
		filepath.Join(configmanager.YuniKornTestConfig.LogDir, "Predicates_junit.xml"))
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "TestPredicates", []ginkgo.Reporter{junitReporter})
}

//type Benchmarker ginkgo.Benchmarker
var Fail = ginkgo.Fail

var Describe = ginkgo.Describe
var DescribeTable = table.DescribeTable
var Entry = table.Entry
var Context = ginkgo.Context
var It = ginkgo.It
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach

// Declarations for Gomega DSL
var RegisterFailHandler = gomega.RegisterFailHandler
var Ω = gomega.Ω

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var BeNil = gomega.BeNil
var BeTrue = gomega.BeTrue
var HaveOccurred = gomega.HaveOccurred
var MatchRegexp = gomega.MatchRegexp
var BeZero = gomega.BeZero
var BeEquivalentTo = gomega.BeEquivalentTo
