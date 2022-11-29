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

package conf

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	schedulerconf "github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

const (
	AdmissionControllerPrefix = "admissionController."
	WebHookPrefix             = AdmissionControllerPrefix + "webHook."
	FilteringPrefix           = AdmissionControllerPrefix + "filtering."
	AccessControlPrefix       = AdmissionControllerPrefix + "accessControl."

	// webhook configuration
	AMWebHookAMServiceName           = WebHookPrefix + "amServiceName"
	AMWebHookSchedulerServiceAddress = WebHookPrefix + "schedulerServiceAddress"

	// filtering configuration
	AMFilteringProcessNamespaces = FilteringPrefix + "processNamespaces"
	AMFilteringBypassNamespaces  = FilteringPrefix + "bypassNamespaces"
	AMFilteringLabelNamespaces   = FilteringPrefix + "labelNamespaces"
	AMFilteringNoLabelNamespaces = FilteringPrefix + "noLabelNamespaces"

	// access control configuration
	AMAccessControlBypassAuth       = AccessControlPrefix + "bypassAuth"
	AMAccessControlTrustControllers = AccessControlPrefix + "trustControllers"
	AMAccessControlSystemUsers      = AccessControlPrefix + "systemUsers"
	AMAccessControlExternalUsers    = AccessControlPrefix + "externalUsers"
	AMAccessControlExternalGroups   = AccessControlPrefix + "externalGroups"
)

const (
	// webhook defaults
	DefaultWebHookAmServiceName           = "yunikorn-admission-controller-service"
	DefaultWebHookSchedulerServiceAddress = "yunikorn-service:9080"

	// filtering defaults
	DefaultFilteringProcessNamespaces = ""
	DefaultFilteringBypassNamespaces  = "^kube-system$"
	DefaultFilteringLabelNamespaces   = ""
	DefaultFilteringNoLabelNamespaces = ""

	// access control defaults
	DefaultAccessControlBypassAuth       = false
	DefaultAccessControlTrustControllers = true
	DefaultAccessControlSystemUsers      = "^system:serviceaccount:kube-system:"
	DefaultAccessControlExternalUsers    = ""
	DefaultAccessControlExternalGroups   = ""
)

type AdmissionControllerConf struct {
	namespace  string
	kubeConfig string

	// mutable values require locking
	enableConfigHotRefresh  bool
	policyGroup             string
	amServiceName           string
	schedulerServiceAddress string
	processNamespaces       []*regexp.Regexp
	bypassNamespaces        []*regexp.Regexp
	labelNamespaces         []*regexp.Regexp
	noLabelNamespaces       []*regexp.Regexp
	bypassAuth              bool
	trustControllers        bool
	systemUsers             []*regexp.Regexp
	externalUsers           []*regexp.Regexp
	externalGroups          []*regexp.Regexp
	configMaps              []*v1.ConfigMap

	configMapInformer informersv1.ConfigMapInformer
	stopChan          chan struct{}
	lock              sync.RWMutex
}

func NewAdmissionControllerConf(configMaps []*v1.ConfigMap) *AdmissionControllerConf {
	acc := &AdmissionControllerConf{
		namespace:  schedulerconf.GetSchedulerNamespace(),
		kubeConfig: schedulerconf.GetDefaultKubeConfigPath(),
	}
	acc.updateConfigMaps(configMaps, true)
	return acc
}

func (acc *AdmissionControllerConf) StartInformers(kubeClient client.KubeClient) {
	informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient.GetClientSet(), 0, informers.WithNamespace(acc.namespace))
	acc.stopChan = make(chan struct{})
	informerFactory.Start(acc.stopChan)
	acc.configMapInformer = informerFactory.Core().V1().ConfigMaps()
	acc.configMapInformer.Informer().AddEventHandler(&configMapUpdateHandler{conf: acc})
	go acc.configMapInformer.Informer().Run(acc.stopChan)
	if err := acc.waitForSync(time.Second, 30*time.Second); err != nil {
		log.Logger().Warn("Failed to sync informers", zap.Error(err))
	}
}

func (acc *AdmissionControllerConf) StopInformers() {
	if acc.stopChan != nil {
		close(acc.stopChan)
	}
}

func (acc *AdmissionControllerConf) GetNamespace() string {
	return acc.namespace
}

func (acc *AdmissionControllerConf) GetKubeConfig() string {
	return acc.kubeConfig
}

func (acc *AdmissionControllerConf) GetEnableConfigHotRefresh() bool {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.enableConfigHotRefresh
}

func (acc *AdmissionControllerConf) GetPolicyGroup() string {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.policyGroup
}

func GetPendingPolicyGroup(configs map[string]string) string {
	return parseConfigString(configs, schedulerconf.CMSvcPolicyGroup, schedulerconf.DefaultPolicyGroup)
}

func (acc *AdmissionControllerConf) GetAmServiceName() string {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.amServiceName
}

func (acc *AdmissionControllerConf) GetSchedulerServiceAddress() string {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.schedulerServiceAddress
}

func (acc *AdmissionControllerConf) GetProcessNamespaces() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.processNamespaces
}

func (acc *AdmissionControllerConf) GetBypassNamespaces() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.bypassNamespaces
}

func (acc *AdmissionControllerConf) GetLabelNamespaces() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.labelNamespaces
}

func (acc *AdmissionControllerConf) GetNoLabelNamespaces() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.noLabelNamespaces
}

func (acc *AdmissionControllerConf) GetBypassAuth() bool {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.bypassAuth
}

func (acc *AdmissionControllerConf) GetTrustControllers() bool {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.trustControllers
}

func (acc *AdmissionControllerConf) GetSystemUsers() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.systemUsers
}

func (acc *AdmissionControllerConf) GetExternalUsers() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.externalUsers
}

func (acc *AdmissionControllerConf) GetExternalGroups() []*regexp.Regexp {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	return acc.externalGroups
}

func (acc *AdmissionControllerConf) waitForSync(interval time.Duration, timeout time.Duration) error {
	return utils.WaitForCondition(func() bool {
		return acc.configMapInformer.Informer().HasSynced()
	}, interval, timeout)
}

type configMapUpdateHandler struct {
	conf *AdmissionControllerConf
}

func (h *configMapUpdateHandler) OnAdd(obj interface{}) {
	cm := utils.Convert2ConfigMap(obj)
	if idx, ok := h.configMapIndex(cm); ok {
		h.conf.configUpdated(idx, cm)
	}
}

func (h *configMapUpdateHandler) OnUpdate(_, newObj interface{}) {
	cm := utils.Convert2ConfigMap(newObj)
	if idx, ok := h.configMapIndex(cm); ok {
		h.conf.configUpdated(idx, cm)
	}
}

func (h *configMapUpdateHandler) OnDelete(obj interface{}) {
	var cm *v1.ConfigMap
	switch t := obj.(type) {
	case *v1.ConfigMap:
		cm = t
	case cache.DeletedFinalStateUnknown:
		cm = utils.Convert2ConfigMap(obj)
	default:
		log.Logger().Warn("unable to convert to configmap")
		return
	}
	if idx, ok := h.configMapIndex(cm); ok {
		h.conf.configUpdated(idx, nil)
	}
}

func (h *configMapUpdateHandler) configMapIndex(configMap *v1.ConfigMap) (int, bool) {
	if configMap == nil {
		return -1, false
	}
	if configMap.Namespace != h.conf.namespace {
		return -1, false
	}
	switch configMap.Name {
	case constants.DefaultConfigMapName:
		return 0, true
	case constants.ConfigMapName:
		return 1, true
	default:
		return -1, false
	}
}

func (acc *AdmissionControllerConf) configUpdated(index int, configMap *v1.ConfigMap) {
	configMaps := acc.GetConfigMaps()
	configMaps[index] = configMap
	acc.updateConfigMaps(configMaps, false)
}

func (acc *AdmissionControllerConf) GetConfigMaps() []*v1.ConfigMap {
	acc.lock.RLock()
	defer acc.lock.RUnlock()

	result := make([]*v1.ConfigMap, 0)
	result = append(result, acc.configMaps...)
	return result
}

func (acc *AdmissionControllerConf) updateConfigMaps(configMaps []*v1.ConfigMap, initial bool) {
	acc.lock.Lock()
	defer acc.lock.Unlock()

	// check for enable config hot refresh
	if !initial && !acc.enableConfigHotRefresh {
		log.Logger().Warn("Config hot-refresh is disabled, ignoring configuration update")
		return
	}

	acc.configMaps = configMaps
	configs := schedulerconf.FlattenConfigMaps(configMaps)

	// hot refresh
	acc.enableConfigHotRefresh = parseConfigBool(configs, schedulerconf.CMSvcEnableConfigHotRefresh, schedulerconf.DefaultEnableConfigHotRefresh)

	// logging
	logLevel := parseConfigInt(configs, schedulerconf.CMLogLevel, schedulerconf.DefaultLoggingLevel)
	log.GetZapConfigs().Level.SetLevel(zapcore.Level(logLevel))

	// scheduler
	acc.policyGroup = parseConfigString(configs, schedulerconf.CMSvcPolicyGroup, schedulerconf.DefaultPolicyGroup)

	// webhook
	acc.amServiceName = parseConfigString(configs, AMWebHookAMServiceName, DefaultWebHookAmServiceName)
	acc.schedulerServiceAddress = parseConfigString(configs, AMWebHookSchedulerServiceAddress, DefaultWebHookSchedulerServiceAddress)

	// filtering
	acc.processNamespaces = parseConfigRegexps(configs, AMFilteringProcessNamespaces, DefaultFilteringProcessNamespaces)
	acc.bypassNamespaces = parseConfigRegexps(configs, AMFilteringBypassNamespaces, DefaultFilteringBypassNamespaces)
	acc.labelNamespaces = parseConfigRegexps(configs, AMFilteringLabelNamespaces, DefaultFilteringLabelNamespaces)
	acc.noLabelNamespaces = parseConfigRegexps(configs, AMFilteringNoLabelNamespaces, DefaultFilteringNoLabelNamespaces)

	// access control
	acc.bypassAuth = parseConfigBool(configs, AMAccessControlBypassAuth, DefaultAccessControlBypassAuth)
	acc.trustControllers = parseConfigBool(configs, AMAccessControlTrustControllers, DefaultAccessControlTrustControllers)
	acc.systemUsers = parseConfigRegexps(configs, AMAccessControlSystemUsers, DefaultAccessControlSystemUsers)
	acc.externalUsers = parseConfigRegexps(configs, AMAccessControlExternalUsers, DefaultAccessControlExternalUsers)
	acc.externalGroups = parseConfigRegexps(configs, AMAccessControlExternalGroups, DefaultAccessControlExternalGroups)

	acc.dumpConfigurationInternal()
}

func (acc *AdmissionControllerConf) DumpConfiguration() {
	acc.lock.RLock()
	defer acc.lock.RUnlock()
	acc.dumpConfigurationInternal()
}

func (acc *AdmissionControllerConf) dumpConfigurationInternal() {
	log.Logger().Info("Loaded admission controller configuration",
		zap.String("namespace", acc.namespace),
		zap.String("kubeConfig", acc.kubeConfig),
		zap.String("policyGroup", acc.policyGroup),
		zap.String("amServiceName", acc.amServiceName),
		zap.String("schedulerServiceAddress", acc.schedulerServiceAddress),
		zap.Strings("processNamespaces", regexpsString(acc.processNamespaces)),
		zap.Strings("bypassNamespaces", regexpsString(acc.bypassNamespaces)),
		zap.Strings("labelNamespaces", regexpsString(acc.labelNamespaces)),
		zap.Strings("noLabelNamespaces", regexpsString(acc.noLabelNamespaces)),
		zap.Bool("bypassAuth", acc.bypassAuth),
		zap.Bool("trustControllers", acc.trustControllers),
		zap.Strings("systemUsers", regexpsString(acc.systemUsers)),
		zap.Strings("externalUsers", regexpsString(acc.externalUsers)),
		zap.Strings("externalGroups", regexpsString(acc.externalGroups)))
}

func regexpsString(regexes []*regexp.Regexp) []string {
	result := make([]string, 0)
	for _, regex := range regexes {
		result = append(result, regex.String())
	}
	return result
}

func parseConfigRegexps(config map[string]string, key string, defaultValue string) []*regexp.Regexp {
	value := parseConfigString(config, key, defaultValue)
	result, err := parseRegexes(value)
	if err != nil {
		log.Logger().Error(fmt.Sprintf("Unable to parse regex values '%s' for configuration '%s', using default value '%s'",
			value, key, defaultValue), zap.Error(err))
		result, err = parseRegexes(defaultValue)
		if err != nil {
			log.Logger().Fatal("BUG: can't parse default regex pattern", zap.Error(err))
		}
	}
	return result
}

func parseConfigBool(config map[string]string, key string, defaultValue bool) bool {
	value := parseConfigString(config, key, fmt.Sprintf("%t", defaultValue))
	result, err := strconv.ParseBool(value)
	if err != nil {
		log.Logger().Error(fmt.Sprintf("Unable to parse bool value '%s' for configuration '%s', using default value '%t'",
			value, key, defaultValue), zap.Error(err))
		result = defaultValue
	}
	return result
}

func parseConfigInt(config map[string]string, key string, defaultValue int) int {
	value := parseConfigString(config, key, fmt.Sprintf("%d", defaultValue))
	result, err := strconv.ParseInt(value, 10, 31)
	if err != nil {
		log.Logger().Error(fmt.Sprintf("Unable to parse int value '%s' for configuration '%s', using default value '%d'",
			value, key, defaultValue), zap.Error(err))
		return defaultValue
	}
	return int(result)
}

func parseConfigString(config map[string]string, key string, defaultValue string) string {
	if value, ok := config[key]; ok {
		return value
	}
	return defaultValue
}

func parseRegexes(patterns string) ([]*regexp.Regexp, error) {
	result := make([]*regexp.Regexp, 0)
	for _, pattern := range strings.Split(patterns, ",") {
		pattern = strings.TrimSpace(pattern)
		if len(pattern) == 0 {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Logger().Error("Unable to compile regular expression", zap.String("pattern", pattern), zap.Error(err))
			return nil, err
		}
		result = append(result, re)
	}
	return result, nil
}
