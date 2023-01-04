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

package admission

import (
	ctx "context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/pki"
)

const (
	secretName        = "admission-controller-secrets"
	validatingWebhook = "yunikorn-admission-controller-validations"
	validateConfHook  = "admission-webhook.yunikorn.validate-conf"
	mutatingWebhook   = "yunikorn-admission-controller-mutations"
	mutatePodsWebhook = "admission-webhook.yunikorn.mutate-pods"
	caCert1Path       = "cacert1.pem"
	caCert2Path       = "cacert2.pem"
	caPrivateKey1Path = "cakey1.pem"
	caPrivateKey2Path = "cakey2.pem"
)

// WebhookManager is used to handle all registration requirements for the webhook, including certificates
type WebhookManager interface {
	// LoadCACertificates is used to load CA certs from K8s secrets and update if needed
	LoadCACertificates() error

	// InstallWebhooks is used to install or update webhooks
	InstallWebhooks() error

	// GenerateServerCertificate is used to generate a server certificate chain
	GenerateServerCertificate() (*tls.Certificate, error)

	// WaitForCertificateExpiration blocks until certificates need to be renewed
	WaitForCertificateExpiration()
}

type webhookManagerImpl struct {
	conf             *conf.AdmissionControllerConf
	namespace        string
	serviceName      string
	clientset        kubernetes.Interface
	conflictAttempts int

	// mutable values (require locking)
	caCert1    *x509.Certificate
	caKey1     *rsa.PrivateKey
	caCert2    *x509.Certificate
	caKey2     *rsa.PrivateKey
	expiration time.Time

	sync.RWMutex
}

// NewWebhookManager is used to create a new webhook manager
func NewWebhookManager(conf *conf.AdmissionControllerConf) (WebhookManager, error) {
	kubeconfig, err := client.CreateRestConfig(conf.GetKubeConfig())
	if err != nil {
		log.Logger().Error("Unable to create kubernetes config", zap.Error(err))
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		log.Logger().Error("Unable to create kubernetes clientset", zap.Error(err))
		return nil, err
	}
	return newWebhookManagerImpl(conf, clientset), nil
}

func newWebhookManagerImpl(conf *conf.AdmissionControllerConf, clientset kubernetes.Interface) *webhookManagerImpl {
	wm := &webhookManagerImpl{
		conf:             conf,
		clientset:        clientset,
		conflictAttempts: 10,
	}

	return wm
}

func (wm *webhookManagerImpl) LoadCACertificates() error {
	attempts := 0
	for {
		updated, err := wm.loadCaCertificatesInternal()
		if err != nil {
			return err
		}
		if !updated {
			return nil
		}
		attempts++
		if attempts >= wm.conflictAttempts {
			return errors.New("webhook: Unable to update CA certificates after max attempts reached")
		}
	}
}

func (wm *webhookManagerImpl) GenerateServerCertificate() (*tls.Certificate, error) {
	caCert, caKey, err := wm.getBestCACertificate()
	if err != nil {
		log.Logger().Error("Unable to find best CA certificate", zap.Error(err))
		return nil, err
	}

	serviceName := wm.conf.GetAmServiceName()
	namespace := wm.conf.GetNamespace()

	commonName := fmt.Sprintf("%s.%s.svc", serviceName, namespace)
	dnsNames := []string{
		wm.serviceName,
		fmt.Sprintf("%s.%s", serviceName, namespace),
		fmt.Sprintf("%s.%s.svc", serviceName, namespace),
	}

	log.Logger().Info("Generating server certificate...")

	cert, key, err := pki.GenerateServerCertificate(commonName, dnsNames, caCert, caKey)
	if err != nil {
		log.Logger().Error("Unable to generate server certificate", zap.Error(err))
		return nil, err
	}

	log.Logger().Info("Generated server certificate",
		zap.String("commonName", cert.Subject.CommonName),
		zap.Strings("dnsNames", cert.DNSNames),
		zap.Time("notBefore", cert.NotBefore),
		zap.Time("notAfter", cert.NotAfter),
		zap.Stringer("issuer", cert.Issuer),
		zap.Int64("issuerSerialNumber", caCert.SerialNumber.Int64()))

	certChain := make([]*x509.Certificate, 0)
	certChain = append(certChain, cert)
	certChain = append(certChain, caCert)

	certPemChain, err := pki.EncodeCertChainPem(certChain)
	if err != nil {
		log.Logger().Error("Unable to encode certificate chain", zap.Error(err))
		return nil, err
	}

	keyPem, err := pki.EncodePrivateKeyPem(key)
	if err != nil {
		log.Logger().Error("Unable to encode private key", zap.Error(err))
	}

	pair, err := tls.X509KeyPair(*certPemChain, *keyPem)
	if err != nil {
		return nil, err
	}

	return &pair, nil
}

func (wm *webhookManagerImpl) InstallWebhooks() error {
	attempts := 0
	for {
		recheck, err := wm.installValidatingWebhook()
		if err != nil {
			return err
		}
		if !recheck {
			break
		}
		// safety valve: if the webhook keeps changing, break out eventually
		attempts++
		if attempts >= wm.conflictAttempts {
			log.Logger().Error("Unable to install validating webhook after max attempts")
			return errors.New("webhook: unable to install validating webhook after max attempts")
		}
	}

	attempts = 0
	for {
		recheck, err := wm.installMutatingWebhook()
		if err != nil {
			return err
		}
		if !recheck {
			break
		}
		// safety valve: if the webhook keeps changing, break out eventually
		attempts++
		if attempts >= wm.conflictAttempts {
			log.Logger().Error("Unable to install mutating webhook after max attempts")
			return errors.New("webhook: unable to install mutating webhook after max attempts")
		}
	}

	return nil
}

func (wm *webhookManagerImpl) WaitForCertificateExpiration() {
	renewTime := wm.getExpiration().AddDate(0, 0, -30)
	time.Sleep(time.Until(renewTime))
}

func (wm *webhookManagerImpl) getExpiration() time.Time {
	wm.RLock()
	defer wm.RUnlock()
	return wm.expiration
}

func (wm *webhookManagerImpl) installValidatingWebhook() (bool, error) {
	log.Logger().Info("Checking for existing validating webhook...")

	caBundle, err := wm.encodeCaBundle()
	if err != nil {
		log.Logger().Error("Unable to encode CA bundle", zap.Error(err))
		return false, err
	}

	hook, err := wm.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx.Background(), validatingWebhook, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Logger().Error("Unable to read validating webhook", zap.String("name", validatingWebhook), zap.Error(err))
			return false, err
		}
		log.Logger().Info("Unable to find validating webhook, will create it", zap.String("name", validatingWebhook))
		hook = nil
	}

	if hook == nil {
		// create
		hook = wm.createEmptyValidatingWebhook()
		wm.populateValidatingWebhook(hook, caBundle)

		// sanity check to ensure that the hook is well-formed before we update it
		err = wm.checkValidatingWebhook(hook)
		if err != nil {
			log.Logger().Error("BUG: Validating webhook is invalid", zap.Error(err))
			return false, err
		}

		log.Logger().Info("Creating validating webhook", zap.String("webhook", hook.Name))
		_, err = wm.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx.Background(), hook, metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
				// go around again
				return true, nil
			}
			log.Logger().Error("Unable to install validating webhook", zap.Error(err))
			return false, err
		}
	} else {
		err = wm.checkValidatingWebhook(hook)
		if err == nil {
			log.Logger().Info("Validating webhook OK")
			return false, nil
		}

		// update
		wm.populateValidatingWebhook(hook, caBundle)

		// sanity check to ensure that the hook is well-formed before we update it
		err = wm.checkValidatingWebhook(hook)
		if err != nil {
			log.Logger().Error("BUG: Validating webhook is invalid", zap.Error(err))
			return false, err
		}

		log.Logger().Info("Updating validating webhook", zap.String("webhook", hook.Name))
		_, err = wm.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Update(ctx.Background(), hook, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				// go around again
				return true, nil
			}
			log.Logger().Error("Unable to update validating webhook", zap.Error(err))
			return false, err
		}
	}

	return true, nil
}

func (wm *webhookManagerImpl) installMutatingWebhook() (bool, error) {
	log.Logger().Info("Checking for existing mutating webhook...")

	caBundle, err := wm.encodeCaBundle()
	if err != nil {
		log.Logger().Error("Unable to encode CA bundle", zap.Error(err))
		return false, err
	}

	hook, err := wm.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx.Background(), mutatingWebhook, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Logger().Error("Unable to read mutating webhook", zap.String("name", mutatingWebhook), zap.Error(err))
			return false, err
		}
		log.Logger().Info("Unable to find mutating webhook, will create it", zap.String("name", mutatingWebhook))
		hook = nil
	}

	if hook == nil {
		// create
		hook = wm.createEmptyMutatingWebhook()
		wm.populateMutatingWebhook(hook, caBundle)

		// sanity check to ensure that the hook is well-formed before we update it
		err = wm.checkMutatingWebhook(hook)
		if err != nil {
			log.Logger().Error("BUG: Mutating webhook is invalid", zap.Error(err))
			return false, err
		}

		log.Logger().Info("Creating mutating webhook", zap.String("webhook", hook.Name))
		_, err = wm.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(ctx.Background(), hook, metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
				// go around again
				return true, nil
			}
			log.Logger().Error("Unable to install mutating webhook", zap.Error(err))
			return false, err
		}
	} else {
		err = wm.checkMutatingWebhook(hook)
		if err == nil {
			log.Logger().Info("Mutating webhook OK")
			return false, nil
		}

		// update
		wm.populateMutatingWebhook(hook, caBundle)

		// sanity check to ensure that the hook is well-formed before we update it
		err = wm.checkMutatingWebhook(hook)
		if err != nil {
			log.Logger().Error("BUG: Mutating webhook is invalid", zap.Error(err))
			return false, err
		}

		log.Logger().Info("Updating mutating webhook", zap.String("hook", hook.Name))
		_, err = wm.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(ctx.Background(), hook, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				// go around again
				return true, nil
			}
			log.Logger().Error("Unable to update mutating webhook", zap.Error(err))
			return false, err
		}
	}

	return true, nil
}

func (wm *webhookManagerImpl) checkValidatingWebhook(webhook *v1.ValidatingWebhookConfiguration) error {
	ignore := v1.Ignore
	none := v1.SideEffectClassNone
	path := "/validate-conf"

	value, ok := webhook.ObjectMeta.GetLabels()["app"]
	if !ok || value != "yunikorn" {
		return errors.New("webhook: missing label app=yunikorn")
	}

	if len(webhook.Webhooks) != 1 {
		return errors.New("webhook: wrong webhook count")
	}

	hook := webhook.Webhooks[0]
	if hook.Name != validateConfHook {
		return errors.New("webhook: wrong webhook name")
	}

	cc := hook.ClientConfig
	svc := cc.Service
	if svc == nil {
		return errors.New("webhook: missing service")
	}

	if svc.Name != wm.conf.GetAmServiceName() {
		return errors.New("webhook: wrong service name")
	}

	if svc.Namespace != wm.conf.GetNamespace() {
		return errors.New("webhook: wrong service namespace")
	}

	if svc.Path == nil || *svc.Path != path {
		return errors.New("webhook: wrong service path")
	}

	err := wm.validateCaBundle(cc.CABundle)
	if err != nil {
		return err
	}

	rules := hook.Rules
	if len(rules) != 1 {
		return errors.New("webhook: wrong rule count")
	}

	rule := rules[0]
	if len(rule.Operations) != 2 || rule.Operations[0] != v1.Create || rule.Operations[1] != v1.Update {
		return errors.New("webhook: wrong operations")
	}

	if len(rule.APIGroups) != 1 || rule.APIGroups[0] != "" {
		return errors.New("webhook: wrong api groups")
	}

	if len(rule.APIVersions) != 1 || rule.APIVersions[0] != "v1" {
		return errors.New("webhook: wrong api versions")
	}

	if len(rule.Resources) != 1 || rule.Resources[0] != "configmaps" {
		return errors.New("webhook: wrong resources")
	}

	if hook.FailurePolicy == nil || *hook.FailurePolicy != ignore {
		return errors.New("webhook: wrong failure policy")
	}

	if hook.SideEffects == nil || *hook.SideEffects != none {
		return errors.New("webhook: wrong side effects")
	}

	return nil
}

func (wm *webhookManagerImpl) checkMutatingWebhook(webhook *v1.MutatingWebhookConfiguration) error {
	ignore := v1.Ignore
	none := v1.SideEffectClassNone
	path := "/mutate"

	value, ok := webhook.ObjectMeta.GetLabels()["app"]
	if !ok || value != "yunikorn" {
		return errors.New("webhook: missing label app=yunikorn")
	}

	if len(webhook.Webhooks) != 1 {
		return errors.New("mutate webhook: wrong webhook count")
	}

	hook := webhook.Webhooks[0]
	if hook.Name != mutatePodsWebhook {
		return errors.New("webhook: wrong webhook name")
	}

	cc := hook.ClientConfig
	svc := cc.Service
	if svc == nil {
		return errors.New("webhook: missing service")
	}

	if svc.Name != wm.conf.GetAmServiceName() {
		return errors.New("webhook: wrong service name")
	}

	if svc.Namespace != wm.conf.GetNamespace() {
		return errors.New("webhook: wrong service namespace")
	}

	if svc.Path == nil || *svc.Path != path {
		return errors.New("webhook: wrong service path")
	}

	err := wm.validateCaBundle(cc.CABundle)
	if err != nil {
		return err
	}

	rules := hook.Rules
	if len(rules) != 1 {
		return errors.New("webhook: wrong rule count")
	}

	rule := rules[0]
	if len(rule.Operations) != 2 || rule.Operations[0] != v1.Create || rule.Operations[1] != v1.Update {
		return errors.New("webhook: wrong operations")
	}

	if len(rule.APIGroups) != 3 || rule.APIGroups[0] != "" ||
		rule.APIGroups[1] != "apps" || rule.APIGroups[2] != "batch" {
		return errors.New("webhook: wrong api groups")
	}

	if len(rule.APIVersions) != 1 || rule.APIVersions[0] != "v1" {
		return errors.New("webhook: wrong api versions")
	}

	if len(rule.Resources) != 7 || rule.Resources[0] != "pods" ||
		rule.Resources[1] != "deployments" || rule.Resources[2] != "replicasets" ||
		rule.Resources[3] != "statefulsets" || rule.Resources[4] != "daemonsets" ||
		rule.Resources[5] != "jobs" || rule.Resources[6] != "cronjobs" {
		return errors.New("webhook: wrong resources")
	}

	if hook.FailurePolicy == nil || *hook.FailurePolicy != ignore {
		return errors.New("webhook: wrong failure policy")
	}

	if hook.SideEffects == nil || *hook.SideEffects != none {
		return errors.New("webhook: wrong side effects")
	}

	return nil
}

func (wm *webhookManagerImpl) validateCaBundle(bundle []byte) error {
	wm.RLock()
	defer wm.RUnlock()

	pem, err := pki.EncodeCertChainPem([]*x509.Certificate{wm.caCert1, wm.caCert2})
	if err != nil {
		return err
	}

	if len(bundle) != len(*pem) {
		return errors.New("webhook: certs don't match")
	}

	for i := 0; i < len(*pem); i++ {
		if bundle[i] != (*pem)[i] {
			return errors.New("webhook: certs don't match")
		}
	}

	return nil
}

func (wm *webhookManagerImpl) encodeCaBundle() ([]byte, error) {
	wm.RLock()
	defer wm.RUnlock()

	if wm.caCert1 == nil || wm.caCert2 == nil {
		return nil, errors.New("webhook: CA certificates are not yet initialized")
	}

	pem, err := pki.EncodeCertChainPem([]*x509.Certificate{wm.caCert1, wm.caCert2})
	if err != nil {
		return nil, err
	}

	return *pem, nil
}

func (wm *webhookManagerImpl) createEmptyValidatingWebhook() *v1.ValidatingWebhookConfiguration {
	return &v1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{},
		Webhooks:   []v1.ValidatingWebhook{},
	}
}

func (wm *webhookManagerImpl) populateValidatingWebhook(webhook *v1.ValidatingWebhookConfiguration, caBundle []byte) {
	ignore := v1.Ignore
	none := v1.SideEffectClassNone
	path := "/validate-conf"

	namespace := wm.conf.GetNamespace()
	serviceName := wm.conf.GetAmServiceName()

	webhook.ObjectMeta.Name = validatingWebhook
	webhook.ObjectMeta.Labels = map[string]string{"app": "yunikorn"}
	webhook.Webhooks = []v1.ValidatingWebhook{
		{
			Name: validateConfHook,
			ClientConfig: v1.WebhookClientConfig{
				Service:  &v1.ServiceReference{Name: serviceName, Namespace: namespace, Path: &path},
				CABundle: caBundle,
			},
			Rules: []v1.RuleWithOperations{{
				Operations: []v1.OperationType{v1.Create, v1.Update},
				Rule:       v1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"configmaps"}},
			}},
			FailurePolicy:           &ignore,
			AdmissionReviewVersions: []string{"v1"},
			SideEffects:             &none,
		},
	}
}

func (wm *webhookManagerImpl) createEmptyMutatingWebhook() *v1.MutatingWebhookConfiguration {
	return &v1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{},
		Webhooks:   []v1.MutatingWebhook{},
	}
}

func (wm *webhookManagerImpl) populateMutatingWebhook(webhook *v1.MutatingWebhookConfiguration, caBundle []byte) {
	ignore := v1.Ignore
	none := v1.SideEffectClassNone
	path := "/mutate"

	namespace := wm.conf.GetNamespace()
	serviceName := wm.conf.GetAmServiceName()

	webhook.ObjectMeta.Name = mutatingWebhook
	webhook.ObjectMeta.Labels = map[string]string{"app": "yunikorn"}
	webhook.Webhooks = []v1.MutatingWebhook{
		{
			Name: mutatePodsWebhook,
			ClientConfig: v1.WebhookClientConfig{
				Service:  &v1.ServiceReference{Name: serviceName, Namespace: namespace, Path: &path},
				CABundle: caBundle,
			},
			Rules: []v1.RuleWithOperations{{
				Operations: []v1.OperationType{v1.Create, v1.Update},
				Rule: v1.Rule{APIGroups: []string{"", "apps", "batch"}, APIVersions: []string{"v1"}, Resources: []string{
					"pods", "deployments", "replicasets", "statefulsets", "daemonsets", "jobs", "cronjobs"}},
			}},
			FailurePolicy:           &ignore,
			AdmissionReviewVersions: []string{"v1"},
			SideEffects:             &none,
		},
	}
}

// gets the best certificate / private key pair to use (one with latest expiration)
func (wm *webhookManagerImpl) getBestCACertificate() (*x509.Certificate, *rsa.PrivateKey, error) {
	wm.RLock()
	defer wm.RUnlock()

	if wm.caCert1 == nil || wm.caCert2 == nil {
		return nil, nil, errors.New("webhook: CA certificates are not yet initialized")
	}

	if wm.caCert2.NotAfter.After(wm.caCert1.NotAfter) {
		return wm.caCert2, wm.caKey2, nil
	}
	return wm.caCert1, wm.caKey1, nil
}

func (wm *webhookManagerImpl) loadCaCertificatesInternal() (bool, error) {
	wm.Lock()
	defer wm.Unlock()

	namespace := wm.conf.GetNamespace()
	secret, err := wm.clientset.CoreV1().Secrets(namespace).Get(ctx.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		log.Logger().Error("Unable to retrieve admission-controller-secrets secrets", zap.Error(err))
		return false, err
	}

	// initially, data may not be present
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}

	dirty := false

	cert1, key1, err := getAndValidateCertificate(secret.Data, caCert1Path, caPrivateKey1Path)
	if err != nil {
		log.Logger().Info("Unable to get CA certificate #1", zap.Error(err))
	}

	cert2, key2, err := getAndValidateCertificate(secret.Data, caCert2Path, caPrivateKey2Path)
	if err != nil {
		log.Logger().Info("Unable to get CA certificate #2", zap.Error(err))
	}

	if cert1 == nil {
		log.Logger().Info("Generating CA Certificate #1...")
		notAfter := time.Now().AddDate(1, 0, 0)
		if cert2 == nil {
			// stagger expiration dates so that there is ~ 6 months between them
			notAfter = notAfter.AddDate(0, -6, 0)
		}
		cert1, key1, err = pki.GenerateCACertificate(notAfter)
		if err != nil {
			log.Logger().Error("Unable to generate CA certificate #1", zap.Error(err))
			return false, err
		}
		dirty = true
	}

	if cert2 == nil {
		log.Logger().Info("Generating CA Certificate #2...")
		cert2, key2, err = pki.GenerateCACertificate(time.Now().AddDate(1, 0, 0))
		if err != nil {
			log.Logger().Error("Unable to generate CA certificate #2", zap.Error(err))
			return false, err
		}
		dirty = true
	}

	if dirty {
		log.Logger().Info("CA certificates have changed, updating secrets")

		cert1Pem, err := pki.EncodeCertificatePem(cert1)
		if err != nil {
			log.Logger().Error("Unable to encode CA certificate #1", zap.Error(err))
			return false, err
		}
		key1Pem, err := pki.EncodePrivateKeyPem(key1)
		if err != nil {
			log.Logger().Error("Unable to encode CA private key #1", zap.Error(err))
			return false, err
		}
		cert2Pem, err := pki.EncodeCertificatePem(cert2)
		if err != nil {
			log.Logger().Error("Unable to encode CA certificate #2", zap.Error(err))
			return false, err
		}
		key2Pem, err := pki.EncodePrivateKeyPem(key2)
		if err != nil {
			log.Logger().Error("Unable to encode CA private key #2", zap.Error(err))
			return false, err
		}
		secret.Data[caCert1Path] = *cert1Pem
		secret.Data[caPrivateKey1Path] = *key1Pem
		secret.Data[caCert2Path] = *cert2Pem
		secret.Data[caPrivateKey2Path] = *key2Pem

		_, err = wm.clientset.CoreV1().Secrets(namespace).Update(ctx.Background(), secret, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) {
				// signal to caller that we need to be run again
				return true, nil
			}
			// report error to caller
			log.Logger().Error("Unable to update secrets", zap.Error(err))
			return false, err
		}

		// update successful, tell caller to re-run
		return true, err
	}

	log.Logger().Info("Got CA certificate #1",
		zap.Int64("serialNumber", cert1.SerialNumber.Int64()),
		zap.Time("notAfter", cert1.NotAfter))
	log.Logger().Info("Got CA certificate #2",
		zap.Int64("serialNumber", cert2.SerialNumber.Int64()),
		zap.Time("notAfter", cert2.NotAfter))

	wm.caCert1 = cert1
	wm.caKey1 = key1
	wm.caCert2 = cert2
	wm.caKey2 = key2
	wm.expiration = cert1.NotAfter
	if cert2.NotAfter.Before(cert1.NotAfter) {
		wm.expiration = cert2.NotAfter
	}

	return false, nil
}

func getAndValidateCertificate(secretData map[string][]byte, certName string, keyName string) (*x509.Certificate, *rsa.PrivateKey, error) {
	certPem, ok := secretData[certName]
	if !ok {
		return nil, nil, fmt.Errorf("webhook: no certificate found with id %s", certName)
	}
	privateKeyPem, ok := secretData[keyName]
	if !ok {
		return nil, nil, fmt.Errorf("webhook: no private key found with id %s", keyName)
	}

	cert, err := pki.DecodeCertificatePem(&certPem)
	if err != nil {
		return nil, nil, err
	}

	privateKey, err := pki.DecodePrivateKeyPem(&privateKeyPem)
	if err != nil {
		return nil, nil, err
	}

	cutoff := time.Now().AddDate(0, 0, 90)

	if cert.NotAfter.Before(cutoff) {
		return nil, nil, fmt.Errorf("webhook: ca certificate %s will expire within 90 days", certName)
	}
	return cert, privateKey, nil
}
