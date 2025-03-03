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
	"context"
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	arv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/apache/yunikorn-k8shim/pkg/admission/pki"
)

var (
	cacert1    *x509.Certificate
	cacert2    *x509.Certificate
	cakey1     *rsa.PrivateKey
	cakey2     *rsa.PrivateKey
	cacert1Pem []byte
	cacert2Pem []byte
	cakey1Pem  []byte
	cakey2Pem  []byte
	caBundle   []byte
	wmTestOnce sync.Once
)

func testSetupOnce(t *testing.T) {
	wmTestOnce.Do(func() {
		cacert1, cakey1 = caCertKeyPair(t, 180)
		cacert2, cakey2 = caCertKeyPair(t, 365)
		cacert1Pem = certPem(t, cacert1)
		cacert2Pem = certPem(t, cacert2)
		cakey1Pem = keyPem(t, cakey1)
		cakey2Pem = keyPem(t, cakey2)
		caBundle = caBundlePem(t, []*x509.Certificate{cacert1, cacert2})
	})
}

func TestLoadCACertificatesWhereValid(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()

	spec := createSecret()
	secret, err := clientset.CoreV1().Secrets(spec.Namespace).Create(context.Background(), spec, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to create secret")
	addCert(t, secret, cacert1, cakey1, 1)
	addCert(t, secret, cacert2, cakey2, 2)

	wm := newWebhookManagerImpl(createConfig(), clientset)
	err = wm.LoadCACertificates()
	assert.NilError(t, err, "failed to load CA certificates")
}

func TestLoadCACertificatesWithMissingSecret(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()

	wm := newWebhookManagerImpl(createConfig(), clientset)
	err := wm.LoadCACertificates()
	assert.ErrorContains(t, err, "not found", "get secrets didn't fail")
}

func TestLoadCACertificatesWithEmptySecret(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()

	spec := createSecret()
	_, err := clientset.CoreV1().Secrets(spec.Namespace).Create(context.Background(), spec, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to create secret")

	wm := newWebhookManagerImpl(createConfig(), clientset)
	err = wm.LoadCACertificates()
	assert.NilError(t, err, "failed to load CA certificates")
	secret, err := clientset.CoreV1().Secrets("default").Get(context.Background(), "admission-controller-secrets", metav1.GetOptions{})
	assert.NilError(t, err, "secret not found")
	_, ok := secret.Data["cacert1.pem"]
	assert.Assert(t, ok, "cacert2.pem not found")
	_, ok = secret.Data["cacert2.pem"]
	assert.Assert(t, ok, "cacert1.pem not found")
	_, ok = secret.Data["cakey1.pem"]
	assert.Assert(t, ok, "cakey1.pem not found")
	_, ok = secret.Data["cakey2.pem"]
	assert.Assert(t, ok, "cakey2.pem not found")
}

func TestLoadCACertificatesWithConflict(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()

	spec := createSecret()
	spec.ObjectMeta.SetAnnotations(map[string]string{"conflict": "true"})
	_, err := clientset.CoreV1().Secrets(spec.Namespace).Create(context.Background(), spec, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to create secret")

	wm := newWebhookManagerImpl(createConfig(), clientset)
	wm.conflictAttempts = 0
	err = wm.LoadCACertificates()
	assert.ErrorContains(t, err, "max attempts", "update secrets didn't fail")
}

func TestGenerateServerCertificate(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)

	cert, err := wm.GenerateServerCertificate()
	assert.NilError(t, err, "generate server certificate failed")
	assert.Equal(t, 2, len(cert.Certificate), "wrong cert length")

	servercert, err := x509.ParseCertificate(cert.Certificate[0])
	assert.NilError(t, err, "unable to parse server cert")
	assert.Equal(t, servercert.Subject.CommonName, "yunikorn-admission-controller-service.default.svc", "wrong CN")

	cacert, err := x509.ParseCertificate(cert.Certificate[1])
	assert.NilError(t, err, "unable to parse ca cert")
	assert.Assert(t, cacert.Equal(cacert2), "wrong ca cert selected")
}

func TestGenerateServerCertificateWithNoCACertificates(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := newWebhookManagerImpl(createConfig(), clientset)

	_, err := wm.GenerateServerCertificate()
	assert.ErrorContains(t, err, "CA certificate", "get best CA cert didn't fail")
}

func TestInstallWebhooksWithNoCACertificates(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := newWebhookManagerImpl(createConfig(), clientset)

	err := wm.InstallWebhooks()
	assert.ErrorContains(t, err, "CA certificate", "missing certs were not detected")
}

func TestInstallWebhooksWhenAlreadyPresent(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, caBundle)
	_, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(context.Background(), vh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add validating webhook")

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, caBundle)
	_, err = clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(context.Background(), mh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add mutating webhook")

	err = wm.InstallWebhooks()
	assert.NilError(t, err, "Install webhooks failed")
	vh, err = clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "yunikorn-admission-controller-validations", metav1.GetOptions{})
	assert.NilError(t, err, "validating webhook not found")
	err = wm.checkValidatingWebhook(vh)
	assert.NilError(t, err, "validating webhook is malformed")
	assert.Equal(t, vh.Generation, int64(0), "wrong generation for validating webhook")

	mh, err = clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), "yunikorn-admission-controller-mutations", metav1.GetOptions{})
	assert.NilError(t, err, "mutating webhook not found")
	err = wm.checkMutatingWebhook(mh)
	assert.NilError(t, err, "mutating webhook is malformed")
	assert.Equal(t, mh.Generation, int64(0), "wrong generation for mutating webhook")
}

func TestInstallWebhooksWithNoHooksPresent(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)

	err := wm.InstallWebhooks()
	assert.NilError(t, err, "Install webhooks failed")
	vh, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "yunikorn-admission-controller-validations", metav1.GetOptions{})
	assert.NilError(t, err, "validating webhook not found")
	err = wm.checkValidatingWebhook(vh)
	assert.NilError(t, err, "validating webhook is malformed")

	mh, err := clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), "yunikorn-admission-controller-mutations", metav1.GetOptions{})
	assert.NilError(t, err, "mutating webhook not found")
	err = wm.checkMutatingWebhook(mh)
	assert.NilError(t, err, "mutating webhook is malformed")
}

func TestInstallWebhooksWithWrongData(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, []byte{0})
	_, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(context.Background(), vh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add validating webhook")

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, []byte{0})
	_, err = clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(context.Background(), mh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add mutating webhook")

	err = wm.InstallWebhooks()
	assert.NilError(t, err, "Install webhooks failed")
	vh, err = clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "yunikorn-admission-controller-validations", metav1.GetOptions{})
	assert.NilError(t, err, "validating webhook not found")
	err = wm.checkValidatingWebhook(vh)
	assert.NilError(t, err, "validating webhook is malformed")

	mh, err = clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), "yunikorn-admission-controller-mutations", metav1.GetOptions{})
	assert.NilError(t, err, "mutating webhook not found")
	err = wm.checkMutatingWebhook(mh)
	assert.NilError(t, err, "mutating webhook is malformed")
}

func TestInstallWebhooksWithValidationConflict(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)
	wm.conflictAttempts = 0

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, []byte{0})
	vh.ObjectMeta.SetAnnotations(map[string]string{"conflict": "true"})
	_, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(context.Background(), vh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add validating webhook")

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, caBundle)
	_, err = clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(context.Background(), mh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add mutating webhook")

	err = wm.InstallWebhooks()
	assert.ErrorContains(t, err, "max attempts", "update webhook didn't fail")
}

func TestInstallWebhooksWithMutationConflict(t *testing.T) {
	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)
	wm.conflictAttempts = 0

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, caBundle)
	_, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(context.Background(), vh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add validating webhook")

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, []byte{0})
	mh.ObjectMeta.SetAnnotations(map[string]string{"conflict": "true"})
	_, err = clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(context.Background(), mh, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to add mutating webhook")

	err = wm.InstallWebhooks()
	assert.ErrorContains(t, err, "max attempts", "update webhook didn't fail")
}

func TestCheckValidatingWebhook(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		mutator  func(*arv1.ValidatingWebhookConfiguration)
	}{
		{name: "Valid", expected: "", mutator: func(_ *arv1.ValidatingWebhookConfiguration) {}},
		{name: "MissingLabel", expected: "missing label", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			delete(h.Labels, "app")
		}},
		{name: "WrongLabel", expected: "missing label", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Labels["app"] = "invalid-label"
		}},
		{name: "WrongWebhookCount", expected: "webhook count", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks = make([]arv1.ValidatingWebhook, 0)
		}},
		{name: "WrongWebhookName", expected: "webhook name", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Name = "invalid-hook-name"
		}},
		{name: "MissingService", expected: "missing service", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service = nil
		}},
		{name: "WrongServiceName", expected: "service name", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service.Name = "invalid-service-name"
		}},
		{name: "WrongServiceNamespace", expected: "service namespace", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service.Namespace = "invalid-namespace"
		}},
		{name: "NilServicePath", expected: "service path", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service.Path = nil
		}},
		{name: "WrongServicePath", expected: "service path", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			var path = "test"
			h.Webhooks[0].ClientConfig.Service.Path = &path
		}},
		{name: "BadCABundle", expected: "certs", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.CABundle = []byte{}
		}},
		{name: "WrongRuleCount", expected: "rule count", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules = []arv1.RuleWithOperations{}
		}},
		{name: "WrongOpCount", expected: "operations", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Operations = []arv1.OperationType{}
		}},
		{name: "MissingCreateOp", expected: "operations", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Operations[0] = arv1.Connect
		}},
		{name: "MissingUpdateOp", expected: "operations", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Operations[1] = arv1.Connect
		}},
		{name: "MissingAPIGroups", expected: "api groups", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIGroups = []string{}
		}},
		{name: "WrongAPIGroups", expected: "api groups", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIGroups[0] = "invalid-group"
		}},
		{name: "MissingAPIVersions", expected: "api versions", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIVersions = []string{}
		}},
		{name: "WrongAPIVersions", expected: "api versions", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIVersions[0] = "invalid-version"
		}},
		{name: "MissingResources", expected: "resources", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Resources = []string{}
		}},
		{name: "WrongResources", expected: "resources", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Resources[0] = "invalid-resource"
		}},
		{name: "MissingFailurePolicy", expected: "failure policy", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].FailurePolicy = nil
		}},
		{name: "WrongFailurePolicy", expected: "failure policy", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			fail := arv1.Fail
			h.Webhooks[0].FailurePolicy = &fail
		}},
		{name: "MissingSideEffects", expected: "side effects", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			h.Webhooks[0].SideEffects = nil
		}},
		{name: "WrongSideEffects", expected: "side effects", mutator: func(h *arv1.ValidatingWebhookConfiguration) {
			some := arv1.SideEffectClassSome
			h.Webhooks[0].SideEffects = &some
		}},
	}

	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vh := wm.createEmptyValidatingWebhook()
			wm.populateValidatingWebhook(vh, caBundle)
			c.mutator(vh)
			err := wm.checkValidatingWebhook(vh)
			if c.expected == "" {
				assert.NilError(t, err, "check failed")
			} else {
				assert.ErrorContains(t, err, c.expected, "check succeeded or had wrong failure mode")
			}
		})
	}
}

func TestCheckMutatingWebhook(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		mutator  func(*arv1.MutatingWebhookConfiguration)
	}{
		{name: "Valid", expected: "", mutator: func(_ *arv1.MutatingWebhookConfiguration) {}},
		{name: "MissingLabel", expected: "missing label", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			delete(h.Labels, "app")
		}},
		{name: "WrongLabel", expected: "missing label", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Labels["app"] = "invalid-app"
		}},
		{name: "WrongWebhookCount", expected: "webhook count", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks = make([]arv1.MutatingWebhook, 0)
		}},
		{name: "WrongWebhookName", expected: "webhook name", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Name = "invalid-hook-name"
		}},
		{name: "MissingService", expected: "missing service", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service = nil
		}},
		{name: "WrongServiceName", expected: "service name", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service.Name = "invalid-service-name"
		}},
		{name: "WrongServiceNamespace", expected: "service namespace", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service.Namespace = "invalid-namespace"
		}},
		{name: "NilServicePath", expected: "service path", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.Service.Path = nil
		}},
		{name: "WrongServicePath", expected: "service path", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			var path = "test"
			h.Webhooks[0].ClientConfig.Service.Path = &path
		}},
		{name: "BadCABundle", expected: "certs", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].ClientConfig.CABundle = []byte{}
		}},
		{name: "WrongRuleCount", expected: "rule count", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules = []arv1.RuleWithOperations{}
		}},
		{name: "WrongOpCount", expected: "operations", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Operations = []arv1.OperationType{}
		}},
		{name: "MissingCreateOp", expected: "operations", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Operations[0] = arv1.Connect
		}},
		{name: "MissingAPIGroups", expected: "api groups", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIGroups = []string{}
		}},
		{name: "WrongAPIGroups", expected: "api groups", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIGroups[0] = "invalid-group"
		}},
		{name: "MissingAPIVersions", expected: "api versions", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIVersions = []string{}
		}},
		{name: "WrongAPIVersions", expected: "api versions", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].APIVersions[0] = "invalid-version"
		}},
		{name: "MissingResources", expected: "resources", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Resources = []string{}
		}},
		{name: "WrongResources", expected: "resources", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].Rules[0].Resources[0] = "invalid-resource"
		}},
		{name: "MissingFailurePolicy", expected: "failure policy", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].FailurePolicy = nil
		}},
		{name: "WrongFailurePolicy", expected: "failure policy", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			fail := arv1.Fail
			h.Webhooks[0].FailurePolicy = &fail
		}},
		{name: "MissingSideEffects", expected: "side effects", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			h.Webhooks[0].SideEffects = nil
		}},
		{name: "WrongSideEffects", expected: "side effects", mutator: func(h *arv1.MutatingWebhookConfiguration) {
			some := arv1.SideEffectClassSome
			h.Webhooks[0].SideEffects = &some
		}},
	}

	testSetupOnce(t)
	clientset := fake.NewClientset()
	wm := createPopulatedWm(clientset)

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mh := wm.createEmptyMutatingWebhook()
			wm.populateMutatingWebhook(mh, caBundle)
			c.mutator(mh)
			err := wm.checkMutatingWebhook(mh)
			if c.expected == "" {
				assert.NilError(t, err, "check failed")
			} else {
				assert.ErrorContains(t, err, c.expected, "check succeeded or had wrong failure mode")
			}
		})
	}
}

func createPopulatedWm(clientset kubernetes.Interface) *webhookManagerImpl {
	wm := newWebhookManagerImpl(createConfig(), clientset)
	wm.caCert1 = cacert1
	wm.caKey1 = cakey1
	wm.caCert2 = cacert2
	wm.caKey2 = cakey2

	return wm
}

func createSecret() *v1.Secret {
	return &v1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "admission-controller-secrets"},
	}
}

func addCert(t *testing.T, secret *v1.Secret, cert *x509.Certificate, key *rsa.PrivateKey, index int) {
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[fmt.Sprintf("cacert%d.pem", index)] = certPem(t, cert)
	secret.Data[fmt.Sprintf("cakey%d.pem", index)] = keyPem(t, key)
}

func caBundlePem(t *testing.T, certs []*x509.Certificate) []byte {
	pem, err := pki.EncodeCertChainPem(certs)
	assert.NilError(t, err, "failed to create CA certificate")
	return *pem
}

func caCertKeyPair(t *testing.T, days int) (*x509.Certificate, *rsa.PrivateKey) {
	cert, key, err := pki.GenerateCACertificate(time.Now().AddDate(0, 0, days))
	assert.NilError(t, err, "failed to create CA certificate")
	return cert, key
}

func certPem(t *testing.T, cert *x509.Certificate) []byte {
	pem, err := pki.EncodeCertificatePem(cert)
	assert.NilError(t, err, "failed to encode certificate")
	return *pem
}

func keyPem(t *testing.T, key *rsa.PrivateKey) []byte {
	pem, err := pki.EncodePrivateKeyPem(key)
	assert.NilError(t, err, "failed to encode certificate")
	return *pem
}
