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
	"net/http"
	"sync"
	"testing"
	"time"

	"gotest.tools/assert"
	arv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	admissionregistrationv1 "k8s.io/client-go/kubernetes/typed/admissionregistration/v1"
	fakeadmissionregistrationv1 "k8s.io/client-go/kubernetes/typed/admissionregistration/v1/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	fakecorev1 "k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"github.com/apache/yunikorn-k8shim/pkg/pki"
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
	clientset := fakeClientSet()

	secret := createSecret()
	addCert(t, secret, cacert1, cakey1, 1)
	addCert(t, secret, cacert2, cakey2, 2)
	clientset.secrets["default/admission-controller-secrets"] = secret

	wm := newWebhookManagerImpl(createConfig(), clientset)
	err := wm.LoadCACertificates()
	assert.NilError(t, err, "failed to load CA certificates")
}

func TestLoadCACertificatesWithMissingSecret(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()

	wm := newWebhookManagerImpl(createConfig(), clientset)
	err := wm.LoadCACertificates()
	assert.ErrorContains(t, err, string(metav1.StatusReasonNotFound), "get secrets didn't fail")
}

func TestLoadCACertificatesWithEmptySecret(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()

	secret := createSecret()
	clientset.secrets["default/admission-controller-secrets"] = secret

	wm := newWebhookManagerImpl(createConfig(), clientset)
	err := wm.LoadCACertificates()
	assert.NilError(t, err, "failed to load CA certificates")
	secret, ok := clientset.secrets["default/admission-controller-secrets"]
	assert.Assert(t, ok, "secret not found")
	_, ok = secret.Data["cacert1.pem"]
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
	clientset := fakeClientSet()

	secret := createSecret()
	secret.ObjectMeta.SetAnnotations(map[string]string{"conflict": "true"})
	clientset.secrets["default/admission-controller-secrets"] = secret

	wm := newWebhookManagerImpl(createConfig(), clientset)
	wm.conflictAttempts = 0
	err := wm.LoadCACertificates()
	assert.ErrorContains(t, err, "max attempts", "update secrets didn't fail")
}

func TestGenerateServerCertificate(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
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
	clientset := fakeClientSet()
	wm := newWebhookManagerImpl(createConfig(), clientset)

	_, err := wm.GenerateServerCertificate()
	assert.ErrorContains(t, err, "CA certificate", "get best CA cert didn't fail")
}

func TestInstallWebhooksWithNoCACertificates(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
	wm := newWebhookManagerImpl(createConfig(), clientset)

	err := wm.InstallWebhooks()
	assert.ErrorContains(t, err, "CA certificate", "missing certs were not detected")
}

func TestInstallWebhooksWhenAlreadyPresent(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
	wm := createPopulatedWm(clientset)

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, caBundle)
	clientset.validatingWebhooks["yunikorn-admission-controller-validations"] = vh

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, caBundle)
	clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"] = mh

	err := wm.InstallWebhooks()
	assert.NilError(t, err, "Install webhooks failed")
	vh, ok := clientset.validatingWebhooks["yunikorn-admission-controller-validations"]
	assert.Assert(t, ok, "validating webhook not found")
	err = wm.checkValidatingWebhook(vh)
	assert.NilError(t, err, "validating webhook is malformed")
	assert.Equal(t, vh.Generation, int64(0), "wrong generation for validating webhook")

	mh, ok = clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"]
	assert.Assert(t, ok, "mutating webhook not found")
	err = wm.checkMutatingWebhook(mh)
	assert.NilError(t, err, "mutating webhook is malformed")
	assert.Equal(t, mh.Generation, int64(0), "wrong generation for mutating webhook")
}

func TestInstallWebhooksWithNoHooksPresent(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
	wm := createPopulatedWm(clientset)

	err := wm.InstallWebhooks()
	assert.NilError(t, err, "Install webhooks failed")
	vh, ok := clientset.validatingWebhooks["yunikorn-admission-controller-validations"]
	assert.Assert(t, ok, "validating webhook not found")
	err = wm.checkValidatingWebhook(vh)
	assert.NilError(t, err, "validating webhook is malformed")

	mh, ok := clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"]
	assert.Assert(t, ok, "mutating webhook not found")
	err = wm.checkMutatingWebhook(mh)
	assert.NilError(t, err, "mutating webhook is malformed")
}

func TestInstallWebhooksWithWrongData(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
	wm := createPopulatedWm(clientset)

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, []byte{0})
	clientset.validatingWebhooks["yunikorn-admission-controller-validations"] = vh

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, []byte{0})
	clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"] = mh

	err := wm.InstallWebhooks()
	assert.NilError(t, err, "Install webhooks failed")
	vh, ok := clientset.validatingWebhooks["yunikorn-admission-controller-validations"]
	assert.Assert(t, ok, "validating webhook not found")
	err = wm.checkValidatingWebhook(vh)
	assert.NilError(t, err, "validating webhook is malformed")
	assert.Equal(t, vh.Generation, int64(1), "wrong generation for validating webhook")

	mh, ok = clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"]
	assert.Assert(t, ok, "mutating webhook not found")
	err = wm.checkMutatingWebhook(mh)
	assert.NilError(t, err, "mutating webhook is malformed")
	assert.Equal(t, mh.Generation, int64(1), "wrong generation for mutating webhook")
}

func TestInstallWebhooksWithValidationConflict(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
	wm := createPopulatedWm(clientset)
	wm.conflictAttempts = 0

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, []byte{0})
	vh.ObjectMeta.SetAnnotations(map[string]string{"conflict": "true"})
	clientset.validatingWebhooks["yunikorn-admission-controller-validations"] = vh

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, caBundle)
	clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"] = mh

	err := wm.InstallWebhooks()
	assert.ErrorContains(t, err, "max attempts", "update webhook didn't fail")
}

func TestInstallWebhooksWithMutationConflict(t *testing.T) {
	testSetupOnce(t)
	clientset := fakeClientSet()
	wm := createPopulatedWm(clientset)
	wm.conflictAttempts = 0

	vh := wm.createEmptyValidatingWebhook()
	wm.populateValidatingWebhook(vh, caBundle)
	clientset.validatingWebhooks["yunikorn-admission-controller-validations"] = vh

	mh := wm.createEmptyMutatingWebhook()
	wm.populateMutatingWebhook(mh, []byte{0})
	mh.ObjectMeta.SetAnnotations(map[string]string{"conflict": "true"})
	clientset.mutatingWebhooks["yunikorn-admission-controller-mutations"] = mh

	err := wm.InstallWebhooks()
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
	clientset := fakeClientSet()
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
	clientset := fakeClientSet()
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

// K8s API mocks

type fakeClient struct {
	fake.Clientset
	secrets            map[string]*v1.Secret
	validatingWebhooks map[string]*arv1.ValidatingWebhookConfiguration
	mutatingWebhooks   map[string]*arv1.MutatingWebhookConfiguration
}

type fakeCoreV1 struct {
	fakecorev1.FakeCoreV1
	client *fakeClient
}

type fakeSecrets struct {
	fakecorev1.FakeSecrets
	core *fakeCoreV1
	ns   string
}

type fakeAdmissionregistrationV1 struct {
	fakeadmissionregistrationv1.FakeAdmissionregistrationV1
	client *fakeClient
}

type fakeValidatingWebhookConfigurations struct {
	fakeadmissionregistrationv1.FakeValidatingWebhookConfigurations
	ar *fakeAdmissionregistrationV1
}

type fakeMutatingWebhookConfigurations struct {
	fakeadmissionregistrationv1.FakeMutatingWebhookConfigurations
	ar *fakeAdmissionregistrationV1
}

func fakeClientSet() *fakeClient {
	return &fakeClient{
		secrets:            make(map[string]*v1.Secret),
		validatingWebhooks: make(map[string]*arv1.ValidatingWebhookConfiguration),
		mutatingWebhooks:   make(map[string]*arv1.MutatingWebhookConfiguration),
	}
}

func (c *fakeClient) CoreV1() corev1.CoreV1Interface {
	return &fakeCoreV1{client: c}
}

func (c *fakeCoreV1) Secrets(namespace string) corev1.SecretInterface {
	return &fakeSecrets{core: c, ns: namespace}
}

func (c *fakeClient) AdmissionregistrationV1() admissionregistrationv1.AdmissionregistrationV1Interface {
	return &fakeAdmissionregistrationV1{client: c}
}

func (ar *fakeAdmissionregistrationV1) MutatingWebhookConfigurations() admissionregistrationv1.MutatingWebhookConfigurationInterface {
	return &fakeMutatingWebhookConfigurations{ar: ar}
}

func (ar *fakeAdmissionregistrationV1) ValidatingWebhookConfigurations() admissionregistrationv1.ValidatingWebhookConfigurationInterface {
	return &fakeValidatingWebhookConfigurations{ar: ar}
}

func (s *fakeSecrets) Get(_ context.Context, name string, _ metav1.GetOptions) (*v1.Secret, error) {
	secret, ok := s.core.client.secrets[fmt.Sprintf("%s/%s", s.ns, name)]
	if !ok {
		return nil, notFoundErr()
	}

	return secret, nil
}

func (s *fakeSecrets) Update(_ context.Context, secret *v1.Secret, opts metav1.UpdateOptions) (*v1.Secret, error) {
	existing, ok := s.core.client.secrets[fmt.Sprintf("%s/%s", s.ns, secret.Name)]
	if !ok || secret.Namespace != s.ns {
		return nil, notFoundErr()
	}

	if _, ok := existing.GetAnnotations()["conflict"]; ok {
		return nil, conflictErr()
	}

	secret.Generation++
	s.core.client.secrets[fmt.Sprintf("%s/%s", s.ns, secret.Name)] = secret
	return secret, nil
}

func (c *fakeValidatingWebhookConfigurations) Get(_ context.Context, name string, _ metav1.GetOptions) (*arv1.ValidatingWebhookConfiguration, error) {
	hook, ok := c.ar.client.validatingWebhooks[name]
	if !ok {
		return nil, notFoundErr()
	}
	return hook, nil
}

func (c *fakeValidatingWebhookConfigurations) Create(_ context.Context, hook *arv1.ValidatingWebhookConfiguration, _ metav1.CreateOptions) (*arv1.ValidatingWebhookConfiguration, error) {
	if _, ok := c.ar.client.validatingWebhooks[hook.Name]; ok {
		return nil, conflictErr()
	}
	c.ar.client.validatingWebhooks[hook.Name] = hook
	return hook, nil
}

func (c *fakeValidatingWebhookConfigurations) Update(_ context.Context, hook *arv1.ValidatingWebhookConfiguration, _ metav1.UpdateOptions) (*arv1.ValidatingWebhookConfiguration, error) {
	existing, ok := c.ar.client.validatingWebhooks[hook.Name]
	if !ok {
		return nil, notFoundErr()
	}

	if _, ok := existing.GetAnnotations()["conflict"]; ok {
		return nil, conflictErr()
	}

	hook.Generation++
	c.ar.client.validatingWebhooks[hook.Name] = hook
	return hook, nil
}

func (c *fakeMutatingWebhookConfigurations) Get(_ context.Context, name string, opts metav1.GetOptions) (*arv1.MutatingWebhookConfiguration, error) {
	hook, ok := c.ar.client.mutatingWebhooks[name]
	if !ok {
		return nil, notFoundErr()
	}
	return hook, nil
}

func (c *fakeMutatingWebhookConfigurations) Create(_ context.Context, hook *arv1.MutatingWebhookConfiguration, _ metav1.CreateOptions) (*arv1.MutatingWebhookConfiguration, error) {
	if _, ok := c.ar.client.mutatingWebhooks[hook.Name]; ok {
		return nil, conflictErr()
	}
	c.ar.client.mutatingWebhooks[hook.Name] = hook
	return hook, nil
}

func (c *fakeMutatingWebhookConfigurations) Update(_ context.Context, hook *arv1.MutatingWebhookConfiguration, opts metav1.UpdateOptions) (*arv1.MutatingWebhookConfiguration, error) {
	existing, ok := c.ar.client.mutatingWebhooks[hook.Name]
	if !ok {
		return nil, notFoundErr()
	}

	if _, ok := existing.GetAnnotations()["conflict"]; ok {
		return nil, conflictErr()
	}

	hook.Generation++
	c.ar.client.mutatingWebhooks[hook.Name] = hook
	return hook, nil
}

func notFoundErr() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusNotFound, Message: string(metav1.StatusReasonNotFound), Reason: metav1.StatusReasonNotFound}}
}

func conflictErr() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusConflict, Message: string(metav1.StatusReasonConflict), Reason: metav1.StatusReasonConflict}}
}
