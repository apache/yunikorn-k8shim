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

package webhookpki

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	admissionpki "github.com/apache/yunikorn-k8shim/pkg/admission/pki"
)

const testNamespace = "yunikorn"

func testObjects() (*corev1.Secret, *admissionregistrationv1.ValidatingWebhookConfiguration) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultSecretName, Namespace: testNamespace},
		Type:       corev1.SecretTypeOpaque,
	}
	configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultWebhookConfigName},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name:                    DefaultWebhookName,
				AdmissionReviewVersions: []string{"v1"},
				SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
				FailurePolicy:           ptr.To(admissionregistrationv1.Fail),
				ClientConfig: admissionregistrationv1.WebhookClientConfig{Service: &admissionregistrationv1.ServiceReference{
					Name: DefaultServiceName, Namespace: testNamespace, Path: ptr.To(WebhookPath), Port: ptr.To(WebhookServicePort),
				}},
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{QueueAPIGroup},
						APIVersions: []string{QueueAPIVersion},
						Resources:   []string{QueueResource},
						Scope:       ptr.To(admissionregistrationv1.NamespacedScope),
					},
				}},
			},
			{
				Name: "unrelated.example.com",
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					CABundle: []byte("unrelated-trust"),
					URL:      ptr.To("https://example.com/validate"),
				},
			},
		},
	}
	return secret, configuration
}

func testClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(admissionregistrationv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func testConfig(t *testing.T, certDir string) Config {
	t.Helper()
	config := DefaultConfig(testNamespace, certDir, DefaultCertName, DefaultKeyName)
	config.Now = time.Now
	config.ReconcileInterval = time.Hour
	config.WebhookPort = WebhookServicePort
	config.LiveTLSCheck = func(context.Context, *x509.CertPool) error { return nil }
	config.ServiceTLSCheck = func(context.Context, *x509.CertPool) error { return nil }
	return config
}

func mustNewManager(t *testing.T, k8sClient client.Client, config Config) *Manager {
	t.Helper()
	manager, err := New(k8sClient, config)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestPrepareInitializesPersistentStateAndServingIdentity(t *testing.T) {
	secret, configuration := testObjects()
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)
	if prepareErr := manager.Prepare(context.Background()); prepareErr != nil {
		t.Fatalf("prepare: %v", prepareErr)
	}

	freshSecret := &corev1.Secret{}
	if getErr := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.SecretName, Namespace: config.Namespace}, freshSecret); getErr != nil {
		t.Fatal(getErr)
	}
	state, err := loadCAState(freshSecret, config.Now())
	if err != nil {
		t.Fatalf("stored CA state is invalid: %v", err)
	}
	if state.active.cert.Equal(state.next.cert) {
		t.Error("active and standby CA certificates are equal")
	}

	freshConfiguration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, freshConfiguration); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(freshConfiguration.Webhooks[0].ClientConfig.CABundle, state.bundle()) {
		t.Error("named webhook did not receive active and standby trust")
	}
	if got := freshConfiguration.Webhooks[1].ClientConfig.CABundle; !bytes.Equal(got, []byte("unrelated-trust")) {
		t.Errorf("unrelated webhook was changed: %q", got)
	}

	leaf := loadLeaf(t, config)
	wantDNSNames := []string{
		DefaultServiceName,
		DefaultServiceName + "." + testNamespace,
		DefaultServiceName + "." + testNamespace + ".svc",
		DefaultServiceName + "." + testNamespace + ".svc.cluster.local",
	}
	for _, dnsName := range wantDNSNames {
		if err := leaf.VerifyHostname(dnsName); err != nil {
			t.Errorf("serving certificate missing DNS SAN %q: %v", dnsName, err)
		}
	}
	if err := manager.Checker()(nil); err != nil {
		t.Errorf("readiness failed after preparation: %v", err)
	}
}

func TestManagerRunsOnEveryReplica(t *testing.T) {
	secret, configuration := testObjects()
	manager, err := New(testClient(t, secret, configuration), testConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if manager.NeedLeaderElection() {
		t.Fatal("per-pod serving certificate maintenance must not wait for leader election")
	}
}

func TestPreparePreservesCAStateAcrossRestart(t *testing.T) {
	secret, configuration := testObjects()
	k8sClient := testClient(t, secret, configuration)
	firstConfig := testConfig(t, t.TempDir())
	first := mustNewManager(t, k8sClient, firstConfig)
	if err := first.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}

	stored := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: firstConfig.SecretName, Namespace: firstConfig.Namespace}, stored); err != nil {
		t.Fatal(err)
	}
	wantData := copySecretData(stored.Data)

	secondConfig := testConfig(t, t.TempDir())
	second := mustNewManager(t, k8sClient, secondConfig)
	if err := second.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: secondConfig.SecretName, Namespace: secondConfig.Namespace}, stored); err != nil {
		t.Fatal(err)
	}
	for name, want := range wantData {
		if !bytes.Equal(stored.Data[name], want) {
			t.Errorf("persistent CA field %s changed across restart", name)
		}
	}
	if err := second.Checker()(nil); err != nil {
		t.Errorf("second manager readiness failed: %v", err)
	}
}

func TestPrepareRefusesMalformedNonEmptySecret(t *testing.T) {
	secret, configuration := testObjects()
	secret.Data = map[string][]byte{ActiveCACertKey: []byte("not-a-certificate")}
	wantData := copySecretData(secret.Data)
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)

	if err := manager.Prepare(context.Background()); err == nil {
		t.Fatal("expected malformed non-empty Secret to be rejected")
	}
	fresh := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.SecretName, Namespace: config.Namespace}, fresh); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fresh.Data[ActiveCACertKey], wantData[ActiveCACertKey]) || len(fresh.Data) != 1 {
		t.Errorf("malformed Secret was overwritten: %v", fresh.Data)
	}
}

func TestPrepareRotatesCAWithTrustOverlap(t *testing.T) {
	secret, configuration := testObjects()
	active, err := generateCA(time.Now().Add(30 * 24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	standby, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	secret.Data = caState{active: active, next: standby}.secretData()
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)

	if prepareErr := manager.Prepare(context.Background()); prepareErr != nil {
		t.Fatalf("prepare rotation: %v", prepareErr)
	}
	fresh := &corev1.Secret{}
	if getErr := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.SecretName, Namespace: config.Namespace}, fresh); getErr != nil {
		t.Fatal(getErr)
	}
	state, err := loadCAState(fresh, config.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !state.active.cert.Equal(standby.cert) {
		t.Error("standby CA was not promoted to active")
	}
	if state.next.cert.Equal(active.cert) || state.next.cert.Equal(standby.cert) {
		t.Error("new standby CA was not generated")
	}
	webhookConfiguration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, webhookConfiguration); err != nil {
		t.Fatal(err)
	}
	rolloverPool := x509.NewCertPool()
	if !rolloverPool.AppendCertsFromPEM(webhookConfiguration.Webhooks[0].ClientConfig.CABundle) {
		t.Fatal("rollover trust bundle is invalid")
	}
	if _, err := active.cert.Verify(x509.VerifyOptions{Roots: rolloverPool}); err != nil {
		t.Errorf("old active CA was removed before serving reload: %v", err)
	}
	if _, err := standby.cert.Verify(x509.VerifyOptions{Roots: rolloverPool}); err != nil {
		t.Errorf("promoted CA is absent from rollover trust: %v", err)
	}
	if state.previous == nil || !state.previous.Equal(active.cert) {
		t.Fatal("retiring CA was not persisted for multi-replica overlap")
	}
	leaf := loadLeaf(t, config)
	pool := x509.NewCertPool()
	pool.AddCert(state.active.cert)
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: manager.dnsNames()[2], Roots: pool}); err != nil {
		t.Errorf("rotated serving certificate is not signed by promoted CA: %v", err)
	}
	if err := manager.Checker()(nil); err != nil {
		t.Errorf("readiness failed after CA rotation: %v", err)
	}

	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, webhookConfiguration); err != nil {
		t.Fatal(err)
	}
	wantBundle := state.bundle()
	if !bytes.Equal(webhookConfiguration.Webhooks[0].ClientConfig.CABundle, wantBundle) {
		t.Error("stable reconciliation did not publish promoted and new standby trust")
	}
}

func TestPrepareRecoversFromExpiredActiveCA(t *testing.T) {
	secret, configuration := testObjects()
	active, err := generateCA(time.Now().Add(24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	standby, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	secret.Data = caState{active: active, next: standby}.secretData()
	config := testConfig(t, t.TempDir())
	config.Now = func() time.Time { return active.cert.NotAfter.Add(time.Minute) }
	k8sClient := testClient(t, secret, configuration)
	manager := mustNewManager(t, k8sClient, config)

	if prepareErr := manager.Prepare(context.Background()); prepareErr != nil {
		t.Fatalf("recover expired active CA: %v", prepareErr)
	}
	fresh := &corev1.Secret{}
	key := types.NamespacedName{Name: config.SecretName, Namespace: config.Namespace}
	if getErr := k8sClient.Get(context.Background(), key, fresh); getErr != nil {
		t.Fatal(getErr)
	}
	state, err := loadCAState(fresh, config.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !state.active.cert.Equal(standby.cert) {
		t.Fatal("valid standby CA was not promoted after active CA expiry")
	}
}

func TestExpiredPreviousCAIsRemovedFromPublishedTrust(t *testing.T) {
	secret, configuration := testObjects()
	active, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	next, err := generateCA(time.Now().Add(caValidity + caStagger))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	state := caState{
		active:              active,
		next:                next,
		previous:            previous.cert,
		previousPEM:         previous.certPEM,
		previousRetireAfter: time.Now().Add(-time.Minute),
	}
	secret.Data = state.secretData()
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	fresh := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, fresh); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(fresh.Webhooks[0].ClientConfig.CABundle) {
		t.Fatal("published trust is malformed")
	}
	if _, err := previous.cert.Verify(x509.VerifyOptions{Roots: roots}); err == nil {
		t.Fatal("expired retirement grace left the previous CA trusted")
	}
}

func TestPrepareRequiresNamedWebhookTarget(t *testing.T) {
	secret, configuration := testObjects()
	configuration.Webhooks[0].ClientConfig.Service.Name = "different-service"
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)
	if err := manager.Prepare(context.Background()); err == nil {
		t.Fatal("expected mismatched webhook Service target to fail")
	}
}

func TestPrepareRequiresFailClosedActiveQueueRule(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*admissionregistrationv1.ValidatingWebhook)
	}{
		{
			name: "failure policy is nil",
			mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) {
				webhook.FailurePolicy = nil
			},
		},
		{
			name: "queue rule is absent",
			mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) {
				webhook.Rules[0].Resources = []string{"not-queues"}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret, configuration := testObjects()
			tt.mutate(&configuration.Webhooks[0])
			k8sClient := testClient(t, secret, configuration)
			config := testConfig(t, t.TempDir())
			manager := mustNewManager(t, k8sClient, config)
			if err := manager.Prepare(context.Background()); err == nil {
				t.Fatal("expected invalid webhook safety configuration to fail")
			}
		})
	}
}

func TestRuntimeReconcileActivatesFailClosedPolicy(t *testing.T) {
	secret, configuration := testObjects()
	configuration.Webhooks[0].FailurePolicy = ptr.To(admissionregistrationv1.Ignore)
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.reconcileRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	fresh := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.Webhooks[0].FailurePolicy == nil || *fresh.Webhooks[0].FailurePolicy != admissionregistrationv1.Fail {
		t.Fatalf("failure policy = %v, want Fail", fresh.Webhooks[0].FailurePolicy)
	}
}

func TestCheckerMakesBootstrapEndpointReadyBeforeFailClosedActivation(t *testing.T) {
	secret, configuration := testObjects()
	configuration.Webhooks[0].FailurePolicy = ptr.To(admissionregistrationv1.Ignore)
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Checker()(nil); err != nil {
		t.Fatalf("bootstrap listener should become ready before fail-closed activation: %v", err)
	}
	fresh := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.Webhooks[0].FailurePolicy == nil || *fresh.Webhooks[0].FailurePolicy != admissionregistrationv1.Ignore {
		t.Fatalf("prepare changed bootstrap failure policy to %v", fresh.Webhooks[0].FailurePolicy)
	}
}

func TestRuntimeReconcileWaitsForRoutableServiceBeforeFailClosedActivation(t *testing.T) {
	secret, configuration := testObjects()
	configuration.Webhooks[0].FailurePolicy = ptr.To(admissionregistrationv1.Ignore)
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	config.ServiceTLSCheck = func(context.Context, *x509.CertPool) error {
		return errors.New("Service has no ready endpoints")
	}
	manager := mustNewManager(t, k8sClient, config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.reconcileRuntime(context.Background()); err == nil {
		t.Fatal("expected unavailable webhook Service to delay activation")
	}
	fresh := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: config.WebhookConfigName}, fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.Webhooks[0].FailurePolicy == nil || *fresh.Webhooks[0].FailurePolicy != admissionregistrationv1.Ignore {
		t.Fatalf("failure policy = %v, want Ignore until the Service is routable", fresh.Webhooks[0].FailurePolicy)
	}
}

func TestCheckerTrustsOnlyTheActiveCAForTheLiveListener(t *testing.T) {
	secret, configuration := testObjects()
	active, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	next, err := generateCA(time.Now().Add(caValidity + caStagger))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	secret.Data = caState{
		active:              active,
		next:                next,
		previous:            previous.cert,
		previousPEM:         previous.certPEM,
		previousRetireAfter: time.Now().Add(time.Hour),
	}.secretData()
	config := testConfig(t, t.TempDir())
	config.LiveTLSCheck = func(_ context.Context, roots *x509.CertPool) error {
		if _, verifyErr := active.cert.Verify(x509.VerifyOptions{Roots: roots}); verifyErr != nil {
			return errors.New("active CA is not trusted")
		}
		if _, verifyErr := previous.cert.Verify(x509.VerifyOptions{Roots: roots}); verifyErr == nil {
			return errors.New("retiring CA is still accepted for pod readiness")
		}
		return nil
	}
	manager := mustNewManager(t, testClient(t, secret, configuration), config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Checker()(nil); err != nil {
		t.Fatal(err)
	}
}

func TestCheckerIgnoresRecoveredMaintenanceError(t *testing.T) {
	secret, configuration := testObjects()
	manager := mustNewManager(t, testClient(t, secret, configuration), testConfig(t, t.TempDir()))
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.setLastError(errors.New("transient maintenance failure"))
	if err := manager.Checker()(nil); err != nil {
		t.Fatalf("usable live identity should remain ready: %v", err)
	}
}

func TestCheckerRejectsIncompleteWebhookContract(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*admissionregistrationv1.ValidatingWebhook)
	}{
		{name: "path", mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) {
			webhook.ClientConfig.Service.Path = ptr.To("/wrong")
		}},
		{name: "port", mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) {
			webhook.ClientConfig.Service.Port = ptr.To[int32](9443)
		}},
		{name: "versions", mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) { webhook.Rules[0].APIVersions = nil }},
		{name: "operations", mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) {
			webhook.Rules[0].Operations = []admissionregistrationv1.OperationType{admissionregistrationv1.Create}
		}},
		{name: "scope", mutate: func(webhook *admissionregistrationv1.ValidatingWebhook) {
			webhook.Rules[0].Scope = ptr.To(admissionregistrationv1.ClusterScope)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret, configuration := testObjects()
			tt.mutate(&configuration.Webhooks[0])
			manager := mustNewManager(t, testClient(t, secret, configuration), testConfig(t, t.TempDir()))
			if err := manager.Prepare(context.Background()); err == nil {
				t.Fatal("expected incomplete webhook contract to be rejected")
			}
		})
	}
}

func TestCheckerRejectsTrustWithoutActiveCA(t *testing.T) {
	secret, configuration := testObjects()
	k8sClient := testClient(t, secret, configuration)
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, k8sClient, config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}

	unrelated, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	fresh := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	key := types.NamespacedName{Name: config.WebhookConfigName}
	if err := k8sClient.Get(context.Background(), key, fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Webhooks[0].ClientConfig.CABundle = unrelated.certPEM
	if err := k8sClient.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if err := manager.Checker()(nil); err == nil {
		t.Fatal("expected readiness to reject published trust without the active CA")
	}
}

func loadLeaf(t *testing.T, config Config) *x509.Certificate {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(filepath.Join(config.CertDir, config.CertName), filepath.Join(config.CertDir, config.KeyName))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf
}

func copySecretData(data map[string][]byte) map[string][]byte {
	copy := make(map[string][]byte, len(data))
	for key, value := range data {
		copy[key] = bytes.Clone(value)
	}
	return copy
}

func TestServingFilesUseRestrictedPermissions(t *testing.T) {
	secret, configuration := testObjects()
	config := testConfig(t, t.TempDir())
	manager := mustNewManager(t, testClient(t, secret, configuration), config)
	if err := manager.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	keyInfo, err := os.Stat(filepath.Join(config.CertDir, config.KeyName))
	if err != nil {
		t.Fatal(err)
	}
	if got := keyInfo.Mode().Perm(); got != 0o600 {
		t.Errorf("serving key mode = %o, want 600", got)
	}
	certInfo, err := os.Stat(filepath.Join(config.CertDir, config.CertName))
	if err != nil {
		t.Fatal(err)
	}
	if got := certInfo.Mode().Perm(); got != 0o644 {
		t.Errorf("serving certificate mode = %o, want 644", got)
	}
}

func TestLoadCAStateRejectsMismatchedKey(t *testing.T) {
	first, err := generateCA(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	secondCert, secondKey, err := admissionpki.GenerateCACertificate(time.Now().Add(caValidity))
	if err != nil {
		t.Fatal(err)
	}
	secondCertPEM, err := admissionpki.EncodeCertificatePem(secondCert)
	if err != nil {
		t.Fatal(err)
	}
	secondKeyPEM, err := admissionpki.EncodePrivateKeyPem(secondKey)
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{Data: map[string][]byte{
		ActiveCACertKey: first.certPEM,
		ActiveCAKeyKey:  *secondKeyPEM,
		NextCACertKey:   *secondCertPEM,
		NextCAKeyKey:    *secondKeyPEM,
	}}
	if _, err := loadCAState(secret, time.Now()); err == nil {
		t.Fatal("expected mismatched active certificate and key to fail")
	}
}
