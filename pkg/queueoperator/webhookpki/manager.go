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

// Package webhookpki manages the Queue validating webhook's private CA state,
// serving identity, and published Kubernetes trust bundle.
package webhookpki

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus"

	admissionpki "github.com/apache/yunikorn-k8shim/pkg/admission/pki"
)

const (
	DefaultSecretName              = "yunikorn-queue-operator-webhook-ca" //nolint:gosec // Kubernetes resource name, not a credential.
	DefaultWebhookConfigName       = "yunikorn-queue-operator"
	DefaultWebhookName             = "vqueue.yunikorn.apache.org"
	DefaultServiceName             = "yunikorn-queue-operator-webhook"
	DefaultCertDir                 = "/tmp/queue-operator-webhook-certs"
	DefaultCertName                = "tls.crt"
	DefaultKeyName                 = "tls.key"
	QueueResource                  = "queues"
	QueueAPIGroup                  = "yunikorn.apache.org"
	QueueAPIVersion                = "v1alpha1"
	WebhookPath                    = "/validate-yunikorn-apache-org-v1alpha1-queue"
	WebhookServicePort       int32 = 443

	ActiveCACertKey          = "active-ca.crt"
	ActiveCAKeyKey           = "active-ca.key"
	NextCACertKey            = "next-ca.crt"
	NextCAKeyKey             = "next-ca.key"
	PreviousCACertKey        = "previous-ca.crt"
	PreviousCARetireAfterKey = "previous-ca-retire-after"

	caValidity             = 2 * 365 * 24 * time.Hour
	caStagger              = 365 * 24 * time.Hour
	caRenewBefore          = 180 * 24 * time.Hour
	leafRenewBefore        = 30 * 24 * time.Hour
	reconcileInterval      = time.Minute
	retryInterval          = 5 * time.Second
	caTrustRetirementGrace = 24 * time.Hour
)

var (
	certificateExpiry = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "queue_operator",
			Name:      "webhook_certificate_expiry_timestamp_seconds",
			Help:      "Unix expiry timestamp for the Queue webhook serving certificate and CA certificates.",
		},
		[]string{"certificate"},
	)
	certificateRotations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "queue_operator",
			Name:      "webhook_certificate_rotations_total",
			Help:      "Total Queue webhook certificate rotations by certificate class.",
		},
		[]string{"certificate"},
	)
)

func init() {
	metrics.Registry.MustRegister(certificateExpiry, certificateRotations)
}

type Config struct {
	Namespace           string
	SecretName          string
	WebhookConfigName   string
	WebhookName         string
	ServiceName         string
	WebhookPort         int32
	CertDir             string
	CertName            string
	KeyName             string
	Now                 func() time.Time
	ReconcileInterval   time.Duration
	RetryInterval       time.Duration
	ProbeAddress        string
	ServiceProbeAddress string
	LiveTLSCheck        func(context.Context, *x509.CertPool) error
	ServiceTLSCheck     func(context.Context, *x509.CertPool) error
}

func DefaultConfig(namespace, certDir, certName, keyName string) Config {
	return Config{
		Namespace:         namespace,
		SecretName:        DefaultSecretName,
		WebhookConfigName: DefaultWebhookConfigName,
		WebhookName:       DefaultWebhookName,
		ServiceName:       DefaultServiceName,
		WebhookPort:       WebhookServicePort,
		CertDir:           certDir,
		CertName:          certName,
		KeyName:           keyName,
		Now:               time.Now,
		ReconcileInterval: reconcileInterval,
		RetryInterval:     retryInterval,
		ProbeAddress:      "127.0.0.1:9443",
	}
}

type Manager struct {
	client client.Client
	config Config

	mu      sync.RWMutex
	lastErr error
}

var _ manager.Runnable = &Manager{}

func (*Manager) NeedLeaderElection() bool { return false }

func New(k8sClient client.Client, config Config) (*Manager, error) {
	if k8sClient == nil {
		return nil, errors.New("webhook PKI client is required")
	}
	if config.Namespace == "" || config.SecretName == "" || config.WebhookConfigName == "" ||
		config.WebhookName == "" || config.ServiceName == "" || config.CertDir == "" ||
		config.CertName == "" || config.KeyName == "" || config.WebhookPort <= 0 {
		return nil, errors.New("webhook PKI configuration contains an empty required field")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.ReconcileInterval <= 0 {
		config.ReconcileInterval = reconcileInterval
	}
	if config.RetryInterval <= 0 {
		config.RetryInterval = retryInterval
	}
	if config.ProbeAddress == "" {
		config.ProbeAddress = "127.0.0.1:9443"
	}
	if config.ServiceProbeAddress == "" {
		config.ServiceProbeAddress = net.JoinHostPort(
			config.ServiceName+"."+config.Namespace+".svc",
			strconv.Itoa(int(config.WebhookPort)),
		)
	}
	return &Manager{client: k8sClient, config: config}, nil
}

func (m *Manager) Prepare(ctx context.Context) error {
	err := m.reconcile(ctx)
	m.setLastError(err)
	return err
}

func (m *Manager) Start(ctx context.Context) error {
	delay := time.Duration(0)
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			err := m.reconcileRuntime(ctx)
			m.setLastError(err)
			if err != nil {
				logf.FromContext(ctx).Error(err, "Queue webhook PKI reconciliation failed")
				delay = m.config.RetryInterval
				continue
			}
			delay = m.config.ReconcileInterval
		}
	}
}

func (m *Manager) Checker() healthz.Checker {
	return func(request *http.Request) error {
		m.mu.RLock()
		lastErr := m.lastErr
		m.mu.RUnlock()
		ctx := context.Background()
		if request != nil {
			ctx = request.Context()
		}
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		readyErr := m.checkReady(ctx)
		if readyErr == nil {
			return nil
		}
		return errors.Join(readyErr, lastErr)
	}
}

func (m *Manager) reconcileRuntime(ctx context.Context) error {
	if err := m.reconcile(ctx); err != nil {
		return err
	}
	if err := m.checkActiveLiveTLS(ctx, false); err != nil {
		return err
	}
	// Readiness exposes the local listener as a Service endpoint while the
	// webhook still ignores connection failures. Activate fail-closed admission
	// only after that routable Service identity is usable.
	if err := m.checkActiveLiveTLS(ctx, true); err != nil {
		return err
	}
	return m.ensureFailurePolicy(ctx, admissionregistrationv1.Fail)
}

func (m *Manager) setLastError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastErr = err
}

type caMaterial struct {
	cert    *x509.Certificate
	key     *rsa.PrivateKey
	certPEM []byte
	keyPEM  []byte
}

type caState struct {
	active              caMaterial
	next                caMaterial
	previous            *x509.Certificate
	previousPEM         []byte
	previousRetireAfter time.Time
}

func (m *Manager) reconcile(ctx context.Context) error {
	if err := os.MkdirAll(m.config.CertDir, 0o700); err != nil {
		return fmt.Errorf("create webhook certificate directory: %w", err)
	}

	state, caRotated, err := m.ensureCAState(ctx)
	if err != nil {
		return err
	}
	if !caRotated {
		if publishErr := m.publishTrust(ctx, state); publishErr != nil {
			return publishErr
		}
	}
	if state.previous == nil {
		if removeErr := m.removeExpiredPreviousCA(ctx); removeErr != nil {
			return removeErr
		}
	}
	leaf, rotated, err := m.ensureServingCertificate(state)
	if err != nil {
		return err
	}
	if rotated {
		certificateRotations.WithLabelValues("serving").Inc()
	}
	certificateExpiry.WithLabelValues("active_ca").Set(float64(state.active.cert.NotAfter.Unix()))
	certificateExpiry.WithLabelValues("next_ca").Set(float64(state.next.cert.NotAfter.Unix()))
	certificateExpiry.WithLabelValues("serving").Set(float64(leaf.NotAfter.Unix()))
	return nil
}

func (m *Manager) removeExpiredPreviousCA(ctx context.Context) error {
	return retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		secret := &corev1.Secret{}
		key := types.NamespacedName{Name: m.config.SecretName, Namespace: m.config.Namespace}
		if err := m.client.Get(ctx, key, secret); err != nil {
			return err
		}
		if len(secret.Data[PreviousCACertKey]) == 0 && len(secret.Data[PreviousCARetireAfterKey]) == 0 {
			return nil
		}
		retireAfter, err := time.Parse(time.RFC3339, string(secret.Data[PreviousCARetireAfterKey]))
		if err != nil || m.config.Now().Before(retireAfter) {
			return nil
		}
		delete(secret.Data, PreviousCACertKey)
		delete(secret.Data, PreviousCARetireAfterKey)
		return m.client.Update(ctx, secret)
	})
}

func (m *Manager) ensureCAState(ctx context.Context) (caState, bool, error) {
	var state caState
	rotated := false
	err := retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		rotated = false
		secret := &corev1.Secret{}
		key := types.NamespacedName{Name: m.config.SecretName, Namespace: m.config.Namespace}
		if err := m.client.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("required webhook CA Secret %s/%s does not exist", key.Namespace, key.Name)
			}
			return fmt.Errorf("get webhook CA Secret: %w", err)
		}

		var err error
		if len(secret.Data) == 0 {
			state, err = m.generateInitialCAState()
			if err != nil {
				return err
			}
			secret.Data = state.secretData()
			if updateErr := m.client.Update(ctx, secret); updateErr != nil {
				return updateErr
			}
			return nil
		}

		state, err = loadCAState(secret, m.config.Now())
		if err != nil {
			state, err = m.recoverExpiredActiveCA(secret)
			if err != nil {
				return fmt.Errorf("webhook CA Secret %s/%s is non-empty but invalid; refusing to overwrite it: %w",
					key.Namespace, key.Name, err)
			}
			if publishErr := m.publishTrust(ctx, state); publishErr != nil {
				return publishErr
			}
			if servingErr := m.ensureServingCertificateForCA(state.active); servingErr != nil {
				return servingErr
			}
			secret.Data = state.secretData()
			if updateErr := m.client.Update(ctx, secret); updateErr != nil {
				return updateErr
			}
			rotated = true
			certificateRotations.WithLabelValues("ca").Inc()
			return nil
		}
		if state.active.cert.NotAfter.After(m.config.Now().Add(caRenewBefore)) {
			return nil
		}
		if !state.next.cert.NotAfter.After(m.config.Now().Add(caRenewBefore)) {
			return errors.New("standby webhook CA does not have enough remaining validity for a safe issuer switch")
		}

		// Publish trust for the standby CA before making it active. This update
		// is intentionally separate from the Secret update to preserve overlap
		// if the process exits between phases.
		if publishErr := m.publishTrust(ctx, state); publishErr != nil {
			return publishErr
		}
		if servingErr := m.ensureServingCertificateForCA(state.next); servingErr != nil {
			return servingErr
		}
		replacement, err := generateCA(m.config.Now().Add(caValidity + caStagger))
		if err != nil {
			return err
		}
		state = caState{
			active:              state.next,
			next:                replacement,
			previous:            state.active.cert,
			previousPEM:         state.active.certPEM,
			previousRetireAfter: m.config.Now().Add(caTrustRetirementGrace),
		}
		secret.Data = state.secretData()
		if err := m.client.Update(ctx, secret); err != nil {
			return err
		}
		rotated = true
		certificateRotations.WithLabelValues("ca").Inc()
		return nil
	})
	return state, rotated, err
}

func (m *Manager) recoverExpiredActiveCA(secret *corev1.Secret) (caState, error) {
	now := m.config.Now()
	active, activeErr := loadCA(secret.Data, ActiveCACertKey, ActiveCAKeyKey, time.Time{})
	if activeErr != nil {
		return caState{}, activeErr
	}
	if now.Before(active.cert.NotBefore) || now.Before(active.cert.NotAfter) {
		return caState{}, errors.New("active webhook CA is invalid for a reason other than expiry")
	}
	standby, standbyErr := loadCA(secret.Data, NextCACertKey, NextCAKeyKey, now)
	if standbyErr != nil {
		return caState{}, standbyErr
	}
	if !standby.cert.NotAfter.After(now.Add(caRenewBefore)) {
		return caState{}, errors.New("standby webhook CA does not have enough remaining validity for recovery")
	}
	replacement, err := generateCA(now.Add(caValidity + caStagger))
	if err != nil {
		return caState{}, err
	}
	return caState{active: standby, next: replacement}, nil
}

func (m *Manager) generateInitialCAState() (caState, error) {
	active, err := generateCA(m.config.Now().Add(caValidity))
	if err != nil {
		return caState{}, err
	}
	next, err := generateCA(m.config.Now().Add(caValidity + caStagger))
	if err != nil {
		return caState{}, err
	}
	return caState{active: active, next: next}, nil
}

func generateCA(notAfter time.Time) (caMaterial, error) {
	cert, key, err := admissionpki.GenerateCACertificate(notAfter)
	if err != nil {
		return caMaterial{}, fmt.Errorf("generate webhook CA: %w", err)
	}
	certPEM, err := admissionpki.EncodeCertificatePem(cert)
	if err != nil {
		return caMaterial{}, err
	}
	keyPEM, err := admissionpki.EncodePrivateKeyPem(key)
	if err != nil {
		return caMaterial{}, err
	}
	return caMaterial{cert: cert, key: key, certPEM: *certPEM, keyPEM: *keyPEM}, nil
}

func loadCAState(secret *corev1.Secret, now time.Time) (caState, error) {
	active, err := loadCA(secret.Data, ActiveCACertKey, ActiveCAKeyKey, now)
	if err != nil {
		return caState{}, err
	}
	next, err := loadCA(secret.Data, NextCACertKey, NextCAKeyKey, now)
	if err != nil {
		return caState{}, err
	}
	if active.cert.Equal(next.cert) {
		return caState{}, errors.New("active and standby webhook CAs must differ")
	}
	state := caState{active: active, next: next}
	previousPEM := secret.Data[PreviousCACertKey]
	retireAfterRaw := secret.Data[PreviousCARetireAfterKey]
	if len(previousPEM) == 0 && len(retireAfterRaw) == 0 {
		return state, nil
	}
	if len(previousPEM) == 0 || len(retireAfterRaw) == 0 {
		return caState{}, errors.New("previous webhook CA and retirement timestamp must be stored together")
	}
	previous, err := loadCertificate(previousPEM, PreviousCACertKey)
	if err != nil {
		return caState{}, err
	}
	retireAfter, err := time.Parse(time.RFC3339, string(retireAfterRaw))
	if err != nil {
		return caState{}, fmt.Errorf("parse %s: %w", PreviousCARetireAfterKey, err)
	}
	if now.Before(retireAfter) {
		state.previous = previous
		state.previousPEM = previousPEM
		state.previousRetireAfter = retireAfter
	}
	return state, nil
}

func loadCertificate(certPEM []byte, certKey string) (*x509.Certificate, error) {
	certBlock, certRest := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(certRest)) != 0 {
		return nil, fmt.Errorf("%s must contain exactly one certificate", certKey)
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", certKey, err)
	}
	if !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, fmt.Errorf("%s is not a valid CA certificate", certKey)
	}
	return cert, nil
}

func loadCA(data map[string][]byte, certKey, privateKey string, now time.Time) (caMaterial, error) {
	certPEM, ok := data[certKey]
	if !ok || len(certPEM) == 0 {
		return caMaterial{}, fmt.Errorf("missing %s", certKey)
	}
	keyPEM, ok := data[privateKey]
	if !ok || len(keyPEM) == 0 {
		return caMaterial{}, fmt.Errorf("missing %s", privateKey)
	}
	certBlock, certRest := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(certRest)) != 0 {
		return caMaterial{}, fmt.Errorf("%s must contain exactly one certificate", certKey)
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return caMaterial{}, fmt.Errorf("parse %s: %w", certKey, err)
	}
	keyBlock, keyRest := pem.Decode(keyPEM)
	if keyBlock == nil || keyBlock.Type != "RSA PRIVATE KEY" || len(bytes.TrimSpace(keyRest)) != 0 {
		return caMaterial{}, fmt.Errorf("%s must contain exactly one RSA private key", privateKey)
	}
	key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		return caMaterial{}, fmt.Errorf("parse %s: %w", privateKey, err)
	}
	publicKey, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok || !publicKey.Equal(&key.PublicKey) {
		return caMaterial{}, fmt.Errorf("certificate and private key for %s do not match", certKey)
	}
	if !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return caMaterial{}, fmt.Errorf("%s is not a valid CA certificate", certKey)
	}
	if !now.IsZero() && (now.Before(cert.NotBefore) || !now.Before(cert.NotAfter)) {
		return caMaterial{}, fmt.Errorf("%s is not currently valid", certKey)
	}
	if err := cert.CheckSignatureFrom(cert); err != nil {
		return caMaterial{}, fmt.Errorf("%s is not self-signed: %w", certKey, err)
	}
	return caMaterial{cert: cert, key: key, certPEM: certPEM, keyPEM: keyPEM}, nil
}

func (state caState) secretData() map[string][]byte {
	data := map[string][]byte{
		ActiveCACertKey: state.active.certPEM,
		ActiveCAKeyKey:  state.active.keyPEM,
		NextCACertKey:   state.next.certPEM,
		NextCAKeyKey:    state.next.keyPEM,
	}
	if state.previous != nil {
		data[PreviousCACertKey] = state.previousPEM
		data[PreviousCARetireAfterKey] = []byte(state.previousRetireAfter.UTC().Format(time.RFC3339))
	}
	return data
}

func (state caState) bundle() []byte {
	bundle := make([]byte, 0, len(state.previousPEM)+len(state.active.certPEM)+len(state.next.certPEM))
	if state.previous != nil {
		bundle = append(bundle, state.previousPEM...)
	}
	bundle = append(bundle, state.active.certPEM...)
	bundle = append(bundle, state.next.certPEM...)
	return bundle
}

func (m *Manager) publishTrust(ctx context.Context, state caState) error {
	bundle := state.bundle()
	return retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		key := types.NamespacedName{Name: m.config.WebhookConfigName}
		if err := m.client.Get(ctx, key, configuration); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("required ValidatingWebhookConfiguration %s does not exist", key.Name)
			}
			return fmt.Errorf("get ValidatingWebhookConfiguration: %w", err)
		}
		webhook, err := m.namedWebhook(configuration)
		if err != nil {
			return err
		}
		if bytes.Equal(webhook.ClientConfig.CABundle, bundle) {
			return nil
		}
		webhook.ClientConfig.CABundle = bundle
		if err := m.client.Update(ctx, configuration); err != nil {
			return fmt.Errorf("publish webhook CA bundle: %w", err)
		}
		return nil
	})
}

func (m *Manager) ensureServingCertificateForCA(ca caMaterial) error {
	certPath := filepath.Join(m.config.CertDir, m.config.CertName)
	keyPath := filepath.Join(m.config.CertDir, m.config.KeyName)
	dnsNames := m.dnsNames()
	cert, key, err := admissionpki.GenerateServerCertificate(dnsNames[2], dnsNames, ca.cert, ca.key)
	if err != nil {
		return fmt.Errorf("generate webhook serving certificate: %w", err)
	}
	certPEM, err := admissionpki.EncodeCertificatePem(cert)
	if err != nil {
		return err
	}
	keyPEM, err := admissionpki.EncodePrivateKeyPem(key)
	if err != nil {
		return err
	}
	if writeErr := writeServingPair(certPath, keyPath, *certPEM, *keyPEM); writeErr != nil {
		return writeErr
	}
	_, err = m.loadAndVerifyServingCertificate(certPath, keyPath, ca)
	if err == nil {
		certificateRotations.WithLabelValues("serving").Inc()
	}
	return err
}

func (m *Manager) namedWebhook(configuration *admissionregistrationv1.ValidatingWebhookConfiguration) (*admissionregistrationv1.ValidatingWebhook, error) {
	for i := range configuration.Webhooks {
		webhook := &configuration.Webhooks[i]
		if webhook.Name != m.config.WebhookName {
			continue
		}
		service := webhook.ClientConfig.Service
		if service == nil || service.Name != m.config.ServiceName || service.Namespace != m.config.Namespace {
			return nil, fmt.Errorf("webhook %s does not target Service %s/%s", m.config.WebhookName, m.config.Namespace, m.config.ServiceName)
		}
		if err := validateWebhookContract(webhook, m.config.WebhookPort); err != nil {
			return nil, err
		}
		if webhook.FailurePolicy == nil || (*webhook.FailurePolicy != admissionregistrationv1.Ignore && *webhook.FailurePolicy != admissionregistrationv1.Fail) {
			return nil, fmt.Errorf("webhook %s must use failurePolicy Ignore or Fail", m.config.WebhookName)
		}
		return webhook, nil
	}
	return nil, fmt.Errorf("ValidatingWebhookConfiguration %s does not contain webhook %s", m.config.WebhookConfigName, m.config.WebhookName)
}

func validateWebhookContract(webhook *admissionregistrationv1.ValidatingWebhook, webhookPort int32) error {
	service := webhook.ClientConfig.Service
	if service.Path == nil || *service.Path != WebhookPath || service.Port == nil || *service.Port != webhookPort {
		return fmt.Errorf("webhook %s has an unexpected service path or port", webhook.Name)
	}
	if len(webhook.AdmissionReviewVersions) != 1 || webhook.AdmissionReviewVersions[0] != "v1" {
		return fmt.Errorf("webhook %s must accept admissionReviewVersion v1", webhook.Name)
	}
	if webhook.SideEffects == nil || *webhook.SideEffects != admissionregistrationv1.SideEffectClassNone {
		return fmt.Errorf("webhook %s must declare sideEffects None", webhook.Name)
	}
	for _, rule := range webhook.Rules {
		if rule.Scope == nil || *rule.Scope != admissionregistrationv1.NamespacedScope ||
			!contains(rule.APIGroups, QueueAPIGroup) || !contains(rule.APIVersions, QueueAPIVersion) ||
			!contains(rule.Resources, QueueResource) ||
			!containsOperation(rule.Operations, admissionregistrationv1.Create) ||
			!containsOperation(rule.Operations, admissionregistrationv1.Update) {
			continue
		}
		return nil
	}
	return fmt.Errorf("webhook %s does not contain the expected Queue validation rule", webhook.Name)
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsOperation(values []admissionregistrationv1.OperationType, want admissionregistrationv1.OperationType) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (m *Manager) ensureServingCertificate(state caState) (*x509.Certificate, bool, error) {
	certPath := filepath.Join(m.config.CertDir, m.config.CertName)
	keyPath := filepath.Join(m.config.CertDir, m.config.KeyName)
	if leaf, err := m.loadAndVerifyServingCertificate(certPath, keyPath, state.active); err == nil &&
		leaf.NotAfter.After(m.config.Now().Add(leafRenewBefore)) {
		return leaf, false, nil
	}

	dnsNames := m.dnsNames()
	cert, key, err := admissionpki.GenerateServerCertificate(dnsNames[2], dnsNames, state.active.cert, state.active.key)
	if err != nil {
		return nil, false, fmt.Errorf("generate webhook serving certificate: %w", err)
	}
	certPEM, err := admissionpki.EncodeCertificatePem(cert)
	if err != nil {
		return nil, false, err
	}
	keyPEM, err := admissionpki.EncodePrivateKeyPem(key)
	if err != nil {
		return nil, false, err
	}
	if writeErr := writeServingPair(certPath, keyPath, *certPEM, *keyPEM); writeErr != nil {
		return nil, false, writeErr
	}
	leaf, err := m.loadAndVerifyServingCertificate(certPath, keyPath, state.active)
	return leaf, true, err
}

func writeServingPair(certPath, keyPath string, certPEM, keyPEM []byte) error {
	keyTemp, err := writeTemp(filepath.Dir(keyPath), ".tls-key-", keyPEM, 0o600)
	if err != nil {
		return err
	}
	defer os.Remove(keyTemp)
	certTemp, err := writeTemp(filepath.Dir(certPath), ".tls-cert-", certPEM, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(certTemp)
	if err := os.Rename(keyTemp, keyPath); err != nil {
		return fmt.Errorf("install webhook serving key: %w", err)
	}
	if err := os.Rename(certTemp, certPath); err != nil {
		return fmt.Errorf("install webhook serving certificate: %w", err)
	}
	return nil
}

func writeTemp(dir, pattern string, data []byte, mode os.FileMode) (string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	name := file.Name()
	defer func() {
		_ = file.Close()
	}()
	if err := file.Chmod(mode); err != nil {
		return "", err
	}
	if _, err := file.Write(data); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return name, nil
}

func (m *Manager) loadAndVerifyServingCertificate(certPath, keyPath string, active caMaterial) (*x509.Certificate, error) {
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	if len(pair.Certificate) != 1 {
		return nil, fmt.Errorf("serving certificate file must contain exactly one certificate, got %d", len(pair.Certificate))
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(active.cert)
	_, err = leaf.Verify(x509.VerifyOptions{
		DNSName:     m.dnsNames()[2],
		Roots:       pool,
		CurrentTime: m.config.Now(),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return nil, fmt.Errorf("verify serving certificate against active CA: %w", err)
	}
	return leaf, nil
}

func (m *Manager) dnsNames() []string {
	return []string{
		m.config.ServiceName,
		m.config.ServiceName + "." + m.config.Namespace,
		m.config.ServiceName + "." + m.config.Namespace + ".svc",
		m.config.ServiceName + "." + m.config.Namespace + ".svc.cluster.local",
	}
}

func (m *Manager) checkReady(ctx context.Context) error {
	secret := &corev1.Secret{}
	if err := m.client.Get(ctx, types.NamespacedName{Name: m.config.SecretName, Namespace: m.config.Namespace}, secret); err != nil {
		return err
	}
	state, stateErr := loadCAState(secret, m.config.Now())
	if stateErr != nil {
		return stateErr
	}
	certPath := filepath.Join(m.config.CertDir, m.config.CertName)
	keyPath := filepath.Join(m.config.CertDir, m.config.KeyName)
	if _, err := m.loadAndVerifyServingCertificate(certPath, keyPath, state.active); err != nil {
		return err
	}

	configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := m.client.Get(ctx, types.NamespacedName{Name: m.config.WebhookConfigName}, configuration); err != nil {
		return err
	}
	webhook, err := m.namedWebhook(configuration)
	if err != nil {
		return err
	}
	publishedRoots := x509.NewCertPool()
	if !publishedRoots.AppendCertsFromPEM(webhook.ClientConfig.CABundle) {
		return errors.New("published webhook CA bundle is empty or malformed")
	}
	if _, err := state.active.cert.Verify(x509.VerifyOptions{Roots: publishedRoots, CurrentTime: m.config.Now()}); err != nil {
		return fmt.Errorf("active CA is not covered by published webhook trust: %w", err)
	}
	activeRoots := x509.NewCertPool()
	activeRoots.AddCert(state.active.cert)
	return m.checkTLSWithRoots(ctx, activeRoots, m.config.ProbeAddress, m.config.LiveTLSCheck)
}

func (m *Manager) ensureFailurePolicy(ctx context.Context, policy admissionregistrationv1.FailurePolicyType) error {
	return retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		if err := m.client.Get(ctx, types.NamespacedName{Name: m.config.WebhookConfigName}, configuration); err != nil {
			return err
		}
		webhook, err := m.namedWebhook(configuration)
		if err != nil {
			return err
		}
		if webhook.FailurePolicy != nil && *webhook.FailurePolicy == policy {
			return nil
		}
		webhook.FailurePolicy = &policy
		return m.client.Update(ctx, configuration)
	})
}

func (m *Manager) checkActiveLiveTLS(ctx context.Context, service bool) error {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: m.config.SecretName, Namespace: m.config.Namespace}
	if err := m.client.Get(ctx, key, secret); err != nil {
		return err
	}
	state, err := loadCAState(secret, m.config.Now())
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AddCert(state.active.cert)
	if service {
		return m.checkTLSWithRoots(ctx, roots, m.config.ServiceProbeAddress, m.config.ServiceTLSCheck)
	}
	return m.checkTLSWithRoots(ctx, roots, m.config.ProbeAddress, m.config.LiveTLSCheck)
}

func (m *Manager) checkTLSWithRoots(
	ctx context.Context,
	roots *x509.CertPool,
	address string,
	check func(context.Context, *x509.CertPool) error,
) error {
	if check != nil {
		return check(ctx, roots)
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("connect to webhook TLS listener at %s: %w", address, err)
	}
	defer connection.Close()
	tlsConnection := tls.Client(connection, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: m.dnsNames()[2],
	})
	if err := tlsConnection.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("verify live webhook TLS identity on %s: %w", address, err)
	}
	return nil
}

func LocalProbeAddress(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
