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

// Package main is the entry point for the (opt-in) YuniKorn Queue Operator.
//
// The operator watches queue.yunikorn.k8s.io/v1alpha1 Queue CRs and
// materialises them into the yunikorn-configs ConfigMap that the YuniKorn
// scheduler consumes. See deployments/queue-operator/README.md for the
// install flow and rationale (YUNIKORN-3192).
package main

import (
	"crypto/tls"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC) to
	// ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/controller"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
}

// options bundles every flag main() parses. Kept as a plain struct (rather
// than a global) so the flag wiring stays testable.
type options struct {
	metricsAddr          string
	probeAddr            string
	enableLeaderElection bool
	secureMetrics        bool
	enableHTTP2          bool
	metricsCertPath      string
	metricsCertName      string
	metricsCertKey       string
	enableWebhook        bool
	webhookCertPath      string
	webhookCertName      string
	webhookCertKey       string
}

func parseFlags() (options, zap.Options) {
	var opts options
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", "0",
		"The address the metrics endpoint binds to. Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&opts.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&opts.secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.BoolVar(&opts.enableWebhook, "webhook-enabled", false,
		"Enable the validating webhook server. Requires --webhook-cert-path to be set.")
	flag.StringVar(&opts.webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&opts.webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&opts.webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&opts.metricsCertPath, "metrics-cert-path", "", "The directory that contains the metrics server certificate.")
	flag.StringVar(&opts.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&opts.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&opts.enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts, zapOpts
}

// buildTLSOpts returns the shared []func(*tls.Config) applied to both the
// metrics and webhook servers.
//
// http/2 is disabled unless --enable-http2 is set — see
// https://github.com/advisories/GHSA-qppj-fm5r-hxr3 and
// https://github.com/advisories/GHSA-4374-p667-p6c8 for the Stream
// Cancellation and Rapid Reset CVEs this mitigates.
func buildTLSOpts(enableHTTP2 bool) []func(*tls.Config) {
	if enableHTTP2 {
		return nil
	}
	return []func(*tls.Config){
		func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		},
	}
}

func newWebhookServer(opts options, tlsOpts []func(*tls.Config)) webhook.Server {
	whOpts := webhook.Options{TLSOpts: tlsOpts}
	if opts.webhookCertPath != "" {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", opts.webhookCertPath,
			"webhook-cert-name", opts.webhookCertName,
			"webhook-cert-key", opts.webhookCertKey)
		whOpts.CertDir = opts.webhookCertPath
		whOpts.CertName = opts.webhookCertName
		whOpts.KeyName = opts.webhookCertKey
	}
	return webhook.NewServer(whOpts)
}

func newMetricsOptions(opts options, tlsOpts []func(*tls.Config)) metricsserver.Options {
	m := metricsserver.Options{
		BindAddress:   opts.metricsAddr,
		SecureServing: opts.secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if opts.secureMetrics {
		// Protect the metrics endpoint with authn/authz — only clients
		// with the metrics-reader ClusterRole can scrape.
		m.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	if opts.metricsCertPath != "" {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", opts.metricsCertPath,
			"metrics-cert-name", opts.metricsCertName,
			"metrics-cert-key", opts.metricsCertKey)
		m.CertDir = opts.metricsCertPath
		m.CertName = opts.metricsCertName
		m.KeyName = opts.metricsCertKey
	}
	return m
}

func main() {
	opts, zapOpts := parseFlags()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	setupLog.Info("Initializing queue operator",
		"metricsAddr", opts.metricsAddr,
		"probeAddr", opts.probeAddr,
		"leaderElection", opts.enableLeaderElection)

	tlsOpts := buildTLSOpts(opts.enableHTTP2)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                newMetricsOptions(opts, tlsOpts),
		WebhookServer:          newWebhookServer(opts, tlsOpts),
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       "04cd498c.queue.yunikorn.k8s.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
	setupLog.Info("Manager initialized successfully")

	reconciler := &controller.QueueReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		// GetEventRecorderFor uses the old events API but is still the
		// broadly-compatible option across supported controller-runtime
		// releases; the newer GetEventRecorder is a controller-runtime
		// v0.24+ convenience.
		//nolint:staticcheck
		Recorder: mgr.GetEventRecorderFor("queue-controller"),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Queue")
		os.Exit(1)
	}
	setupLog.Info("Queue controller initialized",
		"targetNamespace", reconciler.TargetNamespace,
		"partitionName", reconciler.PartitionName,
		"placementRulesCount", len(reconciler.PlacementRules))

	if opts.enableWebhook {
		if opts.webhookCertPath == "" {
			setupLog.Error(nil, "--webhook-cert-path is required when --webhook-enabled is set")
			os.Exit(1)
		}
		if err := (&queuev1alpha1.Queue{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "Queue")
			os.Exit(1)
		}
		setupLog.Info("Validating webhook registered")
	} else {
		setupLog.Info("Webhook disabled")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
