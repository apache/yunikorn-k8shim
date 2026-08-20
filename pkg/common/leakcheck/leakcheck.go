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

// Package leakcheck wraps uber-go/goleak so that goroutine leak detection can be
// switched on for a package by adding a main_test.go with:
//
//	func TestMain(m *testing.M) {
//		leakcheck.VerifyTestMain(m)
//	}
//
// A package that leaks something nobody else leaks passes its own exemptions as
// extra options, so the shared list stays as small as the evidence allows:
//
//	func TestMain(m *testing.M) {
//		leakcheck.VerifyTestMain(m, leakcheck.ShimSchedulerOptions()...)
//	}
//
// Those per-package groups are defined here rather than inlined into the
// main_test.go files, so that every exemption in the repository is documented in
// this one file.
//
// Every test package under pkg is instrumented. The suites under test/e2e are
// not: they are ginkgo suites that run against a live cluster rather than as
// part of "make test", and they leave client-go informers running on purpose, so
// a check there would need exemptions for goroutines that are meant to be alive.
//
// Three properties of goleak v1.3.0 are worth knowing before trusting a green
// run:
//
//   - The leak check only runs when m.Run() returns 0, so a leak introduced
//     together with a test failure only shows up on the CI round trip after the
//     failure is fixed.
//   - goleak polls for up to about 430ms waiting for goroutines to finish before
//     it reports them, so a teardown that is slower than that on a loaded CI
//     machine is reported as a leak.
//   - VerifyTestMain calls os.Exit itself. See the warning on VerifyTestMain
//     before passing goleak.Cleanup as an extra option.
package leakcheck

import (
	"slices"
	"testing"

	"go.uber.org/goleak"
)

// options returns the goleak exemptions shared by every instrumented package.
//
// goleak already filters out the goroutines that the testing, runtime and
// tracing packages run themselves, so every entry below is a goroutine that
// really does outlive the test binary today. IgnoreTopFunction matches on the
// top stack frame only, IgnoreAnyFunction on any frame below the creator.
// Neither is bounded by count, so the list stops new KINDS of leak from being
// added, it is not a proof that the exempted counts stay put.
//
// Two matching caveats: the entries that key on compiler-assigned closure names
// (".func1", ".func2") are positional, so inserting an earlier closure in the
// same enclosing function silently breaks the match (goleak v1.3.0 cannot match
// the creator frame); and a test that catches one of these goroutines mid-body,
// rather than parked in its select, sees a different top frame and can fail
// spuriously.
//
// Exemptions are split two ways. By lifetime: ecosystemOptions is for Kubernetes
// goroutines that never stop by design and is permanent, while the rest are the
// baseline of YuniKorn-owned leaks that was present when detection was switched
// on and is meant to be burned down, not extended. And by blast radius: only
// shapes that more than one package leaks live in this shared list, because an
// entry here switches the ratchet off everywhere. Everything else belongs in a
// per-package group like ShimSchedulerOptions. Each burn-down entry names its
// goroutine and the JIRA that tracks removing it.
func options() []goleak.Option {
	return slices.Concat(ecosystemOptions(), sharedOptions())
}

// ecosystemOptions returns the permanent exemptions: Kubernetes ecosystem
// goroutines that never stop by design.
//
// Empty, and that is a result rather than an oversight. The shim's unit tests
// drive fake clientsets and never call SharedInformerFactory.Start, so every
// leak the baseline run found is YuniKorn-owned. Add an entry here only for a
// goroutine that a Kubernetes library starts and offers no way to stop, and keep
// it as specific as the observed top frame allows.
func ecosystemOptions() []goleak.Option {
	return nil
}

// sharedOptions returns the burn-down entries that more than one package leaks.
func sharedOptions() []goleak.Option {
	return []goleak.Option{
		// DRA resource-slice tracker sync monitor (tracker.StartTracker). See YUNIKORN-3371.
		goleak.IgnoreTopFunction("k8s.io/dynamic-resource-allocation/resourceslice/tracker.(*Tracker).initInformers.func1"),
	}
}

// ShimSchedulerOptions returns the burn-down entries that only pkg/shim leaks,
// because only it runs a whole mock cluster: a real KubernetesShim driving a
// real yunikorn-core ServiceContext. Pass them from pkg/shim's TestMain rather
// than adding them to the shared list, which would switch these shapes off for
// the other seventeen packages as well.
func ShimSchedulerOptions() []goleak.Option {
	return slices.Concat(shimOwnedOptions(), coreServiceOptions())
}

// shimOwnedOptions returns the pkg/shim entries whose cause is in shim code.
func shimOwnedOptions() []goleak.Option {
	return []goleak.Option{
		// Placeholder-manager cleanup loop (leaked by Stop()-after-failed-Run). See YUNIKORN-3368.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-k8shim/pkg/cache.(*PlaceholderManager).Start.func1"),

		// Dispatcher event loop (leaked by Stop()-after-failed-Run). See YUNIKORN-3368.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-k8shim/pkg/dispatcher.Start.func1"),

		// A shim doScheduling loop (BackoffUntilWithContext). See YUNIKORN-3367.
		// Broadest entry here: matches every wait.Until/Forever/JitterUntil in
		// pkg/shim, and this apimachinery frame name has moved before
		// (BackoffUntil -> BackoffUntilWithContext), so a dep bump can silently
		// break the match.
		goleak.IgnoreTopFunction("k8s.io/apimachinery/pkg/util/wait.BackoffUntilWithContext"),

		// AssumePod retry on the RM proxy loop. See YUNIKORN-3369. Keyed on the
		// callback frame, not the top frame (time.Sleep), which is too broad.
		goleak.IgnoreAnyFunction("github.com/apache/yunikorn-k8shim/pkg/cache.(*AsyncRMCallback).UpdateAllocation"),
	}
}

// coreServiceOptions returns the pkg/shim entries that come from yunikorn-core:
// service goroutines the shim starts in-process and cannot stop in tests; they
// burn down once the core is restartable and the shim calls StopAll
// (YUNIKORN-3370). A few map to specific core defects, noted per entry.
func coreServiceOptions() []goleak.Option {
	return []goleak.Option{
		// Scheduler event handlers (Scheduler.StartService). See YUNIKORN-3370.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).handleAllocEvent"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).handleNodeEvent"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).handleInfraEvent"),

		// Scheduler background loops (Scheduler.StartService). See YUNIKORN-3370.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).internalSchedule"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).internalInspectOutstandingRequests"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).internalQuotaPreemption"),

		// internalSchedule wedged on an RM reply shutdown never delivers. See YUNIKORN-3365.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*ClusterContext).notifyRMNewAllocation"),

		// Node usage monitor and health checker (Scheduler.StartService). See YUNIKORN-3370.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*nodesResourceUsageMonitor).start.func1"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*HealthChecker).startInternal.func2"),

		// Partition cleaners (partitionManager.Run). See YUNIKORN-3366.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanRoot"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanExpiredApps"),

		// User/group cache cleaner. See YUNIKORN-3366.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/common/security.(*UserGroupCache).run"),

		// RM proxy event loop (RMProxy.StartService). See YUNIKORN-3370.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/rmproxy.(*RMProxy).handleRMEvents"),

		// Event system handler and publisher; the handler is the clearest
		// evidence core is not restartable in-process. See YUNIKORN-3363 and
		// YUNIKORN-3370.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventSystemImpl).StartServiceWithPublisher.func2"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventPublisher).StartService.func1"),

		// Internal metrics collector (ServiceContext). See YUNIKORN-3370.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/metrics.(*internalMetricsCollector).StartService.func1"),

		// Web service accept loop (WebService.StartWebApp). See YUNIKORN-3370.
		// Keyed on the yunikorn-core frame, not the top frame
		// (runtime_pollWait), which would exempt every network-blocked
		// goroutine.
		goleak.IgnoreAnyFunction("github.com/apache/yunikorn-core/pkg/webservice.(*WebService).StartWebApp.func1"),
	}
}

// VerifyTestMain runs m and fails the test binary if goroutines leaked. Any
// extra options are applied on top of the shared exemptions, so a package can
// carry an exemption of its own without widening the list for everyone.
//
// It calls os.Exit, so it must be the last statement of TestMain. Do not pass
// goleak.Cleanup as an extra option: goleak calls it instead of os.Exit, so a
// cleanup function that does not exit itself turns a leak failure into a silent
// pass.
func VerifyTestMain(m *testing.M, extra ...goleak.Option) {
	goleak.VerifyTestMain(m, slices.Concat(options(), extra)...)
}
