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
//   - The leak check only runs when m.Run() returns 0. A package with a failing
//     test is never checked for leaks, so a leak introduced together with a test
//     failure only shows up on the CI round trip after the failure is fixed.
//   - goleak polls for up to about 430ms (20 retries, backing off from 1µs to a
//     100ms cap) waiting for goroutines to finish before it reports them. A
//     teardown that is slower than that on a loaded CI machine is reported as a
//     leak, which is the most likely source of a flake here.
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
// What this list does and does not buy us, precisely:
//
//   - goleak already filters out the goroutines that the testing, runtime and
//     tracing packages run themselves, so every entry below is a goroutine that
//     really does outlive the test binary today.
//   - IgnoreTopFunction matches on the top stack frame only, IgnoreAnyFunction on
//     any frame below the creator. Neither is bounded by count, so the baseline
//     stops new KINDS of leak from being added but does not stop new instances
//     of the shapes already listed. It is a ratchet against regression, not a
//     proof that the exempted counts stay put.
//   - Matching also assumes the goroutine is parked in its select when goleak
//     takes the stack snapshot, which holds for the default tick intervals used
//     in tests. A test that shortens an interval enough to catch one of these
//     mid-body would see a different top frame and a spurious failure.
//
// Exemptions are split two ways. By lifetime: section A, ecosystemOptions, is for
// Kubernetes goroutines that never stop by design and is permanent, while
// section B is the baseline of YuniKorn-owned leaks that was present when
// detection was switched on and is meant to be burned down, not extended. And by
// blast radius: only shapes that more than one package leaks live in this shared
// list, because an entry here switches the ratchet off everywhere. Everything
// else belongs in a per-package group like ShimSchedulerOptions.
//
// Each section B entry names the goroutine, what starts it, and what has to
// change before the entry can be deleted.
//
// Several entries key on compiler-assigned closure names (".func1", ".func2").
// Those names are positional: adding another closure earlier in the same
// enclosing function renumbers them and the exemption silently stops matching,
// turning the affected package red. goleak v1.3.0 cannot match on the creator
// frame, so there is no more robust spelling available. For the shim-owned
// entries the durable fix is to hoist the goroutine bodies into named methods,
// which is a production change and belongs in the follow-up that fixes the leaks
// themselves. For the borrowed ones a yunikorn-core or Kubernetes bump can
// renumber them at any time.
func options() []goleak.Option {
	return slices.Concat(ecosystemOptions(), sharedOptions())
}

// ecosystemOptions returns section A: Kubernetes ecosystem goroutines that never
// stop by design.
//
// Empty, and that is a result rather than an oversight. The shim's unit tests
// drive fake clientsets and never call SharedInformerFactory.Start, so no
// reflector, workqueue or watch goroutine is ever created, and klog is reached
// through klog.NewKlogr() without its flush daemon. Every leak the baseline run
// found is YuniKorn-owned, so nothing is exempt permanently.
//
// Add an entry here only for a goroutine that a Kubernetes library starts and
// offers no way to stop. Keep it as specific as the observed top frame allows,
// and keep it out of section B, which is a burn-down list.
func ecosystemOptions() []goleak.Option {
	return nil
}

// sharedOptions returns the section B entries that more than one package leaks.
func sharedOptions() []goleak.Option {
	return []goleak.Option{
		// DRA resource slice tracker sync monitor, started by
		// tracker.StartTracker. Leaked by pkg/cache, pkg/plugin/predicates,
		// pkg/plugin/support and pkg/shim, which all reach it through
		// cache.NewContext or support.SharedDRAManager. Both call StartTracker
		// with an uncancellable context, so the monitor's only two exits are
		// unreachable: the context is never cancelled, and the informers it
		// waits on are never started so they never sync. Tracker.Stop() would
		// cancel it, but neither caller keeps a handle that a shutdown path can
		// reach. Harmless in production, where a Context is built once and lives
		// as long as the process, but a test that builds a Context leaks one of
		// these per tracker for good. Delete this entry once the tracker is
		// created with a cancellable context, or once Context grows a shutdown
		// that calls Tracker.Stop().
		goleak.IgnoreTopFunction("k8s.io/dynamic-resource-allocation/resourceslice/tracker.(*Tracker).initInformers.func1"),
	}
}

// ShimSchedulerOptions returns the section B entries that only pkg/shim leaks,
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
		// Placeholder manager cleanup loop and dispatcher event loop, started by
		// KubernetesShim.Run through PlaceholderManager.Start and
		// dispatcher.Start. Suspected production defect rather than a test
		// problem: KubernetesShim.Stop() only stops them from inside the
		// "case ss.stopChan <- struct{}{}" arm of a select that has a default
		// branch. When Run() fails before doScheduling() nothing receives on
		// stopChan yet, so Stop() takes the default branch, logs "scheduler is
		// already stopped" and returns having stopped neither component. Run()
		// calls Stop() itself when registration or state initialization fails,
		// so the shim leaks both goroutines on every failed startup.
		// TestSchedulerRegistrationFailed reproduces it; in a full package run
		// the dispatcher goroutine is masked because it is a package singleton
		// that a later test stops, but it leaks whenever that test runs alone.
		// Delete both entries once Stop() releases what it owns regardless of
		// how far Run() got.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-k8shim/pkg/cache.(*PlaceholderManager).Start.func1"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-k8shim/pkg/dispatcher.Start.func1"),

		// A shim scheduling loop, started by KubernetesShim.doScheduling as one
		// of two wait.Until goroutines that share ss.stopChan. Suspected
		// production defect: stopChan is unbuffered and KubernetesShim.Stop()
		// sends a single value to it instead of closing it, so exactly one of
		// the two loops wakes up and returns. The other keeps running for the
		// lifetime of the process, scheduling applications on a shim that has
		// been told to stop. Which of schedule and checkOutstandingApps survives
		// is a race, and both park in the same Kubernetes backoff frame, so the
		// exemption cannot tell them apart.
		//
		// Two warnings for whoever triages this next. It is the broadest entry
		// anywhere in this file: the top frame belongs to k8s.io/apimachinery,
		// so within pkg/shim it exempts every wait.Until, wait.Forever and
		// wait.JitterUntil loop, including ones nobody has written yet. And the
		// frame name is an apimachinery implementation detail that has already
		// moved once, from BackoffUntil to BackoffUntilWithContext when the
		// context-aware variants were introduced; a dependency bump can rename
		// it again, at which point this entry silently stops matching and
		// pkg/shim turns red. Delete it once Stop() closes stopChan instead of
		// sending to it, which stops both loops.
		goleak.IgnoreTopFunction("k8s.io/apimachinery/pkg/util/wait.BackoffUntilWithContext"),

		// The core RM proxy event loop, wedged inside the shim's own callback.
		// AsyncRMCallback.UpdateAllocation retries AssumePod through
		// retry.OnError with a 30 step backoff, and it runs on the
		// RMProxy.handleRMEvents goroutine. Suspected production defect: the
		// retry has no stop channel and no context, so a pod that cannot be
		// assumed blocks all RM event handling for the length of the backoff and
		// cannot be interrupted by shutdown, because nothing the shim or the
		// core can stop reaches the loop. TestAssumePodError reproduces it.
		// Keyed on the callback frame rather than the top frame, which is
		// time.Sleep and far too broad to exempt. Delete this entry once the
		// retry aborts on shutdown.
		goleak.IgnoreAnyFunction("github.com/apache/yunikorn-k8shim/pkg/cache.(*AsyncRMCallback).UpdateAllocation"),
	}
}

// coreServiceOptions returns the pkg/shim entries that come from yunikorn-core.
// Every one of them is a service that entrypoint.StartAllServices() starts,
// leaked for a single reason: MockScheduler.stop() stops the shim and the mocked
// API provider but never calls coreContext.StopAll(), so the core half of the
// mock cluster keeps running after every test that builds one.
// The obvious fix does not work yet, so do not re-apply it without
// re-measuring. Adding StopAll() to MockScheduler.stop() does clear fifteen of
// these seventeen entries, but it also makes TestAssumePodError fail
// intermittently: 3 failures in 12 executions at -count=3 and 3 in 10 at
// -count=5, against 0 in 35 without it, all on the same machine. It only shows
// up when the package runs more than once per binary, which is why -count=1
// looks clean. The cause is that core's services are not restartable in-process:
// the event system singleton below is the clearest example, and StopAll() also
// tears down process-wide config callbacks and the user/group cache singleton
// that the next StartAllServices() then reuses. Delete this whole group once
// core can be stopped and restarted in one process, at which point
// MockScheduler.stop() can call StopAll().
//
// Two of these leak in yunikorn-core's own suite as well, for causes a StopAll()
// here would not address: notifyRMNewAllocation parks on an unbuffered reply
// channel when shutdown races the allocation path, and the user/group cache is a
// singleton that outlives its ClusterContext.
func coreServiceOptions() []goleak.Option {
	return []goleak.Option{
		// Scheduler event handlers, started by Scheduler.StartService.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).handleAllocEvent"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).handleNodeEvent"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).handleInfraEvent"),

		// Scheduler background loops, started by Scheduler.StartService.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).internalSchedule"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).internalInspectOutstandingRequests"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*Scheduler).internalQuotaPreemption"),

		// The internalSchedule goroutine again, caught a few frames deeper while
		// it waits for an RM proxy reply that shutdown never delivers.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*ClusterContext).notifyRMNewAllocation"),

		// Node resource usage monitor and health checker, started by
		// Scheduler.StartService alongside the loops above.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*nodesResourceUsageMonitor).start.func1"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*HealthChecker).startInternal.func2"),

		// Partition cleaners, started by partitionManager.Run when the mock
		// cluster's configuration adds a partition.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanRoot"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanExpiredApps"),

		// User/group cache cleaner, started once by security.GetUserGroupCache
		// when a partition resolves users.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/common/security.(*UserGroupCache).run"),

		// RM proxy event loop, started by RMProxy.StartService.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/rmproxy.(*RMProxy).handleRMEvents"),

		// Event system handler and publisher, started by
		// EventSystemImpl.StartService. The handler is also the clearest
		// evidence that core is not restartable in-process:
		// StartServiceWithPublisher starts a handler goroutine unconditionally
		// without clearing the stopped flag or recreating the channel that
		// Stop() nils out, and Stop() returns early once that flag is set, so
		// after one stop every later start leaks a handler that nothing can
		// reach.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventSystemImpl).StartServiceWithPublisher.func2"),
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventPublisher).StartService.func1"),

		// Internal metrics collector, started by ServiceContext when the history
		// size is non-zero.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/metrics.(*internalMetricsCollector).StartService.func1"),

		// Web service accept loop, started by WebService.StartWebApp. Keyed on
		// the yunikorn-core frame rather than the top frame, which is
		// internal/poll.runtime_pollWait and would exempt every goroutine in the
		// binary that is blocked on network I/O.
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
