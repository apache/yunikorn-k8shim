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

// Deterministic reproduction of the YUNIKORN-3376 Task.pod race with testing/synctest.
//
// The pattern: replacing a probabilistic soak with a synctest interleaving test.
//
// Races between two shim goroutines used to be reproduced by running both in a loop for
// a minute under -race and hoping the schedule lined up. That is slow and flaky. The race
// detector does not actually need the two accesses to overlap in wall-clock time: it
// reports whenever two accesses to the same location are unordered by happens-before and
// at least one is a write. So a reproduction only has to (1) run both accesses, and
// (2) keep them happens-before unordered. synctest supplies the missing third property:
// a deterministic order in which they run, via the bubble's fake clock.
//
// The recipe used below, reusable for the FSM/lock interleaving tests we want next:
//   - Build the objects on the bubble's root goroutine, then start one goroutine per
//     conflicting operation inside the bubble.
//   - Order the operations with time.Sleep. Inside a bubble the clock only advances when
//     every goroutine is durably blocked, so a 1ms sleep is guaranteed to wake before a
//     2ms sleep. The ordering is exact and costs no real time.
//   - Join on a sync.WaitGroup created inside the bubble, and only then assert.
//   - The oracle is the race detector: run under -race, no report is a pass.
//
// Constraints found while writing this (worth knowing before copying the pattern):
//   - Do not sequence the two operations through the root goroutine, i.e. do not start
//     the first, synctest.Wait for it, then start the second. That chains a happens-before
//     edge writer -> root -> reader through the goroutine start, the accesses become
//     ordered, and the detector correctly reports nothing - the reproduction silently
//     stops reproducing. Ordering has to come from the clock, not from the root.
//   - synctest.Wait is not the way to wait for a sleep to elapse. A sleeping goroutine is
//     already durably blocked, so Wait returns straight away, and the clock only advances
//     once nothing is waiting. The root has to durably block itself, hence the WaitGroup.
//     Getting this wrong leaves the operations unexecuted and the bubble panics with
//     "main bubble goroutine has exited but blocked goroutines remain".
//   - The detector suppresses repeat reports with identical stacks, so several orderings
//     in one binary may only produce one report. Each ordering is a subtest, so run them
//     one at a time (-run '.../update-first') or set GORACE=suppress_equal_stacks=0 when
//     verifying that an ordering still reproduces.
//   - Everything the bubble touches must be created inside it. Channels, timers and
//     WaitGroups created outside and operated on inside (or the reverse) are fatal. That
//     rules out driving these tests through shared package-level machinery such as the
//     dispatcher; keep them at the level of direct method calls on freshly built objects.
//   - The bubble cannot be entered with deadlock detection turned on, which is why the test
//     skips itself there. pkg/locking is go-deadlock, and go-deadlock arms a time.AfterFunc
//     per lock acquisition and recycles the timers through a sync.Pool. A timer allocated
//     for a lock taken inside the bubble is later handed to a lock taken outside it, and
//     "fatal error: reset of synctest timer from outside bubble" takes down the whole test
//     binary, not just this test. That is why the guard tests the tracking flag rather than
//     the deadlock build tag: the flag is what actually arms the timers, and make test sets
//     the tag and DEADLOCK_DETECTION_ENABLED together but nothing forces them to agree.
//     go-deadlock does have a fix for this, it stops pooling under its own deadlock_synctest
//     build tag, but that tag also swaps every mutex in the process for a channel based one,
//     so adopting it is a decision for the whole repo rather than for one test.
//
// The race itself: checkPodMetadataBeforeScheduling runs on the scheduling goroutine and
// read task.pod without the task lock, while SetTaskPod replaces it under the lock from
// the informer update handler.
package cache

import (
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sEvents "k8s.io/client-go/tools/events"

	corelocking "github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
)

// newInconsistentMetadataPod builds a pod carrying both the canonical and the deprecated
// app-id and queue keys, so that checkPodMetadataBeforeScheduling always reaches
// logIgnoredPodMetadata and reads the pod a second time from there.
func newInconsistentMetadataPod(name string) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				constants.CanonicalLabelApplicationID: appID1,
				constants.CanonicalLabelQueueName:     queueNameA,
				constants.LabelApplicationID:          appID2,
				constants.LabelQueueName:              queueNameB,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
	}
}

// TestTaskPodRaceSynctest drives SetTaskPod (informer update) and
// checkPodMetadataBeforeScheduling (scheduling path) against one task from two goroutines
// in a synctest bubble, in every order the bubble can express. On unfixed code every
// ordering reports a data race; the assertions below additionally pin down which pod the
// metadata check observed, which proves the fake clock really did order the two.
func TestTaskPodRaceSynctest(t *testing.T) {
	if corelocking.IsTrackingEnabled() {
		t.Skip("synctest cannot be used while deadlock detection is on, go-deadlock recycles the " +
			"lock timers it creates inside the bubble and resetting one outside it is a fatal error")
	}

	const (
		oldPodName = "pod-before-update"
		newPodName = "pod-after-update"
	)

	testCases := []struct {
		name string
		// delays are fake-clock only: the bubble makes the shorter one wake first, always
		updateDelay     time.Duration
		checkDelay      time.Duration
		expectedPodName string
	}{
		{
			// no ordering at all, the two operations are simply concurrent
			name:            "concurrent",
			updateDelay:     0,
			checkDelay:      0,
			expectedPodName: "",
		},
		{
			// informer update lands first, the scheduling path sees the new pod
			name:            "update-first",
			updateDelay:     time.Millisecond,
			checkDelay:      2 * time.Millisecond,
			expectedPodName: newPodName,
		},
		{
			// scheduling path runs first, it still sees the pod the task was created with
			name:            "check-first",
			updateDelay:     2 * time.Millisecond,
			checkDelay:      time.Millisecond,
			expectedPodName: oldPodName,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// the fake recorder is created outside the bubble on purpose: its channel must
			// not become bubble owned, the events are read back after the bubble exits
			recorder := k8sEvents.NewFakeRecorder(1024)
			events.SetRecorder(recorder)
			defer events.SetRecorder(events.NewMockedRecorder())

			synctest.Test(t, func(t *testing.T) {
				app := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{}, nil)
				oldPod := newInconsistentMetadataPod(oldPodName)
				newPod := newInconsistentMetadataPod(newPodName)
				// both pods request the same resources, so SetTaskPod does not have to
				// update the allocation and the task needs no scheduler context
				task := NewTask("task01", app, nil, oldPod)

				// the WaitGroup has to be created in the bubble, and waiting on it is what
				// durably blocks the root and lets the fake clock run the sleeps below
				var wg sync.WaitGroup
				wg.Add(2)

				// informer update handler
				go func() {
					defer wg.Done()
					if tc.updateDelay > 0 {
						time.Sleep(tc.updateDelay)
					}
					task.SetTaskPod(newPod)
				}()

				// scheduling path, Application.scheduleTasks calls this before InitTask
				go func() {
					defer wg.Done()
					if tc.checkDelay > 0 {
						time.Sleep(tc.checkDelay)
					}
					task.checkPodMetadataBeforeScheduling()
				}()

				wg.Wait()

				assert.Equal(t, newPodName, task.GetTaskPod().Name, "informer update did not run")
			})

			// one warning per inconsistent metadata type: app-id and queue
			assert.Equal(t, 2, len(recorder.Events), "expected an app-id and a queue warning")
			for i := 0; i < 2; i++ {
				event := <-recorder.Events
				assert.Assert(t, strings.Contains(event, "Found multiple"), "unexpected event %s", event)
				if tc.expectedPodName != "" {
					assert.Assert(t, strings.Contains(event, tc.expectedPodName),
						"metadata check observed the wrong pod, expected %s in %s", tc.expectedPodName, event)
				}
			}
		})
	}
}
