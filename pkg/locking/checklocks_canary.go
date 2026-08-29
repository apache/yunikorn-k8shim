//go:build checklocks_canary

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

package locking

import "fmt"

// This file is never built, the build constraint above is never set. It is the fixture for
// the self test of the "vetlock" make target: files named on the go vet command line are
// analysed even when a build constraint excludes them, so the target can hand this file to
// the analysis without it ever becoming part of the shim.
//
// The fixture holds a violation of each class that is in use: setValue writes
// a guarded field without holding the lock, reEnter calls a method that must not be called
// with the lock held while holding it, doubleLock takes one lock twice, and the callback
// table reaches the second of those from a body whose lock is named by the value it asserts.
// The self test asserts that the analysis reports all four. Without that assertion checklocks could
// silently stop finding anything at all and every run would still be green: the lock
// wrappers are recognised by their declaration in locking.go, so removing it, renaming the
// types, changing the forwarding methods or moving to a version that behaves differently
// all end in an analysis that reports less than it did. The fixture uses the wrappers of
// this package, which makes the self test cover the whole chain: wrapper type, forwarding
// methods, field annotation and lock precondition.
//
// Two more classes are here for a different reason: nothing in the shim states them any more.
// The exclusion of a method that takes its own lock is derived from the body, and a structure
// states the guard for its fields once instead of once per field, and between them they let
// 179 hand written annotations be deleted. An analyser that stopped deriving either would
// take that protection with it and leave every other message of this fixture intact, so both
// are required by name: see derivedReentrantCall and structGuardViolation below.

// canary carries a lock class, which is what lockblocking keys on: it reports a wait made while
// a CLASSED lock is held, so a type with no class of its own would leave waitUnderLock below
// silent and the coverage lost without a single message going missing elsewhere.
//
// +lockclass:canary.Canary
type canary struct {
	lock RWMutex
	// +checklocks:lock
	value int
}

// setValueLocked writes the guarded field the way it is supposed to be done. It must never
// be reported, a violation here means the forwarding methods stopped working.
func (c *canary) setValueLocked(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value = value
}

// setValue writes the guarded field without holding the lock. The self test in the Makefile
// requires the analysis to report "invalid field access" for this line.
func (c *canary) setValue(value int) {
	c.value = value
}

// setValueSelfLocking takes the lock itself, which makes it invalid to call while the lock
// is held. It must never be reported itself.
// +checklocksexclude:c.lock
func (c *canary) setValueSelfLocking(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value = value
}

// reEnter calls a method that excludes the lock while holding it, the self deadlock shape.
// The self test in the Makefile requires the analysis to report "must not hold" for this
// line.
func (c *canary) reEnter(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.setValueSelfLocking(value)
}

// relock takes and releases the lock twice over, which is balanced. It must never be reported.
func (c *canary) relock(value int) {
	c.lock.Lock()
	c.value = value
	c.lock.Unlock()
	c.lock.Lock()
	c.value = value
	c.lock.Unlock()
}

// doubleLock takes the lock a second time on the same path, the self deadlock shape at its
// simplest. The self test in the Makefile requires the analysis to report "already locked"
// for this line.
//
// The diagnostic only exists while the wrappers declare themselves lock primitives. Without
// that declaration the forwarding methods need a "+checklocksignore" each, and an ignore is
// read at every call site of the function that carries it, so the whole class disappears for
// every wrapper lock in the shim while every other message of this fixture stays exactly as
// it is.
func (c *canary) doubleLock(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lock.Lock()
	c.value = value
}

// derivedCanary is the subject of the derived exclusion fixture. Its lock and its field are
// apart from the ones above so a diagnostic about either can only have come from here.
type derivedCanary struct {
	lock         RWMutex
	derivedValue int // +checklocks:lock
}

// derivedSelfLocking carries no exclusion annotation on purpose. It takes its own lock, which
// is all the analysis needs to know that a caller holding that lock deadlocks, so the fact is
// derived from this body. The hand written exclusions the shim used to carry on every method
// of this shape were deleted on the strength of that derivation.
func (d *derivedCanary) derivedSelfLocking(value int) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.derivedValue = value
}

// derivedReentrantCall holds the lock over a call to a method that takes it. Nothing states
// the exclusion, so the analysis must have derived it, and the self test requires the message
// by the callee's name: the exclusions written by hand elsewhere in this fixture would keep it
// green otherwise.
func (d *derivedCanary) derivedReentrantCall(value int) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.derivedSelfLocking(value)
}

// structGuardCanary states the guard once, on the type, instead of once per field. Every field
// is guarded by it except the ones opting out, which is how the shim caches are annotated since
// the per field guards were collapsed.
//
// +checklocksguardedby:lock
type structGuardCanary struct {
	lock               RWMutex
	structGuardedValue int
	// +checklocksunguarded
	structFixedValue int
}

// structGuardedWrite writes the guarded field with the lock held. It must never be reported.
func (s *structGuardCanary) structGuardedWrite(value int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.structGuardedValue = value
}

// structGuardViolation writes a field that carries no annotation of its own without the lock,
// so it is only guarded while the annotation on the type expands to it. The self test requires
// the message by the field's name. The second write must never be reported: an expansion that
// ignored the opt out would report both, and requiring only the first message would pass in
// that world too.
func (s *structGuardCanary) structGuardViolation(value int) {
	s.structGuardedValue = value
	s.structFixedValue = value
}

// callbackCanary is the subject of the callback fixture below. Its guarded field is named
// apart from the one above so a diagnostic about it can only have come from there.
type callbackCanary struct {
	lock RWMutex
	// +checklocks:lock
	callbackValue int
}

// callbackSelfLocking takes the subject's own lock, so holding it on entry would deadlock.
// +checklocksexclude:c.lock
func (c *callbackCanary) callbackSelfLocking(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.callbackValue = value
}

// callbackEvent stands in for what a state machine library hands a callback: the subject
// arrives inside an interface and the body recovers it by asserting a type.
type callbackEvent struct {
	Args []any
}

// callbackTable is the shape the fsm callbacks in pkg/cache have: a table of literals handed
// to a library, each stating the lock its caller holds by naming the value its own body
// recovers, because that value exists nowhere else to be named.
//
// Both polarities are in the one literal. The write through the asserted subject is correct
// and must never be reported, and the call below it must be, because the guard put that
// subject's lock in scope and the callee takes it again.
//
// The second is what the self test requires, and it is the only message here that a guard
// which stopped binding would take with it: a guard matching nothing records no lock,
// silently, and then the call is fine while the write is reported instead. Requiring the
// report on the WRITE would pass in both worlds and prove nothing.
func callbackTable() map[string]func(*callbackEvent) {
	return map[string]func(*callbackEvent){
		// +checklocks:event.Args[0].(*callbackCanary).lock
		"enter": func(event *callbackEvent) {
			subject := event.Args[0].(*callbackCanary)
			subject.callbackValue = 1
			subject.callbackSelfLocking(2)
		},
	}
}

// String reads a guarded field from a method that is evaluated wherever a log entry is encoded,
// which the stringer analysis reports. The guarded field check is silenced here on purpose: it
// is the usual way this hazard is hidden, and the stringer analysis is meant to report it
// anyway. That also leaves setValue as the only source of the guarded field diagnostic, so the
// self test covers one fixture per analysis.
// +checklocksignore
func (c *canary) String() string {
	return fmt.Sprintf("canary %d", c.value)
}

// waitUnderLock waits on a channel with the lock held, which the blocking analysis reports.
func (c *canary) waitUnderLock(ch chan int) int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return <-ch
}
