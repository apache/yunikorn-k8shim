package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestTaskGroupInstanceCountMap(t *testing.T) {
	counts := NewTaskGroupInstanceCountMap()
	assert.Equal(t, counts.Size(), 0)
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g1"), int32(0))
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g2"), int32(0))

	counts = NewTaskGroupInstanceCountMap()
	counts.AddOne("g1")
	counts.AddOne("g1")
	counts.AddOne("g1")
	counts.AddOne("g2")
	counts.AddOne("g2")
	assert.Equal(t, counts.Size(), 2)
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g1"), int32(3))
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g2"), int32(2))

	counts.DeleteOne("g1")
	counts.DeleteOne("g2")
	assert.Equal(t, counts.Size(), 2)
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g1"), int32(2))
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g2"), int32(1))

	counts1 := NewTaskGroupInstanceCountMap()
	counts2 := NewTaskGroupInstanceCountMap()
	assert.Equal(t, counts1.Equals(counts2), true)
	counts1.Add("g1", 101)
	counts2.Add("g1", 101)
	assert.Equal(t, counts1.Equals(counts2), true)
	counts1.Add("g1", 100)
	counts2.Add("g1", 101)
	assert.Equal(t, counts1.Equals(counts2), false)

	counts1 = NewTaskGroupInstanceCountMap()
	counts2 = NewTaskGroupInstanceCountMap()
	counts1.AddOne("g1")
	counts1.AddOne("g2")
	counts1.AddOne("g3")
	counts1.AddOne("g4")
	counts1.AddOne("g5")
	counts2.AddOne("g5")
	counts2.AddOne("g4")
	counts2.AddOne("g3")
	counts2.AddOne("g2")
	counts2.AddOne("g1")
	assert.Equal(t, counts1.Equals(counts2), true)

	counts1 = NewTaskGroupInstanceCountMap()
	counts2 = NewTaskGroupInstanceCountMap()
	counts1.AddOne("g1")
	counts1.AddOne("g2")
	counts2.AddOne("g1")
	assert.Equal(t, counts1.Equals(counts2), false)

	counts1 = NewTaskGroupInstanceCountMap()
	counts2 = NewTaskGroupInstanceCountMap()
	counts1.AddOne("g1")
	counts2.AddOne("g2")
	counts2.AddOne("g1")
	assert.Equal(t, counts1.Equals(counts2), false)

	var nilOne *TaskGroupInstanceCountMap
	var nilTwo *TaskGroupInstanceCountMap
	assert.Equal(t, nilOne.Equals(nilTwo), true)

	empty := NewTaskGroupInstanceCountMap()
	assert.Equal(t, nilOne.Equals(empty), false)
}
