package fsm

import (
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"testing"
)

func TestJobScheduling(t *testing.T) {
	configData := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        resources:
          guaranteed:
            memory: 200
            vcore: 20
          max:
            memory: 200
            vcore: 20
        queues:
          -
            name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
`
	// init and register scheduler
	cluster := FakeCluster{}
	cluster.init(configData)
	cluster.start()
	defer cluster.stop()

	// ensure scheduler state
	cluster.waitForSchedulerState(t, common.States().Scheduler.Registered)

	// register nodes
	cluster.addNode("test.host.01", 100, 10)
	cluster.addNode("test.host.02", 100, 10)

	// create job and tasks
	job0001 := cluster.newJob("job0001", "root.a")
	cluster.addTask("task0001", common.CreateResource(10, 1), job0001)
	cluster.addTask("task0002", common.CreateResource(10, 1), job0001)

	// add job to context
	cluster.addJob(job0001)

	// wait for scheduling job and tasks
	// verify job state
	cluster.waitAndAssertJobState(t, "job0001", common.States().Job.Running)
	cluster.waitAndAssertTaskState(t, "job0001", "task0001", common.States().Task.Bound)
	cluster.waitAndAssertTaskState(t, "job0001", "task0002", common.States().Task.Bound)
}