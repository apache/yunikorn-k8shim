<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

# A simple example to demo preemption

## Description
This example includes yamls to
1. Create priority queue with preemption fence. ([yunikorn-configs.yaml](./yunikorn-configs.yaml))
2. Create application with PriorityClass. ([normal-priority-job-with-9999-priority-class.yaml](./normal-priority-job-with-9999-priority-class.yaml))

By following the steps below, you will be able to see the below behaviors of preemption
1. A task cannot preempt tasks outside its preemption fence.
2. Preemption is aiming to ensure that the queue's resource usage reaches at least the guaranteed amount of resources.
3. Preemption can never leave a queue lower than its guaranteed capacity.

## Run the example workloads
   ```shell script
   # Update YuniKorn config
   kubectl apply -f yunikorn-configs.yaml

   # Create low-priority jobs
   kubectl apply -f low-priority-job-outside-fence.yaml
   kubectl apply -f low-priority-job.yaml
   kubectl get pods | grep Running
   # Should have 10 pods in Running.
   # Expected: 5 outside fence, 5 low-priority.

   # Create normal-priority job
   kubectl apply -f normal-priority-job.yaml
   kubectl get pods | grep Running
   # Preemption is triggered after a specified delay, with the aim of ensuring that each queue's resource usage reaches at least the guaranteed amount of resources. A task cannot preempt tasks outside its preemption fence.
   # Wait 1~2 minutes for preemption to occur.
   # Expected: 5 outside fence, 2 low-priority, 3 normal-priority.
  
   # Remove low-priority job
   kubectl delete -f low-priority-job.yaml
   kubectl get pods | grep Running
   # Expected: 5 outside fence, 5 normal-priority.

   # Create high-priority job
   kubectl apply -f high-priority-job.yaml
   kubectl get pods | grep Running
   # Preemption can never leave a queue lower than its guaranteed capacity.
   # Wait 1~2 minutes for preemption to occur.
   # Expected: 5 outside fence, 3 normal-priority, 2 high-priority.

   # Remove normal-priority job
   kubectl delete -f normal-priority-job.yaml
   kubectl get pods | grep Running
   # Expected: 5 outside fence, 5 high-priority.

   # Create normal-priority job with PriorityClass
   kubectl apply -f normal-priority-job-with-9999-priority-class.yaml
   kubectl get pods | grep Running
   # After applying priority.offset in the queue, the job has the highest priority.
   # Wait 1~2 minutes for preemption to occur.
   # Expected: 5 outside fence, 3 high-priority, 2 normal-priority-job-with-9999-priority-class.

   #cleanup
   kubectl delete -f normal-priority-job-with-9999-priority-class.yaml
   kubectl delete -f high-priority-job.yaml 
   kubectl delete -f low-priority-job-outside-fence.yaml
   ```

