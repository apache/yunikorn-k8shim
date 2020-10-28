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

# Gang in application level

The following script runs a given number of jobs, and each one is a simulated job that requires gang scheduling support. Where the job only starts to execute its tasks when the cluster has the min member of instances running.

```shell script
./cmd/gangDeploy.sh <job number> <gang member> <task run time(sec)>
```
Note: if you prefer to manually launch such jobs, please refer to the `gang-coordinator.yaml` `gang-job.yaml`. Where:
* [gang-coordinator.yaml](./gang-coordinator.yaml): the coordinator service used to coordinate the execution of each job's tasks.
* [gang-job](./gang-job.yaml): the simulated job that requires gang members to be started before executing its tasks.
