# Kubernetes Unity Scheduler

Embedded unity scheduler is a customized k8s scheduler, with pre-defined name `unified-scheduler`.
Pods whose spec contains field `schedulerName: unified-scheduler` will be scheduled by this scheduler.


## Build and Run

### Build binary on laptop

```
make build
```

### Run binary on laptop, connects to Kubernetes cluster via local `kubectl`

```
make run
```

### Build scheduler docker image

```
make image
```

### Deploy a job

```
kubectl create -f deployments/nigix/nginxjob.yaml 
```
this job will be pending as it asks to be scheduled by `unity-scheduler`.

### Deploy scheduler on Kubernetes

```
kubectl create -f deployments/scheduler/scheduler.yaml
```

then previous job can be scheduled.

## Options 

### Logging level

```
# logging level
0: FATAL
1: ERROR
2: WARN
3: INFO
4: DEBUG
5: VERBOSE

# log VERBOSE to stderr
./unity_scheduler -logtostderr=true -v=5

# log INFO to file under certain dir
./unity_scheduler -log_dir=/path/to/logs -v=3

# more options
./unity_scheduler -help
```
