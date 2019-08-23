# Admission Controller to inject SchedulerName on-the-fly

This directory contains resources to create an admission controller web-hook which can inject
`schedulerName` to pod's spec before admitting it. This can be used to deploy on an existing
Kubernetes cluster and route all pods to YuniKorn, which can be treated as an alternative way
to replace default scheduler.

## Steps

### Launch the admission controller

Create the admission controller web-hook, run following command

```shell script
./admission_util.sh create
```
this command does following tasks

- Create private key and certificate
- Sign and approve certificate with K8s `CertificateSigningRequest`
- Create a K8s `secret` which stores the private key and signed certificate
- Apply the secret to admission controller web-hook's deployment, insert the `caBundle`
- Create the admission controller web-hook and launch it on K8s

### Test the admission controller

Launch a pod with following spec, note this spec did not specify `schedulerName`,
without the admission controller, it should be scheduled by default scheduler without any issue.

```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: sleep
    applicationId: "application-sleep-0001"
    queue: "root.sandbox"
  name: task0
spec:
  containers:
    - name: sleep-30s
      image: "alpine:latest"
      command: ["sleep", "30"]
      resources:
        requests:
          cpu: "100m"
          memory: "500M"
```

However, after the admission controller is started, this pod will keep at pending state (if yunikorn is not running).
Review the spec,

```shell script
kubectl get pod task0 -o yaml 
```

you'll see the `schedulerName` has been injected with value `yunikorn`.

### Stop and delete the admission controller

Use following command to cleanup all resources

```shell script
./admission_util.sh delete
```