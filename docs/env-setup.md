# Environment Setup

## 1. Setup a Local Kubernetes cluster on laptop

There are several ways to setup a local development environment for Kubernetes, most common ones are `Minikube` ([docs](https://kubernetes.io/docs/setup/minikube/)) and `docker-desktop`.
`Minikube` provisions a local Kubernetes cluster on several VMs (via VirtualBox etc). `docker-desktop` on the other hand, sets up Kubernetes cluster in docker containers.
In this tutorial, we'll use the latter approach, which is simpler and lightweight.

### 1.1 Installation

Download and install [Docker-Desktop](https://www.docker.com/products/docker-desktop) on your laptop. Latest version has an embedded version of Kubernetes so no additional install is needed.
Just simply follow the instruction [here](https://docs.docker.com/docker-for-mac/#kubernetes) to get Kubernetes up and running within docker-desktop.

Once Kubernetes is started in docker desktop, you should see something similar below:

![Kubernetes in Docker Desktop](images/docker-desktop.png)

that means

1. Kubernetes is running.
2. Kubectl context is set to `docker-desktop`, it can connect to this instance.

### 1.2 Access dashboard

From terminal, run `kubectl proxy`, then access UI from http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/#!/login.

### 1.3 Access local Kubernetes cluster

```
kubectl -n kube-system get secret
kubectl -n kube-system describe secret kubernetes-dashboard-token-tf6n8
```

### 1.4 Access remote Kubernetes cluster

Assume you have already installed remote Kubernetes cluster, you need 2 steps

1. Get kube `config` file from remote cluster, copy to the laptop
2. Properly set kubectl context with the new config (Detail step to be added...)
3. Switch context using `kubectl config use-context kubernetes-admin@kubernetes`

```
kubectl config get-contexts
CURRENT   NAME                          CLUSTER                      AUTHINFO             NAMESPACE
          docker-for-desktop            docker-for-desktop-cluster   docker-for-desktop
*         kubernetes-admin@kubernetes   kubernetes                   kubernetes-admin
```

More docs can be found [here](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/)  