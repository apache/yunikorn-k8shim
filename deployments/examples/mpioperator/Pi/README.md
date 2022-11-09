# MPI Operator

For more details, please read https://github.com/kubeflow/mpi-operator for deploying this operator.

This example assumes that the mpi operator v2beta is deployed to your kubernetes environment.

# Pure MPI example

This example shows to run a pure MPI application.

The program prints some basic information about the workers.
Then, it calculates an approximate value for pi.

```
kubectl create -f pi.yaml
```

We added Yunikorn labels to the Pi example to demonstrate using the yunikorn scheduler.