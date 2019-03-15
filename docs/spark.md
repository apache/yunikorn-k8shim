# Spark on Kubernetes

Before these steps, first refer to [README](../README.md) to launch yunikorn-scheduler on Kubernetes.

### Steps

1. Get latest spark from github (only latest code supports to specify pod template), URL: https://github.com/apache/spark

2. Build spark with Kubernetes support

```
./build/mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.0 -Phive -Pkubernetes -Phive-thriftserver -DskipTests package
```

3. Run spark submit
```
./bin/spark-submit --master k8s://http://localhost:8001 --deploy-mode cluster --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.container.image=sunilgovind/spark:my-tag \
  --conf spark.kubernetes.driver.podTemplateFile=/path/to/driver.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=/path/to/executor.yaml \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0-SNAPSHOT.jar
```

### Pod templates:

[driver.yaml](../deployments/spark/driver.yaml)

[executor.yaml](../deployments/spark/exectuor.yaml)

Note, following labels must be set in order to be scheduled with yunikorn-scheduler:

* jobId: spark-00001
* queue: queue_a
* schedulerName: unity-scheduler
