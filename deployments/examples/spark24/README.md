# Spark on K8s with YuniKorn


1. Download a proper version Spark tarball from [this link](https://spark.apache.org/downloads.html).

2. Extract the tarball to local workspace.

3. Go to the extracted directory, build a docker image for Spark, e.g

   `./bin/docker-image-tool.sh -r yunikorn -t 2.4.4 build`

   the images

   ```shell script
   REPOSITORY                                               TAG                   IMAGE ID            CREATED             SIZE
   yunikorn/spark-r                                         2.4.4                 d09beef3c67f        6 minutes ago       759MB
   yunikorn/spark-py                                        2.4.4                 bd51b0362f1f        6 minutes ago       465MB
   yunikorn/spark                                           2.4.4                 bef3218b4621        6 minutes ago       373MB
   ```

   Note, you will need to push the docker images to a repository if you want to run Spark on a remote K8s cluster.

4. Please refer to [Spark on K8s doc](https://spark.apache.org/docs/latest/running-on-kubernetes.html). Run spark-submit:

    ```shell script
    # spark submit command
    ./bin/spark-submit \
       --master k8s://http://localhost:8001 --deploy-mode cluster --name spark-pi \
       --class org.apache.spark.examples.SparkPi \
       --conf spark.executor.instances=1 \
       --conf spark.kubernetes.container.image=yunikorn/spark:2.4.4 \
       --conf spark.kubernetes.driver.label.queue="root.sandbox" \
       --conf spark.kubernetes.driver.label.queue="root.sandbox" \
       --conf spark.kubernetes.executor.label.queue="root.sandbox" \
       local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar
    ```

    Tips, to find out the name of the example jar:
    ```shell script
    ls -R | grep example | grep jar
    ```