FROM scratch
ADD k8s_yunikorn_scheduler /k8s_yunikorn_scheduler
ADD queues.yaml /queues.yaml
ENTRYPOINT ["/k8s_yunikorn_scheduler"]
CMD ["-logEncoding=console", "-logLevel=-1", "-clusterId=mycluster", "-clusterVersion=latest"]