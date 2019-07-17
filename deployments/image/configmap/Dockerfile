FROM alpine:latest
ADD k8s_yunikorn_scheduler /k8s_yunikorn_scheduler
ENTRYPOINT ["/k8s_yunikorn_scheduler"]
CMD ["-logEncoding=console", "-logLevel=-1", "-clusterId=mycluster", "-clusterVersion=latest"]