FROM alpine:latest
ADD k8s_yunikorn_scheduler /k8s_yunikorn_scheduler
ENTRYPOINT ["/k8s_yunikorn_scheduler"]
CMD ["-logtostderr=true", "-v=5", "-clusterid=mycluster", "-clusterversion=0.1"]