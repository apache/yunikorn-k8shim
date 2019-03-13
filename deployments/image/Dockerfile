FROM scratch
ADD unity_scheduler /unity_scheduler
ADD queues.yaml /queues.yaml
ENTRYPOINT ["/unity_scheduler"]
CMD ["-logtostderr=true", "-v=5"]