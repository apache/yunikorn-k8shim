FROM alpine:latest

# admission controller bundles
RUN apk add curl
RUN apk add jq
RUN apk add --update openssl
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.14.7/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl
COPY admission-controller-init-scripts/admission_util.sh /
COPY admission-controller-init-scripts/configs.properties /
COPY admission-controller-init-scripts/generate-signed-ca.sh /
COPY admission-controller-init-scripts/server.yaml.template /
RUN chmod +x /admission_util.sh
RUN chmod +x /generate-signed-ca.sh

# scheduler binary
ADD k8s_yunikorn_scheduler /k8s_yunikorn_scheduler
ENTRYPOINT ["/k8s_yunikorn_scheduler"]
CMD ["-logEncoding=console", "-logLevel=-1", "-clusterId=mycluster", "-clusterVersion=latest"]
