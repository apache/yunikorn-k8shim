BINARY=k8s_yunikorn_scheduler
OUTPUT=_output
BIN_DIR=bin
RELEASE_BIN_DIR=${OUTPUT}/bin
LOCAL_CONF=conf
CONF_FILE=queues.yaml
REPO=github.infra.cloudera.com/yunikorn/k8s-shim/pkg

IMAGE_TAG=yunikorn/scheduler-core
IMAGE_VERSION=0.1.0
DATE=$(shell date +%FT%T%z)

init:
	mkdir -p ${BIN_DIR}
	mkdir -p ${RELEASE_BIN_DIR}
	if [ ! -d ./vendor ]; then dep ensure; fi

build: init
	go build -o=${BINARY} --ldflags '-X main.version=${IMAGE_VERSION} -X main.date=${DATE}' ./pkg/scheduler/

build_image: init
	GOOS=linux GOARCH=amd64 \
	go build -a \
	--ldflags '-extldflags "-static" -X main.version=${IMAGE_VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	-o=${RELEASE_BIN_DIR}/${BINARY} \
	./pkg/scheduler/

image: build_image
	cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image/file
	cp ${LOCAL_CONF}/${CONF_FILE} ./deployments/image/file
	GOOS=linux
	docker build ./deployments/image/file -t ${IMAGE_TAG}:${IMAGE_VERSION}
	docker push ${IMAGE_TAG}:${IMAGE_VERSION}
	rm -f ./deployments/image/file/${BINARY}
	rm -f ./deployments/image/file/${CONF_FILE}

image2: build_image
	cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image/configmap
	GOOS=linux
	docker build ./deployments/image/configmap -t ${IMAGE_TAG}:0.1.10
	docker push ${IMAGE_TAG}:0.1.10
	rm -f ./deployments/image/configmap/${BINARY}

run: build
	cp ${LOCAL_CONF}/${CONF_FILE} .
	./${BINARY} -logtostderr=true -v=5 -kubeconfig=$(HOME)/.kube/config -interval=1 \
	-clusterid=mycluster -clusterversion=0.1 -name=yunikorn -policygroup=queues

test:
	go test ./... -cover
	go vet $(REPO)...

clean:
	rm -rf ${OUTPUT}
	rm -rf ${BIN_DIR}
	rm -f ${CONF_FILE} ${BINARY}
	rm -f ./deployments/image/file/${BINARY}
	rm -f ./deployments/image/file/${CONF_FILE}
	rm -f ./deployments/image/configmap/${BINARY}
	rm -f ./deployments/image/configmap/${CONF_FILE}

