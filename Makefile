BINARY=unity_scheduler
OUTPUT=_output
BIN_DIR=bin
RELEASE_BIN_DIR=${OUTPUT}/bin
LOCAL_CONF=conf
CONF_FILE=queues.yaml

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
	cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image
	cp ${LOCAL_CONF}/${CONF_FILE} ./deployments/image
	GOOS=linux
	docker build ./deployments/image -t ${IMAGE_TAG}:${IMAGE_VERSION}
	docker push ${IMAGE_TAG}:${IMAGE_VERSION}
	rm -f ./deployments/image/${BINARY}
	rm -f ./deployments/image/${CONF_FILE}

run: build
	cp ${LOCAL_CONF}/${CONF_FILE} .
	./${BINARY} --logtostderr=true -v=5 -kubeconfig=/Users/wyang/.kube/config
	

clean:
	rm -rf ${OUTPUT}
	rm -rf ${BIN_DIR}
	rm -f ${CONF_FILE} ${BINARY}
	rm -f ./deployments/image/${BINARY}
	rm -f ./deployments/image/${CONF_FILE}

