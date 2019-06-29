#
# Copyright 2019 Cloudera, Inc.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Check if this is at least GO 1.11 for Go Modules
GO_VERSION := $(shell go version | awk '$$3 ~ /go1.(10|0-9])/ {print $$3}')
ifdef GO_VERSION
$(error Build requires go 1.11 or later)
endif

BINARY=k8s_yunikorn_scheduler
OUTPUT=_output
BIN_DIR=bin
RELEASE_BIN_DIR=${OUTPUT}/bin
LOCAL_CONF=conf
CONF_FILE=queues.yaml
REPO=github.com/cloudera/k8s-shim/pkg

IMAGE_TAG=yunikorn/scheduler-core
IMAGE_VERSION=0.3.5
DATE=$(shell date +%FT%T%z)

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

init:
	mkdir -p ${BIN_DIR}
	mkdir -p ${RELEASE_BIN_DIR}

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
	docker build ./deployments/image/configmap -t ${IMAGE_TAG}:0.3.5
	docker push ${IMAGE_TAG}:0.3.5
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

