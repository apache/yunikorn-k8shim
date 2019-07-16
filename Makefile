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
RELEASE_BIN_DIR=${OUTPUT}/bin
LOCAL_CONF=conf
CONF_FILE=queues.yaml
REPO=github.com/cloudera/yunikorn-k8shim/pkg

# Version parameters
DATE=$(shell date +%FT%T%z)
ifeq ($(VERSION),)
VERSION := 0.1.0
endif

# Image build parameters
ifeq ($(TAG),)
TAG := yunikorn/yunikorn-scheduler-k8s
endif

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

.PHONY: common-check-license
common-check-license:
	@echo "checking license header"
	@licRes=$$(grep -Lr --include="*.go" "Copyright 20[1-2][0-9] Cloudera" .) ; \
	if [ -n "$${licRes}" ]; then \
		echo "following files have incorrect license header:\n$${licRes}" ; \
		exit 1; \
	fi

.PHONY: init
init:
	mkdir -p ${RELEASE_BIN_DIR}

.PHONY: build
build: init
	@echo "building scheduler binary"
	go build -o=${RELEASE_BIN_DIR}/${BINARY} -race -ldflags \
	'-X main.version=${VERSION} -X main.date=${DATE}' \
	./pkg/scheduler/

.PHONY: build_image
build_image: init
	@echo "building binary for scheduler docker image"
	GOOS=linux GOARCH=amd64 \
	go build -a -o=${RELEASE_BIN_DIR}/${BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	./pkg/scheduler/

.PHONY: image
image: build_image
	@echo "building scheduler docker image"
	cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image/configmap
	@coreSHA=$$(go list -m "github.com/cloudera/yunikorn-core" | cut -d "-" -f4) ; \
	siSHA=$$(go list -m "github.com/cloudera/yunikorn-scheduler-interface" | cut -d "-" -f5) ; \
	shimSHA=$$(git rev-parse --short=12 HEAD) ; \
	docker build ./deployments/image/configmap -t ${TAG}:${VERSION} \
	--label "yunikorn-core-revision=$${coreSHA}" \
	--label "yunikorn-scheduler-interface-revision=$${siSHA}" \
	--label "yunikorn-k8shim-revision=$${shimSHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}"
	rm -f ./deployments/image/configmap/${BINARY}

.PHONY: run
run: build
	@echo "running scheduler locally"
	cp ${LOCAL_CONF}/${CONF_FILE} ${RELEASE_BIN_DIR}
	cd ${RELEASE_BIN_DIR} && ./${BINARY} -kubeConfig=$(HOME)/.kube/config -interval=1 \
	-clusterId=mycluster -clusterVersion=0.1 -name=yunikorn -policyGroup=queues \
	-logEncoding=console -logLevel=-1

.PHONY: test
test:
	@echo "running unit tests"
	go test ./... -cover -race -tags deadlock
	go vet $(REPO)...

.PHONY: clean
clean:
	go clean ./...
	rm -rf ${OUTPUT} ${CONF_FILE} ${BINARY} \
	./deployments/image/file/${BINARY} \
	./deployments/image/file/${CONF_FILE} \
	./deployments/image/configmap/${BINARY} \
	./deployments/image/configmap/${CONF_FILE}