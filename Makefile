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

# Make sure we are in the same directory as the Makefile
BASE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

BINARY=k8s_yunikorn_scheduler
OUTPUT=_output
RELEASE_BIN_DIR=${OUTPUT}/bin
ADMISSION_CONTROLLER_BIN_DIR=${OUTPUT}/admission-controllers/
POD_ADMISSION_CONTROLLER_BINARY=scheduler-admission-controller
LOCAL_CONF=conf
CONF_FILE=queues.yaml
REPO=github.com/cloudera/yunikorn-k8shim/pkg

# Version parameters
DATE=$(shell date +%FT%T%z)
ifeq ($(VERSION),)
VERSION := latest
endif

# Image build parameters
# This tag of the image must be changed when pushed to a public repository.
ifeq ($(TAG),)
TAG := yunikorn/yunikorn-scheduler-k8s
endif

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

.PHONY: common-check-license
common-check-license:
	@echo "checking license header"
	@licRes=$$(grep -Lr --include="*.go" "Copyright 20[1-2][0-9] Cloudera" .) ; \
	if [ -n "$${licRes}" ]; then \
		echo "following files have incorrect license header:\n$${licRes}" ; \
		exit 1; \
	fi

.PHONY: run
run: build
	@echo "running scheduler locally"
	@cp ${LOCAL_CONF}/${CONF_FILE} ${RELEASE_BIN_DIR}
	cd ${RELEASE_BIN_DIR} && ./${BINARY} -kubeConfig=$(HOME)/.kube/config -interval=1s \
	-clusterId=mycluster -clusterVersion=${VERSION} -name=yunikorn -policyGroup=queues \
	-logEncoding=console -logLevel=-1

# Create output directories
.PHONY: init
init:
	mkdir -p ${RELEASE_BIN_DIR}
	mkdir -p ${ADMISSION_CONTROLLER_BIN_DIR}

# Build scheduler binary for dev and test
.PHONY: build
build: init
	@echo "building scheduler binary"
	go build -o=${RELEASE_BIN_DIR}/${BINARY} -race -ldflags \
	'-X main.version=${VERSION} -X main.date=${DATE}' \
    ./pkg/shim/
	@chmod +x ${RELEASE_BIN_DIR}/${BINARY}

# Build scheduler binary in a production ready version
.PHONY: scheduler
scheduler: init
	@echo "building binary for scheduler docker image"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -a -o=${RELEASE_BIN_DIR}/${BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	./pkg/shim/

# Build a scheduler image based on the production ready version
.PHONY: sched_image
sched_image: scheduler
	@echo "building scheduler docker image"
	@cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image/configmap
	@sed -i'.bkp' 's/clusterVersion=.*"/clusterVersion=${VERSION}"/' deployments/image/configmap/Dockerfile
	@coreSHA=$$(go list -m "github.com/cloudera/yunikorn-core" | cut -d "-" -f4) ; \
	siSHA=$$(go list -m "github.com/cloudera/yunikorn-scheduler-interface" | cut -d "-" -f5) ; \
	shimSHA=$$(git rev-parse --short=12 HEAD) ; \
	docker build ./deployments/image/configmap -t ${TAG}:${VERSION} \
	--label "yunikorn-core-revision=$${coreSHA}" \
	--label "yunikorn-scheduler-interface-revision=$${siSHA}" \
	--label "yunikorn-k8shim-revision=$${shimSHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}"
	@mv -f deployments/image/configmap/Dockerfile.bkp deployments/image/configmap/Dockerfile
	@rm -f ./deployments/image/configmap/${BINARY}

# Build admission controller binary in a production ready version
.PHONY: admission
admission: init
	@echo "building admission controller binary"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -a -o=${ADMISSION_CONTROLLER_BIN_DIR}/${POD_ADMISSION_CONTROLLER_BINARY} -ldflags \
    '-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
    -tags netgo -installsuffix netgo \
    ./pkg/plugin/admissioncontrollers/webhook

# Build an admission controller image based on the production ready version
.PHONY: adm_image
adm_image: admission
	@echo "building admission controller docker images"
	@cp ${ADMISSION_CONTROLLER_BIN_DIR}/${POD_ADMISSION_CONTROLLER_BINARY} ./deployments/image/admission
	docker build ./deployments/image/admission -t yunikorn/scheduler-admission-controller:${VERSION}
	@rm -f ./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Build all images based on the production ready version
.PHONY: image
image: sched_image adm_image

# Run the tests after building
.PHONY: test
test:
	@echo "running unit tests"
	go test ./... -cover -race -tags deadlock
	go vet $(REPO)...

# Simple clean of generated files only (no local cleanup).
.PHONY: clean
clean:
	go clean -r -x ./...
	rm -rf ${OUTPUT} ${CONF_FILE} ${BINARY} \
	./deployments/image/file/${BINARY} \
	./deployments/image/file/${CONF_FILE} \
	./deployments/image/configmap/${BINARY} \
	./deployments/image/configmap/${CONF_FILE} \
	./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}
