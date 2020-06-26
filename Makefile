#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
REPO=github.com/apache/incubator-yunikorn-k8shim/pkg

# Version parameters
DATE=$(shell date +%FT%T%z)
ifeq ($(VERSION),)
VERSION := latest
endif

# Kubeconfig
ifeq ($(KUBECONFIG),)
KUBECONFIG := $(HOME)/.kube/config
endif

# Image build parameters
# This tag of the image must be changed when pushed to a public repository.
ifeq ($(REGISTRY),)
REGISTRY := apache
endif

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

.PHONY: lint
lint:
	@echo "running golangci-lint"
	@lintBin=$$(go env GOPATH)/bin/golangci-lint ; \
	if [ ! -f "$${lintBin}" ]; then \
		lintBin=$$(echo ./bin/golangci-lint) ; \
		if [ ! -f "$${lintBin}" ]; then \
			echo "golangci-lint executable not found" ; \
			exit 1; \
		fi \
	fi ; \
	headSHA=$$(git rev-parse --short=12 origin/HEAD) ; \
	$${lintBin} run --new-from-rev=$${headSHA}

.PHONY: license-check
license-check:
	@echo "checking license header"
	@licRes=$$(grep -Lr --include=*.{go,sh,md,yaml,yml,mod} "Licensed to the Apache Software Foundation" .) ; \
	if [ -n "$${licRes}" ]; then \
		echo "following files have incorrect license header:\n$${licRes}" ; \
		exit 1; \
	fi

.PHONY: run
run: build
	@echo "running scheduler locally"
	@cp ${LOCAL_CONF}/${CONF_FILE} ${RELEASE_BIN_DIR}
	cd ${RELEASE_BIN_DIR} && ./${BINARY} -kubeConfig=$(KUBECONFIG) -interval=1s \
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
	@mkdir -p ./deployments/image/configmap/admission-controller-init-scripts
	@cp -r ./deployments/admission-controllers/scheduler/*  deployments/image/configmap/admission-controller-init-scripts/
	@sed -i -e 's@\$${REGISTRY}@'"${REGISTRY}"'@g' deployments/image/configmap/admission-controller-init-scripts/templates/server.yaml.template
	@sed -i -e 's@\$${VERSION}@'"${VERSION}"'@g' deployments/image/configmap/admission-controller-init-scripts/templates/server.yaml.template
	@sed -i'.bkp' 's/clusterVersion=.*"/clusterVersion=${VERSION}"/' deployments/image/configmap/Dockerfile
	@coreSHA=$$(go list -m "github.com/apache/incubator-yunikorn-core" | cut -d "-" -f5) ; \
	siSHA=$$(go list -m "github.com/apache/incubator-yunikorn-scheduler-interface" | cut -d "-" -f6) ; \
	shimSHA=$$(git rev-parse --short=12 HEAD) ; \
	docker build ./deployments/image/configmap -t ${REGISTRY}/yunikorn:scheduler-${VERSION} \
	--label "yunikorn-core-revision=$${coreSHA}" \
	--label "yunikorn-scheduler-interface-revision=$${siSHA}" \
	--label "yunikorn-k8shim-revision=$${shimSHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}"
	@mv -f ./deployments/image/configmap/Dockerfile.bkp ./deployments/image/configmap/Dockerfile
	@rm -f ./deployments/image/configmap/${BINARY}
	@rm -rf ./deployments/image/configmap/admission-controller-init-scripts/

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
	docker build ./deployments/image/admission -t ${REGISTRY}/yunikorn:admission-${VERSION}
	@rm -f ./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Build all images based on the production ready version
.PHONY: image
image: sched_image adm_image

.PHONY: push
push: image
	@echo "push docker images"
	echo "${DOCKER_PASSWORD}" | docker login -u "${DOCKER_USERNAME}" --password-stdin
	docker push ${REGISTRY}/yunikorn:scheduler-${VERSION}
	docker push ${REGISTRY}/yunikorn:admission-${VERSION}

# Run the tests after building
.PHONY: test
test: clean
	@echo "running unit tests"
	go test ./pkg/... -cover -race -tags deadlock -coverprofile=coverage.txt -covermode=atomic
	go vet $(REPO)...

# Simple clean of generated files only (no local cleanup).
.PHONY: clean
clean:
	@echo "cleaning up caches and output"
	go clean -cache -testcache -r -x ./... 2>&1 >/dev/null
	rm -rf ${OUTPUT} ${CONF_FILE} ${BINARY} \
	./deployments/image/configmap/${BINARY} \
	./deployments/image/configmap/${CONF_FILE} \
	./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Run the e2e tests, this assumes yunikorn is running under yunikorn-ns namespace
.PHONY: e2e_test
e2e_test:
	@echo "running e2e tests"
	cd ./test/e2e && ginkgo -r -v -timeout=2h -- -yk-namespace "yunikorn" -kube-config $(KUBECONFIG)
