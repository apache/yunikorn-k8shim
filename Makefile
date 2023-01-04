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

# Check if this GO tools version used is at least the version of go specified in
# the go.mod file. The version in go.mod should be in sync with other repos.
GO_VERSION := $(shell go version | awk '{print substr($$3, 3, 10)}')
MOD_VERSION := $(shell cat .go_version) 

GM := $(word 1,$(subst ., ,$(GO_VERSION)))
MM := $(word 1,$(subst ., ,$(MOD_VERSION)))
FAIL := $(shell if [ $(GM) -lt $(MM) ]; then echo MAJOR; fi)
ifdef FAIL
$(error Build should be run with at least go $(MOD_VERSION) or later, found $(GO_VERSION))
endif
GM := $(word 2,$(subst ., ,$(GO_VERSION)))
MM := $(word 2,$(subst ., ,$(MOD_VERSION)))
FAIL := $(shell if [ $(GM) -lt $(MM) ]; then echo MINOR; fi)
ifdef FAIL
$(error Build should be run with at least go $(MOD_VERSION) or later, found $(GO_VERSION))
endif

# Make sure we are in the same directory as the Makefile
BASE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

BINARY=k8s_yunikorn_scheduler
PLUGIN_BINARY=kube-scheduler
OUTPUT=_output
DEV_BIN_DIR=${OUTPUT}/dev
RELEASE_BIN_DIR=${OUTPUT}/bin
ADMISSION_CONTROLLER_BIN_DIR=${OUTPUT}/admission-controllers/
POD_ADMISSION_CONTROLLER_BINARY=scheduler-admission-controller
GANG_BIN_DIR=${OUTPUT}/gang
GANG_CLIENT_BINARY=simulation-gang-worker
GANG_SERVER_BINARY=simulation-gang-coordinator
REPO=github.com/apache/yunikorn-k8shim/pkg

# Version parameters
DATE=$(shell date +%FT%T%z)
ifeq ($(VERSION),)
VERSION := latest
endif

# Kernel (OS) Name
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')

# Allow architecture to be overwritten
ifeq ($(HOST_ARCH),)
HOST_ARCH := $(shell uname -m)
endif

# Build architecture settings:
# EXEC_ARCH defines the architecture of the executables that gets compiled
# DOCKER_ARCH defines the architecture of the docker image
# Both vars must be set, an unknown architecture defaults to amd64
ifeq (x86_64, $(HOST_ARCH))
EXEC_ARCH := amd64
DOCKER_ARCH := amd64
else ifeq (i386, $(HOST_ARCH))
EXEC_ARCH := 386
DOCKER_ARCH := i386
else ifneq (,$(filter $(HOST_ARCH), arm64 aarch64))
EXEC_ARCH := arm64
DOCKER_ARCH := arm64v8
else ifeq (armv7l, $(HOST_ARCH))
EXEC_ARCH := arm
DOCKER_ARCH := arm32v7
else
$(info Unknown architecture "${HOST_ARCH}" defaulting to: amd64)
EXEC_ARCH := amd64
DOCKER_ARCH := amd64
endif

# Image hashes
CORE_SHA=$(shell go list -m "github.com/apache/yunikorn-core" | cut -d "-" -f4)
SI_SHA=$(shell go list -m "github.com/apache/yunikorn-scheduler-interface" | cut -d "-" -f5)
SHIM_SHA=$(shell git rev-parse --short=12 HEAD)

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

LINTBASE := $(shell go env GOPATH)/bin
LINTBIN  := $(LINTBASE)/golangci-lint
$(LINTBIN):
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LINTBASE) v1.50.1
	stat $@ > /dev/null 2>&1

# Run lint against the previous commit for PR and branch build
# In dev setup look at all changes on top of master
.PHONY: lint
lint: $(LINTBIN)
	@echo "running golangci-lint"
	git symbolic-ref -q HEAD && REV="origin/HEAD" || REV="HEAD^" ; \
	headSHA=$$(git rev-parse --short=12 $${REV}) ; \
	echo "checking against commit sha $${headSHA}" ; \
	${LINTBIN} run --new-from-rev=$${headSHA}

.PHONY: install_shellcheck
SHELLCHECK_PATH := "$(BASE_DIR)shellcheck"
SHELLCHECK_VERSION := "v0.8.0"
SHELLCHECK_ARCHIVE := "shellcheck-$(SHELLCHECK_VERSION).$(OS).$(HOST_ARCH).tar.xz"
install_shellcheck:
	@echo ${SHELLCHECK_PATH}
	@if command -v "shellcheck" &> /dev/null; then \
		exit 0 ; \
	elif [ -x ${SHELLCHECK_PATH} ]; then \
		exit 0 ; \
	elif [ "${HOST_ARCH}" = "arm64" ]; then \
		echo "Unsupported architecture 'arm64'" \
		exit 1 ; \
	else \
		curl -sSfL https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/${SHELLCHECK_ARCHIVE} | tar -x -J --strip-components=1 shellcheck-${SHELLCHECK_VERSION}/shellcheck ; \
	fi

# Check scripts
.PHONY: check_scripts
ALLSCRIPTS := $(shell find . -name '*.sh')
check_scripts: install_shellcheck
	@echo "running shellcheck"
	@if command -v "shellcheck" &> /dev/null; then \
		shellcheck ${ALLSCRIPTS} ; \
	elif [ -x ${SHELLCHECK_PATH} ]; then \
		${SHELLCHECK_PATH} ${ALLSCRIPTS} ; \
	else \
		echo "shellcheck not found: failing target" \
		exit 1; \
	fi

.PHONY: license-check
# This is a bit convoluted but using a recursive grep on linux fails to write anything when run
# from the Makefile. That caused the pull-request license check run from the github action to
# always pass. The syntax for find is slightly different too but that at least works in a similar
# way on both Mac and Linux. Excluding all .git* files from the checks.
license-check:
	@echo "checking license headers:"
ifeq (darwin,$(OS))
	$(shell find -E . -not -path "./.git*" -regex ".*\.(go|sh|md|conf|yaml|yml|html|mod)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > LICRES)
else
	$(shell find . -not -path "./.git*" -regex ".*\.\(go\|sh\|md\|conf\|yaml\|yml\|html\|mod\)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > LICRES)
endif
	@if [ -s LICRES ]; then \
		echo "following files are missing license header:" ; \
		cat LICRES ; \
		rm -f LICRES ; \
		exit 1; \
	fi ; \
	rm -f LICRES
	@echo "  all OK"

# Check that we use pseudo versions in master
.PHONY: pseudo
BRANCH := $(shell git branch --show-current)
CORE_REF := $(shell go list -m -f '{{ .Version }}' github.com/apache/yunikorn-core)
CORE_MATCH := $(shell expr "${CORE_REF}" : "v0.0.0-")
SI_REF := $(shell go list -m -f '{{ .Version }}' github.com/apache/yunikorn-scheduler-interface)
SI_MATCH := $(shell expr "${SI_REF}" : "v0.0.0-")
pseudo:
	@echo "pseudo version check"
	@if [ "${BRANCH}" = "master" ]; then \
		if [ ${SI_MATCH} -ne 7 ] || [ ${CORE_MATCH} -ne 7 ]; then \
			echo "YuniKorn references MUST all be pseudo versions:" ; \
			echo " core ref: ${CORE_REF}" ; \
			echo " SI ref:   ${SI_REF}" ; \
			exit 1; \
		fi \
	fi
	@echo "  all OK"

.PHONY: run
run: build
	@echo "running scheduler locally"
	cd ${DEV_BIN_DIR} && ./${BINARY} -kubeConfig=$(KUBECONFIG) -interval=1s \
	-clusterId=mycluster -clusterVersion=${VERSION} -policyGroup=queues \
	-logEncoding=console -logLevel=-1

.PHONY: run_plugin
run_plugin: build_plugin
	@echo "running scheduler plugin locally"
	cd ${DEV_BIN_DIR} && \
	  ./${PLUGIN_BINARY} \
	    --address=0.0.0.0 \
	    --leader-elect=false \
	    --config=../../conf/scheduler-config-local.yaml \
	    -v=2 \
	    --yk-scheduler-name=yunikorn \
	    --yk-kube-config=$(KUBECONFIG) \
	    --yk-cluster-id=yk \
	    --yk-scheduling-interval=1s \
	    --yk-log-level=1

# Create output directories
.PHONY: init
init:
	mkdir -p ${DEV_BIN_DIR}
	mkdir -p ${RELEASE_BIN_DIR}
	mkdir -p ${ADMISSION_CONTROLLER_BIN_DIR}
	./scripts/plugin-conf-gen.sh $(KUBECONFIG) "conf/scheduler-config.yaml" "conf/scheduler-config-local.yaml"

# Build scheduler binary for dev and test
.PHONY: build
build: init
	@echo "building scheduler binary"
	go build -o=${DEV_BIN_DIR}/${BINARY} -race -ldflags \
	'-X main.version=${VERSION} -X main.date=${DATE}' \
	./pkg/cmd/shim/
	@chmod +x ${DEV_BIN_DIR}/${BINARY}

.PHONY: build_plugin
build_plugin: init
	@echo "building scheduler plugin binary"
	go build -o=${DEV_BIN_DIR}/${PLUGIN_BINARY} -race -ldflags \
	'-X main.version=${VERSION} -X main.date=${DATE}' \
	./pkg/cmd/schedulerplugin/
	@chmod +x ${DEV_BIN_DIR}/${PLUGIN_BINARY}

# Build scheduler binary in a production ready version
.PHONY: scheduler
scheduler: init
	@echo "building binary for scheduler docker image"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${RELEASE_BIN_DIR}/${BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	./pkg/cmd/shim/

# Build plugin binary in a production ready version
.PHONY: plugin
plugin: init
	@echo "building binary for plugin docker image"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${RELEASE_BIN_DIR}/${PLUGIN_BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	./pkg/cmd/schedulerplugin/
	
# Build a scheduler image based on the production ready version
.PHONY: sched_image
sched_image: scheduler
	@echo "building scheduler docker image"
	@cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image/configmap
	@sed -i'.bkp' 's/clusterVersion=.*"/clusterVersion=${VERSION}"/' deployments/image/configmap/Dockerfile
	docker build ./deployments/image/configmap -t ${REGISTRY}/yunikorn:scheduler-${DOCKER_ARCH}-${VERSION} \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	@mv -f ./deployments/image/configmap/Dockerfile.bkp ./deployments/image/configmap/Dockerfile
	@rm -f ./deployments/image/configmap/${BINARY}

# Build a plugin image based on the production ready version
.PHONY: plugin_image
plugin_image: plugin
	@echo "building plugin docker image"
	@cp ${RELEASE_BIN_DIR}/${PLUGIN_BINARY} ./deployments/image/plugin
	@cp conf/scheduler-config.yaml ./deployments/image/plugin/scheduler-config.yaml
	@sed -i'.bkp' 's/clusterVersion=.*"/clusterVersion=${VERSION}"/' deployments/image/plugin/Dockerfile
	docker build ./deployments/image/plugin -t ${REGISTRY}/yunikorn:scheduler-plugin-${DOCKER_ARCH}-${VERSION} \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	@mv -f ./deployments/image/plugin/Dockerfile.bkp ./deployments/image/plugin/Dockerfile
	@rm -f ./deployments/image/plugin/${PLUGIN_BINARY}
	@rm -f ./deployments/image/plugin/scheduler-config.yaml

# Build admission controller binary in a production ready version
.PHONY: admission
admission: init
	@echo "building admission controller binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${ADMISSION_CONTROLLER_BIN_DIR}/${POD_ADMISSION_CONTROLLER_BINARY} -ldflags \
    '-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
    -tags netgo -installsuffix netgo \
    ./pkg/cmd/admissioncontroller

# Build an admission controller image based on the production ready version
.PHONY: adm_image
adm_image: admission
	@echo "building admission controller docker image"
	@cp ${ADMISSION_CONTROLLER_BIN_DIR}/${POD_ADMISSION_CONTROLLER_BINARY} ./deployments/image/admission
	docker build ./deployments/image/admission -t ${REGISTRY}/yunikorn:admission-${DOCKER_ARCH}-${VERSION} \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	@rm -f ./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Build gang web server and client binary in a production ready version
.PHONY: simulation
simulation:
	@echo "building gang web client binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${GANG_BIN_DIR}/${GANG_CLIENT_BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	./pkg/simulation/gang/gangclient
	@echo "building gang web server binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${GANG_BIN_DIR}/${GANG_SERVER_BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE}' \
	-tags netgo -installsuffix netgo \
	./pkg/simulation/gang/webserver

# Build gang test images based on the production ready version
.PHONY: simulation_image
simulation_image: simulation
	@echo "building gang test docker images"
	@cp ${GANG_BIN_DIR}/${GANG_CLIENT_BINARY} ./deployments/image/gang/gangclient
	@cp ${GANG_BIN_DIR}/${GANG_SERVER_BINARY} ./deployments/image/gang/webserver
	docker build ./deployments/image/gang/gangclient -t ${REGISTRY}/yunikorn:simulation-gang-worker-${VERSION} \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	docker build ./deployments/image/gang/webserver -t ${REGISTRY}/yunikorn:simulation-gang-coordinator-${VERSION} \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	@rm -f ./deployments/image/gang/gangclient/${GANG_CLIENT_BINARY}
	@rm -f ./deployments/image/gang/webserver/${GANG_SERVER_BINARY}

# Build all images based on the production ready version
.PHONY: image
image: sched_image plugin_image adm_image

# Build a web server image ONLY to be used in e2e tests
.PHONY: webtest_image
webtest_image:
	@echo "building web server image for automated e2e tests"
	docker build ./deployments/image/webtest -t ${REGISTRY}/yunikorn:webtest-${DOCKER_ARCH}-${VERSION} \
	--label "yunikorn-e2e-web-image" \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/

#Generate the CRD code with code-generator (release-1.14)

# If you want to re-run the code-generator to generate code,
# Please make sure the directory structure must be the example.
# ex: github.com/apache/yunikorn-k8shim
# Also you need to set you GOPATH environmental variables first.
# If GOPATH is empty, we will set it to "$HOME/go".
.PHONY: code_gen
code_gen:
	@echo "Generating CRD code"
	./scripts/update-codegen.sh

# Run the tests after building
.PHONY: test
test: clean
	@echo "running unit tests"
	go test ./pkg/... -cover -race -tags deadlock -coverprofile=coverage.txt -covermode=atomic
	go vet $(REPO)...

# Generate FSM graphs (dot/png)
.PHONY: fsm_graph
fsm_graph: clean
	@echo "generating FSM graphs"
	go test -tags graphviz -run 'Test.*FsmGraph' ./pkg/shim ./pkg/cache
	scripts/generate-fsm-graph-images.sh

# Simple clean of generated files only (no local cleanup).
.PHONY: clean
clean:
	@echo "cleaning up caches and output"
	go clean -cache -testcache -r -x ./... 2>&1 >/dev/null
	rm -rf ${OUTPUT} ${BINARY} \
	./deployments/image/configmap/${BINARY} \
	./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Print arch variables
.PHONY: arch
arch:
	@echo DOCKER_ARCH=$(DOCKER_ARCH)
	@echo EXEC_ARCH=$(EXEC_ARCH)

# Run the e2e tests, this assumes yunikorn is running under yunikorn namespace
.PHONY: e2e_test
e2e_test:
	@echo "running e2e tests"
	cd ./test/e2e && ginkgo -r -v -timeout=2h -- -yk-namespace "yunikorn" -kube-config $(KUBECONFIG)
