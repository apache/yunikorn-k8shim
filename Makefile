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
TEST_SERVER_BINARY=web-test-server
TOOLS_DIR=tools
REPO=github.com/apache/yunikorn-k8shim/pkg

# Version parameters
DATE=$(shell date +%FT%T%z)
ifeq ($(VERSION),)
VERSION := latest
endif

# Test selection
ifeq ($(E2E_TEST),)
E2E_TEST := "*"
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
DOCKER_ARCH := arm64
else ifeq (armv7l, $(HOST_ARCH))
EXEC_ARCH := arm
DOCKER_ARCH := arm32v7
else
$(info Unknown architecture "${HOST_ARCH}" defaulting to: amd64)
EXEC_ARCH := amd64
DOCKER_ARCH := amd64
endif

# shellcheck
SHELLCHECK_VERSION=v0.9.0
SHELLCHECK_BIN=${TOOLS_DIR}/shellcheck
SHELLCHECK_ARCHIVE := shellcheck-$(SHELLCHECK_VERSION).$(OS).$(HOST_ARCH).tar.xz
ifeq (darwin, $(OS))
ifeq (arm64, $(HOST_ARCH))
SHELLCHECK_ARCHIVE := shellcheck-$(SHELLCHECK_VERSION).$(OS).x86_64.tar.xz
endif
else ifeq (linux, $(OS))
ifeq (armv7l, $(HOST_ARCH))
SHELLCHECK_ARCHIVE := shellcheck-$(SHELLCHECK_VERSION).$(OS).armv6hf.tar.xz
endif
endif

# golangci-lint
GOLANGCI_LINT_VERSION=1.53.3
GOLANGCI_LINT_BIN=$(TOOLS_DIR)/golangci-lint
GOLANGCI_LINT_ARCHIVE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH).tar.gz
GOLANGCI_LINT_ARCHIVEBASE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH)

# kubectl
KUBECTL_VERSION=v1.27.3
KUBECTL_BIN=$(TOOLS_DIR)/kubectl

# kind
KIND_VERSION=v0.20.0
KIND_BIN=$(TOOLS_DIR)/kind

# helm
HELM_VERSION=v3.12.1
HELM_BIN=$(TOOLS_DIR)/helm
HELM_ARCHIVE=helm-$(HELM_VERSION)-$(OS)-$(EXEC_ARCH).tar.gz
HELM_ARCHIVE_BASE=$(OS)-$(EXEC_ARCH)

# spark
export SPARK_VERSION=3.3.1
export SPARK_HOME=$(BASE_DIR)$(TOOLS_DIR)/spark
export SPARK_SUBMIT_CMD=$(SPARK_HOME)/bin/spark-submit
export SPARK_PYTHON_IMAGE=docker.io/apache/spark-py:v$(SPARK_VERSION)

# Image hashes
CORE_SHA=$(shell go list -f '{{.Version}}' -m "github.com/apache/yunikorn-core" | cut -d "-" -f3)
SI_SHA=$(shell go list -f '{{.Version}}' -m "github.com/apache/yunikorn-scheduler-interface" | cut -d "-" -f3)
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
export ACK_GINKGO_DEPRECATIONS=2.9.0
GO111MODULE := on
export GO111MODULE

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

# Create output directories
.PHONY: init
init:
	mkdir -p ${DEV_BIN_DIR}
	mkdir -p ${RELEASE_BIN_DIR}
	mkdir -p ${ADMISSION_CONTROLLER_BIN_DIR}
	./scripts/plugin-conf-gen.sh $(KUBECONFIG) "conf/scheduler-config.yaml" "conf/scheduler-config-local.yaml"

# Install tools
.PHONY: tools
tools: $(SHELLCHECK_BIN) $(GOLANGCI_LINT_BIN) $(KUBECTL_BIN) $(KIND_BIN) $(HELM_BIN) $(SPARK_SUBMIT_CMD)

# Install shellcheck
$(SHELLCHECK_BIN):
	@echo "installing shellcheck $(SHELLCHECK_VERSION)"
	@mkdir -p "$(TOOLS_DIR)"
	@curl -sSfL "https://github.com/koalaman/shellcheck/releases/download/$(SHELLCHECK_VERSION)/$(SHELLCHECK_ARCHIVE)" \
		| tar -x -J --strip-components=1 -C "$(TOOLS_DIR)" "shellcheck-$(SHELLCHECK_VERSION)/shellcheck"

# Install golangci-lint
$(GOLANGCI_LINT_BIN):
	@echo "installing golangci-lint v$(GOLANGCI_LINT_VERSION)"
	@mkdir -p "$(TOOLS_DIR)"
	@curl -sSfL "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/$(GOLANGCI_LINT_ARCHIVE)" \
		| tar -x -z --strip-components=1 -C "$(TOOLS_DIR)" "$(GOLANGCI_LINT_ARCHIVEBASE)/golangci-lint"

# Install kubectl
$(KUBECTL_BIN):
	@echo "installing kubectl $(KUBECTL_VERSION)"
	@mkdir -p "$(TOOLS_DIR)"
	@curl -sSfL -o "$(KUBECTL_BIN)" \
		"https://storage.googleapis.com/kubernetes-release/release/$(KUBECTL_VERSION)/bin/$(OS)/$(EXEC_ARCH)/kubectl" && \
		chmod +x "$(KUBECTL_BIN)"

# Install kind
$(KIND_BIN):
	@echo "installing kind $(KIND_VERSION)"
	@mkdir -p "$(TOOLS_DIR)"
	@curl -sSfL -o "$(KIND_BIN)" \
		"https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-$(OS)-$(EXEC_ARCH)" && \
		chmod +x "$(KIND_BIN)"

# Install helm
$(HELM_BIN):
	@echo "installing helm $(HELM_VERSION)"
	@mkdir -p "$(TOOLS_DIR)"
	@curl -sSfL "https://get.helm.sh/$(HELM_ARCHIVE)" \
		| tar -x -z --strip-components=1 -C "$(TOOLS_DIR)" "$(HELM_ARCHIVE_BASE)/helm"

# Install spark
$(SPARK_SUBMIT_CMD):
	@echo "installing spark $(SPARK_VERSION)"
	@rm -rf "$(SPARK_HOME)" "$(SPARK_HOME).tmp"
	@mkdir -p "$(SPARK_HOME).tmp"
	@curl -sSfL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
		| tar -x -z --strip-components=1 -C "$(SPARK_HOME).tmp" 
	@mv -f "$(SPARK_HOME).tmp" "$(SPARK_HOME)"

# Run lint against the previous commit for PR and branch build
# In dev setup look at all changes on top of master
.PHONY: lint
lint: $(GOLANGCI_LINT_BIN)
	@echo "running golangci-lint"
	@git symbolic-ref -q HEAD && REV="origin/HEAD" || REV="HEAD^" ; \
	headSHA=$$(git rev-parse --short=12 $${REV}) ; \
	echo "checking against commit sha $${headSHA}" ; \
	"${GOLANGCI_LINT_BIN}" run --new-from-rev=$${headSHA}

# Check scripts
.PHONY: check_scripts
ALLSCRIPTS := $(shell find . -name '*.sh' -not -path './_spark/*' -not -path './tools/*' -not -path './build/*')
check_scripts: $(SHELLCHECK_BIN)
	@echo "running shellcheck"
	@"$(SHELLCHECK_BIN)" ${ALLSCRIPTS}

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
	cd ${DEV_BIN_DIR} && \
	KUBECONFIG="$(KUBECONFIG)" ./${BINARY}

.PHONY: run_plugin
run_plugin: build_plugin
	@echo "running scheduler plugin locally"
	cd ${DEV_BIN_DIR} && \
	KUBECONFIG="$(KUBECONFIG)" \
	./${PLUGIN_BINARY} \
	--bind-address=0.0.0.0 \
	--config=../../conf/scheduler-config-local.yaml \
	-v=2

# Build scheduler binary for dev and test
.PHONY: build
FLAG_PREFIX=github.com/apache/yunikorn-k8shim/pkg/conf
build: init
	@echo "building scheduler binary"
	go build -o=${DEV_BIN_DIR}/${BINARY} -race -ldflags \
	'-X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=false -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	./pkg/cmd/shim/
	@chmod +x ${DEV_BIN_DIR}/${BINARY}

.PHONY: build_plugin
build_plugin: init
	@echo "building scheduler plugin binary"
	go build -o=${DEV_BIN_DIR}/${PLUGIN_BINARY} -race -ldflags \
	'-X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=true -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	./pkg/cmd/schedulerplugin/
	@chmod +x ${DEV_BIN_DIR}/${PLUGIN_BINARY}

# Build scheduler binary in a production ready version
.PHONY: scheduler
scheduler: init
	@echo "building binary for scheduler docker image"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${RELEASE_BIN_DIR}/${BINARY} -trimpath -ldflags \
	'-extldflags "-static" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=false -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	-tags netgo -installsuffix netgo \
	./pkg/cmd/shim/

# Build plugin binary in a production ready version
.PHONY: plugin
plugin: init
	@echo "building binary for plugin docker image"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${RELEASE_BIN_DIR}/${PLUGIN_BINARY} -trimpath -ldflags \
	'-extldflags "-static" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=true -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	-tags netgo -installsuffix netgo \
	./pkg/cmd/schedulerplugin/
	
# Build a scheduler image based on the production ready version
.PHONY: sched_image
sched_image: scheduler
	@echo "building scheduler docker image"
	@cp ${RELEASE_BIN_DIR}/${BINARY} ./deployments/image/configmap
	@sed -i'.bkp' 's/clusterVersion=.*"/clusterVersion=${VERSION}"/' deployments/image/configmap/Dockerfile
	DOCKER_BUILDKIT=1 \
	docker build ./deployments/image/configmap -t ${REGISTRY}/yunikorn:scheduler-${DOCKER_ARCH}-${VERSION} \
	--platform "linux/${DOCKER_ARCH}" \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET}
	@mv -f ./deployments/image/configmap/Dockerfile.bkp ./deployments/image/configmap/Dockerfile
	@rm -f ./deployments/image/configmap/${BINARY}

# Build a plugin image based on the production ready version
.PHONY: plugin_image
plugin_image: plugin
	@echo "building plugin docker image"
	@cp ${RELEASE_BIN_DIR}/${PLUGIN_BINARY} ./deployments/image/plugin
	@cp conf/scheduler-config.yaml ./deployments/image/plugin/scheduler-config.yaml
	@sed -i'.bkp' 's/clusterVersion=.*"/clusterVersion=${VERSION}"/' deployments/image/plugin/Dockerfile
	DOCKER_BUILDKIT=1 \
	docker build ./deployments/image/plugin -t ${REGISTRY}/yunikorn:scheduler-plugin-${DOCKER_ARCH}-${VERSION} \
	--platform "linux/${DOCKER_ARCH}" \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET}
	@mv -f ./deployments/image/plugin/Dockerfile.bkp ./deployments/image/plugin/Dockerfile
	@rm -f ./deployments/image/plugin/${PLUGIN_BINARY}
	@rm -f ./deployments/image/plugin/scheduler-config.yaml

# Build admission controller binary in a production ready version
.PHONY: admission
admission: init
	@echo "building admission controller binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${ADMISSION_CONTROLLER_BIN_DIR}/${POD_ADMISSION_CONTROLLER_BINARY} -trimpath -ldflags \
    '-extldflags "-static" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH}' \
    -tags netgo -installsuffix netgo \
    ./pkg/cmd/admissioncontroller

# Build an admission controller image based on the production ready version
.PHONY: adm_image
adm_image: admission
	@echo "building admission controller docker image"
	@cp ${ADMISSION_CONTROLLER_BIN_DIR}/${POD_ADMISSION_CONTROLLER_BINARY} ./deployments/image/admission
	DOCKER_BUILDKIT=1 \
	docker build ./deployments/image/admission -t ${REGISTRY}/yunikorn:admission-${DOCKER_ARCH}-${VERSION} \
	--platform "linux/${DOCKER_ARCH}" \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET}
	@rm -f ./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Build gang web server and client binary in a production ready version
.PHONY: simulation
simulation:
	@echo "building gang web client binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${GANG_BIN_DIR}/${GANG_CLIENT_BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE} -X main.goVersion=${GO_VERSION} -X main.arch=${EXEC_ARCH}' \
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
	DOCKER_BUILDKIT=1 \
	docker build ./deployments/image/gang/gangclient -t ${REGISTRY}/yunikorn:simulation-gang-worker-${VERSION} \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	DOCKER_BUILDKIT=1 \
	docker build ./deployments/image/gang/webserver -t ${REGISTRY}/yunikorn:simulation-gang-coordinator-${VERSION} \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/
	@rm -f ./deployments/image/gang/gangclient/${GANG_CLIENT_BINARY}
	@rm -f ./deployments/image/gang/webserver/${GANG_SERVER_BINARY}

# Build all images based on the production ready version
.PHONY: image
image: sched_image plugin_image adm_image

# Build a web server image ONLY to be used in e2e tests
.PHONY: webtest_image
webtest_image: build_web_test_server_prod
	@echo "building web server image for automated e2e tests"
	@cp ${RELEASE_BIN_DIR}/${TEST_SERVER_BINARY} ./deployments/image/webtest
	DOCKER_BUILDKIT=1 \
	docker build ./deployments/image/webtest -t ${REGISTRY}/yunikorn:webtest-${DOCKER_ARCH}-${VERSION} \
	--label "yunikorn-e2e-web-image" \
	${QUIET} --build-arg ARCH=${DOCKER_ARCH}/

.PHONY: build_web_test_server_dev
build_web_test_server_dev:
	@echo "building local web server binary"
	go build -o=${DEV_BIN_DIR}/${TEST_SERVER_BINARY} -race -ldflags \
	'-X main.version=${VERSION} -X main.date=${DATE} -X main.goVersion=${GO_VERSION} -X main.arch=${EXEC_ARCH}' \
	./pkg/cmd/webtest/
	@chmod +x ${DEV_BIN_DIR}/${TEST_SERVER_BINARY}

.PHONY: build_web_test_server_prod
build_web_test_server_prod:
	@echo "building web server binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" \
	go build -a -o=${RELEASE_BIN_DIR}/${TEST_SERVER_BINARY} -ldflags \
	'-extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE} -X main.goVersion=${GO_VERSION} -X main.arch=${EXEC_ARCH}' \
	-tags netgo -installsuffix netgo \
	./pkg/cmd/webtest/

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

# Run benchmarks
.PHONY: bench
bench:
	@echo "running benchmarks"
	go test -v -run '^Benchmark' -bench . ./pkg/...

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
	@go clean -cache -testcache -r
	@echo "removing generated files"
	@rm -rf ${OUTPUT} ${BINARY} \
		./deployments/image/configmap/${BINARY} \
		./deployments/image/admission/${POD_ADMISSION_CONTROLLER_BINARY}

# Remove all generated content
.PHONY: distclean
distclean: clean
	@echo "removing tools"
	@rm -rf "${TOOLS_DIR}"

# Print arch variables
.PHONY: arch
arch:
	@echo DOCKER_ARCH=$(DOCKER_ARCH)
	@echo EXEC_ARCH=$(EXEC_ARCH)

# Run the e2e tests, this assumes yunikorn is running under yunikorn namespace
.PHONY: e2e_test
e2e_test: tools
	@echo "running e2e tests"
	cd ./test/e2e && \
	ginkgo -r "${E2E_TEST}" -v -keep-going -- -yk-namespace "yunikorn" -kube-config $(KUBECONFIG)
