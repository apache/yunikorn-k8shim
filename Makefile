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

# Go compiler selection
ifeq ($(GO),)
GO := go
endif

GOROOT := $(shell "$(GO)" env GOROOT)
export GOROOT := $(GOROOT)
GO_EXE_PATH := $(GOROOT)/bin

# Check if this GO tools version used is at least the version of go specified in
# the go.mod file. The version in go.mod should be in sync with other repos.
GO_VERSION := $(shell "$(GO)" version | awk '{print substr($$3, 3, 4)}')
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

# Output directories
OUTPUT=build
DEV_BIN_DIR=${OUTPUT}/dev
RELEASE_BIN_DIR=${OUTPUT}/bin
DOCKER_DIR=${OUTPUT}/docker

# Binary names
SCHEDULER_BINARY=yunikorn-scheduler
PLUGIN_BINARY=yunikorn-scheduler-plugin
ADMISSION_CONTROLLER_BINARY=yunikorn-admission-controller
TEST_SERVER_BINARY=web-test-server

TOOLS_DIR=tools
REPO=github.com/apache/yunikorn-k8shim/pkg

# PATH
export PATH := $(BASE_DIR)/$(TOOLS_DIR):$(GO_EXE_PATH):$(PATH)

# Default values for dev cluster
ifeq ($(K8S_VERSION),)
K8S_VERSION := v1.29.4
endif
ifeq ($(CLUSTER_NAME),)
CLUSTER_NAME := yk8s
endif
ifeq ($(PLUGIN),1)
  PLUGIN_OPTS := --plugin
else
  PLUGIN_OPTS := 
endif

# Reproducible builds mode
GO_REPRO_VERSION := $(shell cat .go_repro_version)
ifeq ($(REPRODUCIBLE_BUILDS),1)
  REPRO := 1
else
  REPRO :=
endif

# Build date - Use git commit, then cached build.date, finally current date
# This allows for reproducible builds as long as release tarball contains the build.date file.
DATE := $(shell if [ -d "$(BASE_DIR)/.git" ]; then TZ=UTC0 git --no-pager log -1 --date=iso8601-strict-local --format=%cd 2>/dev/null ; fi || true)
ifeq ($(DATE),)
DATE := $(shell cat "$(BASE_DIR)/build.date" 2>/dev/null || true)
endif
ifeq ($(DATE),)
DATE := $(shell date +%FT%T%z)
endif
DATE := $(shell echo "$(DATE)" > "$(BASE_DIR)/build.date" ; cat "$(BASE_DIR)/build.date")

# Version parameters
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
GOLANGCI_LINT_VERSION=1.57.2
GOLANGCI_LINT_BIN=$(TOOLS_DIR)/golangci-lint
GOLANGCI_LINT_ARCHIVE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH).tar.gz
GOLANGCI_LINT_ARCHIVEBASE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH)

# kubectl
KUBECTL_VERSION=v1.27.7
KUBECTL_BIN=$(TOOLS_DIR)/kubectl

# kind
KIND_VERSION=v0.23.0
KIND_BIN=$(TOOLS_DIR)/kind

# helm
HELM_VERSION=v3.12.1
HELM_BIN=$(TOOLS_DIR)/helm
HELM_ARCHIVE=helm-$(HELM_VERSION)-$(OS)-$(EXEC_ARCH).tar.gz
HELM_ARCHIVE_BASE=$(OS)-$(EXEC_ARCH)

# spark
export SPARK_VERSION=3.3.3
# sometimes the image is not avaiable with $SPARK_VERSION, the minor version must match
export SPARK_PYTHON_VERSION=3.3.1
export SPARK_HOME=$(BASE_DIR)$(TOOLS_DIR)/spark
export SPARK_SUBMIT_CMD=$(SPARK_HOME)/bin/spark-submit
export SPARK_PYTHON_IMAGE=docker.io/apache/spark-py:v$(SPARK_PYTHON_VERSION)

# go-licenses
GO_LICENSES_VERSION=v1.6.0
GO_LICENSES_BIN=$(TOOLS_DIR)/go-licenses

# ginkgo
GINKGO_BIN=$(TOOLS_DIR)/ginkgo

FLAG_PREFIX=github.com/apache/yunikorn-k8shim/pkg/conf

# Image hashes
CORE_SHA=$(shell "$(GO)" list -f '{{.Version}}' -m "github.com/apache/yunikorn-core" | cut -d "-" -f3)
SI_SHA=$(shell "$(GO)" list -f '{{.Version}}' -m "github.com/apache/yunikorn-scheduler-interface" | cut -d "-" -f3)
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

ifeq ($(SCHEDULER_TAG),)
SCHEDULER_TAG := $(REGISTRY)/yunikorn:scheduler-$(DOCKER_ARCH)-$(VERSION)
endif
ifeq ($(PLUGIN_TAG),)
PLUGIN_TAG := $(REGISTRY)/yunikorn:scheduler-plugin-$(DOCKER_ARCH)-$(VERSION)
endif
ifeq ($(ADMISSION_TAG),)
ADMISSION_TAG := $(REGISTRY)/yunikorn:admission-$(DOCKER_ARCH)-$(VERSION)
endif

all:
	$(MAKE) -C $(dir $(BASE_DIR)) init build build_plugin

# Ensure generated files are present
.PHONY: init
init: conf/scheduler-config-local.yaml

# Generate local scheduler config
conf/scheduler-config-local.yaml: conf/scheduler-config.yaml
	./scripts/plugin-conf-gen.sh $(KUBECONFIG) "conf/scheduler-config.yaml" "conf/scheduler-config-local.yaml"

# Install tools
.PHONY: tools
tools: $(SHELLCHECK_BIN) $(GOLANGCI_LINT_BIN) $(KUBECTL_BIN) $(KIND_BIN) $(HELM_BIN) $(SPARK_SUBMIT_CMD) $(GO_LICENSES_BIN) $(GINKGO_BIN)

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

# Install go-licenses
$(GO_LICENSES_BIN):
	@echo "installing go-licenses $(GO_LICENSES_VERSION)"
	@mkdir -p "$(TOOLS_DIR)"
	@GOBIN="$(BASE_DIR)/$(TOOLS_DIR)" "$(GO)" install "github.com/google/go-licenses@$(GO_LICENSES_VERSION)"

$(GINKGO_BIN):
	@echo "installing ginkgo"
	@mkdir -p "$(TOOLS_DIR)"
	@GOBIN="$(BASE_DIR)/$(TOOLS_DIR)" "$(GO)" install "github.com/onsi/ginkgo/v2/ginkgo"

# Run lint against the previous commit for PR and branch build
# In dev setup look at all changes on top of master
.PHONY: lint
lint: $(GOLANGCI_LINT_BIN)
	@echo "running golangci-lint"
	@git symbolic-ref -q HEAD && REV="origin/HEAD" || REV="HEAD^" ; \
	headSHA=$$(git rev-parse --short=12 $${REV}) ; \
	echo "checking against commit sha $${headSHA}" ; \
	"${GOLANGCI_LINT_BIN}" run

# Check scripts
.PHONY: check_scripts
ALLSCRIPTS := $(shell find . -not \( -path ./spark -prune \) -not \( -path ./tools -prune \) -not \( -path ./build -prune \) -name '*.sh')
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
	$(shell mkdir -p "$(OUTPUT)" && find -E . -not \( -path './.git*' -prune \) -not \( -path ./build -prune \) -not \( -path ./tools -prune \) -regex ".*\.(go|sh|md|conf|yaml|yml|html|mod)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > "$(OUTPUT)/license-check.txt")
else
	$(shell mkdir -p "$(OUTPUT)" && find . -not \( -path './.git*' -prune \) -not \( -path ./build -prune \) -not \( -path ./tools -prune \) -regex ".*\.\(go\|sh\|md\|conf\|yaml\|yml\|html\|mod\)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > "$(OUTPUT)/license-check.txt")
endif
	@if [ -s "$(OUTPUT)/license-check.txt" ]; then \
		echo "following files are missing license header:" ; \
		cat "$(OUTPUT)/license-check.txt" ; \
		exit 1; \
	fi
	@echo "  all OK"

# Check licenses of go dependencies
.PHONY: go-license-check
go-license-check: $(GO_LICENSES_BIN)
	@echo "Checking third-party licenses"
	@"$(GO_LICENSES_BIN)" check ./pkg/... ./test/... --include_tests --disallowed_types=forbidden,permissive,reciprocal,restricted,unknown
	@echo "License checks OK"

# Generate third-party dependency licenses
.PHONY: go-license-generate
go-license-generate: $(OUTPUT)/third-party-licenses.md

$(OUTPUT)/third-party-licenses.md: $(GO_LICENSES_BIN) go.mod go.sum
	@echo "Generating third-party licenses file"
	@mkdir -p "$(OUTPUT)"
	@rm -f "$(OUTPUT)/third-party-licenses.md"
	@"$(GO_LICENSES_BIN)" \
		report ./pkg/... \
		--template=./scripts/third-party-licences.md.tpl \
		--ignore github.com/apache/yunikorn-k8shim \
		--ignore github.com/apache/yunikorn-core \
		--ignore github.com/apache/yunikorn-scheduler-interface \
		> "$(OUTPUT)/third-party-licenses.md.tmp"
	@mv "$(OUTPUT)/third-party-licenses.md.tmp" "$(OUTPUT)/third-party-licenses.md"

# Check that we use pseudo versions in master
.PHONY: pseudo
BRANCH := $(shell git branch --show-current)
CORE_REF := $(shell "$(GO)" list -m -f '{{ .Version }}' github.com/apache/yunikorn-core)
CORE_MATCH := $(shell expr "${CORE_REF}" : "v0.0.0-")
SI_REF := $(shell "$(GO)" list -m -f '{{ .Version }}' github.com/apache/yunikorn-scheduler-interface)
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
	KUBECONFIG="$(KUBECONFIG)" ./${SCHEDULER_BINARY}

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
build: $(DEV_BIN_DIR)/$(SCHEDULER_BINARY)

$(DEV_BIN_DIR)/$(SCHEDULER_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building scheduler binary"
	@mkdir -p "$(DEV_BIN_DIR)"
	"$(GO)" build \
	-o=${DEV_BIN_DIR}/${SCHEDULER_BINARY} \
	-race \
	-ldflags '-buildid= -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=false -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	./pkg/cmd/shim/

.PHONY: build_plugin
build_plugin: $(DEV_BIN_DIR)/$(PLUGIN_BINARY)

$(DEV_BIN_DIR)/$(PLUGIN_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building scheduler plugin binary"
	@mkdir -p "$(DEV_BIN_DIR)"
	"$(GO)" build \
	-o=${DEV_BIN_DIR}/${PLUGIN_BINARY} \
	-race \
	-ldflags '-buildid= -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=true -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	./pkg/cmd/schedulerplugin/

# Build scheduler binary in a production ready version
.PHONY: scheduler
scheduler: $(RELEASE_BIN_DIR)/$(SCHEDULER_BINARY)

$(RELEASE_BIN_DIR)/$(SCHEDULER_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building binary for scheduler docker image"
	@mkdir -p "$(RELEASE_BIN_DIR)"
ifeq ($(REPRO),1)
	docker run -t --rm=true --volume "$(BASE_DIR):/buildroot" "golang:$(GO_REPRO_VERSION)" sh -c "cd /buildroot && \
	CGO_ENABLED=0 GOOS=linux GOARCH=\"${EXEC_ARCH}\" go build \
	-a \
	-o=${RELEASE_BIN_DIR}/${SCHEDULER_BINARY} \
	-trimpath \
	-ldflags '-buildid= -extldflags \"-static\" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=false -X ${FLAG_PREFIX}.goVersion=${GO_REPRO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/shim/"
else
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" "$(GO)" build \
	-a \
	-o=${RELEASE_BIN_DIR}/${SCHEDULER_BINARY} \
	-trimpath \
	-ldflags '-buildid= -extldflags "-static" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=false -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/shim/
endif

# Build plugin binary in a production ready version
.PHONY: plugin
plugin: $(RELEASE_BIN_DIR)/$(PLUGIN_BINARY)

$(RELEASE_BIN_DIR)/$(PLUGIN_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building binary for plugin docker image"
	@mkdir -p "$(RELEASE_BIN_DIR)"
ifeq ($(REPRO),1)
	docker run -t --rm=true --volume "$(BASE_DIR):/buildroot" "golang:$(GO_REPRO_VERSION)" sh -c "cd /buildroot && \
	CGO_ENABLED=0 GOOS=linux GOARCH=\"${EXEC_ARCH}\" go build \
	-a \
	-o=${RELEASE_BIN_DIR}/${PLUGIN_BINARY} \
	-trimpath \
	-ldflags '-buildid= -extldflags \"-static\" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=true -X ${FLAG_PREFIX}.goVersion=${GO_REPRO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/schedulerplugin/"
else
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" "$(GO)" build \
	-a \
	-o=${RELEASE_BIN_DIR}/${PLUGIN_BINARY} \
	-trimpath \
	-ldflags '-buildid= -extldflags "-static" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.isPluginVersion=true -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH} -X ${FLAG_PREFIX}.coreSHA=${CORE_SHA} -X ${FLAG_PREFIX}.siSHA=${SI_SHA} -X ${FLAG_PREFIX}.shimSHA=${SHIM_SHA}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/schedulerplugin/
endif
	
# Build a scheduler image based on the production ready version
.PHONY: sched_image
sched_image: $(OUTPUT)/third-party-licenses.md scheduler docker/scheduler
	@echo "building scheduler docker image"
	@rm -rf "$(DOCKER_DIR)/scheduler"
	@mkdir -p "$(DOCKER_DIR)/scheduler"
	@cp -a "docker/scheduler/." "$(DOCKER_DIR)/scheduler/."
	@cp "$(RELEASE_BIN_DIR)/$(SCHEDULER_BINARY)" "$(DOCKER_DIR)/scheduler/."
	@cp -a LICENSE NOTICE "$(OUTPUT)/third-party-licenses.md" "$(DOCKER_DIR)/scheduler/."
	DOCKER_BUILDKIT=1 docker build \
	"$(DOCKER_DIR)/scheduler" \
	-t "$(SCHEDULER_TAG)" \
	--platform "linux/${DOCKER_ARCH}" \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET}

# Build a plugin image based on the production ready version
.PHONY: plugin_image
plugin_image: $(OUTPUT)/third-party-licenses.md plugin docker/plugin conf/scheduler-config.yaml
	@echo "building plugin docker image"
	@rm -rf "$(DOCKER_DIR)/plugin"
	@mkdir -p "$(DOCKER_DIR)/plugin"
	@cp -a "docker/plugin/." "$(DOCKER_DIR)/plugin/."
	@cp "$(RELEASE_BIN_DIR)/$(PLUGIN_BINARY)" "$(DOCKER_DIR)/plugin/."
	@cp -a LICENSE NOTICE "$(OUTPUT)/third-party-licenses.md" "$(DOCKER_DIR)/plugin/."
	@cp conf/scheduler-config.yaml "$(DOCKER_DIR)/plugin/scheduler-config.yaml"
	DOCKER_BUILDKIT=1 docker build \
	"$(DOCKER_DIR)/plugin" \
	-t "$(PLUGIN_TAG)" \
	--platform "linux/${DOCKER_ARCH}" \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET}

# Build admission controller binary in a production ready version
.PHONY: admission
admission: $(RELEASE_BIN_DIR)/$(ADMISSION_CONTROLLER_BINARY)

$(RELEASE_BIN_DIR)/$(ADMISSION_CONTROLLER_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building admission controller binary"
	@mkdir -p "$(RELEASE_BIN_DIR)"
ifeq ($(REPRO),1)
	docker run -t --rm=true --volume "$(BASE_DIR):/buildroot" "golang:$(GO_REPRO_VERSION)" sh -c "cd /buildroot && \
	CGO_ENABLED=0 GOOS=linux GOARCH=\"${EXEC_ARCH}\" go build \
	-a \
	-o=$(RELEASE_BIN_DIR)/$(ADMISSION_CONTROLLER_BINARY) \
	-trimpath \
	-ldflags '-buildid= -extldflags \"-static\" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.goVersion=${GO_REPRO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/admissioncontroller"
else
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" "$(GO)" build \
	-a \
	-o=$(RELEASE_BIN_DIR)/$(ADMISSION_CONTROLLER_BINARY) \
	-trimpath \
	-ldflags '-buildid= -extldflags "-static" -X ${FLAG_PREFIX}.buildVersion=${VERSION} -X ${FLAG_PREFIX}.buildDate=${DATE} -X ${FLAG_PREFIX}.goVersion=${GO_VERSION} -X ${FLAG_PREFIX}.arch=${EXEC_ARCH}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/admissioncontroller
endif

# Build an admission controller image based on the production ready version
.PHONY: adm_image
adm_image: $(OUTPUT)/third-party-licenses.md admission docker/admission
	@echo "building admission controller docker image"
	@rm -rf "$(DOCKER_DIR)/admission"
	@mkdir -p "$(DOCKER_DIR)/admission"
	@cp -a "docker/admission/." "$(DOCKER_DIR)/admission/."
	@cp "$(RELEASE_BIN_DIR)/$(ADMISSION_CONTROLLER_BINARY)" "$(DOCKER_DIR)/admission/."
	@cp -a LICENSE NOTICE "$(OUTPUT)/third-party-licenses.md" "$(DOCKER_DIR)/admission/."
	DOCKER_BUILDKIT=1 docker build \
	"$(DOCKER_DIR)/admission" \
	-t "$(ADMISSION_TAG)" \
	--platform "linux/${DOCKER_ARCH}" \
	--label "yunikorn-core-revision=${CORE_SHA}" \
	--label "yunikorn-scheduler-interface-revision=${SI_SHA}" \
	--label "yunikorn-k8shim-revision=${SHIM_SHA}" \
	--label "BuildTimeStamp=${DATE}" \
	--label "Version=${VERSION}" \
	${QUIET}

# Build all images based on the production ready version
.PHONY: image
image: sched_image plugin_image adm_image

# Build a web server image ONLY to be used in e2e tests
.PHONY: webtest_image
webtest_image: $(OUTPUT)/third-party-licenses.md build_web_test_server_prod docker/webtest
	@echo "building web server image for automated e2e tests"
	@rm -rf "$(DOCKER_DIR)/webtest"
	@mkdir -p "$(DOCKER_DIR)/webtest"
	@cp -a "docker/webtest/." "$(DOCKER_DIR)/webtest/."
	@cp "$(RELEASE_BIN_DIR)/$(TEST_SERVER_BINARY)" "$(DOCKER_DIR)/webtest/."
	@cp -a LICENSE NOTICE "$(OUTPUT)/third-party-licenses.md" "$(DOCKER_DIR)/webtest/."
	DOCKER_BUILDKIT=1 docker build \
	"$(DOCKER_DIR)/webtest" \
	-t "${REGISTRY}/yunikorn:webtest-${DOCKER_ARCH}-${VERSION}" \
	--label "yunikorn-e2e-web-image" \
	${QUIET}

.PHONY: build_web_test_server_dev
build_web_test_server_dev: $(DEV_BIN_DIR)/$(TEST_SERVER_BINARY)

$(DEV_BIN_DIR)/$(TEST_SERVER_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building local web server binary"
	"$(GO)" build \
	-o="$(DEV_BIN_DIR)/$(TEST_SERVER_BINARY)" \
	-race \
	-ldflags '-buildid= -X main.version=${VERSION} -X main.date=${DATE} -X main.goVersion=${GO_VERSION} -X main.arch=${EXEC_ARCH}' \
	./pkg/cmd/webtest/

.PHONY: build_web_test_server_prod
build_web_test_server_prod: $(RELEASE_BIN_DIR)/$(TEST_SERVER_BINARY)

$(RELEASE_BIN_DIR)/$(TEST_SERVER_BINARY): go.mod go.sum $(shell find pkg)
	@echo "building web server binary"
	CGO_ENABLED=0 GOOS=linux GOARCH="${EXEC_ARCH}" "$(GO)" build \
	-a \
	-o="$(RELEASE_BIN_DIR)/$(TEST_SERVER_BINARY)" \
	-ldflags '-buildid= -extldflags "-static" -X main.version=${VERSION} -X main.date=${DATE} -X main.goVersion=${GO_VERSION} -X main.arch=${EXEC_ARCH}' \
	-tags netgo \
	-installsuffix netgo \
	./pkg/cmd/webtest/

# Run the tests after building
.PHONY: test
test: export DEADLOCK_DETECTION_ENABLED = true
test: export DEADLOCK_TIMEOUT_SECONDS = 10
test: export DEADLOCK_EXIT = true
test:
	@echo "running unit tests"
	@mkdir -p "$(OUTPUT)"
	"$(GO)" clean -testcache
	"$(GO)" test ./pkg/... -cover -race -tags deadlock -coverprofile="$(OUTPUT)/coverage.txt" -covermode=atomic
	"$(GO)" vet "$(REPO)"...

# Run benchmarks
.PHONY: bench
bench:
	@echo "running benchmarks"
	"$(GO)" clean -testcache
	"$(GO)" test -v -run '^Benchmark' -bench . ./pkg/...

# Generate FSM graphs (dot/png)
.PHONY: fsm_graph
fsm_graph:
	@echo "generating FSM graphs"
	"$(GO)" clean -testcache
	"$(GO)" test -tags graphviz -run 'Test.*FsmGraph' ./pkg/cache
	scripts/generate-fsm-graph-images.sh

# Remove generated build artifacts
.PHONY: clean
clean:
	@echo "cleaning up caches and output"
	@"$(GO)" clean -cache -testcache -r
	@echo "removing generated files"
	@rm -rf "${OUTPUT}"

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

# Start dev cluster
start-cluster: $(KIND_BIN)
	@"$(KIND_BIN)" delete cluster --name="$(CLUSTER_NAME)" || :
	@./scripts/run-e2e-tests.sh -a install -n "$(CLUSTER_NAME)" -v "kindest/node:$(K8S_VERSION)" $(PLUGIN_OPTS)

# Stop dev cluster
stop-cluster: $(KIND_BIN)
	@"$(KIND_BIN)" delete cluster --name="$(CLUSTER_NAME)"

# Run the e2e tests, this assumes yunikorn is running under yunikorn namespace
.PHONY: e2e_test
e2e_test: tools
	@echo "running e2e tests"
	cd ./test/e2e && \
	ginkgo -r $(E2E_TEST) -v -keep-going -- -yk-namespace "yunikorn" -kube-config $(KUBECONFIG)
