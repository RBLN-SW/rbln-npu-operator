# VERSION defines the project version for the bundle.
include $(CURDIR)/versions.mk

BUNDLE_PACKAGE ?= rbln-npu-operator

CHANNELS ?= candidate,fast,stable

DEFAULT_CHANNEL ?= stable

PLATFORMS ?= linux/arm64,linux/amd64

PLATFORM ?= linux/amd64

MODULE := github.com/rebellions-sw/rbln-npu-operator

# Component image versions (can be overridden)
# All versions will be used as image tags
OPERATOR_VERSION ?= $(VERSION)
DEVICE_PLUGIN_VERSION ?= latest
METRICS_EXPORTER_VERSION ?= latest
NPU_DISCOVERY_VERSION ?= latest
VFIO_MANAGER_VERSION ?= latest

# Set the Operator SDK version to use. By default, what is installed on the system is used.
# This is useful for CI or a project to utilize a specific version of the operator-sdk toolkit.
OPERATOR_SDK_VERSION ?= v1.38.0

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.34.1

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Cross-platform build configuration
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))

# Setting SHELL to bash allows bash commands to be executed by recipes.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	@echo "Generating CRDs from the codebase"
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: sync-crds
sync-crds:
	@echo "Syncing CRDs into Helm packages..."
	cp $(PROJECT_DIR)/config/crd/bases/* $(PROJECT_DIR)/deployments/rbln-npu-operator/crds

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ensure-gofumpt ## Run go fmt against code.
	@echo "Running go fmt..."
	gofumpt -l . && [ -z "$$(gofumpt -l .)" ] || (echo "Formatting issues found"; exit 1)
	@echo "Go fmt completed."

.PHONY: fmt-fix
fmt-fix:
	gofumpt -l -w .

.PHONY: ensure-gofumpt
ensure-gofumpt: ## Install gofumpt if not present
	@echo "Ensuring gofumpt is installed..."
	@command -v gofumpt >/dev/null || go install mvdan.cc/gofumpt@latest
	@echo "gofumpt installation complete."

.PHONY: vet
vet: ## Run go vet against code.
	@echo "Running go vet..."
	go vet ./...
	@echo "Go vet completed."

.PHONY: unit-tests
unit-tests: envtest
	@echo "Setting up test environment..."
	@echo "ENVTEST_K8S_VERSION: $(ENVTEST_K8S_VERSION)"
	@echo "LOCALBIN: $(LOCALBIN)"
	@KUBEBUILDER_ASSETS_PATH="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)"; \
	echo "KUBEBUILDER_ASSETS: $$KUBEBUILDER_ASSETS_PATH"; \
	echo "Running Go tests..."; \
	KUBEBUILDER_ASSETS="$$KUBEBUILDER_ASSETS_PATH" go test -v $$(go list ./... | grep -v /e2e) -coverprofile cover.out
	@echo "Tests completed."

.PHONY: test-e2e
test-e2e: ## Run the e2e tests against a Kind k8s instance that is spun up.
	go test ./test/e2e/ -v -ginkgo.v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	@echo "Running golangci-lint..."
	$(GOLANGCI_LINT) run
	@echo "golangci-lint completed."

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

##@ Build

BUILD_FLAGS = -ldflags "-s -w"
.PHONY: build
build:
	@echo "Building the project..."
	go build $(BUILD_FLAGS) ./...
	@echo "Build completed."

.PHONY: cmd
cmd: ## Build the main executable
	@echo "Building npu-operator executable..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o npu-operator $(BUILD_FLAGS) $(COMMAND_BUILD_OPTIONS) $(MODULE)/cmd/npu-operator
	@echo "npu-operator executable built successfully."

.PHONY: cmd-validator
cmd-validator: ## Build the validator executable
	@echo "Building rbln-validator executable..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o rbln-validator $(BUILD_FLAGS) $(COMMAND_BUILD_OPTIONS) $(MODULE)/cmd/rbln-validator
	@echo "rbln-validator executable built successfully."

.PHONY: cmds
cmds: cmd cmd-validator ## Build all executables

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/npu-operator


.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMAGE}
	$(KUSTOMIZE) build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image $(IMAGE_NAME)=${IMAGE}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply $(APPLY_FLAGS) -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

LOCALBIN ?= $(PROJECT_DIR)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

KUBECTL ?= kubectl
APPLY_FLAGS ?= --server-side
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

KUSTOMIZE_VERSION ?= v5.4.2
CONTROLLER_TOOLS_VERSION ?= v0.20.1
ENVTEST_VERSION ?= release-0.22

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	@echo "Ensuring golangci-lint is available..."
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@echo "golangci-lint setup complete."

define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) GOTOOLCHAIN=go1.25.7 go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

# CHANNELS define the bundle channels used in the bundle.
# Add a new line here if you would like to change its default config. (E.g CHANNELS = "candidate,fast,stable")
# To re-generate a bundle for other specific channels without changing the standard setup, you can:
# - use the CHANNELS as arg of the bundle target (e.g make bundle CHANNELS=candidate,fast,stable)
# - use environment variables to overwrite this value (e.g export CHANNELS="candidate,fast,stable")
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

# DEFAULT_CHANNEL defines the default channel used in the bundle.
# Add a new line here if you would like to change its default config. (E.g DEFAULT_CHANNEL = "stable")
# To re-generate a bundle for any other default channel without changing the default setup, you can:
# - use the DEFAULT_CHANNEL as arg of the bundle target (e.g make bundle DEFAULT_CHANNEL=stable)
# - use environment variables to overwrite this value (e.g export DEFAULT_CHANNEL="stable")
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

.PHONY: operator-sdk
OPERATOR_SDK ?= $(LOCALBIN)/operator-sdk
operator-sdk: ## Download operator-sdk locally if necessary.
ifeq (,$(wildcard $(OPERATOR_SDK)))
ifeq (, $(shell which operator-sdk 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPERATOR_SDK)) ;\
	OS=$(shell go env GOOS) && ARCH=$(shell go env GOARCH) && \
	curl -sSLo $(OPERATOR_SDK) https://github.com/operator-framework/operator-sdk/releases/download/$(OPERATOR_SDK_VERSION)/operator-sdk_$${OS}_$${ARCH} ;\
	chmod +x $(OPERATOR_SDK) ;\
	}
else
OPERATOR_SDK = $(shell which operator-sdk)
endif
endif


.PHONY: bundle
bundle: manifests kustomize operator-sdk ## Generate bundle manifests and metadata with customizable image versions using tags.
	$(OPERATOR_SDK) generate kustomize manifests -q
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMAGE)
	$(KUSTOMIZE) build config/manifests | $(OPERATOR_SDK) generate bundle $(BUNDLE_GEN_FLAGS)
	$(OPERATOR_SDK) bundle validate ./bundle



.PHONY: verify-manifests-sync
verify-manifests-sync: manifests generate sync-crds
	@echo "Checking if code and manifests are synchronized..."
	@git diff --exit-code -- api config deployments
	@echo "Code and manifests synchronization check completed."

verify-deps:
	@echo "Verifying that all Go dependencies and vendor files are consistent..."
	go mod verify
	@echo "Go mod verify completed."
	go mod tidy
	@git diff --exit-code -- go.sum go.mod
	@echo "Go mod tidy completed."
	go mod vendor
	@git diff --exit-code -- vendor
	@echo "Go vendor completed."

.PHONY: code-check
code-check: vet fmt lint verify-deps verify-manifests-sync

.PHONY: pre-commit-install
pre-commit-install: # Install pre-commit hooks.
	pre-commit install

.PHONY: pre-commit-run
pre-commit-run: # Run pre-commit hooks.
	pre-commit run --all-files

##@ Container Images

# Container build configuration
CONTAINER_TOOL ?= docker

DOCKERFILE ?= $(CURDIR)/Dockerfile
PUSH_ON_BUILD ?= false
BUILD_MULTI_PLATFORM ?= false
DOCKER_BUILD_OPTIONS ?= --output=type=image,push=$(PUSH_ON_BUILD)
BUILDX =

ifeq ($(BUILD_MULTI_PLATFORM),true)
	DOCKER_BUILD_PLATFORM_OPTIONS ?= --platform=linux/amd64,linux/arm64
	BUILDX = buildx
else
	DOCKER_BUILD_PLATFORM_OPTIONS := --platform=linux/amd64
endif

# Image registry and naming configuration
REGISTRY ?= docker.io/rebellions
IMAGE_NAME ?= $(REGISTRY)/rbln-npu-operator

# Image tagging configuration
IMAGE_TAG ?= $(VERSION)
IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

BUNDLE_SEMVER  = $(patsubst v%,%,$(VERSION))
# BUNDLE_GEN_FLAGS are the flags passed to the operator-sdk generate bundle command
BUNDLE_GEN_FLAGS ?= -q --overwrite --version $(BUNDLE_SEMVER) $(BUNDLE_METADATA_OPTS)

# USE_IMAGE_DIGESTS defines if images are resolved via tags or digests
# You can enable this value if you would like to use SHA Based Digests
# To enable set flag to true
USE_IMAGE_DIGESTS ?= false
ifeq ($(USE_IMAGE_DIGESTS), true)
	BUNDLE_GEN_FLAGS += --use-image-digests
endif

.PHONY: opm
OPM = $(LOCALBIN)/opm
opm: ## Download opm locally if necessary.
ifeq (,$(wildcard $(OPM)))
ifeq (,$(shell which opm 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPM)) ;\
	OS=$(shell go env GOOS) && ARCH=$(shell go env GOARCH) && \
	curl -sSLo $(OPM) https://github.com/operator-framework/operator-registry/releases/download/v1.23.0/$${OS}-$${ARCH}-opm ;\
	chmod +x $(OPM) ;\
	}
else
OPM = $(shell which opm)
endif
endif

# The image tag given to the resulting catalog image (e.g. make catalog-build CATALOG_IMG=example.com/operator-catalog:v0.2.0).

# Set CATALOG_BASE_IMG to an existing catalog image tag to add $BUNDLE_IMGS to that image.
ifneq ($(origin CATALOG_BASE_IMG), undefined)
FROM_INDEX_OPT := --from-index $(CATALOG_BASE_IMG)
endif

##@ Main Operator Image

.PHONY: build-image
build-image: ## Build the NPU operator image.
	DOCKER_BUILDKIT=1 \
		$(CONTAINER_TOOL) $(BUILDX) build --pull \
		$(DOCKER_BUILD_OPTIONS) \
		$(DOCKER_BUILD_PLATFORM_OPTIONS) \
		--tag $(IMAGE) \
		--build-arg VERSION="$(VERSION)" \
		--build-arg GOLANG_VERSION="$(GOLANG_VERSION)" \
		--file $(DOCKERFILE) $(CURDIR)

##@ Bundle Images

BUNDLE_IMAGE ?= $(REGISTRY)/rbln-npu-operator-bundle:$(BUNDLE_SEMVER)

build-bundle-image:
	DOCKER_BUILDKIT=1 \
		$(CONTAINER_TOOL) $(BUILDX) build --pull \
		$(DOCKER_BUILD_OPTIONS) \
		$(DOCKER_BUILD_PLATFORM_OPTIONS) \
		--tag $(BUNDLE_IMAGE) \
		--build-arg DEFAULT_CHANNEL=$(DEFAULT_CHANNEL) \
		--file bundle.Dockerfile $(CURDIR)

# Push the bundle image.
push-bundle-image: build-bundle-image
	$(CONTAINER_TOOL) push $(BUNDLE_IMAGE)

##@ Catalog Images

# Build a catalog image by adding bundle images to an empty catalog using the operator package manager tool, 'opm'.
# This recipe invokes 'opm' in 'semver' bundle add mode. For more information on add modes, see:
# https://github.com/operator-framework/community-operators/blob/7f1438c/docs/packaging-operator.md#updating-your-existing-operator
CATALOG_IMAGE ?= $(REGISTRY)/rbln-npu-operator-catalog:$(VERSION)

.PHONY: catalog-build
catalog-build: opm ## Build a catalog image.
	DOCKER_BUILDKIT=1 DOCKER_DEFAULT_PLATFORM=linux/amd64 \
	$(OPM) index add --container-tool $(CONTAINER_TOOL) --mode semver --tag $(CATALOG_IMAGE) --bundles $(BUNDLE_IMAGE) $(FROM_INDEX_OPT)

# Push the catalog image.
.PHONY: catalog-push
catalog-push: ## Push a catalog image.
	$(CONTAINER_TOOL) push $(CATALOG_IMAGE)
