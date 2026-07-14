# VERSION defines the project version for the bundle.
include $(CURDIR)/versions.mk

MODULE := github.com/rebellions-sw/rbln-npu-operator

# Component image versions (can be overridden)
# All versions will be used as image tags
OPERATOR_VERSION ?= $(VERSION)
DEVICE_PLUGIN_VERSION ?= latest
METRICS_EXPORTER_VERSION ?= latest
NPU_DISCOVERY_VERSION ?= latest
VFIO_MANAGER_VERSION ?= latest
NODE_REBOOT_VERSION ?= latest

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

# Chart RBAC is GENERATED from config/rbac (the same source operator-sdk reads
# into the bundle CSV), so the Helm install and the OLM/CSV install can never
# drift. Each chart file keeps its templated header; only the `rules:` block is
# replaced from the matching kubebuilder-generated source. Format: src:dst.
RBAC_SYNC_MAP := \
	config/rbac/role.yaml:deployments/rbln-npu-operator/templates/rbac/clusterrole-controller.yaml \
	config/rbac/leader_election_role.yaml:deployments/rbln-npu-operator/templates/rbac/role-controller.yaml \
	config/rbac/metrics_auth_role.yaml:deployments/rbln-npu-operator/templates/rbac/clusterrole-metrics.yaml

.PHONY: sync-rbac
sync-rbac: ## Regenerate Helm chart RBAC rules from config/rbac (single source of truth).
	@echo "Syncing RBAC rules from config/rbac into Helm chart..."
	@for pair in $(RBAC_SYNC_MAP); do \
		src="$(PROJECT_DIR)/$${pair%%:*}"; dst="$(PROJECT_DIR)/$${pair##*:}"; \
		{ awk '/^rules:/{exit} 1' "$$dst"; awk '/^rules:/{f=1} f' "$$src"; } > "$$dst.tmp"; \
		mv "$$dst.tmp" "$$dst"; \
	done

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: gofumpt ## Run go fmt against code.
	@echo "Running go fmt..."
	$(GOFUMPT) -l . && [ -z "$$($(GOFUMPT) -l .)" ] || (echo "Formatting issues found"; exit 1)
	@echo "Go fmt completed."

.PHONY: fmt-fix
fmt-fix: gofumpt
	$(GOFUMPT) -l -w .

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
		go build -o npu-operator $(BUILD_FLAGS) $(MODULE)/cmd/npu-operator
	@echo "npu-operator executable built successfully."

.PHONY: cmd-validator
cmd-validator: ## Build the validator executable
	@echo "Building rbln-validator executable..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o rbln-validator $(BUILD_FLAGS) $(MODULE)/cmd/rbln-validator
	@echo "rbln-validator executable built successfully."

.PHONY: cmd-crd-apply
cmd-crd-apply: ## Build the crd-apply executable
	@echo "Building crd-apply executable..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o crd-apply $(BUILD_FLAGS) $(MODULE)/cmd/crd-apply
	@echo "crd-apply executable built successfully."

.PHONY: cmds
cmds: cmd cmd-validator cmd-crd-apply ## Build all executables

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
GOFUMPT ?= $(LOCALBIN)/gofumpt

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

.PHONY: gofumpt
gofumpt: $(GOFUMPT) ## Download gofumpt locally if necessary.
$(GOFUMPT): $(LOCALBIN)
	$(call go-install-tool,$(GOFUMPT),mvdan.cc/gofumpt,$(GOFUMPT_VERSION))

define go-install-tool
@[ -f "$(1)-$(3)-go$(GOLANG_VERSION)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) GOTOOLCHAIN=go$(GOLANG_VERSION) go install $${package} ;\
mv $(1) $(1)-$(3)-go$(GOLANG_VERSION) ;\
} ;\
ln -sf $(1)-$(3)-go$(GOLANG_VERSION) $(1)
endef

##@ Checks

.PHONY: verify-manifests-sync
verify-manifests-sync: manifests generate sync-crds sync-rbac
	@echo "Checking if code and manifests are synchronized..."
	@git diff --exit-code -- api config deployments
	@echo "Code and manifests synchronization check completed."

.PHONY: verify-deps
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

# PLATFORM can be set to a single platform (e.g. linux/amd64, linux/arm64)
# to override the default multi-platform logic.
PLATFORM ?=

ifneq ($(PLATFORM),)
	DOCKER_BUILD_PLATFORM_OPTIONS := --platform=$(PLATFORM)
	BUILDX := buildx
else ifeq ($(BUILD_MULTI_PLATFORM),true)
	DOCKER_BUILD_PLATFORM_OPTIONS ?= --platform=linux/amd64,linux/arm64
	BUILDX := buildx
else
	DOCKER_BUILD_PLATFORM_OPTIONS := --platform=linux/amd64
	BUILDX :=
endif

# Image registry and naming configuration
REGISTRY ?= docker.io/rebellions
IMAGE_NAME ?= $(REGISTRY)/rbln-npu-operator

# Image tagging configuration
IMAGE_TAG ?= $(VERSION)
IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

VFIO_MANAGER_IMAGE_NAME ?= $(REGISTRY)/rbln-vfio-manager
VFIO_MANAGER_IMAGE_TAG ?= $(VFIO_MANAGER_VERSION)
VFIO_MANAGER_IMAGE := $(VFIO_MANAGER_IMAGE_NAME):$(VFIO_MANAGER_IMAGE_TAG)

NODE_REBOOT_IMAGE_NAME ?= $(REGISTRY)/rbln-node-reboot
NODE_REBOOT_IMAGE_TAG ?= $(NODE_REBOOT_VERSION)
NODE_REBOOT_IMAGE := $(NODE_REBOOT_IMAGE_NAME):$(NODE_REBOOT_IMAGE_TAG)

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

.PHONY: build-vfio-manager-image
build-vfio-manager-image: ## Build the RBLN VFIO manager image.
	DOCKER_BUILDKIT=1 \
		$(CONTAINER_TOOL) $(BUILDX) build --pull \
		$(DOCKER_BUILD_OPTIONS) \
		$(DOCKER_BUILD_PLATFORM_OPTIONS) \
		--tag $(VFIO_MANAGER_IMAGE) \
		--build-arg VERSION="$(VFIO_MANAGER_VERSION)" \
		--file $(CURDIR)/images/vfio-manager/Dockerfile $(CURDIR)

.PHONY: build-node-reboot-image
build-node-reboot-image: ## Build the RBLN node reboot image.
	DOCKER_BUILDKIT=1 \
		$(CONTAINER_TOOL) $(BUILDX) build --pull \
		$(DOCKER_BUILD_OPTIONS) \
		$(DOCKER_BUILD_PLATFORM_OPTIONS) \
		--tag $(NODE_REBOOT_IMAGE) \
		--build-arg VERSION="$(NODE_REBOOT_VERSION)" \
		--file $(CURDIR)/images/node-reboot/Dockerfile $(CURDIR)

##@ Helm Chart

# Helm chart configuration
HELM ?= helm
CHART_DIR ?= $(PROJECT_DIR)/deployments/rbln-npu-operator
CHART_DIST_DIR ?= $(PROJECT_DIR)/dist
CHART_NAME ?= rbln-npu-operator-chart
CHART_VERSION ?= $(patsubst v%,%,$(VERSION))
CHART_APP_VERSION ?= $(VERSION)

# OCI registry for `helm push`. Default mirrors release.yaml; override for
# pre-release testing on a private registry (e.g., Harbor):
#   make helm-push-oci HELM_REGISTRY=oci://harbor.example.com/rebellions
HELM_REGISTRY ?= oci://docker.io/rebellions

.PHONY: helm-deps
helm-deps: ## Resolve helm chart dependencies (NFD).
	$(HELM) repo add nfd https://kubernetes-sigs.github.io/node-feature-discovery/charts --force-update
	$(HELM) dependency build $(CHART_DIR)

.PHONY: helm-package
helm-package: helm-deps ## Package helm chart into $(CHART_DIST_DIR).
	@mkdir -p $(CHART_DIST_DIR)
	$(HELM) package $(CHART_DIR) \
		--destination $(CHART_DIST_DIR) \
		--version "$(CHART_VERSION)" \
		--app-version "$(CHART_APP_VERSION)"

.PHONY: helm-push-oci
helm-push-oci: helm-package ## Push packaged chart to OCI registry. Run `helm registry login` first.
	$(HELM) push $(CHART_DIST_DIR)/$(CHART_NAME)-$(CHART_VERSION).tgz $(HELM_REGISTRY)

.PHONY: helm-clean
helm-clean: ## Remove packaged charts and downloaded dependency archives.
	rm -rf $(CHART_DIST_DIR)
	rm -f $(CHART_DIR)/charts/*.tgz

##@ OLM Bundle

CHANNELS ?= candidate,fast,stable
DEFAULT_CHANNEL ?= stable

ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

BUNDLE_SEMVER = $(patsubst v%,%,$(VERSION))
BUNDLE_GEN_FLAGS ?= -q --overwrite --version $(BUNDLE_SEMVER) $(BUNDLE_METADATA_OPTS)

USE_IMAGE_DIGESTS ?= false
ifeq ($(USE_IMAGE_DIGESTS), true)
	BUNDLE_GEN_FLAGS += --use-image-digests
endif

BUNDLE_IMAGE ?= $(REGISTRY)/rbln-npu-operator-bundle:$(BUNDLE_SEMVER)

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

.PHONY: build-bundle-image
build-bundle-image:
	DOCKER_BUILDKIT=1 \
		$(CONTAINER_TOOL) $(BUILDX) build --pull \
		$(DOCKER_BUILD_OPTIONS) \
		$(DOCKER_BUILD_PLATFORM_OPTIONS) \
		--tag $(BUNDLE_IMAGE) \
		--build-arg DEFAULT_CHANNEL=$(DEFAULT_CHANNEL) \
		--file bundle.Dockerfile $(CURDIR)

.PHONY: push-bundle-image
push-bundle-image: build-bundle-image
	$(CONTAINER_TOOL) push $(BUNDLE_IMAGE)

##@ OLM Catalog

.PHONY: opm
OPM = $(LOCALBIN)/opm
# Always use the pinned $(OPM_VERSION) (not a stray system opm): FBC bundle-dir
# rendering needs a recent opm, and older binaries fail confusingly.
opm: ## Download the pinned opm ($(OPM_VERSION)) to $(LOCALBIN) if missing.
ifeq (,$(wildcard $(OPM)))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPM)) ;\
	OS=$(shell go env GOOS) && ARCH=$(shell go env GOARCH) && \
	curl -sSLo $(OPM) https://github.com/operator-framework/operator-registry/releases/download/$(OPM_VERSION)/$${OS}-$${ARCH}-opm ;\
	chmod +x $(OPM) ;\
	}
endif

CATALOG_IMAGE ?= $(REGISTRY)/rbln-npu-operator-catalog:$(VERSION)
CATALOG_DIR ?= catalog
# Source the catalog renders from: a bundle image (default, for CI/cluster use)
# or a local bundle directory (CATALOG_BUNDLE=bundle) for image-less PR validation.
CATALOG_BUNDLE ?= $(BUNDLE_IMAGE)

# Render a single-bundle file-based catalog (FBC): olm.package + olm.channel +
# the bundle rendered from $(CATALOG_BUNDLE). The channel head must match the
# bundle's CSV name (rbln-npu-operator.v$(BUNDLE_SEMVER)).
.PHONY: catalog
catalog: opm ## Render and validate an FBC catalog from $(CATALOG_BUNDLE) into $(CATALOG_DIR)/.
	rm -rf $(CATALOG_DIR)
	mkdir -p $(CATALOG_DIR)
	@{ \
		printf 'schema: olm.package\nname: rbln-npu-operator\ndefaultChannel: stable\n---\n'; \
		printf 'schema: olm.channel\nname: stable\npackage: rbln-npu-operator\nentries:\n- name: rbln-npu-operator.v$(BUNDLE_SEMVER)\n---\n'; \
		$(OPM) render $(CATALOG_BUNDLE) --output=yaml; \
	} > $(CATALOG_DIR)/index.yaml
	$(OPM) validate $(CATALOG_DIR)

.PHONY: catalog-build-image
catalog-build-image: ## Build the FBC catalog image (run `make catalog` first).
	DOCKER_BUILDKIT=1 \
		$(CONTAINER_TOOL) $(BUILDX) build --pull \
		$(DOCKER_BUILD_OPTIONS) \
		$(DOCKER_BUILD_PLATFORM_OPTIONS) \
		--tag $(CATALOG_IMAGE) \
		--file catalog.Dockerfile $(CURDIR)

.PHONY: catalog-push
catalog-push: ## Push the catalog image.
	$(CONTAINER_TOOL) push $(CATALOG_IMAGE)
