GO_CMD ?= go

OPERATOR_REGISTRY ?= docker.io
OPERATOR_REPOSITORY ?= rebellions/rbln-npu-operator
VALIDATOR_REGISTRY ?= docker.io
VALIDATOR_REPOSITORY ?= rebellions/rbln-npu-operator-validator

HELM_CHART ?= $(CURDIR)/../../deployments/rbln-npu-operator
KUBE_CONTEXT ?=
PYPI_USERNAME ?=
PYPI_PASSWORD ?=
REGISTRY_USER ?= $(E2E_CONTAINER_REGISTRY_USER)
REGISTRY_PASSWORD ?= $(E2E_CONTAINER_REGISTRY_PASSWORD)
GINKGO_LABEL_FILTER ?=
GO_TEST_TIMEOUT ?= 45m

.PHONY: e2e-test
e2e-test:
	@if [ -z ${KUBECONFIG} ]; then \
		echo "[ERR] KUBECONFIG is missing, must be set"; \
		exit 1; \
	fi
	@if [ -z ${OPERATOR_VERSION} ]; then \
		echo "[ERR] OPERATOR_VERSION is missing, must be set"; \
		exit 1; \
	fi
	$(GO_CMD) test -v . \
		-timeout=$(GO_TEST_TIMEOUT) \
		$(if $(strip $(GINKGO_LABEL_FILTER)),-ginkgo.label-filter=$(GINKGO_LABEL_FILTER)) \
		-args \
		-kubeconfig=$(KUBECONFIG) \
		$(if $(strip $(KUBE_CONTEXT)),-context=$(KUBE_CONTEXT)) \
		-operator-registry=$(OPERATOR_REGISTRY) \
		-operator-repository=$(OPERATOR_REPOSITORY) \
		-operator-version=$(OPERATOR_VERSION) \
		-helm-chart=$(HELM_CHART) \
		-pypi-username=$(PYPI_USERNAME) \
		-pypi-password=$(PYPI_PASSWORD) \
		-registry-user=$(REGISTRY_USER) \
		-registry-password=$(REGISTRY_PASSWORD) \
		-validator-registry=$(VALIDATOR_REGISTRY) \
		-validator-repository=$(VALIDATOR_REPOSITORY)
