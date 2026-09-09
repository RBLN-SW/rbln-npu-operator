VERSION ?= v0.4.4

GOLANG_VERSION ?= 1.25.13

GOLANGCI_LINT_VERSION ?= v2.3.1

TRIVY_VERSION ?= v0.74.0

GOFUMPT_VERSION ?= v0.9.2

REGCTL_VERSION ?= v0.9.1

OPERATOR_SDK_VERSION ?= v1.38.0

# opm for FBC (file-based catalog) render/validate. Aligned with the
# certified-operators FBC Makefile default so the dev catalog matches what ships.
OPM_VERSION ?= v1.46.0

KUSTOMIZE_VERSION ?= v5.4.2

CONTROLLER_TOOLS_VERSION ?= v0.20.1

ENVTEST_K8S_VERSION ?= 1.34.1

ENVTEST_VERSION ?= release-0.22

# OpenShift minors the OLM bundle claims (com.redhat.openshift.versions) and the
# certified-operators catalog templates the release graph check walks. Single
# source: release.yaml patches it into the bundle annotation at rc and GA, and
# hack/release/olm-graph-check.sh reads it when OCP_RANGE is not in the env.
OCP_RANGE ?= v4.16-v4.22
