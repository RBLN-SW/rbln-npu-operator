# Rebellions NPU Operator

Welcome to the **Rebellions NPU Operator** Helm Chart repository.  
This repository provides Helm charts for deploying the **Rebellions NPU Operator**,  
which manages NPU feature discovery, device plugins, and related components on Kubernetes clusters.

For more information, please refer to the [official Rebellions documentation](https://docs.rbln.ai/latest/).

---

## 🚀 Quickstart

```sh
# 1. Install Helm (if not installed)
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 \
   && chmod 700 get_helm.sh \
   && ./get_helm.sh

# 2. Add the Rebellions Helm repository
helm repo add rebellions https://rbln-sw.github.io/rbln-npu-operator
helm repo update

# 3. Install the NPU Operator
helm install --wait --generate-name \
     -n rbln-system --create-namespace \
     rebellions/rbln-npu-operator
```
