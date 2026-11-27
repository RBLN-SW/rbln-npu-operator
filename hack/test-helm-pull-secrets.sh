#!/usr/bin/env bash
set -euo pipefail

chart=deployments/rbln-npu-operator
render=$(mktemp)
trap 'rm -f "$render"' EXIT
helm template ci "$chart" --set driver.enabled=true,driver.deployDefault=true,nfd.enabled=true \
  --set-json 'global.imagePullSecrets=[{"name":"shared"},"mirror"]' \
  --set-string 'operator.imagePullSecrets[0].name=operator,crds.upgrade.imagePullSecrets[0].name=hook' \
  --set-string 'devicePlugin.imagePullSecrets[0]=plugin,devicePlugin.imagePullSecrets[1]=shared' \
  --set-string 'driver.instances.atom.imagePullSecrets[0]=atom,driver.manager.imagePullSecrets[0]=manager' \
  --set-string 'node-feature-discovery.imagePullSecrets[0].name=nfd' > "$render"

check() {
  local actual
  actual=$(yq -N -o=json -I=0 "$1" "$render")
  if [[ "$actual" != "$2" ]]; then
    echo "$1: expected $2, got $actual" >&2
    return 1
  fi
}

pod_secrets='.spec.template.spec.imagePullSecrets'
check "select(.metadata.name == \"ci-rbln-npu-operator-controller-manager\") | $pod_secrets" \
  '[{"name":"shared"},{"name":"mirror"},{"name":"operator"}]'
check "select(.kind == \"Job\" and .metadata.name == \"ci-rbln-npu-operator-crd-upgrade\") | $pod_secrets" \
  '[{"name":"shared"},{"name":"mirror"},{"name":"operator"},{"name":"hook"}]'
check 'select(.kind == "RBLNDriver" and .metadata.name == "rbln-driver") | .spec.imagePullSecrets' \
  '["shared","mirror","drivercred"]'
check 'select(.kind == "RBLNDriver" and .metadata.name == "rbln-driver-atom") | .spec.imagePullSecrets' \
  '["shared","mirror","atom"]'
check 'select(.kind == "RBLNDriver" and .metadata.name == "rbln-driver-atom") | .spec.manager.imagePullSecrets' \
  '["manager"]'
check 'select(.kind == "RBLNClusterPolicy") | .spec.devicePlugin.imagePullSecrets' '["mirror","plugin","shared"]'
for component in draKubeletPlugin deviceConfigManager metricsExporter npuFeatureDiscovery containerToolkit sandboxDevicePlugin validator; do
  check "select(.kind == \"RBLNClusterPolicy\") | .spec.$component.imagePullSecrets" '["shared","mirror"]'
done
check "select(.kind == \"DaemonSet\" and .metadata.name == \"ci-node-feature-discovery-worker\") | $pod_secrets" '[{"name":"nfd"}]'

helm template ci "$chart" --set driver.enabled=true,driver.deployDefault=true \
  --set-json 'global.imagePullSecrets=[]' > "$render"
check "select(.metadata.name == \"ci-rbln-npu-operator-controller-manager\") | $pod_secrets" 'null'
check "select(.kind == \"Job\" and .metadata.name == \"ci-rbln-npu-operator-crd-upgrade\") | $pod_secrets" 'null'
check 'select(.kind == "RBLNClusterPolicy") | .spec.devicePlugin.imagePullSecrets' 'null'
check 'select(.kind == "RBLNDriver" and .metadata.name == "rbln-driver") | .spec.imagePullSecrets' '["drivercred"]'
