/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"time"

	"github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	pypiSecretName  = "pypi-cred"
	pypiRegistry    = "pypi.rebellions.in"
	modelZooRepo    = "https://github.com/rebellions-sw/rbln-model-zoo.git"
	modelZooModel   = "pytorch/vision/detection/yolov10/"
	compilerVersion = "rebel-compiler==0.10.4"
	runnerImage     = "ubuntu:22.04"

	modelZooPodTimeout   = 10 * time.Minute
	modelZooPollInterval = 10 * time.Second
)

const modelZooScript = `set -euo pipefail
export TZ=Asia/Seoul
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime
echo $TZ > /etc/timezone
apt update
apt-get install -y git python3 python3-pip python3-venv ca-certificates
mkdir -p /workspace
python3 -m venv /workspace/.venv
. /workspace/.venv/bin/activate
python -m pip install -U pip setuptools wheel

cat <<EOF > ~/.netrc
machine ` + pypiRegistry + `
login ${PYPI_USER}
password ${PYPI_PASS}
EOF

chmod 600 ~/.netrc

git clone ` + modelZooRepo + `
cd rbln-model-zoo/` + modelZooModel + `
git submodule update --init ultralytics/
python -m pip install -r requirements.txt
python -m pip install -i https://` + pypiRegistry + `/simple ` + compilerVersion + `
echo "[INFO] Running compile.py..."
python compile.py
echo "[INFO] Running inference.py..."
python inference.py
echo "[SUCCESS] Model-zoo inference completed."`

// ensurePyPISecret creates (or overwrites) the pypi-cred Secret. It tolerates a
// leftover from a prior run whose DeferCleanup never fired on the shared e2e
// cluster, overwriting it so the current run's credentials are used.
func ensurePyPISecret(ctx context.Context, client corev1client.CoreV1Interface, namespace, username, password string) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pypiSecretName,
			Namespace: namespace,
		},
		StringData: map[string]string{
			"username": username,
			"password": password,
		},
	}
	_, err := client.Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		_, err = client.Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{})
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// cleanupPyPISecret removes the pypi-cred Secret.
func cleanupPyPISecret(client corev1client.CoreV1Interface, namespace string) {
	_ = client.Secrets(namespace).Delete(context.Background(), pypiSecretName, metav1.DeleteOptions{})
}

// pypiEnvVars returns the environment variable references for PyPI credentials.
func pypiEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name: "PYPI_USER",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: pypiSecretName},
					Key:                  "username",
				},
			},
		},
		{
			Name: "PYPI_PASS",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: pypiSecretName},
					Key:                  "password",
				},
			},
		},
	}
}

// newModelZooContainer returns a container spec for model-zoo compile/inference.
func newModelZooContainer() corev1.Container {
	return corev1.Container{
		Name:    "runner",
		Image:   runnerImage,
		Command: []string{"/bin/bash", "-lc", modelZooScript},
		Env:     pypiEnvVars(),
	}
}

// waitForPodCompletion polls until the pod reaches Succeeded or Failed and
// asserts that it succeeded.
func waitForPodCompletion(ctx context.Context, client corev1client.CoreV1Interface, namespace, podName string) {
	var lastPod *corev1.Pod
	gomega.Eventually(func(g gomega.Gomega) corev1.PodPhase {
		p, err := client.Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		lastPod = p
		return p.Status.Phase
	}).WithTimeout(modelZooPodTimeout).
		WithPolling(modelZooPollInterval).
		WithContext(ctx).
		Should(gomega.BeElementOf(corev1.PodSucceeded, corev1.PodFailed))

	gomega.Expect(lastPod.Status.Phase).To(gomega.Equal(corev1.PodSucceeded), "pod failed: %s", lastPod.Status.Message)
}
