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
	"flag"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	e2elog "github.com/rebellions-sw/rbln-npu-operator/test/e2e/logs"
	e2etestenv "github.com/rebellions-sw/rbln-npu-operator/test/e2e/testenv"
)

type e2eTestConfig struct {
	helmChart string
	namespace string

	operatorRegistry   string
	operatorRepository string
	operatorVersion    string

	pypiUsername string
	pypiPassword string
}

var e2eCfg = e2eTestConfig{}

func TestMain(m *testing.M) {
	flag.StringVar(&e2eCfg.helmChart, "helm-chart", "", "Helm chart to use")
	flag.StringVar(
		&e2eCfg.namespace,
		"namespace",
		"rbln-system",
		"Namespace name to use for the rbln-npu-operator helm deploys",
	)

	flag.StringVar(&e2eCfg.operatorRegistry, "operator-registry", "", "NPU Operator image registry to use")
	flag.StringVar(&e2eCfg.operatorRepository, "operator-repository", "", "NPU Operator image repository to use")
	flag.StringVar(&e2eCfg.operatorVersion, "operator-version", "", "NPU Operator image tag to use")

	flag.StringVar(
		&e2eCfg.pypiUsername,
		"pypi-username",
		os.Getenv("PYPI_USERNAME"),
		"PyPI username for pypi.rebellions.in",
	)
	flag.StringVar(
		&e2eCfg.pypiPassword,
		"pypi-password",
		os.Getenv("PYPI_PASSWORD"),
		"PyPI password for pypi.rebellions.in",
	)
	e2etestenv.RegisterContextFlags(flag.CommandLine)
	flag.Parse()
	os.Exit(m.Run())
}

// Run e2e tests using the Ginkgo runner.
func TestE2E(t *testing.T) {
	e2elog.Init()
	defer e2elog.Close()

	RegisterFailHandler(Fail)
	e2elog.Infof("Starting rbln-npu-operator suite")
	RunSpecs(t, "RBLN NPU operator e2e suite")
}
