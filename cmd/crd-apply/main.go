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

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/internal/logging"
)

func main() {
	logging.SetupFromEnv()

	crdDir := flag.String("crd-dir", "/opt/rbln/crds", "directory containing CRD manifests to apply")
	fieldOwner := flag.String("field-owner", "rbln-npu-operator-crd", "server-side apply field manager")
	flag.Parse()

	cfg, err := ctrl.GetConfig()
	if err != nil {
		slog.Error("Failed to get kubeconfig", "err", err)
		os.Exit(1)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		slog.Error("Failed to create client", "err", err)
		os.Exit(1)
	}
	if err := applyManifests(context.Background(), c, *crdDir, *fieldOwner); err != nil {
		slog.Error("Failed to apply CRDs", "err", err)
		os.Exit(1)
	}
	slog.Info("Applied CRDs successfully", "dir", *crdDir)
}
