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
	"fmt"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	crdDir := flag.String("crd-dir", "/opt/rbln/crds", "directory containing CRD manifests to apply")
	fieldOwner := flag.String("field-owner", "rbln-npu-operator-crd", "server-side apply field manager")
	flag.Parse()

	cfg, err := ctrl.GetConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "get kubeconfig: %v\n", err)
		os.Exit(1)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create client: %v\n", err)
		os.Exit(1)
	}
	if err := applyManifests(context.Background(), c, *crdDir, *fieldOwner); err != nil {
		fmt.Fprintf(os.Stderr, "apply CRDs: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("CRDs applied successfully from", *crdDir)
}
