/*
Copyright 2026 Rebellions Inc.

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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func widgetCRD(metadataExtra string) string {
	return fmt.Sprintf(`apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com%s
spec:
  group: example.com
  names:
    kind: Widget
    listKind: WidgetList
    plural: widgets
    singular: widget
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
`, metadataExtra)
}

func crdGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}
}

func getWidgetCRD(ctx context.Context, c client.Client) (*unstructured.Unstructured, error) {
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(crdGVK())
	err := c.Get(ctx, types.NamespacedName{Name: "widgets.example.com"}, got)
	return got, err
}

func TestApplyManifests(t *testing.T) {
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	c, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "widget.yaml")

	if err := os.WriteFile(path, []byte(widgetCRD("")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := applyManifests(ctx, c, dir, "test-owner"); err != nil {
		t.Fatalf("apply (create): %v", err)
	}
	if _, err := getWidgetCRD(ctx, c); err != nil {
		t.Fatalf("get after create: %v", err)
	}

	if err := os.WriteFile(path, []byte(widgetCRD("\n  labels:\n    hook-test: updated")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := applyManifests(ctx, c, dir, "test-owner"); err != nil {
		t.Fatalf("apply (update): %v", err)
	}
	got, err := getWidgetCRD(ctx, c)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.GetLabels()["hook-test"] != "updated" {
		t.Fatalf("label not applied on update: got labels %v", got.GetLabels())
	}
}
