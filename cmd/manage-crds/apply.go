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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func applyManifests(ctx context.Context, c client.Client, dir, fieldOwner string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return fmt.Errorf("glob %s: %w", dir, err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no manifests found in %s", dir)
	}
	for _, f := range files {
		if err := applyFile(ctx, c, f, fieldOwner); err != nil {
			return err
		}
	}
	return nil
}

func applyFile(ctx context.Context, c client.Client, path, fieldOwner string) error {
	// #nosec G304 -- CRD manifests are read from a fixed operator-controlled directory baked into the image.
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		raw := map[string]interface{}{}
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode %s: %w", path, err)
		}
		if len(raw) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: raw}
		if err := c.Patch(ctx, obj, client.Apply, client.ForceOwnership, client.FieldOwner(fieldOwner)); err != nil {
			return fmt.Errorf("apply %s %q from %s: %w", obj.GetKind(), obj.GetName(), filepath.Base(path), err)
		}
	}
}
