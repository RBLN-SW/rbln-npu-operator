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

package statusfile

import (
	"slices"
	"testing"
)

func TestMissing(t *testing.T) {
	type args struct {
		filenames     []string
		existingFiles []string
	}

	cases := map[string]struct {
		reason string
		args   args
		want   []string
	}{
		"AllFilesExist": {
			reason: "no missing files when every requested file exists",
			args: args{
				filenames:     []string{"toolkit-ready", "partition-ready"},
				existingFiles: []string{"toolkit-ready", "partition-ready"},
			},
			want: []string{},
		},
		"SomeFilesMissing": {
			reason: "only the absent files are reported, in request order",
			args: args{
				filenames:     []string{"toolkit-ready", "partition-ready"},
				existingFiles: []string{"toolkit-ready"},
			},
			want: []string{"partition-ready"},
		},
		"NoFilesRequested": {
			reason: "an empty request has nothing missing",
			args:   args{},
			want:   []string{},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.args.existingFiles {
				if err := CreateEmpty(dir, f); err != nil {
					t.Fatalf("create existing file %s: %v", f, err)
				}
			}

			got := Missing(dir, tc.args.filenames)

			if !slices.Equal(got, tc.want) {
				t.Fatalf("%s: Missing() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}
