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
