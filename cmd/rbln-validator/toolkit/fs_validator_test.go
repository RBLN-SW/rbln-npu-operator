package toolkit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFSValidatorValidate(t *testing.T) {
	type args struct {
		files map[string]string
	}

	type want struct {
		err bool
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"NoFiles": {
			reason: "should fail when the CDI directory is empty",
			args: args{
				files: map[string]string{},
			},
			want: want{
				err: true,
			},
		},
		"NonRBLNFileOnly": {
			reason: "should fail when there is no rbln CDI spec",
			args: args{
				files: map[string]string{
					"other.yaml": "kind: CDI",
				},
			},
			want: want{
				err: true,
			},
		},
		"EmptyRBLNFile": {
			reason: "should fail when the rbln CDI spec is empty",
			args: args{
				files: map[string]string{
					"rbln-empty.yaml": "",
				},
			},
			want: want{
				err: true,
			},
		},
		"NonEmptyRBLNFile": {
			reason: "should succeed when a non-empty rbln CDI spec exists",
			args: args{
				files: map[string]string{
					"rbln-test.yaml": "kind: CDI",
				},
			},
			want: want{
				err: false,
			},
		},
		"MixedFilesWithValidRBLNFile": {
			reason: "should succeed when at least one valid rbln CDI spec exists",
			args: args{
				files: map[string]string{
					"other.yaml":      "kind: CDI",
					"rbln-empty.yaml": "",
					"rbln-valid.yaml": "kind: CDI",
				},
			},
			want: want{
				err: false,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()

			for file, content := range tc.args.files {
				path := filepath.Join(dir, file)
				if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
					t.Fatalf("%s: write file: %v", tc.reason, err)
				}
			}

			err := FSValidator{}.Validate(t.Context(), dir)

			if tc.want.err && err == nil {
				t.Fatalf("%s: expected error, got nil", tc.reason)
			}
			if !tc.want.err && err != nil {
				t.Fatalf("%s: expected no error, got %v", tc.reason, err)
			}
		})
	}
}
