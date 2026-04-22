package v1alpha1

import (
	"strings"
	"testing"
)

func TestRBLNDriverSpec_GetPrecompiledImagePath(t *testing.T) {
	type args struct {
		registry      string
		image         string
		version       string
		osVersion     string
		kernelVersion string
	}
	type expect struct {
		path      string
		errSubstr string
	}
	cases := map[string]struct {
		args   args
		expect expect
	}{
		"builds standard precompiled path": {
			args: args{
				registry:      "repo.rebellions.ai",
				image:         "rebellions/rbln-driver",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0-78-generic",
			},
			expect: expect{
				path: "repo.rebellions.ai/rebellions/rbln-driver:3.0.0-5.15.0-78-generic-22.04",
			},
		},
		"trims trailing slash from registry and leading slash from image": {
			args: args{
				registry:      "repo.rebellions.ai/",
				image:         "/rebellions/rbln-driver",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
			},
			expect: expect{
				path: "repo.rebellions.ai/rebellions/rbln-driver:3.0.0-5.15.0-22.04",
			},
		},
		"rejects missing osVersion": {
			args: args{
				image:         "rebellions/rbln-driver",
				version:       "3.0.0",
				kernelVersion: "5.15.0",
			},
			expect: expect{errSubstr: "osVersion and kernelVersion are required"},
		},
		"rejects missing kernelVersion": {
			args: args{
				image:     "rebellions/rbln-driver",
				version:   "3.0.0",
				osVersion: "22.04",
			},
			expect: expect{errSubstr: "osVersion and kernelVersion are required"},
		},
		"rejects empty driver version": {
			args: args{
				image:         "rebellions/rbln-driver",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
			},
			expect: expect{errSubstr: "driver version is required"},
		},
		"rejects digest in image field": {
			args: args{
				image:         "rebellions/rbln-driver@sha256:abcdef",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
			},
			expect: expect{errSubstr: "image digest is not supported"},
		},
		"rejects digest-prefixed version": {
			args: args{
				image:         "rebellions/rbln-driver",
				version:       "sha256:abcdef",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
			},
			expect: expect{errSubstr: "image digest is not supported"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			spec := &RBLNDriverSpec{
				Registry: tc.args.registry,
				Image:    tc.args.image,
				Version:  tc.args.version,
			}
			got, err := spec.GetPrecompiledImagePath(tc.args.osVersion, tc.args.kernelVersion)
			if tc.expect.errSubstr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (path=%q)", tc.expect.errSubstr, got)
				}
				if !strings.Contains(err.Error(), tc.expect.errSubstr) {
					t.Fatalf("expected error containing %q, got %q", tc.expect.errSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.expect.path {
				t.Fatalf("path mismatch\n  want: %s\n  got:  %s", tc.expect.path, got)
			}
		})
	}
}
