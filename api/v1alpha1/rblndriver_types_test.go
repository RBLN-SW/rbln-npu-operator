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
		family        string
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
				family:        "atom",
			},
			expect: expect{
				path: "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-78-generic-22.04",
			},
		},
		"trims trailing slash from registry and leading slash from image": {
			args: args{
				registry:      "repo.rebellions.ai/",
				image:         "/rebellions/rbln-driver",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{
				path: "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-22.04",
			},
		},
		"injects family before the final path element of a multi-segment image": {
			args: args{
				registry:      "repo.rebellions.ai",
				image:         "a/b/rbln-driver",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "rebel100",
			},
			expect: expect{
				path: "repo.rebellions.ai/a/b/rebel100/rbln-driver:3.0.0-5.15.0-22.04",
			},
		},
		"injects family before a bare image name": {
			args: args{
				registry:      "repo.rebellions.ai",
				image:         "rbln-driver",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{
				path: "repo.rebellions.ai/atom/rbln-driver:3.0.0-5.15.0-22.04",
			},
		},
		"trims trailing slash from image": {
			args: args{
				registry:      "repo.rebellions.ai",
				image:         "rebellions/rbln-driver/",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{
				path: "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-22.04",
			},
		},
		"rejects empty image": {
			args: args{
				image:         "",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{errSubstr: "driver image is required"},
		},
		"rejects image that trims to empty": {
			args: args{
				image:         "/",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{errSubstr: "driver image is required"},
		},
		"rejects missing osVersion": {
			args: args{
				image:         "rebellions/rbln-driver",
				version:       "3.0.0",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{errSubstr: "osVersion and kernelVersion are required"},
		},
		"rejects missing kernelVersion": {
			args: args{
				image:     "rebellions/rbln-driver",
				version:   "3.0.0",
				osVersion: "22.04",
				family:    "atom",
			},
			expect: expect{errSubstr: "osVersion and kernelVersion are required"},
		},
		"rejects missing family": {
			args: args{
				image:         "rebellions/rbln-driver",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
			},
			expect: expect{errSubstr: "NPU family is required to compose the driver image path"},
		},
		"rejects empty driver version": {
			args: args{
				image:         "rebellions/rbln-driver",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{errSubstr: "driver version is required"},
		},
		"rejects digest in image field": {
			args: args{
				image:         "rebellions/rbln-driver@sha256:abcdef",
				version:       "3.0.0",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
			},
			expect: expect{errSubstr: "image digest is not supported"},
		},
		"rejects digest-prefixed version": {
			args: args{
				image:         "rebellions/rbln-driver",
				version:       "sha256:abcdef",
				osVersion:     "22.04",
				kernelVersion: "5.15.0",
				family:        "atom",
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
			got, err := spec.GetPrecompiledImagePath(tc.args.osVersion, tc.args.kernelVersion, tc.args.family)
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
