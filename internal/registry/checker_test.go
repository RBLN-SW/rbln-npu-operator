package registry

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-containerregistry/pkg/name"
	testregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// silentRegistry is the in-memory registry with its default access logging
// (every request, to stderr) turned off so test output stays pristine.
func silentRegistry() *httptest.Server {
	return httptest.NewServer(testregistry.New(testregistry.Logger(log.New(io.Discard, "", 0))))
}

// recordingTransport records every request it forwards, so tests can assert
// on which methods/paths actually went over the wire.
type recordingTransport struct {
	inner http.RoundTripper

	mu       sync.Mutex
	requests []*http.Request
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.requests = append(r.requests, req)
	r.mu.Unlock()

	inner := r.inner
	if inner == nil {
		inner = http.DefaultTransport
	}
	return inner.RoundTrip(req)
}

func (r *recordingTransport) seen() []*http.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*http.Request, len(r.requests))
	copy(out, r.requests)
	return out
}

func newTestRegistry(t *testing.T) string {
	t.Helper()
	srv := silentRegistry()
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func pushTestImage(t *testing.T, repoTag string) {
	t.Helper()
	img, err := random.Image(256, 1)
	if err != nil {
		t.Fatalf("random.Image() error = %v", err)
	}
	ref, err := name.ParseReference(repoTag)
	if err != nil {
		t.Fatalf("name.ParseReference(%q) error = %v", repoTag, err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatalf("remote.Write(%q) error = %v", repoTag, err)
	}
}

func TestChecker_ExistsAndNotFound(t *testing.T) {
	host := newTestRegistry(t)
	pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

	c := NewChecker(logr.Discard())

	cases := map[string]struct {
		ref  string
		want Verdict
	}{
		"ExistingTag": {
			ref:  host + "/rebellions/atom/rbln-driver:3.0.0-k-o",
			want: VerdictExists,
		},
		"MissingTagSameRepo": {
			ref:  host + "/rebellions/atom/rbln-driver:9.9.9-missing",
			want: VerdictNotFound,
		},
		"UnknownRepo": {
			ref:  host + "/rebellions/atom/does-not-exist:1.0.0",
			want: VerdictNotFound,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, cause := c.Check(context.Background(), tc.ref, nil)
			if got != tc.want {
				t.Fatalf("Check(%q) = %v, want %v", tc.ref, got, tc.want)
			}
			if wantNilCause := tc.want == VerdictExists; (cause == nil) != wantNilCause {
				t.Fatalf("Check(%q) cause = %v, want nil=%v", tc.ref, cause, wantNilCause)
			}
		})
	}
}

func TestChecker_PingNotFoundIsUnknownNotNotFound(t *testing.T) {
	// Everything 404s, including /v2/ itself -- e.g. a misrouted ingress or a
	// path-prefixed registry the caller didn't account for. Only the ping GET
	// is issued: it errors before the handshake ever reaches a manifest HEAD.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	c := NewChecker(logr.Discard())

	ref := host + "/rebellions/atom/rbln-driver:1.0.0"
	got, cause := c.Check(context.Background(), ref, nil)
	if got != VerdictUnknown {
		t.Fatalf("Check(%q) = %v, want VerdictUnknown (a ping 404 is not authoritative about the image)", ref, got)
	}
	if cause == nil {
		t.Fatalf("Check(%q) cause = nil, want the underlying ping error", ref)
	}
}

func TestChecker_HeadOnlyDiscipline(t *testing.T) {
	host := newTestRegistry(t)
	pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

	rec := &recordingTransport{}
	c := NewChecker(logr.Discard(), withTransport(rec))

	ref := host + "/rebellions/atom/rbln-driver:3.0.0-k-o"
	if got, _ := c.Check(context.Background(), ref, nil); got != VerdictExists {
		t.Fatalf("Check(%q) = %v, want VerdictExists", ref, got)
	}

	headCount := 0
	for _, req := range rec.seen() {
		if !strings.Contains(req.URL.Path, "/manifests/") {
			continue
		}
		if req.Method == http.MethodGet {
			t.Fatalf("HEAD-only discipline violated: GET %s", req.URL.Path)
		}
		if req.Method == http.MethodHead {
			headCount++
		}
	}
	if headCount < 1 {
		t.Fatal("expected at least one HEAD request to a manifests path, got 0 (assertion would be vacuous otherwise)")
	}
}

// The NotFound path must stay HEAD-only: the documented "retry with GET for
// error details" fallback would reintroduce Docker Hub pull-rate consumption
// on the one verdict this checker exists to produce.
func TestChecker_HeadOnlyDisciplineOnMissingImage(t *testing.T) {
	host := newTestRegistry(t)
	pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

	rec := &recordingTransport{}
	c := NewChecker(logr.Discard(), withTransport(rec))

	ref := host + "/rebellions/atom/rbln-driver:9.9.9-missing"
	if got, _ := c.Check(context.Background(), ref, nil); got != VerdictNotFound {
		t.Fatalf("Check(%q) = %v, want VerdictNotFound", ref, got)
	}

	headCount := 0
	for _, req := range rec.seen() {
		if !strings.Contains(req.URL.Path, "/manifests/") {
			continue
		}
		if req.Method == http.MethodGet {
			t.Fatalf("HEAD-only discipline violated on the NotFound path: GET %s", req.URL.Path)
		}
		if req.Method == http.MethodHead {
			headCount++
		}
	}
	if headCount < 1 {
		t.Fatal("expected at least one HEAD request to a manifests path, got 0 (assertion would be vacuous otherwise)")
	}
}

func TestChecker_Caching(t *testing.T) {
	t.Run("PositiveCacheSurvivesServerClose", func(t *testing.T) {
		srv := silentRegistry()
		host := strings.TrimPrefix(srv.URL, "http://")
		pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

		c := NewChecker(logr.Discard())

		ref := host + "/rebellions/atom/rbln-driver:3.0.0-k-o"
		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictExists {
			t.Fatalf("Check(%q) = %v, want VerdictExists", ref, got)
		}

		srv.Close()

		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictExists {
			t.Fatalf("Check(%q) after server close = %v, want cached VerdictExists", ref, got)
		}
	})

	t.Run("PositiveCacheExpiresToUnknown", func(t *testing.T) {
		srv := silentRegistry()
		host := strings.TrimPrefix(srv.URL, "http://")
		pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

		now := time.Now()
		c := NewChecker(logr.Discard(), withNow(func() time.Time { return now }))

		ref := host + "/rebellions/atom/rbln-driver:3.0.0-k-o"
		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictExists {
			t.Fatalf("Check(%q) = %v, want VerdictExists", ref, got)
		}

		srv.Close()

		now = now.Add(90 * time.Minute)
		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictUnknown {
			t.Fatalf("Check(%q) after positive TTL expiry = %v, want VerdictUnknown (cache expired, server down)", ref, got)
		}
	})

	t.Run("NegativeCacheExpiresToUnknown", func(t *testing.T) {
		srv := silentRegistry()
		host := strings.TrimPrefix(srv.URL, "http://")
		pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

		now := time.Now()
		c := NewChecker(logr.Discard(), withNow(func() time.Time { return now }))

		ref := host + "/rebellions/atom/rbln-driver:9.9.9-missing"
		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictNotFound {
			t.Fatalf("Check(%q) = %v, want VerdictNotFound", ref, got)
		}

		srv.Close()

		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictNotFound {
			t.Fatalf("Check(%q) after server close = %v, want cached VerdictNotFound", ref, got)
		}

		now = now.Add(6 * time.Minute)
		if got, _ := c.Check(context.Background(), ref, nil); got != VerdictUnknown {
			t.Fatalf("Check(%q) after negative TTL expiry = %v, want VerdictUnknown (cache expired, server down)", ref, got)
		}
	})
}

func TestChecker_UnauthorizedIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="x"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	c := NewChecker(logr.Discard())

	ref := host + "/rebellions/atom/rbln-driver:1.0.0"
	got, cause := c.Check(context.Background(), ref, nil)
	if got != VerdictUnknown {
		t.Fatalf("Check(%q) = %v, want VerdictUnknown", ref, got)
	}
	if cause == nil {
		t.Fatalf("Check(%q) cause = nil, want the underlying auth error", ref)
	}
}

func TestChecker_UnreachableHostIsUnknown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("ln.Close() error = %v", err)
	}

	c := NewChecker(logr.Discard())

	ref := addr + "/rebellions/atom/rbln-driver:1.0.0"
	got, cause := c.Check(context.Background(), ref, nil)
	if got != VerdictUnknown {
		t.Fatalf("Check(%q) = %v, want VerdictUnknown", ref, got)
	}
	if cause == nil {
		t.Fatalf("Check(%q) cause = nil, want the underlying dial error", ref)
	}
}

func TestChecker_StalledRegistryTimesOut(t *testing.T) {
	const stallTimeout = 50 * time.Millisecond

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/manifests/") {
			// 10x checkTimeout: comfortable margin against scheduling jitter
			// while keeping httptest.Server.Close()'s wait for this handler
			// to finish (it blocks on in-flight requests) short.
			time.Sleep(10 * stallTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	c := NewChecker(logr.Discard())
	c.checkTimeout = stallTimeout

	ref := host + "/rebellions/atom/rbln-driver:1.0.0"
	start := time.Now()
	got, cause := c.Check(context.Background(), ref, nil)
	elapsed := time.Since(start)

	if got != VerdictUnknown {
		t.Fatalf("Check(%q) = %v, want VerdictUnknown", ref, got)
	}
	if cause == nil {
		t.Fatalf("Check(%q) cause = nil, want the underlying timeout error", ref)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Check(%q) took %v, want well under 1s given checkTimeout=50ms", ref, elapsed)
	}
}

func TestChecker_ConcurrentCheckSameRef(t *testing.T) {
	host := newTestRegistry(t)
	pushTestImage(t, host+"/rebellions/atom/rbln-driver:3.0.0-k-o")

	c := NewChecker(logr.Discard())
	ref := host + "/rebellions/atom/rbln-driver:3.0.0-k-o"

	const n = 8
	results := make([]Verdict, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, _ := c.Check(context.Background(), ref, nil)
			results[i] = v
		}(i)
	}
	wg.Wait()

	for i, v := range results {
		if v != VerdictExists {
			t.Fatalf("goroutine %d: Check(%q) = %v, want VerdictExists", i, ref, v)
		}
	}
}

func TestChecker_Disabled(t *testing.T) {
	rec := &recordingTransport{}
	c := NewChecker(logr.Discard(), WithDisabled(true), withTransport(rec))

	ref := "example.com/rebellions/atom/rbln-driver:1.0.0"
	got, cause := c.Check(context.Background(), ref, nil)
	if got != VerdictSkipped {
		t.Fatalf("Check(%q) = %v, want VerdictSkipped", ref, got)
	}
	if cause != nil {
		t.Fatalf("Check(%q) cause = %v, want nil (disabled means no attempt was made)", ref, cause)
	}
	if n := len(rec.seen()); n != 0 {
		t.Fatalf("disabled Checker made %d requests, want 0", n)
	}
}

func TestChecker_MalformedRefIsUnknown(t *testing.T) {
	c := NewChecker(logr.Discard())

	got, cause := c.Check(context.Background(), "  not a valid ref ::", nil)
	if got != VerdictUnknown {
		t.Fatalf("Check() = %v, want VerdictUnknown", got)
	}
	if cause == nil {
		t.Fatalf("Check() cause = nil, want the underlying parse error")
	}
}

func TestVerdict_String(t *testing.T) {
	cases := map[string]struct {
		v    Verdict
		want string
	}{
		"Unknown":    {v: VerdictUnknown, want: "unknown"},
		"Exists":     {v: VerdictExists, want: "exists"},
		"NotFound":   {v: VerdictNotFound, want: "notFound"},
		"Skipped":    {v: VerdictSkipped, want: "skipped"},
		"OutOfRange": {v: Verdict(99), want: "unknown"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.v.String(); got != tc.want {
				t.Fatalf("Verdict(%d).String() = %q, want %q", tc.v, got, tc.want)
			}
		})
	}
}
