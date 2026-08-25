package registry

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	corev1 "k8s.io/api/core/v1"
)

// Verdict is the outcome of checking whether an image exists in its registry.
// The zero value is VerdictUnknown, so a Verdict left unset defaults to the
// safe "proceed, kubelet decides" behavior rather than the hard-fail NotFound.
type Verdict int

const (
	VerdictUnknown Verdict = iota
	VerdictExists
	// VerdictNotFound is the only hard-fail signal: the registry
	// authoritatively reported the repository or tag does not exist.
	VerdictNotFound
	VerdictSkipped
)

func (v Verdict) String() string {
	switch v {
	case VerdictExists:
		return "exists"
	case VerdictNotFound:
		return "notFound"
	case VerdictSkipped:
		return "skipped"
	default:
		return "unknown"
	}
}

const (
	positiveTTL = time.Hour
	// NegativeTTL is exported so a caller polling for recovery from
	// VerdictNotFound can align its requeue cadence with this cache's own
	// lifetime -- polling faster would just re-hit the cache. It also caps how
	// often an unverifiable ref re-pays defaultCheckTimeout, and how often the
	// cache-miss path logs, since driver reconciles are serialized.
	NegativeTTL = 5 * time.Minute

	// defaultCheckTimeout bounds one Check so a stalled registry cannot wedge
	// the serialized reconciler. A timeout collapses to VerdictUnknown;
	// kubelet stays the final arbiter on whether the image exists.
	defaultCheckTimeout = 10 * time.Second
)

type cacheEntry struct {
	verdict Verdict
	// cause is text, not the original error: that error carries a
	// *transport.Error retaining its *http.Request -- Authorization header
	// included -- for the entry's whole TTL (up to an hour). No caller
	// unwraps it, so a plain error is rebuilt on read.
	cause     string
	expiresAt time.Time
}

// Checker checks driver image existence in its registry via a manifest HEAD
// request, caching verdicts so a reconcile loop doesn't hit the registry on
// every pass.
type Checker struct {
	mu sync.Mutex
	// cache is keyed by image ref only, not pull secrets: a credential change
	// for the same ref rides out the TTL by design, trading a bounded window
	// of staleness for not hashing/comparing secret contents on every check.
	cache map[string]cacheEntry

	disabled     bool
	transport    http.RoundTripper
	log          logr.Logger
	now          func() time.Time
	checkTimeout time.Duration
}

// Option configures a Checker.
type Option func(*Checker)

func NewChecker(log logr.Logger, opts ...Option) *Checker {
	c := &Checker{
		cache:        make(map[string]cacheEntry),
		log:          log,
		now:          time.Now,
		checkTimeout: defaultCheckTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func WithDisabled(disabled bool) Option {
	return func(c *Checker) { c.disabled = disabled }
}

// withTransport is a test seam.
func withTransport(t http.RoundTripper) Option {
	return func(c *Checker) { c.transport = t }
}

// withNow is a test seam for injecting a controllable clock into TTL bookkeeping.
func withNow(now func() time.Time) Option {
	return func(c *Checker) { c.now = now }
}

// Check performs a manifest HEAD -- never GET: Docker Hub counts GET manifest
// requests toward pull rate limits; HEAD is exempt. The returned error is the
// underlying cause when the verdict is VerdictUnknown or VerdictNotFound, and
// nil for VerdictExists/VerdictSkipped.
func (c *Checker) Check(ctx context.Context, imageRef string, pullSecrets []corev1.Secret) (Verdict, error) {
	if c.disabled {
		return VerdictSkipped, nil
	}

	if entry, ok := c.cached(imageRef); ok {
		return entry.verdict, causeError(entry.cause)
	}

	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return c.record(imageRef, VerdictUnknown, err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.checkTimeout)
	defer cancel()

	opts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(keychainFromPullSecrets(pullSecrets)),
	}
	if c.transport != nil {
		opts = append(opts, remote.WithTransport(c.transport))
	}

	_, headErr := remote.Head(ref, opts...)
	return c.record(imageRef, verdictFromError(headErr), headErr)
}

func (c *Checker) cached(imageRef string) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.cache[imageRef]
	if !ok {
		return cacheEntry{}, false
	}
	if !c.now().Before(entry.expiresAt) {
		delete(c.cache, imageRef) // maps never shrink; drop expired entries instead of accumulating them
		return cacheEntry{}, false
	}
	return entry, true
}

// record caches a fresh verdict and, for VerdictUnknown, warns about it here
// rather than at the call site: only a cache miss reaches this function, so the
// TTL doubles as the log's rate limit. Logging per call instead would emit a
// line on every reconcile pass for as long as the registry stays unreachable.
func (c *Checker) record(imageRef string, v Verdict, cause error) (Verdict, error) {
	causeText := ""
	if cause != nil {
		causeText = cause.Error()
	}
	if v == VerdictUnknown {
		c.log.Info("Could not verify whether the driver image exists in its registry",
			"image", imageRef, "error", cause,
			"effect", "pool rendered without image existence verification; kubelet decides at pull time")
	}
	c.mu.Lock()
	c.cache[imageRef] = cacheEntry{verdict: v, cause: causeText, expiresAt: c.now().Add(ttlFor(v))}
	c.mu.Unlock()
	return v, cause
}

// causeError reconstructs a plain error from a cacheEntry's cause text, or
// nil if there was none.
func causeError(cause string) error {
	if cause == "" {
		return nil
	}
	return errors.New(cause)
}

func ttlFor(v Verdict) time.Duration {
	if v == VerdictExists {
		return positiveTTL
	}
	return NegativeTTL
}

// verdictFromError classifies a remote.Head error via errors.As, never string
// matching: message text isn't part of any registry's API contract. NotFound
// is scoped to the HEAD itself -- a 404 from an earlier handshake step (a
// misrouted ingress answering the /v2/ ping) says nothing about the image.
func verdictFromError(err error) Verdict {
	if err == nil {
		return VerdictExists
	}

	var terr *transport.Error
	if errors.As(err, &terr) && terr != nil && terr.Request != nil && terr.Request.Method == http.MethodHead {
		if terr.StatusCode == http.StatusNotFound {
			return VerdictNotFound
		}
	}
	return VerdictUnknown
}
