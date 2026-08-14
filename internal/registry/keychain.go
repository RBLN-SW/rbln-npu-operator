package registry

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	corev1 "k8s.io/api/core/v1"
)

// dockerConfigJSON mirrors the ".dockerconfigjson" secret payload. Reusing
// authn.AuthConfig here gets base64-only "auth" field decoding for free from
// its custom UnmarshalJSON, instead of reimplementing that decode.
type dockerConfigJSON struct {
	Auths map[string]authn.AuthConfig `json:"auths"`
}

// pullSecretKeychain resolves registry auth from parsed Kubernetes pull
// secrets. Anonymous is the fallback for every miss (unknown host, no
// secrets, or a secret that failed to parse) so a broken secret never blocks
// a check harder than having no secret at all.
type pullSecretKeychain struct {
	entries map[string]authn.AuthConfig
}

func keychainFromPullSecrets(secrets []corev1.Secret) authn.Keychain {
	entries := make(map[string]authn.AuthConfig)
	for _, secret := range secrets {
		raw := authEntries(secret)
		hosts := make([]string, 0, len(raw))
		for host := range raw {
			hosts = append(hosts, host)
		}
		sort.Strings(hosts) // deterministic winner when raw keys collide after canonicalization
		for _, host := range hosts {
			entries[canonicalRegistryHost(host)] = raw[host]
		}
	}
	return &pullSecretKeychain{entries: entries}
}

func authEntries(secret corev1.Secret) map[string]authn.AuthConfig {
	switch secret.Type {
	case corev1.SecretTypeDockerConfigJson:
		data, ok := secret.Data[corev1.DockerConfigJsonKey]
		if !ok {
			return nil
		}
		var cfg dockerConfigJSON
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil
		}
		return cfg.Auths
	case corev1.SecretTypeDockercfg:
		data, ok := secret.Data[corev1.DockerConfigKey]
		if !ok {
			return nil
		}
		var auths map[string]authn.AuthConfig
		if err := json.Unmarshal(data, &auths); err != nil {
			return nil
		}
		return auths
	default:
		return nil
	}
}

// canonicalRegistryHost normalizes a dockerconfigjson key or a resolved
// registry string to a bare, lowercase host[:port], so entries match
// regardless of scheme, trailing path, or case -- which also folds the
// historical Docker Hub key forms (https://index.docker.io/v1/) into one.
func canonicalRegistryHost(host string) string {
	host = strings.ToLower(host)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	host = strings.TrimSuffix(host, ":443")
	host = strings.TrimSuffix(host, ":80")
	// Refs are rewritten to index.docker.io on the ref side only, while
	// kubelet's keyring matches all three Hub spellings: without the same
	// aliasing here, a secret keyed docker.io resolves anonymous even though
	// the real pull authenticates.
	if host == "docker.io" || host == "registry-1.docker.io" {
		return "index.docker.io"
	}
	return host
}

func (k *pullSecretKeychain) Resolve(target authn.Resource) (authn.Authenticator, error) {
	cfg, ok := k.entries[canonicalRegistryHost(target.RegistryStr())]
	if !ok {
		return authn.Anonymous, nil
	}
	return authn.FromConfig(cfg), nil
}
