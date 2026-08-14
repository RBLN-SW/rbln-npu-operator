package registry

import (
	"encoding/base64"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

type fakeResource string

func (f fakeResource) String() string      { return string(f) }
func (f fakeResource) RegistryStr() string { return string(f) }

func dockerConfigJSONSecret(auths string) corev1.Secret {
	return corev1.Secret{
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: []byte(`{"auths":` + auths + `}`),
		},
	}
}

func dockercfgSecret(auths string) corev1.Secret {
	return corev1.Secret{
		Type: corev1.SecretTypeDockercfg,
		Data: map[string][]byte{
			corev1.DockerConfigKey: []byte(auths),
		},
	}
}

func TestKeychainFromPullSecrets(t *testing.T) {
	cases := map[string]struct {
		secrets      []corev1.Secret
		host         string
		wantUsername string
		wantPassword string
	}{
		"NoSecretsIsAnonymous": {
			secrets: nil,
			host:    "registry.example.com",
		},
		"DockerConfigJSONMatch": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"registry.example.com":{"username":"u","password":"p"}}`)},
			host:         "registry.example.com",
			wantUsername: "u",
			wantPassword: "p",
		},
		"UnknownHostIsAnonymous": {
			secrets: []corev1.Secret{dockerConfigJSONSecret(`{"registry.example.com":{"username":"u","password":"p"}}`)},
			host:    "other.example.com",
		},
		"DockerHubAliasHTTPS": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"https://index.docker.io/v1/":{"username":"hub","password":"pw"}}`)},
			host:         "index.docker.io",
			wantUsername: "hub",
			wantPassword: "pw",
		},
		"DockerHubAliasHTTP": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"http://index.docker.io/v1/":{"username":"hub2","password":"pw2"}}`)},
			host:         "index.docker.io",
			wantUsername: "hub2",
			wantPassword: "pw2",
		},
		"DockerHubBareDockerIOKeyMatchesCanonicalHost": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"docker.io":{"username":"hub3","password":"pw3"}}`)},
			host:         "index.docker.io",
			wantUsername: "hub3",
			wantPassword: "pw3",
		},
		"DockerHubRegistry1KeyMatchesCanonicalHost": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"registry-1.docker.io":{"username":"hub4","password":"pw4"}}`)},
			host:         "index.docker.io",
			wantUsername: "hub4",
			wantPassword: "pw4",
		},
		"LegacyDockercfg": {
			secrets:      []corev1.Secret{dockercfgSecret(`{"registry.example.com":{"username":"u2","password":"p2"}}`)},
			host:         "registry.example.com",
			wantUsername: "u2",
			wantPassword: "p2",
		},
		"MalformedJSONSkippedButOthersStillWork": {
			secrets: []corev1.Secret{
				dockerConfigJSONSecret(`{not-json`),
				dockerConfigJSONSecret(`{"registry.example.com":{"username":"u3","password":"p3"}}`),
			},
			host:         "registry.example.com",
			wantUsername: "u3",
			wantPassword: "p3",
		},
		"AuthOnlyBase64Decoded": {
			secrets: []corev1.Secret{dockerConfigJSONSecret(
				`{"registry.example.com":{"auth":"` + base64.StdEncoding.EncodeToString([]byte("u4:p4")) + `"}}`,
			)},
			host:         "registry.example.com",
			wantUsername: "u4",
			wantPassword: "p4",
		},
		"NormalizedHostSchemeStripped": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"https://repo.rebellions.ai":{"username":"u5","password":"p5"}}`)},
			host:         "repo.rebellions.ai",
			wantUsername: "u5",
			wantPassword: "p5",
		},
		"NormalizedHostDefaultHTTPSPortStripped": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"repo.rebellions.ai:443":{"username":"u6","password":"p6"}}`)},
			host:         "repo.rebellions.ai",
			wantUsername: "u6",
			wantPassword: "p6",
		},
		"NormalizedHostPathStripped": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"repo.rebellions.ai/rebellions":{"username":"u7","password":"p7"}}`)},
			host:         "repo.rebellions.ai",
			wantUsername: "u7",
			wantPassword: "p7",
		},
		"NormalizedHostCaseInsensitive": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"RePo.ReBeLLions.AI":{"username":"u8","password":"p8"}}`)},
			host:         "repo.rebellions.ai",
			wantUsername: "u8",
			wantPassword: "p8",
		},
		"NonDefaultPortMatchesOnlySamePort": {
			secrets:      []corev1.Secret{dockerConfigJSONSecret(`{"repo.rebellions.ai:5000":{"username":"u9","password":"p9"}}`)},
			host:         "repo.rebellions.ai:5000",
			wantUsername: "u9",
			wantPassword: "p9",
		},
		"NonDefaultPortDoesNotMatchBareHost": {
			secrets: []corev1.Secret{dockerConfigJSONSecret(`{"repo.rebellions.ai:5000":{"username":"u9","password":"p9"}}`)},
			host:    "repo.rebellions.ai",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			kc := keychainFromPullSecrets(tc.secrets)

			auth, err := kc.Resolve(fakeResource(tc.host))
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}

			cfg, err := auth.Authorization()
			if err != nil {
				t.Fatalf("Authorization() error = %v", err)
			}
			if cfg.Username != tc.wantUsername || cfg.Password != tc.wantPassword {
				t.Fatalf("Authorization() = %+v, want username=%q password=%q", cfg, tc.wantUsername, tc.wantPassword)
			}
		})
	}
}

func TestCanonicalRegistryHost_DeterministicWinner(t *testing.T) {
	// Two raw keys that canonicalize to the same host within one secret: the
	// result must be stable across runs, not whichever Go's map iteration
	// happened to visit last.
	secrets := []corev1.Secret{dockerConfigJSONSecret(
		`{"https://repo.rebellions.ai":{"username":"first","password":"first"},"repo.rebellions.ai:443":{"username":"second","password":"second"}}`,
	)}

	var lastUsername string
	for i := 0; i < 20; i++ {
		kc := keychainFromPullSecrets(secrets)
		auth, err := kc.Resolve(fakeResource("repo.rebellions.ai"))
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		cfg, err := auth.Authorization()
		if err != nil {
			t.Fatalf("Authorization() error = %v", err)
		}
		if i == 0 {
			lastUsername = cfg.Username
			continue
		}
		if cfg.Username != lastUsername {
			t.Fatalf("iteration %d: username = %q, want stable %q across repeated construction", i, cfg.Username, lastUsername)
		}
	}
}
