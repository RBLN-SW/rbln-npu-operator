package components

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

func ptr[T any](v T) *T {
	return &v
}

func ComposeImageReference(registry, image string) string {
	registry = strings.TrimSuffix(strings.TrimSpace(registry), "/")
	image = strings.TrimPrefix(strings.TrimSpace(image), "/")
	return fmt.Sprintf("%s/%s", registry, image)
}

func GetObjectHash(obj any) string {
	raw, err := json.Marshal(obj)
	if err != nil {
		raw = fmt.Appendf(nil, "%#v", obj)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
