package components

func ptr[T any](v T) *T {
	return &v
}
