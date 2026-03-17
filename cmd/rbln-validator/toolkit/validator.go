package toolkit

import "context"

type Validator interface {
	Validate(ctx context.Context, cdiRootPath string) error
}
