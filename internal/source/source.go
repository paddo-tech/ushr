package source

import (
	"context"

	"github.com/paddo-tech/ushr/internal/domain"
)

// Source streams queued jobs from an external system (e.g. GitHub Actions).
//
// Subscribe returns a channel that closes when ctx is cancelled. Implementations
// may emit duplicate Jobs; consumers should dedupe by Job identity.
type Source interface {
	Subscribe(ctx context.Context) (<-chan domain.Job, error)
}
