package doctor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestResolve(t *testing.T) {
	db := objmock.NewStore()
	rs, close := refmock.NewStore(t)
	defer close()
	d := NewDoctor(db, rs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, errCh, err := d.Diagnose(ctx, []string{"heads/"}, nil)
	require.NoError(t, err)

	for refIssues := range ch {
		require.NoError(t, d.Resolve(refIssues.Issues))
	}

	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
}
