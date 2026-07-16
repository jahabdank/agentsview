package parser

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryDiskMapGetDistinguishesAbsenceAndFailure(t *testing.T) {
	index, err := newDiscoveryDiskMap()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.Remove(index.path)
		_ = os.Remove(index.path + "-wal")
		_ = os.Remove(index.path + "-shm")
	})

	value, found, err := index.get(t.Context(), "missing")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, value)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = index.get(ctx, "missing")
	assert.ErrorIs(t, err, context.Canceled)

	require.NoError(t, index.db.Close())
	_, _, err = index.get(t.Context(), "missing")
	require.Error(t, err)
	assert.NotErrorIs(t, err, context.Canceled)
}

func TestDiscoveryDiskMapCloseReportsCleanupFailure(t *testing.T) {
	index, err := newDiscoveryDiskMap()
	require.NoError(t, err)
	injected := errors.New("remove discovery index failed")
	index.remove = func(path string) error {
		if path == index.path {
			return injected
		}
		return os.Remove(path)
	}
	t.Cleanup(func() { _ = os.Remove(index.path) })

	err = index.close()

	assert.ErrorIs(t, err, injected)
}
