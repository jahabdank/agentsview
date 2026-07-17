//go:build darwin && cgo

package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVnodeObserverWakesOnEntryCreation(t *testing.T) {
	dir := t.TempDir()
	woke := make(chan struct{}, 1)
	o, err := newVnodeObserver(func() {
		select {
		case woke <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)
	defer o.Close()
	require.NoError(t, o.Add(dir))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "child"), 0o755))
	select {
	case <-woke:
	case <-time.After(30 * time.Second):
		t.Fatal("no wake after directory entry creation")
	}
}

func TestVnodeObserverUsesOneDescriptorRegardlessOfEntries(t *testing.T) {
	dir := t.TempDir()
	for i := range 200 {
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, fmt.Sprintf("f%03d", i)), nil, 0o644))
	}
	o, err := newVnodeObserver(func() {})
	require.NoError(t, err)
	defer o.Close()
	require.NoError(t, o.Add(dir))
	assert.Equal(t, 1, o.watchedCount(),
		"observer must hold one descriptor per directory, not per entry")
}

func TestVnodeObserverRemoveAndCloseAreIdempotent(t *testing.T) {
	dir := t.TempDir()
	o, err := newVnodeObserver(func() {})
	require.NoError(t, err)
	require.NoError(t, o.Add(dir))
	require.NoError(t, o.Remove(dir))
	require.NoError(t, o.Remove(dir)) // second remove is a no-op
	require.NoError(t, o.Close())
	require.NoError(t, o.Close())
}
