// ABOUTME: Tests the bounded recovery path for abandoned daemon-launch syncs.
// ABOUTME: Uses a scratch database and synthetic timeout without starting a daemon.
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/dbtest"
	syncpkg "go.kenn.io/agentsview/internal/sync"
)

func TestRunDeferredStartupSyncFallbackPerformsSkippedSync(t *testing.T) {
	database := dbtest.OpenTestDB(t)
	reconciled := make(chan struct{}, 1)
	engine := syncpkg.NewEngine(database, syncpkg.EngineConfig{
		Machine:                 "local",
		DeferStartupMaintenance: true,
		OnStartupReconciled: func(syncpkg.SyncStats, error) {
			reconciled <- struct{}{}
		},
	})
	t.Cleanup(engine.Close)
	timeout := make(chan time.Time, 1)
	timeout <- time.Now()

	// Under a test binary the worker path is skipped (testing.Testing()), so
	// the fallback runs the in-process sync; database/lock stay unused.
	ran, err := runDeferredStartupSyncFallback(
		t.Context(), config.Config{}, engine, database, nil, nil, timeout,
	)

	require.NoError(t, err)
	assert.True(t, ran)
	assert.False(t, engine.LastSyncStartedAt().IsZero(),
		"timeout fallback must perform the skipped local sync")
	select {
	case <-reconciled:
	case <-time.After(time.Second):
		require.FailNow(t, "deferred fallback did not reconcile watcher startup")
	}
}
