package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/sync"
)

// TestArchiveAuditRetriesWithBackoffOnFailure drives the audit schedule with a
// wait seam that records delays and a stubbed worker that fails six times then
// succeeds. It asserts the obligation is retained (attempts continue), the retry
// delay doubles and caps at archiveAuditInterval, success returns to the daily
// cadence, and no in-process sync ever runs.
func TestArchiveAuditRetriesWithBackoffOnFailure(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, lock := openTestWriteDB(t, cfg)
	engine := sync.NewEngine(database, workerEngineConfig(cfg))
	t.Cleanup(engine.Close)
	em := &scopedEmitter{scopes: make(chan string, 8)}

	attempts := 0
	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, mode string, _ func(workerLine),
	) (workerResult, error) {
		assert.Equal(t, "audit", mode)
		attempts++
		if attempts <= 6 {
			return workerResult{Status: "failed"}, errors.New("audit boom")
		}
		return workerResult{Status: "ok", Synced: 1, DiscoveryComplete: true}, nil
	})
	defer restore()

	var delays []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	wait := func(ctx context.Context, d time.Duration) bool {
		delays = append(delays, d)
		return ctx.Err() == nil
	}
	audit := func(ctx context.Context) bool {
		ok := runArchiveAudit(ctx, cfg, engine, database, lock, em) == nil
		if attempts >= 7 {
			cancel()
		}
		return ok
	}

	runArchiveAuditLoop(ctx, wait, audit)

	assert.Equal(t, 7, attempts, "the audit obligation is retained across failures")
	require.GreaterOrEqual(t, len(delays), 8)
	assert.Equal(t, []time.Duration{
		archiveAuditInterval, // initial daily wait
		1 * time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour,
		16 * time.Hour, archiveAuditInterval, // backoff doubles then caps at 24h
		archiveAuditInterval, // success returns to the daily cadence
	}, delays[:8])
	assert.True(t, engine.LastSync().IsZero(),
		"the audit must never run an in-process sync pass")
}

// TestArchiveAuditEmitsOnDataChange asserts a successful audit that changed data
// emits the sessions scope so connected clients refresh.
func TestArchiveAuditEmitsOnDataChange(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, lock := openTestWriteDB(t, cfg)
	engine := sync.NewEngine(database, workerEngineConfig(cfg))
	t.Cleanup(engine.Close)
	em := &scopedEmitter{scopes: make(chan string, 1)}

	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, _ string, _ func(workerLine),
	) (workerResult, error) {
		return workerResult{Status: "ok", Synced: 2, DiscoveryComplete: true}, nil
	})
	defer restore()

	require.NoError(t, runArchiveAudit(
		context.Background(), cfg, engine, database, lock, em,
	))
	select {
	case scope := <-em.scopes:
		assert.Equal(t, "sessions", scope)
	default:
		require.FailNow(t, "an audit with Synced>0 must emit the sessions scope")
	}
}

// TestArchiveAuditSurfacesWorkerFailureWithoutFallback proves the no-fallback
// rule: a failed worker audit returns an error (so the caller retries), runs no
// in-process sync, and emits nothing.
func TestArchiveAuditSurfacesWorkerFailureWithoutFallback(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, lock := openTestWriteDB(t, cfg)
	engine := sync.NewEngine(database, workerEngineConfig(cfg))
	t.Cleanup(engine.Close)
	em := &scopedEmitter{scopes: make(chan string, 1)}

	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, _ string, _ func(workerLine),
	) (workerResult, error) {
		return workerResult{Status: "failed"}, errors.New("audit boom")
	})
	defer restore()

	err := runArchiveAudit(context.Background(), cfg, engine, database, lock, em)
	require.Error(t, err, "a failed audit must surface the error for retry")
	assert.True(t, engine.LastSync().IsZero(),
		"a failed audit must not fall back to an in-process sync")
	select {
	case <-em.scopes:
		require.FailNow(t, "a failed audit must not emit")
	default:
	}
}

// TestArchiveAuditLoopStopsOnContextCancel guards the shutdown path: a cancelled
// context ends the loop without running an audit.
func TestArchiveAuditLoopStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	audited := false
	runArchiveAuditLoop(ctx,
		func(context.Context, time.Duration) bool { return false },
		func(context.Context) bool { audited = true; return true },
	)
	assert.False(t, audited, "a cancelled context must stop the loop before auditing")
}

// TestSyncWorkerAuditModeRunsSyncPass confirms the audit worker mode shares the
// sync body: it performs a full pass and emits an ok terminal result.
func TestSyncWorkerAuditModeRunsSyncPass(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	var out bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, "audit", &out))
	result := decodeSingleResult(t, &out)
	assert.Equal(t, "ok", result.Status)
	assert.True(t, result.DiscoveryComplete)
	assert.Equal(t, 3, result.Synced)
}
