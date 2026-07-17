package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/sync"
)

// openTestWriteDB opens a writable DB and acquires the write-owner lock for cfg,
// cleaning both up at test end.
func openTestWriteDB(t *testing.T, cfg config.Config) (*db.DB, *writeOwnerLock) {
	t.Helper()
	database, lock, err := openWriteDB(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { closeWriteDB(database, lock) })
	return database, lock
}

// writeOneSession attempts a single write through the DB writer pool.
func writeOneSession(database *db.DB) error {
	return database.UpsertSession(db.Session{
		ID:      "worker-pass-write",
		Project: "handoff",
		Machine: "local",
		Agent:   "claude",
	})
}

// stubLaunchSyncWorker swaps the launchSyncWorker seam for a test double and
// restores it when the returned func runs.
func stubLaunchSyncWorker(
	t *testing.T,
	fn func(
		context.Context, config.Config, string, func(workerLine),
	) (workerResult, error),
) func() {
	t.Helper()
	prev := launchSyncWorker
	launchSyncWorker = fn
	return func() { launchSyncWorker = prev }
}

func TestRunWorkerWritePassYieldsWriteOwnership(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, lock := openTestWriteDB(t, cfg)
	engine := sync.NewEngine(database, sync.EngineConfig{})
	defer engine.Close()

	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, mode string, _ func(workerLine),
	) (workerResult, error) {
		assert.Equal(t, "audit", mode)
		assert.False(t, lock.Held(), "flock released before spawn")
		return workerResult{Status: "ok", DiscoveryComplete: true}, nil
	})
	defer restore()

	result, err := runWorkerWritePass(
		context.Background(), cfg, engine, database, lock, "audit", nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "ok", result.Status)
	assert.True(t, lock.Held(), "flock reacquired after the worker")
	assert.NoError(t, writeOneSession(database), "writer reopened")
}

func TestRunWorkerWritePassReacquiresOnWorkerFailure(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, lock := openTestWriteDB(t, cfg)
	engine := sync.NewEngine(database, sync.EngineConfig{})
	defer engine.Close()

	workerErr := errors.New("worker boom")
	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, _ string, _ func(workerLine),
	) (workerResult, error) {
		return workerResult{Status: "failed"}, workerErr
	})
	defer restore()

	result, err := runWorkerWritePass(
		context.Background(), cfg, engine, database, lock, "audit", nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, workerErr, "worker error must propagate")
	assert.Equal(t, "failed", result.Status)
	assert.True(t, lock.Held(), "flock reacquired even after worker failure")
	assert.NoError(t, writeOneSession(database),
		"writer reopened even after worker failure")
}

func TestReadWorkerResultRequiresExactlyOneResult(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   string
		wantLines int
	}{
		{
			name: "single result",
			input: `{"progress":{"phase":"syncing"}}` + "\n" +
				`{"result":{"status":"ok","discoveryComplete":true}}` + "\n",
			wantLines: 2,
		},
		{
			name:      "zero results is a protocol failure",
			input:     `{"progress":{"phase":"syncing"}}` + "\n",
			wantErr:   "0 terminal results",
			wantLines: 1,
		},
		{
			name: "duplicate results is a protocol failure",
			input: `{"result":{"status":"ok","discoveryComplete":true}}` + "\n" +
				`{"result":{"status":"ok","discoveryComplete":true}}` + "\n",
			wantErr:   "2 terminal results",
			wantLines: 2,
		},
		{
			name: "malformed line is a protocol failure",
			input: "not json\n" +
				`{"result":{"status":"ok","discoveryComplete":true}}` + "\n",
			wantErr:   "malformed",
			wantLines: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := 0
			result, err := readWorkerResult(
				strings.NewReader(tt.input),
				func(workerLine) { seen++ },
			)
			assert.Equal(t, tt.wantLines, seen, "forwarded line count")
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "ok", result.Status)
			assert.True(t, result.DiscoveryComplete)
		})
	}
}

func TestSyncWorkerAcquiresWriteLockWhenDaemonYielded(t *testing.T) {
	dataDir, cfg := writeDBConfigForTest(t)
	t.Setenv(syncWorkerChildEnvVar, "1")
	_, err := WriteDaemonRuntime(dataDir, "127.0.0.1", 9, "test", false)
	require.NoError(t, err)
	t.Cleanup(func() { RemoveDaemonRuntime(dataDir) })

	// The daemon lives but has yielded: writer closed, write lock free.
	database, release, err := openWorkerWriteDB(cfg)
	require.NoError(t, err,
		"worker must acquire while a yielded daemon still lives")
	release()
	database.Close()
}

func TestSyncWorkerRefusedWhenDaemonHoldsWriteLock(t *testing.T) {
	dataDir, cfg := writeDBConfigForTest(t)
	t.Setenv(syncWorkerChildEnvVar, "1")
	_, err := WriteDaemonRuntime(dataDir, "127.0.0.1", 9, "test", false)
	require.NoError(t, err)
	t.Cleanup(func() { RemoveDaemonRuntime(dataDir) })
	holdWriteOwnerLockForTest(t, dataDir) // daemon has NOT yielded the flock

	_, _, err = openWorkerWriteDB(cfg)
	require.Error(t, err,
		"worker must be refused while the daemon still holds the write lock")
	assert.ErrorContains(t, err, "write lock")
}
