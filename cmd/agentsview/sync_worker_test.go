package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
)

// testConfigWithClaudeFixture builds a config pointing at a temp data dir and a
// Claude projects dir seeded with three parseable sessions.
func testConfigWithClaudeFixture(t *testing.T) config.Config {
	t.Helper()
	dataDir := t.TempDir()
	claudeDir := t.TempDir()
	for i := range 3 {
		projDir := filepath.Join(claudeDir, fmt.Sprintf("-home-proj%d", i))
		require.NoError(t, os.MkdirAll(projDir, 0o755))
		content := testjsonl.NewSessionBuilder().
			AddClaudeUser("2026-01-01T00:00:00Z", "hello").
			AddClaudeAssistant("2026-01-01T00:00:01Z", "hi").
			String()
		require.NoError(t, os.WriteFile(
			filepath.Join(projDir, fmt.Sprintf("session%d.jsonl", i)),
			[]byte(content), 0o644,
		))
	}
	return config.Config{
		DataDir:          dataDir,
		DBPath:           filepath.Join(dataDir, "sessions.db"),
		LocalMachineName: "local",
		AgentDirs: map[parser.AgentType][]string{
			parser.AgentClaude: {claudeDir},
		},
	}
}

// decodeSingleResult scans NDJSON worker output and returns the sole terminal
// result, failing the test if the count is not exactly one.
func decodeSingleResult(t *testing.T, out *bytes.Buffer) workerResult {
	t.Helper()
	var results []workerResult
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		var line workerLine
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line),
			"every stdout line must be a workerLine JSON object")
		if line.Result != nil {
			results = append(results, *line.Result)
		}
	}
	require.NoError(t, sc.Err())
	require.Len(t, results, 1, "exactly one terminal result")
	return results[0]
}

func TestSyncWorkerStartupModeSyncsAndEmitsTerminalResult(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	var out bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, "startup", &out))

	var results []workerResult
	sawProgress := false
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var line workerLine
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line),
			"every stdout line must be a workerLine JSON object")
		if line.Progress != nil {
			sawProgress = true
		}
		if line.Result != nil {
			results = append(results, *line.Result)
		}
	}
	require.NoError(t, sc.Err())
	assert.True(t, sawProgress)
	require.Len(t, results, 1, "exactly one terminal result")
	assert.Equal(t, "ok", results[0].Status)
	assert.True(t, results[0].DiscoveryComplete)
	assert.Equal(t, 3, results[0].Synced)
}

func TestSyncWorkerReportsAbortAsFailure(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // aborted before work starts
	var out bytes.Buffer
	err := runSyncWorkerContext(ctx, cfg, "startup", &out)
	require.Error(t, err, "aborted work must not exit zero")
	result := decodeSingleResult(t, &out)
	assert.Equal(t, "aborted", result.Status)
	assert.False(t, result.DiscoveryComplete)
}

func TestSyncWorkerFailsWhenWriteLockHeld(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	holdWriteOwnerLockForTest(t, cfg.DataDir) // hold db.write.lock like a daemon
	var out bytes.Buffer
	err := runSyncWorker(cfg, "startup", &out)
	require.Error(t, err)
	assert.ErrorContains(t, err, "write lock")
}

func TestSyncWorkerRejectsUnknownMode(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	var out bytes.Buffer
	err := runSyncWorker(cfg, "bogus", &out)
	require.Error(t, err)
	assert.ErrorContains(t, err, "unknown sync-worker mode")
}

func TestSyncWorkerResyncBuildModeBuildsReplacement(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	// Seed the archive the worker rebuilds from, then close it so the worker
	// opens it read-only exactly as it does under the daemon's write barrier.
	database, err := db.Open(cfg.DBPath)
	require.NoError(t, err)
	engine := sync.NewEngine(database, workerEngineConfig(cfg))
	require.Equal(t, 3, engine.SyncAll(context.Background(), nil).Synced)
	engine.Close()
	require.NoError(t, database.Close())

	var out bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, "resync-build", &out))
	result := decodeSingleResult(t, &out)
	assert.Equal(t, "ok", result.Status)
	assert.True(t, result.DiscoveryComplete)
	assert.Equal(t, 3, result.Synced)
	assert.FileExists(t, cfg.DBPath+"-resync",
		"worker must leave the built replacement for the daemon to swap")
}

func TestSyncWorkerSyncModeSyncsLikeStartup(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	var out bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, "sync", &out))
	result := decodeSingleResult(t, &out)
	assert.Equal(t, "ok", result.Status)
	assert.True(t, result.DiscoveryComplete)
	assert.Equal(t, 3, result.Synced)
}
