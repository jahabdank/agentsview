package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	syncpkg "go.kenn.io/agentsview/internal/sync"
)

// engineWithDispatchHandler builds an engine over the fixture archive whose
// OnStartupReconciled is the real startup reconciliation handler, so opening
// dispatch (leaving collecting mode) is observable through openedDispatch.
func engineWithDispatchHandler(
	t *testing.T, cfg config.Config, openedDispatch chan<- struct{},
) *syncpkg.Engine {
	t.Helper()
	database := dbtest.OpenTestDBAt(t, cfg.DBPath)
	engine := syncpkg.NewEngine(database, syncpkg.EngineConfig{
		AgentDirs: cfg.AgentDirs,
		Machine:   "local",
		OnStartupReconciled: newStartupReconciliationHandler(
			t.Context(),
			func(context.Context) error { return nil },
			func() {
				select {
				case openedDispatch <- struct{}{}:
				default:
				}
			},
		),
	})
	t.Cleanup(engine.Close)
	return engine
}

// TestStartupWorkerPathOpensWatcherDispatch reproduces the daemon side of the
// worker handshake: after the worker's out-of-process pass, the daemon runs the
// gap reconciliation and acknowledges startup, which must fire
// OnStartupReconciled and open watcher dispatch.
func TestStartupWorkerPathOpensWatcherDispatch(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	opened := make(chan struct{}, 1)
	engine := engineWithDispatchHandler(t, cfg, opened)

	// Gap reconciliation over all watch roots (warm; here it also performs the
	// first sync since the fixture DB is empty), then acknowledge startup with
	// the worker's terminal result.
	gapErr := engine.ReconcileWatchRoots(t.Context(), reconcileRootPaths(cfg), true)
	require.NoError(t, gapErr)
	engine.RecordStartupReconciled(
		statsFromWorkerResult(workerResult{
			Status: "ok", Synced: 3, DiscoveryComplete: true,
		}),
		gapErr,
	)

	select {
	case <-opened:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "watcher dispatch did not open after worker handshake")
	}
}

// TestStartupWorkerFailureFallsBackInProcess asserts that a failed worker launch
// is surfaced (so runServe falls back), and the in-process initial sync still
// reconciles startup and opens dispatch.
func TestStartupWorkerFailureFallsBackInProcess(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)

	restore := stubLaunchSyncWorker(t, func(
		context.Context, config.Config, string, func(workerLine),
	) (workerResult, error) {
		return workerResult{}, errors.New("spawn boom")
	})
	defer restore()

	_, err := runStartupSyncViaWorker(
		t.Context(), cfg, newStartupStateWriter(cfg.DataDir, time.Now),
	)
	require.Error(t, err, "worker failure must be surfaced so the daemon falls back")

	opened := make(chan struct{}, 1)
	engine := engineWithDispatchHandler(t, cfg, opened)
	stats := runInitialSync(t.Context(), engine, nil)
	assert.False(t, stats.Aborted)
	assert.Equal(t, 3, stats.Synced, "in-process fallback syncs the fixture archive")
	select {
	case <-opened:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "in-process fallback did not open dispatch")
	}
}

func TestStatsFromWorkerResultMapsDiscoveryOntoAborted(t *testing.T) {
	complete := statsFromWorkerResult(workerResult{
		Status: "ok", Synced: 5, Skipped: 1, Failed: 0, DiscoveryComplete: true,
	})
	assert.False(t, complete.Aborted)
	assert.True(t, complete.AuthoritativeDiscoveryComplete())
	assert.Equal(t, 5, complete.Synced)

	incomplete := statsFromWorkerResult(workerResult{
		Status: "aborted", DiscoveryComplete: false,
	})
	assert.True(t, incomplete.Aborted)
	assert.False(t, incomplete.AuthoritativeDiscoveryComplete())
}

func TestSyncWorkerChildArgsForwardsServeConfigFlags(t *testing.T) {
	parent := []string{
		"serve", "--host", "0.0.0.0", "--port", "9999",
		"--background", "--pprof",
	}
	args := syncWorkerChildArgs(parent, "startup")

	require.Equal(t, "sync-worker", args[0])
	assert.Equal(t, []string{"--mode", "startup"}, args[1:3])
	assert.Contains(t, args, "--host=0.0.0.0", "serve config flag forwarded")
	assert.Contains(t, args, "--port=9999", "serve config flag forwarded")
	for _, a := range args {
		assert.NotContains(t, a, "background",
			"serve-only lifecycle flag must not reach the worker")
		assert.NotContains(t, a, "pprof",
			"serve-only lifecycle flag must not reach the worker")
	}
}

// TestSyncWorkerRealSpawnEmitsTerminalResult self-execs the built command in
// worker mode against an isolated fixture archive and validates the on-the-wire
// protocol: NDJSON lines, exactly one terminal result, exit 0.
func TestSyncWorkerRealSpawnEmitsTerminalResult(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	claudeDir := cfg.AgentDirs[parser.AgentClaude][0]

	cmd := exec.Command(
		os.Args[0],
		"-test.run=^TestSyncWorkerMainHelperProcess$",
		"--",
		"sync-worker", "--mode", "startup",
	)
	// Minimal, isolated env so no developer/CI agent directory is scanned.
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"AGENTSVIEW_SYNC_WORKER_MAIN_HELPER=1",
		"AGENTSVIEW_DATA_DIR=" + cfg.DataDir,
		"CLAUDE_PROJECTS_DIR=" + claudeDir,
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(),
		"worker must exit 0 on an authoritative pass; stderr:\n%s", stderr.String())

	var results []workerResult
	sawProgress := false
	sc := bufio.NewScanner(&stdout)
	for sc.Scan() {
		var line workerLine
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line),
			"every stdout line must be a workerLine JSON object; got %q", sc.Text())
		if line.Progress != nil {
			sawProgress = true
		}
		if line.Result != nil {
			results = append(results, *line.Result)
		}
	}
	require.NoError(t, sc.Err())
	assert.True(t, sawProgress, "worker must stream progress")
	require.Len(t, results, 1, "exactly one terminal result")
	assert.Equal(t, "ok", results[0].Status)
	assert.True(t, results[0].DiscoveryComplete)
	assert.Equal(t, 3, results[0].Synced)
}

// TestSyncWorkerMainHelperProcess is the re-exec target for the real-spawn test.
// It runs the actual CLI (via main) so os.Args[0] behaves like the agentsview
// binary, then exits 0 to suppress the test framework's own stdout output.
func TestSyncWorkerMainHelperProcess(t *testing.T) {
	if os.Getenv("AGENTSVIEW_SYNC_WORKER_MAIN_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"agentsview"}, os.Args[i+1:]...)
			main()
			os.Exit(0)
		}
	}
	t.Fatal("missing helper args")
}
