package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/spf13/cobra"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/sync"
)

// syncWorkerChildEnvVar marks a process spawned as a sync-worker child by the
// daemon's writer handoff. The daemon closes its writer and releases the write
// lock before spawning, so the child skips the live-writable-daemon rejection
// (the daemon is knowingly yielding); the write-owner flock remains the real
// guard.
const syncWorkerChildEnvVar = "AGENTSVIEW_SYNC_WORKER"

// runningAsSyncWorker reports whether this process is a sync-worker child
// spawned by a daemon that has yielded write ownership for the pass.
func runningAsSyncWorker() bool {
	return os.Getenv(syncWorkerChildEnvVar) == "1"
}

// workerLine is one NDJSON record on the sync-worker's stdout. Every stdout
// line unmarshals to a workerLine; diagnostics go to stderr so the parent can
// parse stdout strictly. A run streams zero or more Progress lines followed by
// exactly one Result line.
type workerLine struct {
	Progress *sync.Progress `json:"progress,omitempty"`
	Result   *workerResult  `json:"result,omitempty"`
}

// workerResult is the single terminal record a sync-worker run emits. Status is
// "ok" only when the pass completed and discovery was authoritative; the parent
// treats anything else, or a missing/duplicate result, as a failed run.
type workerResult struct {
	Status            string `json:"status"` // "ok" | "aborted" | "failed"
	Synced            int    `json:"synced"`
	Skipped           int    `json:"skipped"`
	Failed            int    `json:"failed"`
	DiscoveryComplete bool   `json:"discoveryComplete"`
	Error             string `json:"error,omitempty"`
}

// newSyncWorkerCommand registers the hidden self-exec'd worker. The daemon runs
// it as a short-lived child so archive-scale allocation high-water returns to
// the OS when the child exits, instead of pinning the daemon's RSS.
func newSyncWorkerCommand() *cobra.Command {
	var mode string
	cmd := &cobra.Command{
		Use:          "sync-worker",
		Short:        "Run one heavy sync pass and stream a terminal result",
		Hidden:       true,
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Load config the same flag-aware way `serve` does so config flags
			// the daemon forwarded into the child argv (see syncWorkerChildArgs)
			// override env/config.toml identically to a serve --background child.
			cfg, err := config.LoadPFlags(cmd.Flags())
			if err != nil {
				return fmt.Errorf("sync-worker: loading config: %w", err)
			}
			return runSyncWorker(cfg, mode, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(
		&mode, "mode", "",
		"worker mode: startup, sync, resync-build, audit",
	)
	if err := cmd.MarkFlagRequired("mode"); err != nil {
		panic(err)
	}
	// Register the serve config flags so forwarded overrides parse and apply.
	config.RegisterServePFlags(cmd.Flags())
	return cmd
}

// runSyncWorker runs one worker pass with a background context.
func runSyncWorker(cfg config.Config, mode string, out io.Writer) error {
	return runSyncWorkerContext(context.Background(), cfg, mode, out)
}

// runSyncWorkerContext dispatches on mode, streaming NDJSON progress and exactly
// one terminal result. It returns nil only when the terminal result is Status
// "ok" with authoritative discovery; the child's exit code follows this error.
func runSyncWorkerContext(
	ctx context.Context, cfg config.Config, mode string, out io.Writer,
) error {
	enc := json.NewEncoder(out)
	emit := func(line workerLine) { _ = enc.Encode(line) }
	onProgress := func(p sync.Progress) { emit(workerLine{Progress: &p}) }
	switch mode {
	case "startup", "sync":
		// "sync" is the live-archive foreground pass; its body is identical to
		// "startup" (including the NeedsResync branch for a stale-version
		// archive). Only the daemon-side orchestration differs: "startup" runs
		// before the daemon opens the DB, "sync" runs inside a writer handoff.
		return runSyncWorkerStartup(ctx, cfg, mode, emit, onProgress)
	case "resync-build":
		return runSyncWorkerResyncBuild(ctx, cfg, mode, emit, onProgress)
	case "audit":
		return fmt.Errorf("sync-worker mode %q not implemented yet", mode)
	default:
		return fmt.Errorf("unknown sync-worker mode %q", mode)
	}
}

// runSyncWorkerStartup performs a full sync (or full resync when the data
// version changed) as a self-contained pass, then emits the terminal result. It
// mirrors the sync/resync branch in runServe minus daemon-only wiring (watcher,
// emitter, backfills). mode only labels the terminal error.
func runSyncWorkerStartup(
	ctx context.Context,
	cfg config.Config,
	mode string,
	emit func(workerLine),
	onProgress func(sync.Progress),
) error {
	database, releaseLock, err := openWorkerWriteDB(cfg)
	if err != nil {
		return err
	}
	defer releaseLock()
	defer database.Close()

	// Remove stale temp DB from a prior crashed resync before ResyncAll
	// stages a fresh one, matching runServe's startup cleanup.
	cleanResyncTemp(cfg.DBPath)

	engine := sync.NewEngine(database, workerEngineConfig(cfg))
	defer engine.Close()

	var stats sync.SyncStats
	if database.NeedsResync() {
		stats = engine.ResyncAll(ctx, onProgress)
	} else {
		stats = engine.SyncAll(ctx, onProgress)
	}

	result := workerResultFromStats(ctx, stats)
	emit(workerLine{Result: &result})
	if result.Status != "ok" || !result.DiscoveryComplete {
		return fmt.Errorf("sync worker %s: %s", mode, result.Status)
	}
	return nil
}

// runSyncWorkerResyncBuild builds a replacement archive at the resync temp path
// from a read-only view of the original, then emits the terminal result. The
// daemon holds the write barrier and performs the swap, so the worker never
// opens the live archive writable and needs no write-owner flock. It leaves the
// built database on disk at ResyncTempPath for the daemon to install.
func runSyncWorkerResyncBuild(
	ctx context.Context,
	cfg config.Config,
	mode string,
	emit func(workerLine),
	onProgress func(sync.Progress),
) error {
	// Remove a stale temp DB from a prior crashed resync before building a fresh
	// one, matching runServe's startup cleanup and the in-process build.
	cleanResyncTemp(cfg.DBPath)

	origRO, err := db.OpenReadOnly(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("resync-build: open read-only archive: %w", err)
	}
	defer origRO.Close()

	engine := sync.NewEngine(origRO, workerEngineConfig(cfg))
	defer engine.Close()

	_, stats, buildErr := engine.ResyncBuild(ctx, onProgress)
	result := workerResultFromStats(ctx, stats)
	emit(workerLine{Result: &result})
	if result.Status != "ok" || !result.DiscoveryComplete {
		return fmt.Errorf("sync worker %s: %s", mode, result.Status)
	}
	if buildErr != nil {
		return fmt.Errorf("sync worker %s: %w", mode, buildErr)
	}
	return nil
}

// workerResultFromStats maps engine stats to a terminal result. Cancellation or
// a safety abort is "aborted"; a completed pass with hard parse failures or
// non-authoritative discovery is "failed"; otherwise "ok".
func workerResultFromStats(
	ctx context.Context, stats sync.SyncStats,
) workerResult {
	result := workerResult{
		Synced:            stats.Synced,
		Skipped:           stats.Skipped,
		Failed:            stats.Failed,
		DiscoveryComplete: stats.AuthoritativeDiscoveryComplete(),
	}
	switch {
	case ctx.Err() != nil || stats.Aborted:
		result.Status = "aborted"
		// Aborted discovery is never authoritative, even if the counters
		// happened to complete a provider listing before cancellation.
		result.DiscoveryComplete = false
		if ctx.Err() != nil {
			result.Error = ctx.Err().Error()
		}
	case stats.Failed > 0 || !result.DiscoveryComplete:
		result.Status = "failed"
	default:
		result.Status = "ok"
	}
	return result
}

// openWorkerWriteDB reuses the daemon's direct-write open path — including the
// db.write.lock acquisition — but returns errors instead of exiting. The
// returned func releases the write-owner lock.
func openWorkerWriteDB(cfg config.Config) (*db.DB, func(), error) {
	database, lock, err := openWriteDB(context.Background(), cfg)
	if err != nil {
		return nil, nil, err
	}
	release := func() {
		if err := lock.Close(); err != nil {
			log.Printf("release sqlite write-owner lock: %v", err)
		}
	}
	return database, release, nil
}

// workerEngineConfig mirrors the sync.EngineConfig literal in runServe minus the
// daemon-only callbacks (emitter, watcher reconciliation, deferred maintenance).
func workerEngineConfig(cfg config.Config) sync.EngineConfig {
	return sync.EngineConfig{
		AgentDirs:               cfg.AgentDirs,
		IncludeCwdPrefixes:      cfg.SyncIncludeCwdPrefixes,
		Machine:                 cfg.LocalMachineName,
		BlockedResultCategories: cfg.ResultContentBlockedCategories,
	}
}
