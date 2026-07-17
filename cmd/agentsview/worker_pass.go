package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"

	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/sync"
)

// workerLineMaxBytes caps a single NDJSON line from the worker. A malformed or
// unbounded line is a protocol failure, not a reason to grow memory without
// limit.
const workerLineMaxBytes = 1 << 20 // 1 MB

// launchSyncWorker is the worker-launch seam. Production self-execs the binary;
// tests stub it to exercise runWorkerWritePass without spawning a process.
var launchSyncWorker = launchSyncWorkerProcess

// runWorkerWritePass yields write ownership around a worker run against the live
// archive: it closes the writer (readers keep serving), releases the write
// lock, runs the worker, then unconditionally reacquires the lock and reopens
// the writer. Losing write ownership permanently is worse than a failed pass,
// so reacquisition and reopen run even when the worker fails.
func runWorkerWritePass(
	ctx context.Context,
	cfg config.Config,
	engine *sync.Engine,
	database *db.DB,
	lock *writeOwnerLock,
	mode string,
	onLine func(workerLine),
) (workerResult, error) {
	var result workerResult
	err := engine.RunExclusive(func() error {
		if err := database.CloseWriter(); err != nil {
			return fmt.Errorf("close writer for %s pass: %w", mode, err)
		}
		if err := lock.Release(); err != nil {
			// The writer is still closed; restore it so the daemon keeps
			// writing rather than stranding the archive read-only.
			_ = database.ReopenWriter()
			return fmt.Errorf("release write lock for %s pass: %w", mode, err)
		}

		var workerErr error
		result, workerErr = launchSyncWorker(ctx, cfg, mode, onLine)

		if err := lock.Reacquire(); err != nil {
			return fmt.Errorf(
				"reacquire write lock after %s pass: %w", mode, err,
			)
		}
		if err := database.ReopenWriter(); err != nil {
			return fmt.Errorf("reopen writer after %s pass: %w", mode, err)
		}
		return workerErr
	})
	return result, err
}

// launchSyncWorkerProcess self-execs `sync-worker --mode=<mode>`, decodes the
// child's NDJSON stdout, forwards every line to onLine, and enforces the parent
// parse contract: exactly one valid terminal result with no malformed lines. It
// returns the terminal result and a non-nil error on any protocol violation or
// non-ok terminal status, regardless of the child's exit code.
func launchSyncWorkerProcess(
	ctx context.Context,
	cfg config.Config,
	mode string,
	onLine func(workerLine),
) (workerResult, error) {
	exe, err := os.Executable()
	if err != nil {
		return workerResult{}, fmt.Errorf("finding executable: %w", err)
	}

	cmd := exec.CommandContext(ctx, exe, "sync-worker", "--mode", mode)
	// Config forwarding mirrors startServeBackgroundProcess: the child reloads
	// config itself and only needs the data dir plus the worker marker.
	cmd.Env = append(os.Environ(), syncWorkerChildEnvVar+"=1")
	if cfg.DataDir != "" {
		cmd.Env = append(cmd.Env, "AGENTSVIEW_DATA_DIR="+cfg.DataDir)
	}
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return workerResult{}, fmt.Errorf("sync worker stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return workerResult{}, fmt.Errorf("starting sync worker: %w", err)
	}

	// Drain stdout fully before Wait so the child never blocks on a full pipe.
	result, parseErr := readWorkerResult(stdout, onLine)
	waitErr := cmd.Wait()

	if parseErr != nil {
		return result, fmt.Errorf("%s worker: %w", mode, parseErr)
	}
	if result.Status != "ok" || !result.DiscoveryComplete {
		detail := result.Status
		if result.Error != "" {
			detail = fmt.Sprintf("%s (%s)", result.Status, result.Error)
		}
		return result, fmt.Errorf("%s worker pass reported %s", mode, detail)
	}
	if waitErr != nil {
		return result, fmt.Errorf("%s worker process: %w", mode, waitErr)
	}
	return result, nil
}

// readWorkerResult scans the worker's NDJSON stdout, forwarding each decoded
// line to onLine, and enforces the terminal-record contract: every non-blank
// line must be a valid workerLine and exactly one must carry a Result. Any
// malformed line, or a result count other than one, is a protocol error.
func readWorkerResult(
	r io.Reader, onLine func(workerLine),
) (workerResult, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), workerLineMaxBytes)

	var result workerResult
	resultCount, malformed := 0, 0
	for sc.Scan() {
		raw := sc.Bytes()
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var line workerLine
		if err := json.Unmarshal(raw, &line); err != nil {
			malformed++
			continue
		}
		if onLine != nil {
			onLine(line)
		}
		if line.Result != nil {
			resultCount++
			result = *line.Result
		}
	}
	if err := sc.Err(); err != nil {
		return result, fmt.Errorf("reading worker output: %w", err)
	}
	if malformed > 0 {
		return result, fmt.Errorf(
			"sync worker emitted %d malformed output line(s)", malformed,
		)
	}
	if resultCount != 1 {
		return result, fmt.Errorf(
			"sync worker emitted %d terminal results, want exactly 1",
			resultCount,
		)
	}
	return result, nil
}
