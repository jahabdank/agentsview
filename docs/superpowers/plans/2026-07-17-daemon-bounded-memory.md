# Daemon Bounded Memory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the agentsview daemon genuinely bounded in passive memory and
CPU: no periodic archive-wide scans, no per-entry ancestor descriptors, no
archive-scale allocation high-water retained in the daemon process.

**Architecture:** Four independent workstreams on branch `idle-memory-use`: (1)
fix the warm no-op SyncAll allocation regression by deferring parse-retention
lease acquisition to the parse seam; (2) scope Gemini fingerprints to
per-session resolved metadata instead of whole-root metadata file contents; (3)
replace the fsnotify-kqueue ancestor watch (one fd per directory entry) with a
single-fd `EVFILT_VNODE` observer per ancestor; (4) replace the unscoped
15-minute `SyncAll` with capability-scoped reconciliation plus a daily archive
audit, and move startup sync, full resync, and the audit into an attached worker
process (`agentsview sync-worker`) that exits after each pass so its allocation
high-water returns to the OS.

**Tech Stack:** Go (CGO_ENABLED=1, `fts5` build tag), `golang.org/x/sys/unix`
(already a dependency), SQLite WAL, testify.

## Global Constraints

- Branch: `idle-memory-use`. Do not change branches. Commit every task; never
  amend.
- All Go test commands need `CGO_ENABLED=1 go test -tags fts5 ...` (or
  `make test` / `make test-short`).
- After Go changes run `go fmt ./...` and `go vet ./...` before committing.
- Tests use testify (`require` for aborting checks, `assert` for independent
  checks), table-driven where natural, `t.TempDir()` for temp dirs, and the
  existing `testDB(t)` helper for DB tests.
- Never point any profiling or manual run at live archives or the live daemon's
  data directory; use isolated scratch clones only.
- The SQLite database is a persistent archive: no destructive schema or data
  handling anywhere in this plan.
- Preserve deletion, tombstone, and persistent-archive behavior in every
  regression test touching sync (per AGENTS.md).
- Background work must be bounded by the changed batch, not archive size;
  default new capabilities to unsupported.
- No emojis; keep private hostnames/paths out of code, fixtures, and commits.

**Acceptance for the whole plan:** daemon RSS under 300 MB after startup and
after 24 h of continuous agent activity, with no periodic archive-wide CPU
bursts (verified in Task 10).

______________________________________________________________________

### Task 1: Defer parse-retention lease acquisition to the parse seam

The warm no-op bench gate (`BenchmarkSyncAllWarmNoop`,
`internal/sync/engine_bench_test.go:137`) regressed ~33% in allocs/op on this
branch. Top cause: `processFileWithRetention` (`internal/sync/engine.go:6037`)
heap-allocates a `parseRetentionLease` (`internal/sync/parse_retention.go:48`)
and runs `parseRetentionSourceBytes` (an `os.Lstat` + `os.Stat`,
`parse_retention.go:118`) for **every** source, including the ~100% of sources a
warm pass skips before any parse. The lease exists to bound memory retained by
*parsed* results, so acquisition belongs immediately before parse work, not
before the skip gates.

**Files:**

- Modify: `internal/sync/engine.go` (worker loop ~5323-5375, skip/parse seams in
  `processProviderFile` ~6098-6250 and the legacy parse path inside
  `processFile` ~5963+, caller at ~11398)
- Modify: `internal/sync/parse_retention.go` (no signature changes expected)
- Test: existing `internal/sync/engine_bench_test.go`,
  `internal/sync/perf_invariant_test.go`, plus one new unit test in
  `internal/sync/parse_retention_test.go` (create if absent)

**Interfaces:**

- Consumes:
  `(*parseRetentionBudget).acquire(ctx, sourceBytes) -> (*parseRetentionLease, error)`
  (unchanged).

- Produces: `processResult` gains field `retentionLease *parseRetentionLease`;
  `processFileWithRetention` is deleted; `startWorkers` reads the lease from
  the result. Later tasks do not depend on these internals.

- [ ] **Step 1: Record the baseline**

```bash
cd /Users/wesm/code/agentsview
CGO_ENABLED=1 go test -tags fts5 -run '^$' -bench BenchmarkSyncAllWarmNoop \
  -benchmem -count 6 ./internal/sync | tee /tmp/bench-before.txt
```

Save the output; you will compare after the change. Also capture an allocation
profile to confirm the hot sites:

```bash
CGO_ENABLED=1 go test -tags fts5 -run '^$' -bench BenchmarkSyncAllWarmNoop \
  -benchmem -memprofile /tmp/warm.mprof ./internal/sync
go tool pprof -top -sample_index=alloc_objects \
  ./internal/sync.test /tmp/warm.mprof | head -25
```

Expected: `acquire` / `parseRetentionSourceBytes` and/or
`baselineProcessedSource` near the top of new allocations.

- [ ] **Step 2: Write the failing behavior test**

In `internal/sync/parse_retention_test.go`, assert that a warm no-op pass
acquires no leases. Give `parseRetentionBudget` a test-visible counter:

```go
// in parse_retention.go, add to parseRetentionBudget:
	acquired atomic.Int64 // total successful acquisitions, for tests
```

Increment it in both success returns of `acquire`. Then:

```go
func TestWarmNoopSyncAcquiresNoRetentionLeases(t *testing.T) {
	// Build a small Claude fixture dir, run engine.SyncAll once (cold),
	// then again (warm). Reuse the fixture helper used by
	// BenchmarkSyncAllWarmNoop (engine_bench_test.go:137) for setup.
	e, ctx := newWarmBenchEngine(t) // extract from the benchmark setup
	e.SyncAll(ctx, nil)             // cold pass parses, acquires leases
	before := e.retentionBudget().acquired.Load()
	stats := e.SyncAll(ctx, nil) // warm pass: everything skips
	require.Equal(t, 0, stats.Synced)
	assert.Equal(t, before, e.retentionBudget().acquired.Load(),
		"warm no-op pass must not acquire parse-retention leases")
}
```

If the benchmark setup is not trivially extractable, build the fixture inline
the same way the benchmark does (40 sessions x 30 messages is unnecessary; 5
sessions suffice).

- [ ] **Step 3: Run the test to verify it fails**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestWarmNoopSyncAcquiresNoRetentionLeases ./internal/sync -v
```

Expected: FAIL — the warm pass currently acquires one lease per source.

- [ ] **Step 4: Move acquisition to the parse seam**

**Change A.** Add `retentionLease *parseRetentionLease` to `processResult`.

**Change B.** Delete `processFileWithRetention` (engine.go:6037) and update its
two callers. The `startWorkers` worker loop (engine.go:~5354) becomes:

```go
	result := e.processFile(ctx, file)
	emitResult(syncJob{
		processResult:  result,
		agent:          file.Agent,
		path:           file.Path,
		retentionLease: result.retentionLease,
	})
```

The single-path caller at engine.go:~11398: replace
`res, retentionLease := e.processFileWithRetention(ctx, file)` with
`res := e.processFile(ctx, file)` and `defer res.retentionLease.Release()`.

**Change C.** In `processProviderFile`, acquire immediately after the last cheap
skip gate (after the `openCodeStorageSessionFresh` early return at
engine.go:~6132, before the `e.providerFactories[file.Agent]` parse):

```go
	lease, err := e.retentionBudget().acquire(ctx, parseRetentionSourceBytes(file))
	if err != nil {
		return processResult{err: err}, true
	}
```

Attach `lease` to every `processResult` returned after this point
(`res.retentionLease = lease`), including error returns, so `releaseRetention`
always fires. The second skip-cache read at engine.go:~6248 happens *after* this
seam only if it needs parse inputs; if it is still a pure stat/cache check, move
the acquisition below it.

**Change D.** In the legacy (non-provider) path of `processFile`, do the same:
acquire after `shouldUseCachedSkip`/stat gates, immediately before parsing, and
attach the lease to the returned result. S3 sources (`processS3Session`) follow
the same rule.

**Invariant** (add a comment at the acquisition site): every `processResult`
that holds parsed sessions/messages must carry a lease; every return path after
`acquire` must carry the lease so `syncJob.releaseRetention()` (engine.go:761)
and the `pendingLeases` flush (engine.go:5702) release it exactly once.

- [ ] **Step 5: Run the test to verify it passes**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestWarmNoopSyncAcquiresNoRetentionLeases ./internal/sync -v
```

Expected: PASS.

- [ ] **Step 6: Run the sync package tests and vet**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/sync -count 1
go fmt ./... && go vet ./...
```

Expected: all pass. Pay attention to any retention/pressure tests — the budget's
blocking semantics for genuinely parsed sources must be unchanged.

- [ ] **Step 7: Commit**

```bash
git add internal/sync
git commit -m "perf(sync): acquire parse-retention leases at the parse seam"
```

______________________________________________________________________

### Task 2: Close the warm no-op bench gate

Verify the CI gate math the way `.github/workflows/bench.yml` does, and — only
if the gate still fails — shrink the remaining new warm-path allocations in the
baseline bookkeeping (`collectAndBatch`, engine.go:5397-5462).

**Files:**

- Modify (only if needed): `internal/sync/engine.go` (collectAndBatch)
- Test: bench gate comparison

**Interfaces:** none produced; consumes Task 1's change.

- [ ] **Step 1: Compare against merge-base like CI**

```bash
cd /Users/wesm/code/agentsview
git worktree add /tmp/av-mergebase "$(git merge-base origin/main HEAD)"
(cd /tmp/av-mergebase && make bench-gate BENCH_OUT=/tmp/bench-old.txt) || \
  (cd /tmp/av-mergebase && CGO_ENABLED=1 go test -tags fts5 -run '^$' \
   -bench . -benchmem -count 6 -benchtime 20x \
   ./internal/sync ./internal/db ./internal/secrets > /tmp/bench-old.txt)
CGO_ENABLED=1 go test -tags fts5 -run '^$' -bench . -benchmem -count 6 \
  -benchtime 20x ./internal/sync ./internal/db ./internal/secrets \
  > /tmp/bench-new.txt
go run ./cmd/benchgate -old /tmp/bench-old.txt -new /tmp/bench-new.txt
git worktree remove /tmp/av-mergebase
```

(Check `Makefile:307-319` for the exact `bench-gate` invocation and prefer it
verbatim on both sides.) Expected: gate passes for `BenchmarkSyncAllWarmNoop`.

- [ ] **Step 2 (conditional): shrink baseline bookkeeping allocations**

Only if the gate still fails, and guided by the Step 1 profile from Task 1:

- `baselineProcessedSource` (engine.go:5441) runs for every skipped source on a
  plain `SyncAll` (tracker and runtimeMetrics are nil there) and this is
  REQUIRED — it feeds `ReplaceActiveSessionSourceBaselines`, the deletion
  proof. Do not skip it. Instead:

    - Reuse one `admitted` buffer across `flushBaselineSources` calls instead of
      allocating `make([]db.SessionSourcePath, 0, len(baselineCandidates))` per
      flush (engine.go:5424): hoist
      `admitted := make([]db.SessionSourcePath, 0, reconciliationPageSize)` next
      to `baselineCandidates` and `admitted = admitted[:0]` inside the flush.
    - `e.effectiveSourcePath` (engine.go:4954) only allocates when `pathRewriter`
      is set; no change needed there.

- Re-run Step 1 to confirm.

- [ ] **Step 3: Commit (only if Step 2 changed code)**

```bash
git add internal/sync/engine.go
git commit -m "perf(sync): reuse baseline admission buffer across flushes"
```

If no change was needed, state so in the handoff; make no empty commit.

______________________________________________________________________

### Task 3: Scope Gemini fingerprints to per-session resolved metadata

`geminiSourceSet.Fingerprint` (`internal/parser/gemini_provider.go:398-440`)
folds the size, mtime, and full contents of the root-wide `projects.json` and
`trustedFolders.json` into **every** session's fingerprint
(`geminiProjectMetadataPaths`, gemini_provider.go:520). Any edit to either file
changes every Gemini session's fingerprint and mass-reparses the whole root.
Scope the metadata contribution to what the session actually consumes: its
resolved project and that project's trust entry.

Known one-time effect: every stored Gemini fingerprint changes once when this
ships, causing one re-verify pass of Gemini sources. Note this in the PR.

**Files:**

- Modify: `internal/parser/gemini_provider.go`
- Test: `internal/parser/gemini_copilot_provider_test.go`

**Interfaces:**

- Produces: unexported helper
  `func (s geminiSourceSet) sessionMetadataFingerprint(root, path string) string`
  and `func geminiSessionDirHash(root, path string) (string, bool)`. No
  exported API changes.

- [ ] **Step 1: Write the failing test**

Extend `TestGeminiProviderProjectMetadataChangesClassifyAndFingerprint`
(gemini_copilot_provider_test.go:88) — keep its existing classify assertions and
its assertion that a change to the session's *own* project entry changes the
fingerprint. Add the new scoping property:

```go
	// Adding an unrelated project entry must NOT change this
	// session's fingerprint.
	writeProjectsJSON(t, root, map[string]string{
		sessionDirHash:  "/work/project-a", // unchanged for our session
		"deadbeefcafe1": "/work/other",     // new, unrelated
	})
	fingerprintThree := fingerprintSource(t, provider, source)
	assert.Equal(t, fingerprintTwo.Hash, fingerprintThree.Hash,
		"unrelated projects.json entries must not invalidate the session")
```

Mirror the same pair of assertions for `trustedFolders.json`: changing the trust
entry for the session's resolved project path changes the hash; adding/changing
an entry for an unrelated path does not. Reuse the file helpers the test already
uses for writing metadata files (follow its existing style for writing
`projects.json`).

- [ ] **Step 2: Run the test to verify it fails**

```bash
CGO_ENABLED=1 go test -tags fts5 \
  -run TestGeminiProviderProjectMetadataChangesClassifyAndFingerprint \
  ./internal/parser -v
```

Expected: FAIL on the new "unrelated entry" assertions (today any metadata byte
change alters every hash).

- [ ] **Step 3: Implement per-session metadata scoping**

In `gemini_provider.go`:

```go
// geminiSessionDirHash extracts the tmp/<dirHash>/chats component that keys
// the session's project resolution.
func geminiSessionDirHash(root, path string) (string, bool) {
	rel, ok := relUnder(filepath.Clean(root), filepath.Clean(path))
	if !ok {
		return "", false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) != 4 || parts[0] != "tmp" || parts[2] != geminiChatsDir {
		return "", false
	}
	return parts[1], true
}

// sessionMetadataFingerprint returns the metadata this session's parse
// actually consumes: its resolved project and that project's trust entry.
// Root-wide metadata files must not leak into unrelated sessions'
// fingerprints (they used to mass-invalidate the whole root).
func (s geminiSourceSet) sessionMetadataFingerprint(root, path string) string {
	dirHash, ok := geminiSessionDirHash(root, path)
	if !ok {
		return ""
	}
	project := ResolveGeminiProject(dirHash, buildGeminiProjectMap(root))
	return project + "\x00" + geminiProjectTrustValue(root, project)
}
```

`geminiProjectTrustValue` reads `trustedFolders.json` and returns the entry for
`project` (empty string when the file or entry is absent). Reuse the existing
trusted-folders loader if one exists (check `loadGeminiConfig` / the `projects`
helper used by `DiscoverEach`, gemini_provider.go:150-199); only write a new
parser for the file if none exists.

In `Fingerprint` (gemini_provider.go:398), replace the whole
`for _, metadataPath := range geminiProjectMetadataPaths(root)` loop (including
its `fingerprint.Size` / `fingerprint.MTimeNS` folding) with:

```go
	if _, err := fmt.Fprintf(
		h, "metadata\x00%s\x00", s.sessionMetadataFingerprint(root, path),
	); err != nil {
		return SourceFingerprint{}, err
	}
```

Keep `SourcesForChangedPath` (gemini_provider.go:287) returning `discoverRoot`
for metadata paths: re-fingerprinting the root is now cheap for unaffected
sessions because their hashes no longer change, so the engine skips them without
reparsing.

Note on cost: `buildGeminiProjectMap` re-reads `projects.json` per call, but the
code it replaces read *and content-hashed both metadata files* per call, so this
is not a regression. Do not add caching in this task.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/parser -run Gemini -v
CGO_ENABLED=1 go test -tags fts5 ./internal/parser ./internal/sync -count 1
```

Expected: PASS, including discovery and watch-plan tests (the WatchPlan globs
for `projects.json`/`trustedFolders.json` are untouched).

- [ ] **Step 5: Commit**

```bash
git add internal/parser
git commit -m "fix(parser): scope Gemini fingerprints to per-session metadata"
```

______________________________________________________________________

### Task 4: Single-fd vnode observer for missing-root ancestors (darwin)

On darwin, missing logical roots get shallow coverage on their nearest existing
ancestor (`darwinWatchBackend.RegisterRoots`,
`watch_backend_factory_darwin.go:342-359`) via `acquireShallowLocked` ->
`b.addShallow` -> `fsnotifyBackend.AddShallow` -> fsnotify's kqueue backend,
which opens **one fd per directory entry** of the ancestor (~823 fds when the
ancestor is the home directory). Ancestors only need "an entry
appeared/disappeared", which a single `EVFILT_VNODE` fd on the directory
delivers. Real shallow *roots* (public `AddShallow`,
watch_backend_factory_darwin.go:433) still need per-file modify events and keep
the fsnotify path.

**Files:**

- Create: `internal/sync/vnode_observer_darwin.go`
- Create: `internal/sync/vnode_observer_darwin_test.go`
- Modify: `internal/sync/watch_backend_factory_darwin.go`
- Modify: `internal/sync/watcher_darwin_test.go`

**Interfaces:**

- Produces (darwin && cgo only, consumed inside the backend):

```go
func newVnodeObserver(wake func()) (*vnodeObserver, error)
func (o *vnodeObserver) Add(path string) error
func (o *vnodeObserver) Remove(path string) error
func (o *vnodeObserver) Close() error
func (o *vnodeObserver) watchedCount() int // test hook
```

- Backend gains seam fields `addAncestor, removeAncestor func(string) error` and
  refcount map `ancestors map[string]int`; ancestor call sites stop using
  `acquireShallowLocked`/`releaseShallowLocked`.

- [ ] **Step 1: Write the failing observer unit tests**

`internal/sync/vnode_observer_darwin_test.go` (build tag
`//go:build darwin && cgo`, matching the file that uses it):

```go
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
```

Use the 30 s liveness deadline (CI flake guidance: never 1 s `time.After`
fatals).

- [ ] **Step 2: Run them to verify they fail to compile**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestVnodeObserver ./internal/sync -v
```

Expected: compile FAILURE — `newVnodeObserver` undefined.

- [ ] **Step 3: Implement the observer**

`internal/sync/vnode_observer_darwin.go`:

```go
//go:build darwin && cgo

package sync

import (
	"fmt"
	"path/filepath"
	gosync "sync"

	"golang.org/x/sys/unix"
)

// vnodeObserver watches directories for entry-level changes using one
// EVFILT_VNODE descriptor per directory. It exists for missing-root
// ancestors, where fsnotify's kqueue backend would open one descriptor per
// directory entry. Content changes to files inside the directory are
// deliberately out of scope: ancestors only need create/remove/rename of
// entries, and the lifecycle loop re-evaluates state on every wake.
type vnodeObserver struct {
	mu     gosync.Mutex
	kq     int
	fds    map[string]int
	paths  map[int]string
	wake   func()
	closed bool
}

func newVnodeObserver(wake func()) (*vnodeObserver, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("create ancestor kqueue: %w", err)
	}
	o := &vnodeObserver{
		kq:    kq,
		fds:   make(map[string]int),
		paths: make(map[int]string),
		wake:  wake,
	}
	go o.run()
	return o, nil
}

func (o *vnodeObserver) Add(path string) error {
	path = filepath.Clean(path)
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return fmt.Errorf("vnode observer closed")
	}
	if _, exists := o.fds[path]; exists {
		return nil
	}
	fd, err := unix.Open(path, unix.O_EVTONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open ancestor %s: %w", path, err)
	}
	ev := unix.Kevent_t{}
	unix.SetKevent(&ev, fd, unix.EVFILT_VNODE, unix.EV_ADD|unix.EV_CLEAR)
	ev.Fflags = unix.NOTE_WRITE | unix.NOTE_DELETE |
		unix.NOTE_RENAME | unix.NOTE_REVOKE
	if _, err := unix.Kevent(o.kq, []unix.Kevent_t{ev}, nil, nil); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("register ancestor %s: %w", path, err)
	}
	o.fds[path] = fd
	o.paths[fd] = path
	return nil
}

func (o *vnodeObserver) Remove(path string) error {
	path = filepath.Clean(path)
	o.mu.Lock()
	defer o.mu.Unlock()
	fd, exists := o.fds[path]
	if !exists {
		return nil
	}
	delete(o.fds, path)
	delete(o.paths, fd)
	// Closing the fd removes its kevent registration.
	return unix.Close(fd)
}

func (o *vnodeObserver) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	for path, fd := range o.fds {
		_ = unix.Close(fd)
		delete(o.fds, path)
		delete(o.paths, fd)
	}
	return unix.Close(o.kq) // wakes run() with EBADF
}

func (o *vnodeObserver) watchedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.fds)
}

func (o *vnodeObserver) run() {
	events := make([]unix.Kevent_t, 8)
	for {
		n, err := unix.Kevent(o.kq, nil, events, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return // kq closed
		}
		if n > 0 {
			o.wake()
		}
	}
}
```

- [ ] **Step 4: Run the observer tests**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestVnodeObserver ./internal/sync -v
```

Expected: PASS.

- [ ] **Step 5: Commit the observer**

```bash
git add internal/sync/vnode_observer_darwin.go internal/sync/vnode_observer_darwin_test.go
git commit -m "feat(sync): add single-descriptor darwin vnode observer"
```

- [ ] **Step 6: Write the failing backend integration test**

In `watcher_darwin_test.go`:

```go
func TestDarwinWatcherMissingRootAncestorAvoidsPerEntryWatch(t *testing.T) {
	ancestor := t.TempDir()
	for i := range 50 {
		require.NoError(t, os.WriteFile(
			filepath.Join(ancestor, fmt.Sprintf("entry%02d", i)), nil, 0o644))
	}
	backend, err := newDarwinWatchBackend(nil, time.Millisecond)
	require.NoError(t, err)
	defer backend.Stop()
	var shallowAdds []string
	backend.addShallow = func(path string) error {
		shallowAdds = append(shallowAdds, path)
		return nil
	}
	missing := filepath.Join(ancestor, "provider", "sessions")
	results := backend.RegisterRoots(
		[]WatchRoot{{Path: missing, Recursive: true}}, 1024)
	require.Len(t, results, 1)
	require.True(t, results[0].MissingRootLifecycleOwned)
	assert.Empty(t, shallowAdds,
		"ancestor coverage must not use the per-entry kqueue shallow watch")
	require.NotNil(t, backend.vnode)
	assert.Equal(t, 1, backend.vnode.watchedCount())
}
```

(Adjust the `WatchRoot` literal to the real field names in
`watch_backend.go:16-21`.)

- [ ] **Step 7: Run it to verify it fails**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestDarwinWatcherMissingRootAncestorAvoidsPerEntryWatch ./internal/sync -v
```

Expected: FAIL — ancestors currently go through `addShallow`.

- [ ] **Step 8: Route ancestors through the observer**

In `watch_backend_factory_darwin.go`:

**Change A.** Add fields to `darwinWatchBackend`:

```go
	ancestors      map[string]int
	vnode          *vnodeObserver
	addAncestor    func(string) error
	removeAncestor func(string) error
```

**Change B.** In `newDarwinWatchBackend` (after the `addShallow` wiring at
:255):

```go
	backend.ancestors = make(map[string]int)
	backend.addAncestor = backend.observeAncestor
	backend.removeAncestor = backend.unobserveAncestor
```

**Change C.** Add the default implementations (lazy observer creation so daemons
with no missing roots hold no kqueue):

```go
func (b *darwinWatchBackend) observeAncestor(path string) error {
	if b.vnode == nil {
		observer, err := newVnodeObserver(b.signalLifecycle)
		if err != nil {
			return err
		}
		b.vnode = observer
	}
	return b.vnode.Add(path)
}

func (b *darwinWatchBackend) unobserveAncestor(path string) error {
	if b.vnode == nil {
		return nil
	}
	return b.vnode.Remove(path)
}
```

**Change D.** Add `acquireAncestorLocked`/`releaseAncestorLocked` as exact
copies of `acquireShallowLocked`/`releaseShallowLocked` (:1707-1731) operating
on `b.ancestors` / `b.addAncestor` / `b.removeAncestor`.

**Change E.** Switch every call site whose argument is an *ancestor* (never a
logical root) to the ancestor variants: `RegisterRoots` (:350), the
`processLifecycle` pending branch's ancestor advance/acquire/release (:922-969),
and the root-loss branch that acquires the ancestor before stream teardown
(:817-875). Public `AddShallow`/`Remove` (:433/:450) stay on the shallow path.
Grep to confirm no site was missed:

```bash
rg -n 'acquireShallowLocked|releaseShallowLocked' internal/sync/watch_backend_factory_darwin.go
```

Expected remaining callers: `AddShallow` and `Remove` only.

**Change F.** In `Stop()` (near the `b.kqueue.Stop()` call at :535), close the
observer:

```go
	if b.vnode != nil {
		_ = b.vnode.Close()
	}
```

**Change G.** Error handling: an `addAncestor` failure must keep the existing
failure behavior of ancestor acquisition (the caller already routes registration
failures toward polling obligations; verify with
`TestDarwinWatcherStartupCreateFailureSelectsRecursiveFallbackOnce` and the
fallback tests at watcher_darwin_test.go:1526-1684).

**Change H.** Update the darwin tests that stub `backend.addShallow` /
`backend.removeShallow` **to trace ancestor coverage** (the harness at
watcher_darwin_test.go:655-661 and every test asserting
`"shallow-add:"`/`"shallow-remove:"` traces for ancestors): point the same
recorder functions at `backend.addAncestor` / `backend.removeAncestor`. Keep the
existing trace strings so assertions stay untouched. Tests that exercise real
promotion via `os.Mkdir`
(`TestDarwinWatcherMissingRootRealCreationDeletionRecreation` :988, fallback
tests :1526/:1636) should pass unchanged on the real observer — if one relies on
kqueue-specific delivery, prefer fixing the test's seam, not reintroducing
per-entry watches.

- [ ] **Step 9: Run the darwin watcher suite**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestDarwinWatcher ./internal/sync -count 1
CGO_ENABLED=1 go test -tags fts5 ./internal/sync -count 1
```

Expected: PASS, including the new integration test.

- [ ] **Step 10: Commit**

```bash
git add internal/sync
git commit -m "fix(sync): watch missing-root ancestors with one vnode descriptor"
```

______________________________________________________________________

### Task 5: Capability-scoped periodic reconciliation

`startPeriodicSync` (`cmd/agentsview/main.go:1730-1761`) runs an unscoped
`engine.SyncAll` every 15 minutes — a ~49,500-source enumeration even when every
native watcher is healthy. Native-watched roots already get event-driven sync
plus degraded-coverage polling (the 2-minute `unwatchedPollCoordinator`,
`cmd/agentsview/unwatched_poll.go`, fed by
`OnCoverageDegraded`/`OnPollingRequired` — including watcher startup failure,
main.go:1132-1135 and :1182-1203). The only thing the blanket pass still covers
is providers whose declared watch is shallow (subdir changes invisible):
`ShallowWatch: true` (Cowork :132, OpenHands :224, Aider :692) or
`ShallowWatchRootsFunc` (Codex :145, Hermes :475) in `internal/parser/types.go`.
Scope the 15-minute tick to exactly those roots. The archive-wide safety net
moves to the daily worker audit (Task 9).

**Files:**

- Modify: `internal/parser/types.go`
- Modify: `cmd/agentsview/main.go`
- Test: `internal/parser/types_test.go` (or nearest existing types test file),
  `cmd/agentsview/periodic_sync_test.go` (new),
  `internal/sync/perf_invariant_test.go`

**Interfaces:**

- Produces:

```go
// internal/parser/types.go
func (d AgentDef) NeedsPeriodicReconcile() bool

// cmd/agentsview/main.go
type scheduledSyncEngine interface {
	ReconcileWatchRoots(ctx context.Context, roots []string, full bool) error
}
func scheduledReconcileRoots(cfg config.Config) []string
func runScheduledSyncPass(ctx context.Context, engine scheduledSyncEngine, roots []string)
```

- Consumes: `Engine.ReconcileWatchRoots` (engine.go:2694) — existing scoped,
  spool-backed streaming reconciliation.

- [ ] **Step 1: Write the failing capability test**

```go
func TestNeedsPeriodicReconcile(t *testing.T) {
	shallow := map[AgentType]bool{}
	for _, def := range Registry {
		shallow[def.Type] = def.NeedsPeriodicReconcile()
	}
	// Shallow-watched providers rely on scheduled reconciliation.
	assert.True(t, shallow[AgentCowork])
	assert.True(t, shallow[AgentCodex])
	assert.True(t, shallow[AgentHermes])
	assert.True(t, shallow[AgentOpenHands])
	assert.True(t, shallow[AgentAider])
	// Natively watched providers must not be rescanned on a schedule.
	assert.False(t, shallow[AgentClaude])
	assert.False(t, shallow[AgentGemini])
}
```

(Adjust constant names to the real `AgentType` identifiers in types.go.)

- [ ] **Step 2: Run it to verify it fails to compile, then implement**

```go
// NeedsPeriodicReconcile reports whether the agent's declared watch coverage
// is shallow, so subdirectory changes are invisible to the file watcher and
// scheduled scoped reconciliation must cover them. Derived from the existing
// watch declarations rather than a separate flag so it cannot drift.
func (d AgentDef) NeedsPeriodicReconcile() bool {
	return d.ShallowWatch || d.ShallowWatchRootsFunc != nil
}
```

Run:
`CGO_ENABLED=1 go test -tags fts5 -run TestNeedsPeriodicReconcile ./internal/parser -v`
— PASS.

- [ ] **Step 3: Write the failing scheduler tests**

`cmd/agentsview/periodic_sync_test.go`:

```go
type fakeScheduledEngine struct {
	mu    gosync.Mutex
	calls [][]string
}

func (f *fakeScheduledEngine) ReconcileWatchRoots(
	_ context.Context, roots []string, full bool,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if full {
		return fmt.Errorf("scheduled pass must never request full reconciliation")
	}
	f.calls = append(f.calls, append([]string(nil), roots...))
	return nil
}

func TestScheduledReconcileRootsSelectsOnlyShallowProviders(t *testing.T) {
	home := t.TempDir()
	coworkDir := filepath.Join(home, "cowork")
	claudeDir := filepath.Join(home, "claude")
	require.NoError(t, os.MkdirAll(coworkDir, 0o755))
	require.NoError(t, os.MkdirAll(claudeDir, 0o755))
	cfg := config.Config{ /* set cowork + claude dirs to the two paths,
		following collectWatchRoots' config expectations */ }
	roots := scheduledReconcileRoots(cfg)
	assert.Contains(t, roots, coworkDir)
	assert.NotContains(t, roots, claudeDir,
		"natively watched providers must not be rescanned on the schedule")
}

func TestRunScheduledSyncPassIsScopedAndSkipsWhenEmpty(t *testing.T) {
	engine := &fakeScheduledEngine{}
	runScheduledSyncPass(context.Background(), engine, nil)
	assert.Empty(t, engine.calls, "no roots -> no reconciliation call")
	runScheduledSyncPass(context.Background(), engine, []string{"/a", "/b"})
	require.Len(t, engine.calls, 1)
	assert.Equal(t, []string{"/a", "/b"}, engine.calls[0])
}
```

Build the `cfg` literal the same way `collectWatchRoots(cfg)` consumers in
`main_test.go` do (search for an existing test constructing per-agent dirs).

- [ ] **Step 4: Run to verify failure, then implement**

In `main.go`:

```go
type scheduledSyncEngine interface {
	ReconcileWatchRoots(ctx context.Context, roots []string, full bool) error
}

// scheduledReconcileRoots returns the existing roots of providers whose
// declared watch coverage is shallow. Healthy natively watched roots are
// event-driven; degraded roots are owned by the unwatched-poll coordinator;
// everything else waits for the daily archive audit.
func scheduledReconcileRoots(cfg config.Config) []string {
	var roots []string
	for _, root := range collectWatchRootsForScheduling(cfg) {
		roots = appendUniqueStrings(roots, root)
	}
	return roots
}

func runScheduledSyncPass(
	ctx context.Context, engine scheduledSyncEngine, roots []string,
) {
	if len(roots) == 0 {
		return
	}
	if err := engine.ReconcileWatchRoots(ctx, roots, false); err != nil {
		log.Printf("scheduled reconciliation: %v", err)
	}
}
```

`collectWatchRootsForScheduling` filters the output of the existing
`collectWatchRoots(cfg)` (main.go:1122) to roots whose `AgentDef`
(`parser.Registry` lookup by the root's agent) satisfies
`NeedsPeriodicReconcile()`, returning their `syncDirs()`. Reuse the existing
root struct; do not re-derive directories from scratch.

Replace the ticker body in `startPeriodicSync` (main.go:1755-1760):

```go
		log.Println("Running scheduled reconciliation...")
		idleTracker.Do(func() {
			runScheduledSyncPass(ctx, engine, scheduledReconcileRoots(cfg))
			recomputePendingSessions(engine, database)
		})
```

`engine *sync.Engine` already satisfies `scheduledSyncEngine`
(`ReconcileWatchRoots`, engine.go:2694). Remote-host loops
(`startRemoteHostSync`) are untouched.

- [ ] **Step 5: Add the cardinality regression test**

In `internal/sync/perf_invariant_test.go`, mirror
`TestRebuildLocalAndRemoteContributorsBulkWriteDiscoveredCount` (:113): build a
fixture with a **fixed-size Cowork corpus** (5 sessions) and a **Claude corpus
at two sizes** (5 and 500 sessions). After a full initial `SyncAll`, run
`engine.ReconcileWatchRoots(ctx, []string{coworkRoot}, false)` and capture
observed work (use the same observation hooks that test uses —
discovered/batched counts). Assert the scoped pass's work is identical at both
Claude sizes:

```go
	require.Equal(t, observed[5], observed[500],
		"scheduled scoped reconciliation must not scale with archive size")
```

Also assert an unchanged deletion behavior in the same test: delete one Cowork
source file, re-run the scoped reconcile, and require the session is
tombstoned/removed exactly as a full pass would do (reuse the deletion
assertions from the neighboring test).

- [ ] **Step 6: Run the affected suites**

```bash
CGO_ENABLED=1 go test -tags fts5 ./cmd/agentsview -run 'Scheduled|PeriodicSync|UnwatchedPoll' -count 1
CGO_ENABLED=1 go test -tags fts5 ./internal/sync -run 'PerfInvariant|Cardinality|Reconcil' -count 1
CGO_ENABLED=1 go test -tags fts5 ./cmd/agentsview ./internal/sync ./internal/parser -count 1
```

Expected: PASS.
`TestUnwatchedPollEmptyObligationNeverExpandsToFullReconciliation`
(unwatched_poll_test.go:378) must stay green.

- [ ] **Step 7: Commit**

```bash
git add internal/parser cmd/agentsview internal/sync
git commit -m "feat(sync): scope scheduled reconciliation to shallow-watch providers"
```

______________________________________________________________________

### Task 6: `sync-worker` hidden command with NDJSON progress

An attached worker process runs heavy one-shot passes and exits, returning its
allocation high-water to the OS. The worker is the same binary (self-exec
pattern already exists: `startServeBackgroundProcess`,
`cmd/agentsview/serve_background.go:855`). Protocol: newline-delimited JSON on
stdout; human-readable errors on stderr; exit code 0/1.

**Files:**

- Create: `cmd/agentsview/sync_worker.go`
- Create: `cmd/agentsview/sync_worker_test.go`
- Modify: `cmd/agentsview/cli.go` (register the hidden command,
  `newRootCommand`, cli.go:72-131)

**Interfaces:**

- Produces:

```go
type workerLine struct {
	Progress *sync.Progress   `json:"progress,omitempty"`
	Stats    *workerStats     `json:"stats,omitempty"`
	Error    string           `json:"error,omitempty"`
}
type workerStats struct {
	Synced  int `json:"synced"`
	Skipped int `json:"skipped"`
	Failed  int `json:"failed"`
}
func newSyncWorkerCommand() *cobra.Command // hidden: "sync-worker"
func runSyncWorker(cfg config.Config, mode string, out io.Writer) error
```

Modes in this task: `startup`. (`resync-build` arrives in Task 8, `audit` in
Task 9; `runSyncWorker` should switch on mode from the start.)

- Consumes: `mustOpenWriteDB`-equivalent open (which acquires the
  `db.write.lock` flock, `cmd/agentsview/write_lock.go:30`), `sync.NewEngine`,
  `engine.SyncAll` / `engine.ResyncAll`, `database.NeedsResync()`.

- [ ] **Step 1: Write the failing worker test**

```go
func TestSyncWorkerStartupModeSyncsAndStreamsProgress(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t) // temp data dir + 3 claude sessions;
	// reuse the fixture helpers existing cmd/agentsview sync tests use.
	var out bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, "startup", &out))

	var sawProgress bool
	var stats *workerStats
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var line workerLine
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line),
			"every stdout line must be a workerLine JSON object")
		if line.Progress != nil {
			sawProgress = true
		}
		if line.Stats != nil {
			stats = line.Stats
		}
	}
	require.NoError(t, sc.Err())
	assert.True(t, sawProgress)
	require.NotNil(t, stats, "worker must end with a stats line")
	assert.Equal(t, 3, stats.Synced)

	db := openTestDBReadOnly(t, cfg) // sessions landed in the archive
	assertSessionCount(t, db, 3)
}

func TestSyncWorkerFailsWhenWriteLockHeld(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	lock := acquireTestWriteLock(t, cfg) // hold db.write.lock like a daemon
	defer lock.Release()
	var out bytes.Buffer
	err := runSyncWorker(cfg, "startup", &out)
	require.Error(t, err)
	assert.ErrorContains(t, err, "write lock")
}
```

Follow the existing helpers in `cmd/agentsview` tests for building a config with
fixture session dirs and for the write lock (see `write_lock.go` tests). If no
fixture helper exists, write the three Claude JSONL files inline the way
`parse_diff_test.go` does.

- [ ] **Step 2: Run to verify compile failure**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestSyncWorker ./cmd/agentsview -v
```

Expected: FAIL — `runSyncWorker` undefined.

- [ ] **Step 3: Implement the worker**

`cmd/agentsview/sync_worker.go`:

```go
func newSyncWorkerCommand() *cobra.Command {
	var mode string
	cmd := &cobra.Command{
		Use:    "sync-worker",
		Hidden: true,
		Short:  "Internal: run one heavy sync pass and exit",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfigForCommand(cmd) // same resolution serve uses
			if err != nil {
				return err
			}
			return runSyncWorker(cfg, mode, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&mode, "mode", "startup",
		"startup | resync-build | audit")
	return cmd
}

func runSyncWorker(cfg config.Config, mode string, out io.Writer) error {
	enc := json.NewEncoder(out)
	emit := func(line workerLine) { _ = enc.Encode(line) }
	onProgress := func(p sync.Progress) {
		emit(workerLine{Progress: &p})
	}
	switch mode {
	case "startup":
		database, releaseLock, err := openWorkerWriteDB(cfg)
		if err != nil {
			return err
		}
		defer releaseLock()
		defer database.Close()
		engine := sync.NewEngine(database, workerEngineConfig(cfg))
		var stats sync.SyncStats
		if database.NeedsResync() {
			stats = engine.ResyncAll(context.Background(), onProgress)
		} else {
			stats = engine.SyncAll(context.Background(), onProgress)
		}
		emit(workerLine{Stats: statsLine(stats)})
		return nil
	case "resync-build", "audit":
		return fmt.Errorf("sync-worker mode %q not implemented yet", mode)
	default:
		return fmt.Errorf("unknown sync-worker mode %q", mode)
	}
}
```

`openWorkerWriteDB` wraps the existing daemon open path
(`mustOpenWriteDB`-adjacent code, without the fatal-on-error behavior): it must
acquire the `db.write.lock` flock via `acquireWriteOwnerLock` (write_lock.go:30)
and return `writeOwnerLockHeldError`'s message when held. `workerEngineConfig`
mirrors the `sync.EngineConfig` literal in `runServe` (main.go:215) minus
daemon-only callbacks. `statsLine` maps `sync.SyncStats` fields to `workerStats`
(use the exported accessors the `/sync/status` handler uses).
`loadConfigForCommand` = whatever `newSyncCommand` (cli.go:297) uses for config
resolution; reuse it verbatim.

Register in `newRootCommand` (cli.go:72-131):

```go
	root.AddCommand(newSyncWorkerCommand())
```

- [ ] **Step 4: Run the tests**

```bash
CGO_ENABLED=1 go test -tags fts5 -run TestSyncWorker ./cmd/agentsview -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/agentsview
git commit -m "feat(daemon): add hidden sync-worker command with NDJSON progress"
```

______________________________________________________________________

### Task 7: Daemon startup runs the worker; in-process fallback

Startup catch-up sync (and startup-triggered full resync) is the largest
allocation high-water event in the daemon (~1.1 GB RSS observed after a full
resync). Run it in the worker **before** the daemon opens the archive writable,
then let the daemon proceed. The daemon relays worker progress into
`startup-state.json` so `serve status` and the background launcher keep working
(#1141 behavior preserved).

**Files:**

- Modify: `cmd/agentsview/main.go` (`runServe`: before `mustOpenWriteDB` at :143
  and the initial-sync block at :261-275)
- Create: `cmd/agentsview/startup_worker.go`
- Test: `cmd/agentsview/startup_worker_test.go`

**Interfaces:**

- Produces:

```go
// startup_worker.go
var launchSyncWorker = launchSyncWorkerProcess // stubbable in tests

func launchSyncWorkerProcess(ctx context.Context, cfg config.Config,
	mode string, onLine func(workerLine)) error

func runStartupSyncViaWorker(ctx context.Context, cfg config.Config,
	progress *startupStateWriter) error
```

- Consumes: self-exec pattern from `startServeBackgroundProcess`
  (serve_background.go:855): `os.Executable()`, `exec.Command`, env marker.
  Worker child env marker: `AGENTSVIEW_SYNC_WORKER=1` (new constant beside
  `backgroundChildEnvVar`, serve_background.go:64).

- [ ] **Step 1: Write the failing tests**

```go
func TestRunStartupSyncViaWorkerRelaysPhases(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	progress := newTestStartupStateWriter(t, cfg)
	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, mode string,
		onLine func(workerLine),
	) error {
		require.Equal(t, "startup", mode)
		phase := "initial sync"
		onLine(workerLine{Progress: &sync.Progress{Phase: sync.PhaseSyncing}})
		onLine(workerLine{Stats: &workerStats{Synced: 3}})
		_ = phase
		return nil
	})
	defer restore()
	require.NoError(t, runStartupSyncViaWorker(context.Background(), cfg, progress))
	assertStartupStateContains(t, cfg, "sync") // phase reached the file
}

func TestRunStartupSyncViaWorkerReportsSpawnFailure(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	progress := newTestStartupStateWriter(t, cfg)
	restore := stubLaunchSyncWorker(t, func(
		context.Context, config.Config, string, func(workerLine),
	) error {
		return fmt.Errorf("spawn failed")
	})
	defer restore()
	err := runStartupSyncViaWorker(context.Background(), cfg, progress)
	require.Error(t, err) // caller falls back to in-process sync
}
```

`stubLaunchSyncWorker` swaps the `launchSyncWorker` package var and restores it
(follow the `startServeBackgroundProcessForRun` stub pattern,
serve_background.go:28-29).

- [ ] **Step 2: Run to verify failure, then implement**

`startup_worker.go`:

```go
const syncWorkerChildEnvVar = "AGENTSVIEW_SYNC_WORKER"

func launchSyncWorkerProcess(
	ctx context.Context, cfg config.Config, mode string,
	onLine func(workerLine),
) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable for sync worker: %w", err)
	}
	cmd := exec.CommandContext(ctx, exe,
		"sync-worker", "--mode", mode, "--data-dir", cfg.DataDir)
	cmd.Env = append(os.Environ(), syncWorkerChildEnvVar+"=1")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start sync worker: %w", err)
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var line workerLine
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			continue // tolerate stray output; stderr carries diagnostics
		}
		onLine(line)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("sync worker (mode %s): %w", mode, err)
	}
	return nil
}

func runStartupSyncViaWorker(
	ctx context.Context, cfg config.Config, progress *startupStateWriter,
) error {
	return launchSyncWorker(ctx, cfg, "startup", func(line workerLine) {
		if line.Progress != nil {
			progress.SetPhase(startupPhaseForProgress(*line.Progress))
			progress.SetDetail(line.Progress.Detail)
		}
	})
}
```

Pass the config to the child the same way the CLI resolves it — if the daemon
was configured via flags/env rather than `--data-dir`, forward the same
mechanism `serveBackgroundChildArgs` uses (serve_background.go:904); reuse that
helper's approach rather than inventing flag forwarding.
`startupPhaseForProgress` maps `sync.Progress.Phase` (resync phases included)
onto the strings `startupProgress` already uses ("initial sync", "full resync",
...) — see main.go:261-275.

In `runServe`, insert **before** `mustOpenWriteDB` (main.go:143):

```go
	workerSyncDone := false
	if !opts.SkipInitialSync && !cfg.NoSync && !testing.Testing() {
		startupProgress.SetPhase("initial sync")
		if err := runStartupSyncViaWorker(ctx, cfg, startupProgress); err != nil {
			log.Printf("startup sync worker: %v (falling back to in-process)", err)
		} else {
			workerSyncDone = true
		}
	}
```

and guard the existing initial-sync block (main.go:261) with
`if !opts.SkipInitialSync && !workerSyncDone { ... }` so it remains the
in-process fallback (and the path all Go tests take, via `testing.Testing()` —
the same guard pattern the telemetry reporter uses). The `--skip-initial-sync`
deferred-fallback path (main.go:389-404) is untouched.

- [ ] **Step 3: Run the tests**

```bash
CGO_ENABLED=1 go test -tags fts5 -run 'StartupSync|StartupWorker' ./cmd/agentsview -v
CGO_ENABLED=1 go test -tags fts5 ./cmd/agentsview -count 1
```

Expected: PASS, including `startup_sync_fallback_test.go` and
`TestInitialSyncWatcherStartupOwnerReconciles` (main_test.go:1291).

- [ ] **Step 4: End-to-end smoke on a scratch clone**

```bash
make build
AGENTSVIEW_DATA_DIR=/tmp/av-scratch ./agentsview serve --port 0 &
sleep 2 && ./agentsview serve status; kill %1
```

Use a scratch data dir with a few copied fixture sessions — never a live
archive. Expected: `ps` shows the `sync-worker` child appear and exit before the
server reports ready; `serve status` shows startup phases during the worker run.

- [ ] **Step 5: Commit**

```bash
git add cmd/agentsview
git commit -m "feat(daemon): run startup sync in an attached worker process"
```

______________________________________________________________________

### Task 8: Runtime full resync via worker (build out-of-process, swap in-daemon)

Runtime resync (`POST /api/v1/resync`,
`internal/server/huma_routes_sync.go:229`) currently rebuilds the archive
in-process (`resyncAllWithOptionsLocked`, engine.go:1566-2231), leaving the
daemon at resync high-water forever. Split it: the worker builds the temp DB
(`<db>-resync`) and performs all copy phases reading the original archive
**read-only**; the daemon, holding `syncMu` the whole time so the original is
write-quiescent, performs only the small swap tail.

**Files:**

- Modify: `internal/sync/engine.go` (extract build/swap from
  `resyncAllWithOptionsLocked`)
- Modify: `cmd/agentsview/sync_worker.go` (`resync-build` mode)
- Modify: `cmd/agentsview/main.go` / `internal/server/huma_routes_sync.go`
  wiring (`runResyncWithFallback`, huma_routes_sync.go:251)
- Test: `internal/sync/engine_resync_split_test.go` (new),
  `cmd/agentsview/sync_worker_test.go`

**Interfaces:**

- Produces:

```go
// ResyncBuild builds a complete replacement archive at the returned temp
// path (origPath + "-resync"), including orphan/metadata copy phases, using
// read-only access to the original archive. The caller must guarantee the
// original receives no writes while it runs.
func (e *Engine) ResyncBuild(ctx context.Context, onProgress ProgressFunc) (tempPath string, stats SyncStats, err error)

// SwapResyncDatabase installs a built replacement archive: close
// connections, remove WAL sidecars, rename, reopen, MarkDataCurrent,
// checkpoint. Extracted verbatim from resyncAllWithOptionsLocked's tail
// (engine.go:2170-2219) so the in-process path calls the same code.
func (e *Engine) SwapResyncDatabase(tempPath string) error
```

- Consumes: worker plumbing from Tasks 6-7, `engine.RunExclusive`
  (engine.go:2620).

- [ ] **Step 1: Write the failing split test**

```go
func TestResyncBuildThenSwapMatchesResyncAll(t *testing.T) {
	// Fixture: archive with (a) sessions whose files exist, (b) one orphan
	// session whose source file was deleted after initial sync.
	e, database, cfg := newResyncTestEngine(t) // reuse existing resync test setup
	deleteOneSourceFile(t, cfg)

	tempPath, stats, err := e.ResyncBuild(context.Background(), nil)
	require.NoError(t, err)
	require.FileExists(t, tempPath)
	require.NoError(t, e.SwapResyncDatabase(tempPath))

	assert.False(t, database.NeedsResync())
	assertSessionPresent(t, database, orphanSessionID,
		"orphan sessions must survive the split resync")
	assert.Positive(t, stats.TotalSessions)
	assert.NoFileExists(t, tempPath)
}
```

Base `newResyncTestEngine` on the existing `ResyncAll` engine tests (find them
with `rg -n 'ResyncAll' internal/sync/*_test.go`) — same fixtures, same
assertions style.

- [ ] **Step 2: Run to verify compile failure, then extract**

Mechanical extraction from `resyncAllWithOptionsLocked`:

1. `SwapResyncDatabase(tempPath)` = the exact sequence at engine.go:2170-2219:
   `newDB.Close()` (the build side closes its own handle before returning, so
   the swap starts at) `origDB.CloseConnections()` -> `removeWAL(origPath)` ->
   `os.Rename(tempPath, origPath)` -> `removeWAL(tempPath)` ->
   `origDB.Reopen()` -> `MarkDataCurrent()` ->
   `CheckpointWALTruncateWithRetry`. Keep `shouldAbortResyncSwap`
   (engine.go:1461) and every recovery path (`removeTempDB`, `origDB.Reopen()`
   on failure) inside it.
1. `ResyncBuild` = everything before that tail, with one change: instead of
   `origDB.CloseConnections()` before the copy phases (engine.go:1922), open a
   **separate read-only handle** on `origPath` (`db.OpenReadOnly`, db.go:1246)
   for `CopyExcludedSessionsFrom` / `CopySyncStateFrom` / `CopyInsightsFrom` /
   `CopyModelPricingFrom` / `CopyOrphanedDataFromExcluding` /
   `CopyRecallEntriesFrom` / `CopySessionMetadataFrom` (engine.go:1958-2100).
   If any copy helper takes a path rather than a handle it already works; for
   handle-based helpers, verify they accept a read-only source (they read via
   ATTACH or SELECT; a WAL reader sees a consistent snapshot). Do not modify
   the copy helpers' write side.
1. Rewrite `resyncAllWithOptionsLocked` as `ResyncBuild` + `SwapResyncDatabase`
   so the in-process path (startup worker mode, CLI `sync --full`, fallback)
   exercises the same code. All existing resync tests must pass unchanged.

- [ ] **Step 3: Wire the worker mode and the daemon handler**

`sync_worker.go`, `resync-build` mode: open the ORIGINAL archive read-only (no
write flock needed — the temp DB is a different file and the daemon guarantees
quiescence), build via a build-only engine:

```go
	case "resync-build":
		database, err := db.OpenReadOnly(cfg.DBPath)
		if err != nil {
			return err
		}
		defer database.Close()
		engine := sync.NewEngine(database, workerEngineConfig(cfg))
		tempPath, stats, err := engine.ResyncBuild(context.Background(), onProgress)
		if err != nil {
			emit(workerLine{Error: err.Error()})
			return err
		}
		emit(workerLine{Stats: statsLine(stats)})
		_ = tempPath // daemon derives it as cfg.DBPath + "-resync"
		return nil
```

If `ResyncBuild` requires a writable engine handle for incidental state, prefer
fixing it to accept read-only; the build must not write the original.

Daemon side — in `runResyncWithFallback` (huma_routes_sync.go:251), when not
under test and spawn succeeds:

```go
	err := engine.RunExclusive(func() error {
		if err := launchSyncWorker(ctx, cfg, "resync-build", relayProgress); err != nil {
			return err
		}
		return engine.SwapResyncDatabase(cfg.DBPath + sync.ResyncTempSuffix)
	})
```

Export `ResyncTempSuffix = "-resync"` (currently unexported const,
engine.go:1435) or add an `Engine.ResyncTempPath()` accessor. On worker error or
`testing.Testing()`, fall back to the existing in-process `ResyncAll` path. Emit
the same SSE/broadcaster events the in-process path emits on success.

- [ ] **Step 4: Run the suites**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/sync -run Resync -count 1 -v
CGO_ENABLED=1 go test -tags fts5 ./internal/sync ./internal/server ./cmd/agentsview -count 1
```

Expected: PASS — especially every existing resync preservation test (orphan
copy, exclusions, FTS rebuild, abort recovery).

- [ ] **Step 5: Commit**

```bash
git add internal/sync internal/server cmd/agentsview
git commit -m "feat(sync): build runtime resyncs in the worker, swap in-daemon"
```

______________________________________________________________________

### Task 9: Daily archive audit via worker with writer handoff

With the unscoped 15-minute pass gone (Task 5), silent watcher event loss needs
a rare safety net: a full `SyncAll` once per day, run by the worker against the
**live** archive. The daemon temporarily yields write ownership: it closes its
writer connection (readers stay open, so the API keeps serving), releases the
`db.write.lock` flock, lets the worker sync, then reacquires and reopens. WAL is
exactly the multi-process one-writer/many-readers model this needs.

**Files:**

- Modify: `internal/db/db.go` (`CloseWriter` / `ReopenWriter`)
- Modify: `cmd/agentsview/write_lock.go` (release/reacquire helpers if the
  current lock type lacks them)
- Modify: `cmd/agentsview/main.go` (audit ticker in `startPeriodicSync`)
- Modify: `cmd/agentsview/sync_worker.go` (`audit` mode = the Task 6 `startup`
  body without the resync branch; share the code)
- Test: `internal/db/db_test.go` additions,
  `cmd/agentsview/archive_audit_test.go` (new)

**Interfaces:**

- Produces:

```go
// internal/db
func (db *DB) CloseWriter() error  // close writer pool only; readers stay
func (db *DB) ReopenWriter() error // reopen writer DSN + configureWAL

// cmd/agentsview/main.go
const archiveAuditInterval = 24 * time.Hour
func runArchiveAudit(ctx context.Context, cfg config.Config,
	engine *sync.Engine, database *db.DB, writeLock *writeOwnerLock,
	emitter sync.Emitter) error
```

- Consumes: `launchSyncWorker` (Task 7), `engine.RunExclusive`.

- [ ] **Step 1: Write the failing DB tests**

```go
func TestCloseWriterKeepsReadersServing(t *testing.T) {
	database := testDB(t)
	seedOneSession(t, database)
	require.NoError(t, database.CloseWriter())
	_, err := database.SessionCount(context.Background()) // any reader query
	assert.NoError(t, err, "readers must survive a writer handoff")
	err = writeOneSession(database) // any writer op
	require.Error(t, err, "writes must fail cleanly while the writer is closed")
	require.NoError(t, database.ReopenWriter())
	assert.NoError(t, writeOneSession(database))
}
```

Model the writer/reader pool manipulation on `CloseConnections`/`Reopen`
(db.go:3420/3450): `CloseWriter` swaps the `writer atomic.Pointer[sql.DB]` to
nil under `connMu` and closes the old pool; `ReopenWriter` re-runs the
writer-open half of `Reopen` (writable DSN via `makeDSN(path, false)`,
`SetMaxOpenConns(1)`, `configureWAL`). Writes while closed must return a clear
error ("writer closed for archive audit"), never panic.

- [ ] **Step 2: Implement `CloseWriter`/`ReopenWriter`, run the tests**

```bash
CGO_ENABLED=1 go test -tags fts5 -run 'CloseWriter|ReopenWriter' ./internal/db -v
```

Expected: PASS.

- [ ] **Step 3: Write the failing audit orchestration test**

```go
func TestRunArchiveAuditYieldsWriteOwnershipAroundWorker(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, writeLock := openTestWriteDB(t, cfg)
	engine := sync.NewEngine(database, sync.EngineConfig{})
	var order []string
	restore := stubLaunchSyncWorker(t, func(
		_ context.Context, _ config.Config, mode string, _ func(workerLine),
	) error {
		require.Equal(t, "audit", mode)
		assert.False(t, writeLock.Held(), "flock must be released before spawn")
		order = append(order, "worker")
		return nil
	})
	defer restore()
	require.NoError(t, runArchiveAudit(
		context.Background(), cfg, engine, database, writeLock, nil))
	order = append(order, "after")
	assert.Equal(t, []string{"worker", "after"}, order)
	assert.True(t, writeLock.Held(), "flock must be reacquired after the worker")
	assert.NoError(t, writeOneSession(database), "writer must be reopened")
}
```

Add a `Held() bool` accessor to the write-lock type if absent.

- [ ] **Step 4: Implement the orchestration**

```go
func runArchiveAudit(
	ctx context.Context, cfg config.Config, engine *sync.Engine,
	database *db.DB, writeLock *writeOwnerLock, emitter sync.Emitter,
) error {
	return engine.RunExclusive(func() error {
		if err := database.CloseWriter(); err != nil {
			return fmt.Errorf("close writer for archive audit: %w", err)
		}
		if err := writeLock.Release(); err != nil {
			_ = database.ReopenWriter()
			return fmt.Errorf("release write lock for archive audit: %w", err)
		}
		workerErr := launchSyncWorker(ctx, cfg, "audit", func(workerLine) {})
		if err := writeLock.Reacquire(); err != nil {
			return fmt.Errorf("reacquire write lock after archive audit: %w", err)
		}
		if err := database.ReopenWriter(); err != nil {
			return fmt.Errorf("reopen writer after archive audit: %w", err)
		}
		if workerErr != nil {
			return fmt.Errorf("archive audit worker: %w", workerErr)
		}
		if emitter != nil {
			emitter.Emit("sessions")
		}
		return nil
	})
}
```

The reacquire path must be unconditional (even when the worker failed) — losing
write ownership permanently is worse than a failed audit. Worker `audit` mode
reuses the `startup` body's `SyncAll` branch (never `ResyncAll`); the worker's
engine loads the persisted skip cache from the archive (`skipped_files`,
engine.go:431-441), so a healthy audit is a warm, mostly-skip pass.

Add the ticker to `startPeriodicSync` (alongside the 15-minute ticker):

```go
	auditTicker := time.NewTicker(archiveAuditInterval)
	defer auditTicker.Stop()
```

and a `case <-auditTicker.C:` arm that calls
`idleTracker.Do(func() { ... runArchiveAudit ... })`, logging errors and, when
`testing.Testing()` or spawn fails, falling back to in-process
`engine.SyncAll(ctx, nil)` (log the fallback; the safety net must never silently
disappear).

- [ ] **Step 5: Run the suites**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/db -count 1
CGO_ENABLED=1 go test -tags fts5 ./cmd/agentsview -count 1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/db cmd/agentsview
git commit -m "feat(daemon): run the daily archive audit in the worker"
```

______________________________________________________________________

### Task 10: Whole-branch verification and RSS acceptance gate

**Files:** none created; this is the finish gate.

- [ ] **Step 1: Full local gates**

```bash
make lint    # golangci-lint + NilAway
make test    # full suite, CGO + fts5
go vet ./... && go fmt ./...
```

Expected: zero warnings, zero failures. (Per-task gates miss NilAway and the
non-pgtest unit set — this whole-branch run is mandatory.)

- [ ] **Step 2: Bench gate against merge-base**

Re-run Task 2 Step 1's comparison. Expected: `benchgate` passes for all
benchmarks, including `BenchmarkSyncAllWarmNoop`.

- [ ] **Step 3: E2E**

```bash
make e2e
```

Expected: PASS. The e2e binaries are real processes, so they exercise the worker
spawn paths (`testing.Testing()` is false there).

- [ ] **Step 4: Manual RSS acceptance run (darwin)**

Against an **isolated production-scale clone** of the database and source dirs
(never live archives):

```bash
make build
AGENTSVIEW_DATA_DIR=/path/to/isolated-clone ./agentsview serve --port 0 &
AV_PID=$!
sleep 120  # startup worker done, daemon settled
vmmap --summary $AV_PID | rg 'Physical footprint'
lsof -p $AV_PID | rg -c 'VDIR|VREG'   # descriptor count
```

Then leave it running against the clone while a script appends to session files
periodically (simulating agent activity) and re-measure after several hours; if
practical, run the full 24 h retention observation. Record: physical footprint
after startup, peak during a triggered `POST /resync` (should spike in the
*worker*, not the daemon), footprint after the audit tick, and descriptor count
(ancestor watch should contribute ~1 fd, not ~823).

Acceptance: daemon physical footprint under 300 MB after startup and after the
observation window; no 15-minute CPU bursts in `top`/profiles; worker processes
appear and exit around startup/resync/audit.

- [ ] **Step 5: Document and hand off**

Record the measured numbers in the PR description (no test-plan section — just
the memory outcome as motivation/limitations). State explicitly that the Gemini
fingerprint change causes a one-time re-verify pass.

______________________________________________________________________

## Self-Review Notes

- Spec coverage: option 1's five bullets map to Tasks 5+9 (unscoped SyncAll
  removal + scoped reconciliation + audit safety net), Tasks 6-9 (attached
  worker for startup/resync/audit), Task 4 (one-fd vnode observer), Task 3
  (Gemini invalidation), Tasks 1-2 (bench regression). Acceptance criteria are
  Task 10.
- Type consistency: `workerLine`/`workerStats` defined in Task 6 and consumed in
  Tasks 7-9; `launchSyncWorker` defined in Task 7, consumed in Tasks 8-9;
  `ResyncBuild`/`SwapResyncDatabase` defined in Task 8;
  `CloseWriter`/`ReopenWriter` defined in Task 9 where first used.
- Known judgment calls an implementer may need to adapt (verify against the
  code, they are anchored but the branch is large): exact `WatchRoot` field
  names (Task 4 Step 6), the fixture/lock helper names in `cmd/agentsview`
  tests (Tasks 6-9), and whether the second skip-cache read in
  `processProviderFile` sits above or below the new acquisition seam (Task 1
  Step 4.3).
