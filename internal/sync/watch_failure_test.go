package sync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
)

type reconciliationRetryRootError interface {
	error
	ReconciliationRetryRoots() []string
}

type fingerprintCountingProvider struct {
	*directStreamingProvider
	fingerprintCalls int
}

type changedPathFailureProvider struct {
	parser.ProviderBase
	root              string
	watchPlanErr      error
	classificationErr error
	storedHintScopes  bool
	contextValue      any
	sourceCalls       int
}

func (p *changedPathFailureProvider) WatchPlan(
	context.Context,
) (parser.WatchPlan, error) {
	if p.watchPlanErr != nil {
		return parser.WatchPlan{}, p.watchPlanErr
	}
	return parser.WatchPlan{Roots: []parser.WatchRoot{{Path: p.root}}}, nil
}

func (p *changedPathFailureProvider) SourcesForChangedPath(
	ctx context.Context, _ parser.ChangedPathRequest,
) ([]parser.SourceRef, error) {
	p.sourceCalls++
	p.contextValue = ctx.Value(changedPathContextKey{})
	return nil, p.classificationErr
}

func (p *changedPathFailureProvider) StoredSourceHintScopes(
	req parser.ChangedPathRequest,
) []parser.StoredSourceHintScope {
	if !p.storedHintScopes {
		return nil
	}
	return []parser.StoredSourceHintScope{{Path: req.Path}}
}

func (*changedPathFailureProvider) Parse(
	context.Context, parser.ParseRequest,
) (parser.ParseOutcome, error) {
	return parser.ParseOutcome{}, nil
}

type changedPathFailureFactory struct {
	provider *changedPathFailureProvider
}

func (f changedPathFailureFactory) Definition() parser.AgentDef {
	return f.provider.Definition()
}

func (f changedPathFailureFactory) Capabilities() parser.Capabilities {
	return f.provider.Capabilities()
}

func (f changedPathFailureFactory) NewProvider(
	cfg parser.ProviderConfig,
) parser.Provider {
	f.provider.Config = cfg.Clone()
	return f.provider
}

type changedPathContextKey struct{}

func newChangedPathFailureEngine(
	t *testing.T,
	database *db.DB,
	root string,
	provider *changedPathFailureProvider,
) *Engine {
	t.Helper()
	provider.root = root
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{"changed-path-failure": {root}},
		Machine:   "local",
		ProviderFactories: []parser.ProviderFactory{
			changedPathFailureFactory{provider: provider},
		},
		ProviderMigrationModes: map[parser.AgentType]parser.ProviderMigrationMode{
			"changed-path-failure": parser.ProviderMigrationProviderAuthoritative,
		},
	})
	t.Cleanup(engine.Close)
	return engine
}

func (p *fingerprintCountingProvider) Fingerprint(
	context.Context, parser.SourceRef,
) (parser.SourceFingerprint, error) {
	p.fingerprintCalls++
	return parser.SourceFingerprint{Hash: "stored-hash"}, nil
}

func TestSyncPathsContextPropagatesChangedPathClassificationFailure(
	t *testing.T,
) {
	database := openTestDB(t)
	root := t.TempDir()
	changedPath := filepath.Join(root, "replacement.jsonl")
	missingPath := filepath.Join(root, "previous.jsonl")
	require.NoError(t, os.WriteFile(changedPath, []byte("{}\n"), 0o600))
	require.NoError(t, database.UpsertSession(db.Session{
		ID: "previous", Agent: "changed-path-failure", Project: "project",
		Machine: "local", FilePath: &missingPath,
	}))
	require.NoError(t, database.BaselineActiveSessionSourcePaths(
		t.Context(), "local", []db.SessionSourcePath{{
			Agent: "changed-path-failure", FilePath: missingPath,
		}},
	))
	wantErr := errors.New("changed-path classification failed")
	provider := &changedPathFailureProvider{
		ProviderBase: parser.ProviderBase{
			Def: parser.AgentDef{Type: "changed-path-failure", FileBased: true},
		},
		classificationErr: wantErr,
	}
	engine := newChangedPathFailureEngine(t, database, root, provider)
	ctx := context.WithValue(t.Context(), changedPathContextKey{}, "caller")

	err := engine.SyncPathsContext(ctx, []string{changedPath, missingPath})

	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, "caller", provider.contextValue,
		"changed-path classification must receive the watcher context")
	stored, getErr := database.GetSession(t.Context(), "previous")
	require.NoError(t, getErr)
	assert.NotNil(t, stored,
		"a classification failure must suppress missing-source tombstones")
}

func TestSyncPathsContextPropagatesChangedPathWatchRootFailure(t *testing.T) {
	database := openTestDB(t)
	root := t.TempDir()
	changedPath := filepath.Join(root, "session.jsonl")
	require.NoError(t, os.WriteFile(changedPath, []byte("{}\n"), 0o600))
	wantErr := errors.New("watch root resolution failed")
	provider := &changedPathFailureProvider{
		ProviderBase: parser.ProviderBase{
			Def: parser.AgentDef{Type: "changed-path-failure", FileBased: true},
		},
		watchPlanErr: wantErr,
	}
	engine := newChangedPathFailureEngine(t, database, root, provider)

	err := engine.SyncPathsContext(t.Context(), []string{changedPath})

	require.ErrorIs(t, err, wantErr)
	assert.Zero(t, provider.sourceCalls,
		"classification must stop when its watch-root plan is unresolved")
}

func TestSyncPathsContextPropagatesStoredHintCancellation(t *testing.T) {
	database := openTestDB(t)
	root := t.TempDir()
	changedPath := filepath.Join(root, "container.db")
	require.NoError(t, os.WriteFile(changedPath, []byte("fixture"), 0o600))
	provider := &changedPathFailureProvider{
		ProviderBase: parser.ProviderBase{
			Def: parser.AgentDef{Type: "changed-path-failure", FileBased: true},
			Caps: parser.Capabilities{Source: parser.SourceCapabilities{
				StoredSourceHints: parser.CapabilitySupported,
			}},
		},
		storedHintScopes: true,
	}
	engine := newChangedPathFailureEngine(t, database, root, provider)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := engine.SyncPathsContext(ctx, []string{changedPath})

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, provider.sourceCalls,
		"provider classification must not run without its requested stored hints")
}

func TestSyncPathsContextDoesNotTombstoneAfterIncompleteReplacement(t *testing.T) {
	database := openTestDB(t)
	root := t.TempDir()
	changedPath := filepath.Join(root, "replacement.jsonl")
	missingPath := filepath.Join(root, "previous.jsonl")
	require.NoError(t, os.WriteFile(changedPath, []byte("{}\n"), 0o600))
	require.NoError(t, database.UpsertSession(db.Session{
		ID: "previous", Agent: "watch-failure", Project: "project",
		Machine: "local", FilePath: &missingPath,
	}))
	source := parser.SourceRef{
		Provider: "watch-failure", Key: changedPath,
		DisplayPath: changedPath, FingerprintKey: changedPath,
	}
	provider := &directStreamingProvider{
		ProviderBase: parser.ProviderBase{
			Def: parser.AgentDef{Type: "watch-failure", FileBased: true},
			Caps: parser.Capabilities{Source: parser.SourceCapabilities{
				DiscoverSources: parser.CapabilitySupported,
				WatchSources:    parser.CapabilitySupported,
				FindSource:      parser.CapabilitySupported,
			}},
		},
		source:   &source,
		parseErr: errors.New("replacement parse failed"),
	}
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{"watch-failure": {root}},
		Machine:   "local",
		ProviderFactories: []parser.ProviderFactory{
			directStreamingFactory{provider: provider},
		},
		ProviderMigrationModes: map[parser.AgentType]parser.ProviderMigrationMode{
			"watch-failure": parser.ProviderMigrationProviderAuthoritative,
		},
	})
	t.Cleanup(engine.Close)

	err := engine.SyncPathsContext(t.Context(), []string{changedPath, missingPath})

	require.Error(t, err)
	assert.ErrorContains(t, err, "incomplete")
	stored, getErr := database.GetSession(t.Context(), "previous")
	require.NoError(t, getErr)
	assert.NotNil(t, stored,
		"a failed replacement must not hide the previous archived session")
}

func TestReconcileWatchRootsCommitsHealthyProvidersAndScopesFailedRetry(t *testing.T) {
	database := openTestDB(t)
	healthyRoot := t.TempDir()
	failedRoot := t.TempDir()
	healthyPath := filepath.Join(healthyRoot, "session.jsonl")
	healthyMissingPath := filepath.Join(healthyRoot, "removed.jsonl")
	require.NoError(t, os.WriteFile(healthyPath, []byte("{}\n"), 0o600))
	require.NoError(t, database.UpsertSession(db.Session{
		ID: "healthy-removed", Agent: "healthy-stream", Project: "project",
		Machine: "local", FilePath: &healthyMissingPath,
	}))
	require.NoError(t, database.BaselineActiveSessionSourcePaths(
		t.Context(), "local", []db.SessionSourcePath{{
			Agent: "healthy-stream", FilePath: healthyMissingPath,
		}},
	))
	healthySource := parser.SourceRef{
		Provider: "healthy-stream", Key: healthyPath,
		DisplayPath: healthyPath, FingerprintKey: healthyPath,
	}
	started := time.Unix(1704067200, 0)
	healthy := &directStreamingProvider{
		ProviderBase: parser.ProviderBase{
			Def: parser.AgentDef{Type: "healthy-stream", FileBased: true},
			Caps: parser.Capabilities{Source: parser.SourceCapabilities{
				DiscoverSources:    parser.CapabilitySupported,
				StreamingDiscovery: parser.CapabilitySupported,
				WatchSources:       parser.CapabilitySupported,
				FindSource:         parser.CapabilitySupported,
			}},
		},
		source: &healthySource,
		parseOutcome: parser.ParseOutcome{
			Results: []parser.ParseResultOutcome{{
				Result: parser.ParseResult{Session: parser.ParsedSession{
					ID: "healthy-session", Agent: "healthy-stream",
					Project: "project", Machine: "local",
					StartedAt: started, EndedAt: started,
					File: parser.FileInfo{Path: healthyPath},
				}},
				DataVersion: parser.DataVersionCurrent,
			}},
			ResultSetComplete: true,
		},
	}
	failed := &failingDBBackedProvider{
		ProviderBase: parser.ProviderBase{Def: parser.AgentDef{
			Type: "failed-stream", FileBased: true,
		}},
		err: errors.New("permission denied"), failOnCall: 1,
	}
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{
			"healthy-stream": {healthyRoot},
			"failed-stream":  {failedRoot},
		},
		Machine: "local",
		ProviderFactories: []parser.ProviderFactory{
			directStreamingFactory{provider: healthy},
			failingDBBackedFactory{provider: failed},
		},
		ProviderMigrationModes: map[parser.AgentType]parser.ProviderMigrationMode{
			"healthy-stream": parser.ProviderMigrationProviderAuthoritative,
			"failed-stream":  parser.ProviderMigrationProviderAuthoritative,
		},
	})
	t.Cleanup(engine.Close)

	err := engine.ReconcileWatchRoots(context.Background(), nil, true)

	require.Error(t, err)
	stored, getErr := database.GetSession(t.Context(), "healthy-session")
	require.NoError(t, getErr)
	assert.NotNil(t, stored,
		"one provider failure must not discard healthy provider candidates")
	removed, getErr := database.GetSessionFull(t.Context(), "healthy-removed")
	require.NoError(t, getErr)
	require.NotNil(t, removed)
	require.NotNil(t, removed.DeletionCause)
	assert.Equal(t, "source_missing", *removed.DeletionCause,
		"a failed provider must not discard healthy-scope deletion coverage")
	var retryErr reconciliationRetryRootError
	require.ErrorAs(t, err, &retryErr)
	assert.Equal(t, []string{failedRoot}, retryErr.ReconciliationRetryRoots())
}

func TestReconciliationCandidateDoesNotHashStableClaudeSource(t *testing.T) {
	database := openTestDB(t)
	root := t.TempDir()
	path := filepath.Join(root, "stable-session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	size := info.Size()
	mtime := info.ModTime().UnixNano()
	hash := "stored-hash"
	require.NoError(t, database.UpsertSession(db.Session{
		ID: "stable-session", Agent: string(parser.AgentClaude),
		Project: "project", Machine: "local", FilePath: &path,
		FileSize: &size, FileMtime: &mtime, FileHash: &hash,
		DataVersion: db.CurrentDataVersion(),
	}))
	require.NoError(t, database.SetSessionDataVersion(
		"stable-session", db.CurrentDataVersion(),
	))
	storedSize, storedMtime, stored := database.GetSessionFileInfo("stable-session")
	require.True(t, stored)
	assert.Equal(t, size, storedSize)
	assert.Equal(t, mtime, storedMtime)
	assert.Equal(t, db.CurrentDataVersion(), database.GetSessionDataVersion("stable-session"))
	base := &directStreamingProvider{ProviderBase: parser.ProviderBase{
		Def: parser.AgentDef{Type: parser.AgentClaude, FileBased: true},
	}}
	provider := &fingerprintCountingProvider{directStreamingProvider: base}
	engine := &Engine{db: database}
	source := parser.SourceRef{
		Provider: parser.AgentClaude, Key: path,
		DisplayPath: path, FingerprintKey: path,
	}

	_, admitted := engine.reconciliationCandidate(
		provider, source, []string{root}, nil,
	)

	assert.True(t, admitted)
	assert.Zero(t, provider.fingerprintCalls,
		"duplicate preference must not hash every unchanged Claude transcript")
}
