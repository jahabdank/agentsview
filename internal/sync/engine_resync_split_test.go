package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

// newResyncSplitEngine builds an engine over a fresh archive with three synced
// Claude sessions. It returns the engine, its database, and the source root so
// callers can delete a source file and drive a resync.
func newResyncSplitEngine(t *testing.T) (*Engine, *db.DB, string) {
	t.Helper()
	root := t.TempDir()
	database, err := db.Open(filepath.Join(t.TempDir(), "archive.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	engine := NewEngine(database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}},
		Machine:   "local",
	})
	t.Cleanup(engine.Close)

	for _, name := range []string{"keep0", "keep1", "orphan"} {
		path := filepath.Join(root, "project", name+".jsonl")
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		content := testjsonl.NewSessionBuilder().
			AddClaudeUser("2026-01-01T00:00:00Z", "hello "+name).
			AddClaudeAssistant("2026-01-01T00:00:01Z", "hi "+name).
			String()
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	require.Equal(t, 3, engine.SyncAll(context.Background(), nil).Synced)
	return engine, database, root
}

// TestResyncBuildThenSwapMatchesResyncAll drives the split resync path
// end-to-end: build the replacement, swap it in, reset caches. It must preserve
// an orphan session whose source file was deleted, clean up the temp file, and
// hand off warm skip state so an immediate sync is a no-op.
func TestResyncBuildThenSwapMatchesResyncAll(t *testing.T) {
	e, database, root := newResyncSplitEngine(t)
	require.NoError(t, os.Remove(filepath.Join(root, "project", "orphan.jsonl")))

	tempPath, stats, err := e.ResyncBuild(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, stats.Aborted)
	require.FileExists(t, tempPath)

	require.NoError(t, e.SwapResyncDatabase(tempPath))
	require.NoError(t, e.ResetCachesAfterSwap())

	assert.False(t, database.NeedsResync())
	orphan, err := database.GetSession(context.Background(), "orphan")
	require.NoError(t, err)
	require.NotNil(t, orphan, "orphan sessions must survive the split resync")
	assert.Positive(t, stats.TotalSessions)
	assert.NoFileExists(t, tempPath)

	warm := e.SyncAll(context.Background(), nil)
	assert.Zero(t, warm.Synced, "persisted skip state must survive the swap")
}

// TestSwapWindowRejectsDirectWrites proves the write barrier: with the writer
// closed a direct star write is rejected with ErrWriterClosed, and the rejected
// write is absent from the rebuilt archive after the swap.
func TestSwapWindowRejectsDirectWrites(t *testing.T) {
	e, database, _ := newResyncSplitEngine(t)

	require.NoError(t, database.CloseWriter())
	_, starErr := database.StarSession("keep0")
	assert.ErrorIs(t, starErr, db.ErrWriterClosed,
		"a direct write during the barrier window must be rejected")

	// The build reads the original and writes only the replacement, so it
	// proceeds while the writer is closed. The swap reopens the writer.
	tempPath, _, err := e.ResyncBuild(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, e.SwapResyncDatabase(tempPath))
	require.NoError(t, e.ResetCachesAfterSwap())

	starred, err := database.ListStarredSessionIDs(context.Background())
	require.NoError(t, err)
	assert.NotContains(t, starred, "keep0",
		"a rejected write must not appear in the swapped archive")

	// The writer is usable again after the swap reopened it.
	ok, err := database.StarSession("keep0")
	require.NoError(t, err)
	assert.True(t, ok)
}
