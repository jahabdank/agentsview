package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingUnwatchedPollSyncer struct {
	mu           sync.Mutex
	calls        [][]string
	full         []bool
	wake         chan struct{}
	reconcileErr error
}

func (s *recordingUnwatchedPollSyncer) ReconcileWatchRoots(
	_ context.Context, roots []string, full bool,
) error {
	s.mu.Lock()
	s.calls = append(s.calls, append([]string(nil), roots...))
	s.full = append(s.full, full)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return s.reconcileErr
}

func (s *recordingUnwatchedPollSyncer) snapshot() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([][]string, len(s.calls))
	for i := range s.calls {
		result[i] = append([]string(nil), s.calls[i]...)
	}
	return result
}

func TestUnwatchedPollConcurrentAddDeduplicatesUpdatedRootSet(t *testing.T) {
	ticks := make(chan time.Time)
	syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 4)}
	coordinator := newUnwatchedPollCoordinatorWithTicks(
		t.Context(), syncer, ticks, func() {}, func(run func()) { run() }, nil,
	)
	t.Cleanup(coordinator.Stop)

	additions := [][]string{
		{"/root-b", "/root-a"},
		{"/root-a", "/root-c"},
		{"/root-c", "/root-b"},
	}
	var wg sync.WaitGroup
	addErrors := make(chan error, len(additions))
	for _, roots := range additions {
		wg.Go(func() {
			addErrors <- coordinator.AddRoots(roots)
		})
	}
	wg.Wait()
	close(addErrors)
	for err := range addErrors {
		require.NoError(t, err)
	}

	coordinator.Wake()
	requirePollWithin(t, syncer.wake, time.Second)
	assert.Equal(t, [][]string{{"/root-a", "/root-b", "/root-c"}}, syncer.snapshot())
}

func TestUnwatchedPollTickUsesRootsAddedAfterStart(t *testing.T) {
	ticks := make(chan time.Time, 1)
	syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 2)}
	coordinator := newUnwatchedPollCoordinatorWithTicks(
		t.Context(), syncer, ticks, func() {}, func(run func()) { run() }, nil,
	)
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.AddRoots([]string{"/initial"}))
	require.NoError(t, coordinator.AddRoots([]string{"/runtime"}))

	ticks <- time.Now()
	requirePollWithin(t, syncer.wake, time.Second)

	assert.Equal(t, [][]string{{"/initial", "/runtime"}}, syncer.snapshot())
	assert.Equal(t, []bool{false}, syncer.full,
		"unwatched polling must reconcile the owned scopes authoritatively")
}

func TestUnwatchedPollRemoveRootsStopsReconciliationAfterNativeRecovery(t *testing.T) {
	ticks := make(chan time.Time, 1)
	syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 2)}
	coordinator := newUnwatchedPollCoordinatorWithTicks(
		t.Context(), syncer, ticks, func() {}, func(run func()) { run() }, nil,
	)
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.AddObligation(pollingObligation{
		Key: "recovered-watch", Roots: []string{"/recovered"},
	}))
	require.NoError(t, coordinator.AddObligation(pollingObligation{
		Key: "still-unwatched", Roots: []string{"/still-unwatched"},
	}))
	require.NoError(t, coordinator.RemoveObligation("recovered-watch"))

	coordinator.Wake()
	requirePollWithin(t, syncer.wake, time.Second)

	assert.Equal(t, [][]string{{"/still-unwatched"}}, syncer.snapshot())
}

func TestUnwatchedPollRemovingOneOverlappingObligationKeepsSharedRoot(t *testing.T) {
	ticks := make(chan time.Time)
	syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 2)}
	coordinator := newUnwatchedPollCoordinatorWithTicks(
		t.Context(), syncer, ticks, func() {}, func(run func()) { run() }, nil,
	)
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.AddObligation(pollingObligation{
		Key: "pending", Roots: []string{"/shared"},
	}))
	require.NoError(t, coordinator.AddObligation(pollingObligation{
		Key: "persistent", Roots: []string{"/shared", "/persistent-only"},
	}))
	require.NoError(t, coordinator.RemoveObligation("pending"))

	coordinator.Wake()
	requirePollWithin(t, syncer.wake, time.Second)

	assert.Equal(t,
		[][]string{{"/persistent-only", "/shared"}}, syncer.snapshot())
}

func TestUnwatchedPollEmptyObligationNeverExpandsToFullReconciliation(t *testing.T) {
	ticks := make(chan time.Time)
	syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 1)}
	coordinator := newUnwatchedPollCoordinatorWithTicks(
		t.Context(), syncer, ticks, func() {}, func(run func()) { run() }, nil,
	)
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.AddObligation(pollingObligation{Key: "empty"}))

	coordinator.Wake()

	assert.Never(t, func() bool { return len(syncer.snapshot()) > 0 },
		100*time.Millisecond, 10*time.Millisecond)
}

func TestUnwatchedPollStopIsConcurrentAndRejectsLaterRoots(t *testing.T) {
	ticks := make(chan time.Time)
	syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 1)}
	coordinator := newUnwatchedPollCoordinatorWithTicks(
		context.Background(), syncer, ticks, func() {}, func(run func()) { run() }, nil,
	)
	require.NoError(t, coordinator.AddRoots([]string{"/owned"}))

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			coordinator.Stop()
		})
	}
	wg.Wait()

	assert.ErrorIs(t, coordinator.AddRoots([]string{"/late"}), errUnwatchedPollStopped)
	coordinator.Wake()
	assert.Empty(t, syncer.snapshot())
}

func TestUnwatchedPollAddRootsRacingStopReturnsOwnershipOrStopped(t *testing.T) {
	const attempts = 64
	for i := range attempts {
		ticks := make(chan time.Time)
		syncer := &recordingUnwatchedPollSyncer{wake: make(chan struct{}, 1)}
		ownedSnapshots := make(chan []string, 1)
		coordinator := newUnwatchedPollCoordinatorWithTicks(
			context.Background(), syncer, ticks, func() {}, func(run func()) { run() },
			func(roots []string) {
				ownedSnapshots <- append([]string(nil), roots...)
			},
		)
		start := make(chan struct{})
		addResult := make(chan error, 1)
		stopDone := make(chan struct{})
		root := fmt.Sprintf("/race-root-%d", i)
		go func() {
			<-start
			addResult <- coordinator.AddRoots([]string{root})
		}()
		go func() {
			<-start
			coordinator.Stop()
			close(stopDone)
		}()

		close(start)
		err := requireReceivePollResult(t, addResult, time.Second)
		requirePollWithin(t, stopDone, time.Second)
		if err != nil {
			assert.ErrorIs(t, err, errUnwatchedPollStopped)
		} else {
			owned := requireReceivePollRoots(t, ownedSnapshots, time.Second)
			assert.Contains(t, owned, root)
		}
		assert.ErrorIs(t,
			coordinator.AddRoots([]string{fmt.Sprintf("/late-root-%d", i)}),
			errUnwatchedPollStopped,
		)
	}
}

func requireReceivePollRoots(
	t *testing.T,
	results <-chan []string,
	timeout time.Duration,
) []string {
	t.Helper()
	select {
	case roots := <-results:
		return roots
	case <-time.After(timeout):
		require.FailNow(t, "poll coordinator ownership did not arrive before timeout")
		return nil
	}
}

func requireReceivePollResult(
	t *testing.T,
	results <-chan error,
	timeout time.Duration,
) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(timeout):
		require.FailNow(t, "poll coordinator result did not arrive before timeout")
		return nil
	}
}

func requirePollWithin(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		require.FailNow(t, "poll did not run before timeout")
	}
}
