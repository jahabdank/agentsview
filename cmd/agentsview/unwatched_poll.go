package main

import (
	"context"
	"errors"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"go.kenn.io/agentsview/internal/server"
)

var errUnwatchedPollStopped = errors.New("unwatched poll coordinator stopped")

type unwatchedPollCoordinator interface {
	AddRoots([]string) error
	AddObligation(pollingObligation) error
	RemoveObligation(string) error
	Wake()
	Stop()
}

type unwatchedPollSyncer interface {
	ReconcileWatchRoots(context.Context, []string, bool) error
}

type unwatchedPollAdd struct {
	obligation pollingObligation
	remove     bool
	done       chan struct{}
}

type pollingObligation struct {
	Key   string
	Roots []string
}

type sharedUnwatchedPollCoordinator struct {
	ctx          context.Context
	workerCtx    context.Context
	workerCancel context.CancelFunc
	engine       unwatchedPollSyncer
	ticks        <-chan time.Time
	stopTicker   func()
	doWork       func(func())
	// onRootsOwned is a test observer invoked after installation and before ack.
	onRootsOwned func([]string)
	add          chan unwatchedPollAdd
	// pollWake coalesces ticks and explicit wakes while the serialized worker runs.
	pollWake chan struct{}
	pollDone chan struct{}
	pollMu   sync.Mutex
	// pollRoots is the latest complete snapshot owned by the coordinator loop.
	pollRoots []string
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
}

func newUnwatchedPollCoordinator(
	ctx context.Context,
	engine unwatchedPollSyncer,
	idleTracker *server.IdleTracker,
) unwatchedPollCoordinator {
	ticker := time.NewTicker(unwatchedPollInterval)
	return newUnwatchedPollCoordinatorWithTicks(
		ctx, engine, ticker.C, ticker.Stop, idleTracker.Do, nil,
	)
}

func newUnwatchedPollCoordinatorWithTicks(
	ctx context.Context,
	engine unwatchedPollSyncer,
	ticks <-chan time.Time,
	stopTicker func(),
	doWork func(func()),
	onRootsOwned func([]string),
) *sharedUnwatchedPollCoordinator {
	workerCtx, workerCancel := context.WithCancel(ctx)
	coordinator := &sharedUnwatchedPollCoordinator{
		ctx:          ctx,
		workerCtx:    workerCtx,
		workerCancel: workerCancel,
		engine:       engine,
		ticks:        ticks,
		stopTicker:   stopTicker,
		doWork:       doWork,
		add:          make(chan unwatchedPollAdd),
		pollWake:     make(chan struct{}, 1),
		pollDone:     make(chan struct{}),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		onRootsOwned: onRootsOwned,
	}
	go coordinator.run()
	return coordinator
}

func (c *sharedUnwatchedPollCoordinator) AddRoots(roots []string) error {
	owned := append([]string(nil), roots...)
	slices.Sort(owned)
	owned = slices.Compact(owned)
	return c.AddObligation(pollingObligation{
		Key: "direct:" + strings.Join(owned, "\x00"), Roots: owned,
	})
}

func (c *sharedUnwatchedPollCoordinator) AddObligation(
	obligation pollingObligation,
) error {
	if obligation.Key == "" {
		return errors.New("polling obligation key is empty")
	}
	return c.updateRoots(obligation, false)
}

func (c *sharedUnwatchedPollCoordinator) RemoveObligation(key string) error {
	return c.updateRoots(pollingObligation{Key: key}, true)
}

func (c *sharedUnwatchedPollCoordinator) updateRoots(
	obligation pollingObligation, remove bool,
) error {
	request := unwatchedPollAdd{
		obligation: pollingObligation{
			Key: obligation.Key, Roots: append([]string(nil), obligation.Roots...),
		},
		remove: remove,
		done:   make(chan struct{}),
	}
	select {
	case <-c.done:
		return errUnwatchedPollStopped
	case c.add <- request:
	}
	<-request.done
	return nil
}

func (c *sharedUnwatchedPollCoordinator) Wake() {
	select {
	case <-c.done:
		return
	default:
	}
	c.requestPoll()
}

func (c *sharedUnwatchedPollCoordinator) Stop() {
	c.stopOnce.Do(func() {
		c.workerCancel()
		close(c.stop)
	})
	<-c.done
}

func (c *sharedUnwatchedPollCoordinator) run() {
	defer close(c.done)
	defer c.stopTicker()
	go c.runPollWorker()
	defer func() { <-c.pollDone }()
	obligations := make(map[string][]string)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.stop:
			return
		case request := <-c.add:
			if request.remove {
				delete(obligations, request.obligation.Key)
			} else {
				obligations[request.obligation.Key] = append(
					[]string(nil), request.obligation.Roots...,
				)
			}
			roots := unwatchedPollObligationRoots(obligations)
			c.setPollRoots(roots)
			if c.onRootsOwned != nil {
				c.onRootsOwned(roots)
			}
			close(request.done)
		case <-c.ticks:
			c.requestPoll()
		}
	}
}

func (c *sharedUnwatchedPollCoordinator) setPollRoots(roots []string) {
	c.pollMu.Lock()
	c.pollRoots = append(c.pollRoots[:0], roots...)
	c.pollMu.Unlock()
}

func (c *sharedUnwatchedPollCoordinator) currentPollRoots() []string {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	return append([]string(nil), c.pollRoots...)
}

func (c *sharedUnwatchedPollCoordinator) requestPoll() {
	select {
	case c.pollWake <- struct{}{}:
	default:
	}
}

func (c *sharedUnwatchedPollCoordinator) runPollWorker() {
	defer close(c.pollDone)
	for {
		select {
		case <-c.workerCtx.Done():
			return
		default:
		}
		select {
		case <-c.workerCtx.Done():
			return
		case <-c.pollWake:
			if c.workerCtx.Err() != nil {
				return
			}
			roots := availableUnwatchedPollRoots(c.currentPollRoots())
			if len(roots) == 0 {
				continue
			}
			log.Printf("polling %d unwatched root(s)", len(roots))
			c.doWork(func() {
				if c.workerCtx.Err() != nil {
					return
				}
				pollUnwatchedRootsOnce(c.workerCtx, c.engine, roots)
			})
		}
	}
}

func availableUnwatchedPollRoots(roots []string) []string {
	available := make([]string, 0, len(roots))
	for _, root := range roots {
		if _, err := os.Stat(root); err == nil {
			available = append(available, root)
		}
	}
	return available
}

func unwatchedPollObligationRoots(obligations map[string][]string) []string {
	owned := make(map[string]struct{})
	for _, roots := range obligations {
		for _, root := range roots {
			if root != "" {
				owned[root] = struct{}{}
			}
		}
	}
	return unwatchedPollRoots(owned)
}

func unwatchedPollRoots(owned map[string]struct{}) []string {
	roots := make([]string, 0, len(owned))
	for root := range owned {
		roots = append(roots, root)
	}
	slices.Sort(roots)
	return roots
}

func pollUnwatchedRootsOnce(
	ctx context.Context, engine unwatchedPollSyncer, roots []string,
) {
	if len(roots) == 0 {
		return
	}
	if err := engine.ReconcileWatchRoots(ctx, roots, false); err != nil {
		log.Printf("polling unwatched roots: %v", err)
	}
}
