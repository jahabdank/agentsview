//go:build darwin && cgo

package sync

import (
	"fmt"
	"path/filepath"
	gosync "sync"

	"golang.org/x/sys/unix"
)

// vnodeObserverShutdownIdent is the EVFILT_USER identifier the observer triggers
// to interrupt run()'s blocked Kevent on Close. EVFILT_USER lives in a separate
// keventidentifier namespace from the EVFILT_VNODE file descriptors, so it never
// collides with a watched fd.
const vnodeObserverShutdownIdent = 1

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
	done   chan struct{}
}

func newVnodeObserver(wake func()) (*vnodeObserver, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("create ancestor kqueue: %w", err)
	}
	// Register the shutdown user event before run() blocks so Close can wake it
	// deterministically rather than relying on closing the kqueue mid-Kevent,
	// which is not guaranteed to interrupt the syscall on Darwin.
	shutdown := unix.Kevent_t{}
	unix.SetKevent(&shutdown, vnodeObserverShutdownIdent, unix.EVFILT_USER,
		unix.EV_ADD|unix.EV_CLEAR)
	if _, err := unix.Kevent(kq, []unix.Kevent_t{shutdown}, nil, nil); err != nil {
		_ = unix.Close(kq)
		return nil, fmt.Errorf("register ancestor shutdown event: %w", err)
	}
	o := &vnodeObserver{
		kq:    kq,
		fds:   make(map[string]int),
		paths: make(map[int]string),
		wake:  wake,
		done:  make(chan struct{}),
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
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	trigger := unix.Kevent_t{}
	unix.SetKevent(&trigger, vnodeObserverShutdownIdent, unix.EVFILT_USER, 0)
	trigger.Fflags = unix.NOTE_TRIGGER
	_, triggerErr := unix.Kevent(o.kq, []unix.Kevent_t{trigger}, nil, nil)
	o.mu.Unlock()

	// The user event interrupts run()'s blocked Kevent deterministically. Only
	// if triggering somehow failed do we fall back to closing the kqueue to
	// unblock run(); either way we wait for run() to return before closing the
	// descriptors it reads, so no in-flight event fires after shutdown.
	if triggerErr != nil {
		_ = unix.Close(o.kq)
	}
	<-o.done

	o.mu.Lock()
	defer o.mu.Unlock()
	for path, fd := range o.fds {
		_ = unix.Close(fd)
		delete(o.fds, path)
		delete(o.paths, fd)
	}
	if triggerErr != nil {
		return nil // the kqueue was already closed as the wake fallback
	}
	if err := unix.Close(o.kq); err != nil {
		return fmt.Errorf("closing ancestor kqueue: %w", err)
	}
	return nil
}

func (o *vnodeObserver) watchedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.fds)
}

func (o *vnodeObserver) run() {
	defer close(o.done)
	events := make([]unix.Kevent_t, 8)
	for {
		n, err := unix.Kevent(o.kq, nil, events, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return // kq closed
		}
		shutdown := false
		vnodeEvents := false
		for i := range n {
			if events[i].Filter == unix.EVFILT_USER {
				shutdown = true
			} else {
				vnodeEvents = true
			}
		}
		if vnodeEvents {
			o.wake()
		}
		if shutdown {
			return
		}
	}
}
