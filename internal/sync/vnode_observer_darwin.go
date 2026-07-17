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
