package boot

import "sync"

// runtimeCleanupLifetime lets a reused controller adopt cleanup from later
// subgraph generations. Controller.Close calls Close once; Add remains safe if
// teardown races a rebuild and immediately retires resources added too late.
type runtimeCleanupLifetime struct {
	mu       sync.Mutex
	closed   bool
	cleanups []func()
}

func newRuntimeCleanupLifetime(initial func()) *runtimeCleanupLifetime {
	lifetime := &runtimeCleanupLifetime{}
	lifetime.Add(initial)
	return lifetime
}

func (l *runtimeCleanupLifetime) Add(cleanup func()) {
	if l == nil || cleanup == nil {
		return
	}
	l.mu.Lock()
	if !l.closed {
		l.cleanups = append(l.cleanups, cleanup)
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	cleanup()
}

func (l *runtimeCleanupLifetime) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	cleanups := l.cleanups
	l.cleanups = nil
	l.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}
