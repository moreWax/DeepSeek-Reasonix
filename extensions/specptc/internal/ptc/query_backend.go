package ptc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

// ToolExecutor runs one pure programmatic tool call. Implementations remain
// extension-owned; model queries, search adapters, and injected pure tools all
// share this contract.
type ToolExecutor interface {
	Execute(context.Context, string, []any) (any, error)
}

type queryFuture struct {
	scope      engine.Scope
	callID     string
	traceID    string
	name       string
	preview    string
	cancel     context.CancelFunc
	done       chan struct{}
	startedAt  time.Time
	durationNS atomic.Int64
	finished   atomic.Bool
	result     any
	err        error
	claimed    atomic.Bool
	suppressed atomic.Bool
}

func (f *queryFuture) finishTiming() time.Duration {
	duration := time.Since(f.startedAt)
	f.durationNS.Store(int64(duration))
	f.finished.Store(true)
	return duration
}

func (f *queryFuture) duration() (time.Duration, bool) {
	if !f.finished.Load() {
		return 0, false
	}
	return time.Duration(f.durationNS.Load()), true
}

type suppressedCall struct {
	scope  engine.Scope
	callID string
}

// QueryBackend adapts extension-owned programmatic tools to the common
// speculation engine. It stores results behind opaque handles until the real
// REPL claims them.
type QueryBackend struct {
	executor ToolExecutor
	next     atomic.Uint64

	mu         sync.RWMutex
	engine     *engine.Engine
	futures    map[engine.Handle]*queryFuture
	suppressed map[suppressedCall]struct{}
	onResult   func(string, []byte, any)
	trace      QueryTraceFunc
	serial     atomic.Uint64
}

// NewQueryRuntime constructs a mutually-bound scheduler and result backend.
func NewQueryRuntime(executor ToolExecutor, cfg engine.Config) (*engine.Engine, *QueryBackend, error) {
	if executor == nil {
		return nil, nil, errors.New("ptc: nil tool executor")
	}
	backend := &QueryBackend{
		executor: executor, futures: make(map[engine.Handle]*queryFuture),
		suppressed: make(map[suppressedCall]struct{}),
	}
	scheduler, err := engine.New(backend, cfg)
	if err != nil {
		return nil, nil, err
	}
	backend.engine = scheduler
	return scheduler, backend, nil
}

// SetResultHandler receives successful speculative values by generated call ID.
// It is used by the shadow planner to resume newly unblocked dependencies.
func (b *QueryBackend) SetResultHandler(handler func(string, []byte, any)) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.onResult = handler
	b.mu.Unlock()
}

// SetTraceHandler installs a request-scoped, non-blocking lifecycle observer.
func (b *QueryBackend) SetTraceHandler(trace QueryTraceFunc) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.trace = trace
	b.mu.Unlock()
}

func (b *QueryBackend) emit(event QueryTraceEvent) {
	b.mu.RLock()
	trace := b.trace
	b.mu.RUnlock()
	emitQueryTrace(trace, event)
}

func (b *QueryBackend) SuppressCall(scope engine.Scope, callID string) {
	if b == nil || callID == "" {
		return
	}
	b.mu.Lock()
	b.suppressed[suppressedCall{scope: scope, callID: callID}] = struct{}{}
	var matches []*queryFuture
	for _, future := range b.futures {
		if future.scope == scope && future.callID == callID {
			matches = append(matches, future)
		}
	}
	b.mu.Unlock()
	for _, future := range matches {
		future.suppressed.Store(true)
		future.cancel()
	}
}

// Start admits a call and launches it asynchronously. The scheduler owns all
// concurrency and argument-byte budgets before invoking this method.
func (b *QueryBackend) Start(ctx context.Context, request engine.StartRequest) (engine.Handle, error) {
	values, err := decodeArguments(request.Call.Arguments)
	if err != nil {
		return "", err
	}
	execCtx, cancel := context.WithCancel(ctx)
	handle := engine.Handle(fmt.Sprintf("ptc-%d", b.next.Add(1)))
	startedAt := time.Now()
	future := &queryFuture{
		scope: request.Scope, callID: request.Call.CallID, traceID: string(handle), name: request.Call.Tool,
		preview: queryTracePreview(values), cancel: cancel, done: make(chan struct{}), startedAt: startedAt,
	}
	b.mu.Lock()
	if _, blocked := b.suppressed[suppressedCall{scope: request.Scope, callID: request.Call.CallID}]; blocked {
		b.mu.Unlock()
		cancel()
		return "", context.Canceled
	}
	b.futures[handle] = future
	scheduler := b.engine
	b.mu.Unlock()
	b.emit(QueryTraceEvent{
		Kind: QueryTraceDispatch, ID: future.traceID, Name: request.Call.Tool,
		Preview: future.preview, Speculative: true,
	})
	if scheduler == nil {
		cancel()
		b.mu.Lock()
		delete(b.futures, handle)
		b.mu.Unlock()
		return "", errors.New("ptc: query backend is not bound")
	}
	go func() {
		future.result, future.err = b.executor.Execute(execCtx, request.Call.Tool, values)
		duration := future.finishTiming()
		close(future.done)
		traceKind := QueryTraceReady
		switch {
		case future.suppressed.Load() || execCtx.Err() != nil:
			traceKind = QueryTraceEvicted
		case future.err != nil:
			traceKind = QueryTraceFailed
		}
		b.emit(QueryTraceEvent{
			Kind: traceKind, ID: future.traceID, Name: future.name,
			Preview: future.preview, Speculative: true,
			Duration: duration,
		})
		completion := engine.CompletionReady
		if future.err != nil {
			completion = engine.CompletionFailed
		}
		scheduler.Complete(context.Background(), request.Scope, handle, completion)
		if future.err == nil && !future.suppressed.Load() && execCtx.Err() == nil {
			b.mu.RLock()
			onResult := b.onResult
			b.mu.RUnlock()
			if onResult != nil {
				onResult(request.Call.CallID, append([]byte(nil), request.Call.Arguments...), future.result)
			}
		}
	}()
	return handle, nil
}

// Cancel propagates retraction and turn shutdown to the underlying tool.
func (b *QueryBackend) Cancel(_ context.Context, handle engine.Handle) error {
	b.mu.RLock()
	future := b.futures[handle]
	b.mu.RUnlock()
	if future != nil {
		future.suppressed.Store(true)
		future.cancel()
	}
	return nil
}

// Result waits for one claimed speculative execution.
func (b *QueryBackend) Result(ctx context.Context, handle engine.Handle) (any, error) {
	b.mu.RLock()
	future := b.futures[handle]
	b.mu.RUnlock()
	if future == nil {
		return nil, errors.New("ptc: unknown speculative handle")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-future.done:
		return future.result, future.err
	}
}

// QueryReservation reserves one matching speculative occurrence in caller order
// and resolves it later without allowing concurrent waits to reorder FIFO claims.
type QueryReservation struct {
	backend     *QueryBackend
	handle      engine.Handle
	claimed     bool
	id          string
	name        string
	values      []any
	reserveWait time.Duration
}

// ReserveClaim synchronously selects speculative work but does not wait for its
// result or issue an authoritative fallback call.
func (b *QueryBackend) ReserveClaim(ctx context.Context, scheduler *engine.Engine, scope engine.Scope, name string, values []any) *QueryReservation {
	reservation := &QueryReservation{backend: b, name: name, values: append([]any(nil), values...)}
	arguments, err := encodeArguments(values)
	claimStarted := time.Now()
	if err == nil && scheduler != nil {
		reservation.handle, reservation.claimed = scheduler.Claim(ctx, scope, name, arguments)
	}
	reservation.reserveWait = time.Since(claimStarted)
	if reservation.claimed {
		b.mu.RLock()
		future := b.futures[reservation.handle]
		b.mu.RUnlock()
		if future != nil {
			future.claimed.Store(true)
			reservation.id = future.traceID
			b.emit(QueryTraceEvent{
				Kind: QueryTraceClaimHit, ID: future.traceID, Name: name,
				Preview: queryTracePreview(values), Speculative: true, Hit: true,
				HeadStart: max(claimStarted.Sub(future.startedAt), 0),
			})
		}
		return reservation
	}
	reservation.id = fmt.Sprintf("serial-%d", b.serial.Add(1))
	b.emit(QueryTraceEvent{
		Kind: QueryTraceClaimMiss, ID: reservation.id, Name: name,
		Preview: queryTracePreview(values), Speculative: false,
	})
	return reservation
}

// Resolve waits for reserved work and falls back to the authoritative executor
// after a miss or speculative failure.
func (r *QueryReservation) Resolve(ctx context.Context) (any, bool, error) {
	if r == nil || r.backend == nil {
		return nil, false, errors.New("ptc: nil query reservation")
	}
	wait := r.reserveWait
	if r.claimed {
		waitStarted := time.Now()
		result, err := r.backend.Result(ctx, r.handle)
		wait += time.Since(waitStarted)
		if err == nil {
			r.backend.mu.RLock()
			future := r.backend.futures[r.handle]
			r.backend.mu.RUnlock()
			duration := wait
			if future != nil {
				if measured, finished := future.duration(); finished {
					duration = measured
				}
			}
			saved := max(duration-wait, 0)
			r.backend.emit(QueryTraceEvent{
				Kind: QueryTraceDone, ID: r.id, Name: r.name,
				Preview: queryTracePreview(r.values), Speculative: true, Hit: true,
				Duration: duration, Wait: wait, Saved: saved,
			})
			return result, true, nil
		}
		r.backend.emit(QueryTraceEvent{
			Kind: QueryTraceClaimMiss, ID: r.id, Name: r.name,
			Preview: queryTracePreview(r.values), Speculative: false,
		})
	}
	started := time.Now()
	result, err := r.backend.executor.Execute(ctx, r.name, r.values)
	duration := time.Since(started)
	wait += duration
	kind := QueryTraceDone
	if err != nil {
		kind = QueryTraceFailed
	}
	r.backend.emit(QueryTraceEvent{
		Kind: kind, ID: r.id, Name: r.name,
		Preview: queryTracePreview(r.values), Speculative: false,
		Duration: duration, Wait: wait,
	})
	return result, false, err
}

// ExecuteClaimed returns a speculative result when available and valid;
// failures and misses transparently run the authoritative tool path.
func (b *QueryBackend) ExecuteClaimed(ctx context.Context, scheduler *engine.Engine, scope engine.Scope, name string, values []any) (result any, hit bool, err error) {
	return b.ReserveClaim(ctx, scheduler, scope, name, values).Resolve(ctx)
}

// ReleaseScope removes completed futures after the scheduler has ended a turn.
func (b *QueryBackend) ReleaseScope(scope engine.Scope) {
	b.mu.Lock()
	var evicted []QueryTraceEvent
	for handle, future := range b.futures {
		if future.scope == scope {
			b.suppressed[suppressedCall{scope: scope, callID: future.callID}] = struct{}{}
			future.suppressed.Store(true)
			future.cancel()
			if duration, finished := future.duration(); !future.claimed.Load() && finished {
				evicted = append(evicted, QueryTraceEvent{
					Kind: QueryTraceEvicted, ID: future.traceID, Name: future.name,
					Preview: future.preview, Speculative: true,
					Duration: duration,
				})
			}
			delete(b.futures, handle)
		}
	}
	b.mu.Unlock()
	for _, event := range evicted {
		b.emit(event)
	}
}

func encodeArguments(values []any) (json.RawMessage, error) {
	return json.Marshal(struct {
		Args []any `json:"args"`
	}{Args: values})
}

func decodeArguments(raw json.RawMessage) ([]any, error) {
	var payload struct {
		Args []any `json:"args"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("ptc: decode arguments: %w", err)
	}
	return payload.Args, nil
}
