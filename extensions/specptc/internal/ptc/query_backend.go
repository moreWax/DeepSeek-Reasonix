package ptc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	cancel     context.CancelFunc
	done       chan struct{}
	result     any
	err        error
	suppressed atomic.Bool
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
	future := &queryFuture{scope: request.Scope, callID: request.Call.CallID, cancel: cancel, done: make(chan struct{})}
	b.mu.Lock()
	if _, blocked := b.suppressed[suppressedCall{scope: request.Scope, callID: request.Call.CallID}]; blocked {
		b.mu.Unlock()
		cancel()
		return "", context.Canceled
	}
	b.futures[handle] = future
	scheduler := b.engine
	b.mu.Unlock()
	if scheduler == nil {
		cancel()
		b.mu.Lock()
		delete(b.futures, handle)
		b.mu.Unlock()
		return "", errors.New("ptc: query backend is not bound")
	}
	go func() {
		future.result, future.err = b.executor.Execute(execCtx, request.Call.Tool, values)
		close(future.done)
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
	backend *QueryBackend
	handle  engine.Handle
	claimed bool
	name    string
	values  []any
}

// ReserveClaim synchronously selects speculative work but does not wait for its
// result or issue an authoritative fallback call.
func (b *QueryBackend) ReserveClaim(ctx context.Context, scheduler *engine.Engine, scope engine.Scope, name string, values []any) *QueryReservation {
	reservation := &QueryReservation{backend: b, name: name, values: append([]any(nil), values...)}
	arguments, err := encodeArguments(values)
	if err == nil && scheduler != nil {
		reservation.handle, reservation.claimed = scheduler.Claim(ctx, scope, name, arguments)
	}
	return reservation
}

// Resolve waits for reserved work and falls back to the authoritative executor
// after a miss or speculative failure.
func (r *QueryReservation) Resolve(ctx context.Context) (any, bool, error) {
	if r == nil || r.backend == nil {
		return nil, false, errors.New("ptc: nil query reservation")
	}
	if r.claimed {
		result, err := r.backend.Result(ctx, r.handle)
		if err == nil {
			return result, true, nil
		}
	}
	result, err := r.backend.executor.Execute(ctx, r.name, r.values)
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
	defer b.mu.Unlock()
	for handle, future := range b.futures {
		if future.scope == scope {
			b.suppressed[suppressedCall{scope: scope, callID: future.callID}] = struct{}{}
			future.suppressed.Store(true)
			future.cancel()
			delete(b.futures, handle)
		}
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
