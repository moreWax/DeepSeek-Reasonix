package ptc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

// RLMClient is the sub-model interface consumed by rlm-go's REPL.
type RLMClient interface {
	Query(context.Context, string) (rlmcore.QueryResponse, error)
	QueryBatched(context.Context, []string) ([]rlmcore.QueryResponse, error)
}

// RLMQueryExecutor exposes one RLMClient as a generic speculative tool.
type RLMQueryExecutor struct {
	Client RLMClient
}

func (e RLMQueryExecutor) Execute(ctx context.Context, name string, values []any) (any, error) {
	if e.Client == nil {
		return nil, errors.New("ptc: nil RLM query client")
	}
	if name != "Query" || len(values) != 1 {
		return nil, fmt.Errorf("ptc: unsupported RLM call %q", name)
	}
	prompt, ok := values[0].(string)
	if !ok {
		return nil, errors.New("ptc: RLM query prompt is not a string")
	}
	return e.Client.Query(ctx, prompt)
}

// QueryLimits bound authoritative model-call fanout before any external call.
type QueryLimits struct {
	MaxBatch       int
	MaxPromptBytes int
	MaxConcurrent  int
	MaxCalls       int64
}

func (l QueryLimits) normalized() QueryLimits {
	if l.MaxBatch <= 0 {
		l.MaxBatch = 64
	}
	if l.MaxPromptBytes <= 0 {
		l.MaxPromptBytes = 1 << 20
	}
	if l.MaxConcurrent <= 0 {
		l.MaxConcurrent = 8
	}
	if l.MaxCalls <= 0 {
		l.MaxCalls = 256
	}
	return l
}

type queryGuard struct {
	limits QueryLimits
	sem    chan struct{}
	global chan struct{}
	calls  atomic.Int64
}

func newQueryGuard(limits QueryLimits) *queryGuard {
	return newQueryGuardWithGlobal(limits, nil)
}

func newQueryGuardWithGlobal(limits QueryLimits, global chan struct{}) *queryGuard {
	limits = limits.normalized()
	return &queryGuard{limits: limits, sem: make(chan struct{}, limits.MaxConcurrent), global: global}
}

func (g *queryGuard) acquire(ctx context.Context, prompt string) error {
	if g == nil {
		return errors.New("ptc: nil query guard")
	}
	if len(prompt) > g.limits.MaxPromptBytes {
		return fmt.Errorf("ptc: query prompt exceeds %d bytes", g.limits.MaxPromptBytes)
	}
	if g.calls.Add(1) > g.limits.MaxCalls {
		g.calls.Add(-1)
		return fmt.Errorf("ptc: query call limit %d exceeded", g.limits.MaxCalls)
	}
	select {
	case g.sem <- struct{}{}:
	case <-ctx.Done():
		g.calls.Add(-1)
		return ctx.Err()
	}
	if g.global != nil {
		select {
		case g.global <- struct{}{}:
		case <-ctx.Done():
			<-g.sem
			g.calls.Add(-1)
			return ctx.Err()
		}
	}
	return nil
}

func (g *queryGuard) release() {
	if g.global != nil {
		<-g.global
	}
	<-g.sem
}

type guardedRLMClient struct {
	client RLMClient
	guard  *queryGuard
}

func (c guardedRLMClient) Query(ctx context.Context, prompt string) (rlmcore.QueryResponse, error) {
	if c.client == nil {
		return rlmcore.QueryResponse{}, errors.New("ptc: nil guarded RLM client")
	}
	if err := c.guard.acquire(ctx, prompt); err != nil {
		return rlmcore.QueryResponse{}, err
	}
	defer c.guard.release()
	return c.client.Query(ctx, prompt)
}

func (c guardedRLMClient) QueryBatched(ctx context.Context, prompts []string) ([]rlmcore.QueryResponse, error) {
	responses := make([]rlmcore.QueryResponse, len(prompts))
	for i, prompt := range prompts {
		response, err := c.Query(ctx, prompt)
		if err != nil {
			return nil, err
		}
		responses[i] = response
	}
	return responses, nil
}

// ClaimingRLMClient is installed in the authoritative rlm-go REPL. Calls claim
// matching speculative work and transparently fall back through the same
// underlying client after a miss or failed speculation.
type ClaimingRLMClient struct {
	Scheduler *engine.Engine
	Backend   *QueryBackend
	Scope     engine.Scope
	Guard     *queryGuard
}

type reservedRLMQuery struct {
	reservation *QueryReservation
	err         error
}

func (r reservedRLMQuery) Resolve(ctx context.Context) (rlmcore.QueryResponse, error) {
	if r.err != nil {
		return rlmcore.QueryResponse{}, r.err
	}
	result, _, err := r.reservation.Resolve(ctx)
	if err != nil {
		return rlmcore.QueryResponse{}, err
	}
	response, ok := result.(rlmcore.QueryResponse)
	if !ok {
		return rlmcore.QueryResponse{}, fmt.Errorf("ptc: query result has type %T", result)
	}
	return response, nil
}

// ReserveQuery implements core.ReservingLLMClient. The claim occurs on the
// caller goroutine so async and batch waits cannot reorder duplicate samples.
func (c ClaimingRLMClient) ReserveQuery(ctx context.Context, prompt string) rlmcore.QueryReservation {
	if c.Backend == nil {
		return reservedRLMQuery{err: errors.New("ptc: nil query backend")}
	}
	guard := c.Guard
	if guard == nil {
		guard = newQueryGuard(QueryLimits{})
	}
	if len(prompt) > guard.limits.MaxPromptBytes {
		return reservedRLMQuery{err: fmt.Errorf("ptc: query prompt exceeds %d bytes", guard.limits.MaxPromptBytes)}
	}
	return reservedRLMQuery{reservation: c.Backend.ReserveClaim(ctx, c.Scheduler, c.Scope, "Query", []any{prompt})}
}

func (c ClaimingRLMClient) Query(ctx context.Context, prompt string) (rlmcore.QueryResponse, error) {
	return c.ReserveQuery(ctx, prompt).Resolve(ctx)
}

func (c ClaimingRLMClient) QueryBatched(ctx context.Context, prompts []string) ([]rlmcore.QueryResponse, error) {
	results := make([]rlmcore.QueryResponse, len(prompts))
	if len(prompts) == 0 {
		return results, nil
	}
	guard := c.Guard
	if guard == nil {
		guard = newQueryGuard(QueryLimits{})
		c.Guard = guard
	}
	if len(prompts) > guard.limits.MaxBatch {
		return nil, fmt.Errorf("ptc: query batch exceeds %d prompts", guard.limits.MaxBatch)
	}
	totalBytes := 0
	for _, prompt := range prompts {
		totalBytes += len(prompt)
		if len(prompt) > guard.limits.MaxPromptBytes || totalBytes > guard.limits.MaxPromptBytes {
			return nil, fmt.Errorf("ptc: query batch exceeds %d prompt bytes", guard.limits.MaxPromptBytes)
		}
	}
	reservations := make([]rlmcore.QueryReservation, len(prompts))
	for i, prompt := range prompts {
		reservations[i] = c.ReserveQuery(ctx, prompt)
	}
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for i, reservation := range reservations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := reservation.Resolve(batchCtx)
			if err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
			results[i] = response
		}()
	}
	wg.Wait()
	return results, firstErr
}
