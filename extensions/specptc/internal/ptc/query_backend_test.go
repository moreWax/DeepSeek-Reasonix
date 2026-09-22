package ptc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

type fakeToolExecutor struct {
	mu        sync.Mutex
	calls     []string
	attempts  map[string]int
	started   chan string
	release   <-chan struct{}
	failFirst map[string]bool
	cancelled chan string
}

func newFakeToolExecutor() *fakeToolExecutor {
	return &fakeToolExecutor{
		attempts: make(map[string]int), started: make(chan string, 128),
		failFirst: make(map[string]bool), cancelled: make(chan string, 128),
	}
}

func (f *fakeToolExecutor) Execute(ctx context.Context, name string, values []any) (any, error) {
	prompt, _ := values[0].(string)
	f.mu.Lock()
	f.calls = append(f.calls, name+":"+prompt)
	f.attempts[prompt]++
	attempt := f.attempts[prompt]
	fail := f.failFirst[prompt] && attempt == 1
	f.mu.Unlock()
	select {
	case f.started <- prompt:
	default:
	}
	if f.release != nil {
		select {
		case <-ctx.Done():
			select {
			case f.cancelled <- prompt:
			default:
			}
			return nil, ctx.Err()
		case <-f.release:
		}
	}
	if fail {
		return nil, errors.New("transient failure")
	}
	return fmt.Sprintf("answer:%s:%d", prompt, attempt), nil
}

func (f *fakeToolExecutor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func runtimeScope(name string) engine.Scope {
	return engine.Scope{Generation: 1, SessionID: "session", TurnID: name, AttemptID: "attempt"}
}

type stubbornExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *stubbornExecutor) Execute(context.Context, string, []any) (any, error) {
	close(e.started)
	<-e.release
	return "stale", nil
}

func TestQueryTraceReportsLiveHitAndSavedLatency(t *testing.T) {
	release := make(chan struct{})
	executor := newFakeToolExecutor()
	executor.release = release
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("trace-hit")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var events []QueryTraceEvent
	backend.SetTraceHandler(func(event QueryTraceEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})
	EngineSink{Engine: scheduler, Scope: scope}.Upsert(CallPlan{
		ID: "trace-1", Name: "Query", Values: []any{"prompt"}, Arguments: mustArguments(t, "prompt"),
	})
	waitPromptStarts(t, executor.started, 1)
	time.Sleep(20 * time.Millisecond)
	reservation := backend.ReserveClaim(context.Background(), scheduler, scope, "Query", []any{"prompt"})
	if !reservation.claimed {
		t.Fatal("expected speculative reservation")
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	if result, hit, resolveErr := reservation.Resolve(context.Background()); resolveErr != nil || !hit || result == nil {
		t.Fatalf("Resolve = (%v, %v, %v)", result, hit, resolveErr)
	}
	_, _ = scheduler.End(context.Background(), scope)
	backend.ReleaseScope(scope)

	mu.Lock()
	got := append([]QueryTraceEvent(nil), events...)
	mu.Unlock()
	var dispatch, claim, done *QueryTraceEvent
	for i := range got {
		switch got[i].Kind {
		case QueryTraceDispatch:
			dispatch = &got[i]
		case QueryTraceClaimHit:
			claim = &got[i]
		case QueryTraceDone:
			if got[i].Hit {
				done = &got[i]
			}
		}
	}
	if dispatch == nil || claim == nil || done == nil {
		t.Fatalf("trace events = %+v", got)
	}
	if claim.HeadStart <= 0 || done.Wait <= 0 || done.Saved <= 0 {
		t.Fatalf("claim/done timing = claim %+v done %+v", *claim, *done)
	}
	if done.Duration < done.Wait {
		t.Fatalf("duration %v < wait %v", done.Duration, done.Wait)
	}
}

func TestQueryTraceUsesExecutionIdentityAcrossPlanReplacement(t *testing.T) {
	release := make(chan struct{})
	executor := newFakeToolExecutor()
	executor.release = release
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("trace-replace")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var dispatchIDs []string
	backend.SetTraceHandler(func(event QueryTraceEvent) {
		if event.Kind == QueryTraceDispatch {
			mu.Lock()
			dispatchIDs = append(dispatchIDs, event.ID)
			mu.Unlock()
		}
	})
	sink := EngineSink{Engine: scheduler, Backend: backend, Scope: scope}
	sink.Upsert(CallPlan{ID: "same-logical-id", Name: "Query", Values: []any{"old"}, Arguments: mustArguments(t, "old")})
	waitPromptStarts(t, executor.started, 1)
	sink.Upsert(CallPlan{ID: "same-logical-id", Name: "Query", Values: []any{"new"}, Arguments: mustArguments(t, "new")})
	waitPromptStarts(t, executor.started, 1)
	close(release)
	_, _ = scheduler.End(context.Background(), scope)
	backend.ReleaseScope(scope)
	mu.Lock()
	got := append([]string(nil), dispatchIDs...)
	mu.Unlock()
	if len(got) != 2 || got[0] == got[1] {
		t.Fatalf("dispatch trace IDs = %v, want two execution-unique IDs", got)
	}
}

func TestQueryTraceReportsAuthoritativeMissWait(t *testing.T) {
	release := make(chan struct{})
	executor := newFakeToolExecutor()
	executor.release = release
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	var done QueryTraceEvent
	backend.SetTraceHandler(func(event QueryTraceEvent) {
		if event.Kind == QueryTraceDone {
			done = event
		}
	})
	reservation := backend.ReserveClaim(context.Background(), nil, runtimeScope("trace-miss"), "Query", []any{"prompt"})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(release)
	}()
	result, hit, err := reservation.Resolve(context.Background())
	if err != nil || hit || result == nil {
		t.Fatalf("Resolve = (%v, %v, %v)", result, hit, err)
	}
	if done.Hit || done.Duration <= 0 || done.Wait < done.Duration || done.Saved != 0 {
		t.Fatalf("miss timing = %+v", done)
	}
}

func TestQueryTraceReportsAuthoritativeFailure(t *testing.T) {
	executor := newFakeToolExecutor()
	executor.failFirst["prompt"] = true
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	var failed QueryTraceEvent
	backend.SetTraceHandler(func(event QueryTraceEvent) {
		if event.Kind == QueryTraceFailed && !event.Speculative {
			failed = event
		}
	})
	reservation := backend.ReserveClaim(context.Background(), nil, runtimeScope("trace-failed"), "Query", []any{"prompt"})
	if _, hit, err := reservation.Resolve(context.Background()); err == nil || hit {
		t.Fatalf("Resolve hit=%v err=%v, want authoritative failure", hit, err)
	}
	if failed.Kind != QueryTraceFailed || failed.Wait < failed.Duration || failed.Wait <= 0 {
		t.Fatalf("failure timing = %+v", failed)
	}
}

func TestRetractedSuccessfulExecutionCannotPublishShadowResult(t *testing.T) {
	executor := &stubbornExecutor{started: make(chan struct{}), release: make(chan struct{})}
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("suppress-result")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	published := make(chan string, 1)
	backend.SetResultHandler(func(id string, _ []byte, _ any) { published <- id })
	plan := CallPlan{ID: "retracted", Name: "Query", Arguments: mustArguments(t, "prompt")}
	sink := EngineSink{Engine: scheduler, Backend: backend, Scope: scope}
	sink.Upsert(plan)
	select {
	case <-executor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("execution did not start")
	}
	sink.Retract(plan.ID)
	close(executor.release)
	select {
	case id := <-published:
		t.Fatalf("stale result published for %q", id)
	case <-time.After(100 * time.Millisecond):
	}
	_, _ = scheduler.End(context.Background(), scope)
	backend.ReleaseScope(scope)
}

func TestRLMStreamSpeculatesAndRealCallsClaimWithoutDuplicateExecution(t *testing.T) {
	release := make(chan struct{})
	executor := newFakeToolExecutor()
	executor.release = release
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("overlap")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	bridge := NewStreamBridge(queryPlanner(), nil, EngineSink{Engine: scheduler, Backend: backend, Scope: scope})
	handler := bridge.Handler(nil)
	stream := "```go\nprompts := []string{\"one\", \"two\", \"three\"}\nfor _, prompt := range prompts {\n Query(prompt)\n"
	if err := handler(stream, false); err != nil {
		t.Fatal(err)
	}
	waitPromptStarts(t, executor.started, 3)
	close(release)

	for _, prompt := range []string{"one", "two", "three"} {
		result, hit, err := backend.ExecuteClaimed(context.Background(), scheduler, scope, "Query", []any{prompt})
		if err != nil || !hit {
			t.Fatalf("claim %q = (%v, hit=%v, err=%v)", prompt, result, hit, err)
		}
		if result != "answer:"+prompt+":1" {
			t.Fatalf("claim %q result = %v", prompt, result)
		}
	}
	metrics, err := scheduler.End(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	backend.ReleaseScope(scope)
	if metrics.Hits != 3 || metrics.Misses != 0 || metrics.Dispatched != 3 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if calls := executor.callCount(); calls != 3 {
		t.Fatalf("executions = %d, want 3 speculative executions only", calls)
	}
}

func TestFailedSpeculationFallsBackToAuthoritativeExecution(t *testing.T) {
	executor := newFakeToolExecutor()
	executor.failFirst["retry"] = true
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("fallback")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	EngineSink{Engine: scheduler, Scope: scope}.Upsert(CallPlan{
		ID: "failed", Name: "Query", Values: []any{"retry"}, Arguments: mustArguments(t, "retry"),
	})
	waitPromptStarts(t, executor.started, 1)
	deadline := time.Now().Add(2 * time.Second)
	for {
		result, hit, err := backend.ExecuteClaimed(context.Background(), scheduler, scope, "Query", []any{"retry"})
		if err == nil {
			if hit {
				t.Fatal("failed speculative execution reported as hit")
			}
			if result != "answer:retry:2" {
				t.Fatalf("fallback result = %v", result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fallback did not recover: %v", err)
		}
	}
	_, _ = scheduler.End(context.Background(), scope)
	backend.ReleaseScope(scope)
	if calls := executor.callCount(); calls != 2 {
		t.Fatalf("executions = %d, want failed speculation plus fallback", calls)
	}
}

func TestStreamRetractionCancelsOvershotQueryExecutions(t *testing.T) {
	release := make(chan struct{})
	executor := newFakeToolExecutor()
	executor.release = release
	scheduler, backend, err := NewQueryRuntime(executor, engine.Config{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("retract")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	handler := NewStreamBridge(queryPlanner(), nil, EngineSink{Engine: scheduler, Backend: backend, Scope: scope}).Handler(nil)
	if err := handler("```go\nfor _, prompt := range []string{\"a\", \"b\", \"c\"} {\n Query(prompt)\n", false); err != nil {
		t.Fatal(err)
	}
	waitPromptStarts(t, executor.started, 3)
	if err := handler(" break\n", false); err != nil {
		t.Fatal(err)
	}
	cancelled := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(cancelled) < 2 {
		select {
		case prompt := <-executor.cancelled:
			cancelled[prompt] = true
		case <-deadline:
			t.Fatalf("cancelled = %#v, want two overshot iterations", cancelled)
		}
	}
	close(release)
	_, _ = scheduler.End(context.Background(), scope)
	backend.ReleaseScope(scope)
}

func waitPromptStarts(t *testing.T, started <-chan string, count int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for range count {
		select {
		case <-started:
		case <-deadline:
			t.Fatalf("timed out waiting for %d starts", count)
		}
	}
}

func mustArguments(t *testing.T, prompt string) []byte {
	t.Helper()
	arguments, err := encodeArguments([]any{prompt})
	if err != nil {
		t.Fatal(err)
	}
	return arguments
}
