package ptc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	rlmrepl "github.com/XiaoConstantine/rlm-go/pkg/repl"
	"github.com/esengine/DeepSeek-Reasonix/extensions/specptc/internal/engine"
)

type fakeRLMClient struct {
	mu      sync.Mutex
	prompts []string
	started chan string
	release <-chan struct{}
}

func (f *fakeRLMClient) Query(ctx context.Context, prompt string) (rlmcore.QueryResponse, error) {
	f.mu.Lock()
	f.prompts = append(f.prompts, prompt)
	f.mu.Unlock()
	select {
	case f.started <- prompt:
	default:
	}
	if f.release != nil {
		select {
		case <-ctx.Done():
			return rlmcore.QueryResponse{}, ctx.Err()
		case <-f.release:
		}
	}
	return rlmcore.QueryResponse{Response: "response:" + prompt, PromptTokens: 2, CompletionTokens: 3}, nil
}

func (f *fakeRLMClient) QueryBatched(ctx context.Context, prompts []string) ([]rlmcore.QueryResponse, error) {
	out := make([]rlmcore.QueryResponse, len(prompts))
	for i, prompt := range prompts {
		response, err := f.Query(ctx, prompt)
		if err != nil {
			return nil, err
		}
		out[i] = response
	}
	return out, nil
}

func (f *fakeRLMClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.prompts)
}

type sequencedRLMClient struct {
	mu    sync.Mutex
	calls int
}

func (c *sequencedRLMClient) Query(_ context.Context, _ string) (rlmcore.QueryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return rlmcore.QueryResponse{Response: fmt.Sprintf("sample-%d", c.calls)}, nil
}

func (c *sequencedRLMClient) QueryBatched(ctx context.Context, prompts []string) ([]rlmcore.QueryResponse, error) {
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

func TestGuardedRLMClientLimitsEveryExternalCall(t *testing.T) {
	raw := &fakeRLMClient{started: make(chan string, 8)}
	client := guardedRLMClient{
		client: raw,
		guard:  newQueryGuard(QueryLimits{MaxPromptBytes: 8, MaxConcurrent: 1, MaxCalls: 1}),
	}
	if _, err := client.Query(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(context.Background(), "second"); err == nil {
		t.Fatal("external call limit was not enforced")
	}
	if _, err := client.Query(context.Background(), "123456789"); err == nil {
		t.Fatal("external prompt byte limit was not enforced")
	}
	if raw.count() != 1 {
		t.Fatalf("external calls = %d, want 1", raw.count())
	}
}

func TestClaimingRLMClientRejectsHostileBatchAndPrompt(t *testing.T) {
	client := &fakeRLMClient{started: make(chan string, 8)}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: client}, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("query-limits")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	claiming := ClaimingRLMClient{
		Scheduler: scheduler, Backend: backend, Scope: scope,
		Guard: newQueryGuard(QueryLimits{MaxBatch: 2, MaxPromptBytes: 8, MaxConcurrent: 1, MaxCalls: 2}),
	}
	if _, err := claiming.QueryBatched(context.Background(), []string{"a", "b", "c"}); err == nil {
		t.Fatal("oversized batch was accepted")
	}
	if _, err := claiming.Query(context.Background(), "123456789"); err == nil {
		t.Fatal("oversized prompt was accepted")
	}
	if client.count() != 0 {
		t.Fatalf("rejected inputs executed %d model calls", client.count())
	}
	_, _ = scheduler.End(context.Background(), scope)
	backend.ReleaseScope(scope)
}

func TestRealRLMGoREPLReservesDuplicateAsyncCallsInSourceOrder(t *testing.T) {
	client := &sequencedRLMClient{}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: client}, engine.Config{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("ordered-async")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	code := `h1 := QueryAsync("same")
	h2 := QueryAsync("same")
	println(WaitAsync(h1))
	println(WaitAsync(h2))`
	bridge := NewStreamBridge(Planner{Tools: RLMToolPolicies("")}, nil, EngineSink{Engine: scheduler, Scope: scope})
	if err := bridge.Handler(nil)("```go\n"+code+"\n", false); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		client.mu.Lock()
		calls := client.calls
		client.mu.Unlock()
		if calls == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("speculative calls = %d, want 2", calls)
		}
		time.Sleep(time.Millisecond)
	}
	real := rlmrepl.New(ClaimingRLMClient{Scheduler: scheduler, Backend: backend, Scope: scope})
	defer real.Close()
	result, err := real.Execute(context.Background(), code)
	if err != nil || result.Stderr != "" {
		t.Fatalf("execute error=%v result=%+v", err, result)
	}
	lines := strings.Fields(result.Stdout)
	if len(lines) != 2 || lines[0] == lines[1] ||
		(lines[0] != "sample-1" && lines[0] != "sample-2") ||
		(lines[1] != "sample-1" && lines[1] != "sample-2") {
		t.Fatalf("stdout = %q", result.Stdout)
	}
	metrics, err := scheduler.End(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	backend.ReleaseScope(scope)
	if metrics.Hits != 2 || metrics.Misses != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestRealRLMGoREPLClaimsAsyncSpeculation(t *testing.T) {
	release := make(chan struct{})
	client := &fakeRLMClient{started: make(chan string, 8), release: release}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: client}, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("real-async")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	contextValue := "ctx"
	code := "handle := QueryAsync(\"async\")\nanswer := WaitAsync(handle)\nprintln(answer)"
	bridge := NewStreamBridge(Planner{Tools: RLMToolPolicies(contextValue)}, nil, EngineSink{Engine: scheduler, Scope: scope})
	if err := bridge.Handler(nil)("```go\n"+code+"\n", false); err != nil {
		t.Fatal(err)
	}
	waitPromptStarts(t, client.started, 1)
	close(release)

	real := rlmrepl.New(ClaimingRLMClient{Scheduler: scheduler, Backend: backend, Scope: scope})
	defer real.Close()
	if err := real.LoadContext(contextValue); err != nil {
		t.Fatal(err)
	}
	result, err := real.Execute(context.Background(), code)
	if err != nil || result.Stderr != "" {
		t.Fatalf("execute error=%v result=%+v", err, result)
	}
	metrics, err := scheduler.End(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	backend.ReleaseScope(scope)
	if client.count() != 1 || metrics.Hits != 1 {
		t.Fatalf("calls=%d metrics=%+v", client.count(), metrics)
	}
}

func TestRealRLMGoREPLClaimsStreamedSpeculation(t *testing.T) {
	release := make(chan struct{})
	client := &fakeRLMClient{started: make(chan string, 8), release: release}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: client}, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("real-repl")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	contextValue := "large document"
	code := "answer := Query(\"summarize\")\nprintln(answer)"
	bridge := NewStreamBridge(Planner{Tools: RLMToolPolicies(contextValue)}, nil, EngineSink{Engine: scheduler, Scope: scope})
	if err := bridge.Handler(nil)("```go\n"+code+"\n", false); err != nil {
		t.Fatal(err)
	}
	select {
	case prompt := <-client.started:
		if want := RLMQueryPrompt(contextValue, "summarize"); prompt != want {
			t.Fatalf("speculative prompt = %q, want %q", prompt, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("speculative query did not start while code block was open")
	}
	close(release)

	real := rlmrepl.New(ClaimingRLMClient{Scheduler: scheduler, Backend: backend, Scope: scope})
	defer real.Close()
	if err := real.LoadContext(contextValue); err != nil {
		t.Fatal(err)
	}
	result, err := real.Execute(context.Background(), code)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stderr != "" {
		t.Fatalf("REPL execution stderr: %s", result.Stderr)
	}
	if result.Stdout == "" {
		t.Fatal("REPL produced no output")
	}
	if client.count() != 1 {
		t.Fatalf("sub-model calls = %d, want one speculative execution", client.count())
	}
	metrics, err := scheduler.End(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	backend.ReleaseScope(scope)
	if metrics.Hits != 1 || metrics.Misses != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestClaimingRLMClientReservesDuplicateBatchBeforeConcurrentWaits(t *testing.T) {
	client := &sequencedRLMClient{}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: client}, engine.Config{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("ordered-batch")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	plans := Planner{Tools: RLMToolPolicies("")}.Plan(`QueryBatchedRaw([]string{"same", "same", "same"})`, nil)
	sink := EngineSink{Engine: scheduler, Scope: scope}
	for _, plan := range plans {
		sink.Upsert(plan)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		client.mu.Lock()
		calls := client.calls
		client.mu.Unlock()
		if calls == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("speculative calls = %d, want 3", calls)
		}
		time.Sleep(time.Millisecond)
	}
	responses, err := (ClaimingRLMClient{Scheduler: scheduler, Backend: backend, Scope: scope}).QueryBatched(
		context.Background(), []string{"same", "same", "same"},
	)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(responses))
	for _, response := range responses {
		seen[response.Response] = true
	}
	for i := 1; i <= 3; i++ {
		want := fmt.Sprintf("sample-%d", i)
		if !seen[want] {
			t.Fatalf("responses = %#v, missing %q", responses, want)
		}
	}
	metrics, err := scheduler.End(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	backend.ReleaseScope(scope)
	if metrics.Hits != 3 || metrics.Misses != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestClaimingRLMClientClaimsBatchedElementsIndependently(t *testing.T) {
	client := &fakeRLMClient{started: make(chan string, 8)}
	scheduler, backend, err := NewQueryRuntime(RLMQueryExecutor{Client: client}, engine.Config{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	scope := runtimeScope("real-batch")
	if err := scheduler.Begin(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	plans := Planner{Tools: RLMToolPolicies("")}.Plan(`QueryBatchedRaw([]string{"a", "b", "c"})`, nil)
	sink := EngineSink{Engine: scheduler, Scope: scope}
	for _, plan := range plans {
		sink.Upsert(plan)
	}
	waitPromptStarts(t, client.started, 3)

	responses, err := (ClaimingRLMClient{Scheduler: scheduler, Backend: backend, Scope: scope}).QueryBatched(context.Background(), []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 3 || client.count() != 3 {
		t.Fatalf("responses=%d calls=%d, want 3 each", len(responses), client.count())
	}
	metrics, err := scheduler.End(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	backend.ReleaseScope(scope)
	if metrics.Hits != 3 {
		t.Fatalf("metrics = %+v", metrics)
	}
}
