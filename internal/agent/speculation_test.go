package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"reasonix/internal/event"
	"reasonix/internal/extension"
	"reasonix/internal/extension/dispatch"
	"reasonix/internal/extension/protocol"
	"reasonix/internal/fileops"
	"reasonix/internal/provider"
	"reasonix/internal/tool"
	"reasonix/internal/tool/builtin"
)

type speculationTestClient struct {
	completions chan protocol.SpeculationCompleteParams
	observed    chan protocol.SpeculationObserveParams
	host        extension.SpeculationHost
}

func (c *speculationTestClient) SpeculationContext() protocol.SessionContext {
	return protocol.SessionContext{Generation: 1, SessionID: "session"}
}
func (c *speculationTestClient) BeginSpeculation(context.Context, protocol.SpeculationBeginParams) (protocol.SpeculationBeginResult, error) {
	return protocol.SpeculationBeginResult{Accepted: true}, nil
}
func (c *speculationTestClient) ObserveSpeculation(_ context.Context, params protocol.SpeculationObserveParams) (protocol.SpeculationObserveResult, error) {
	if c.observed != nil {
		c.observed <- params
	}
	return protocol.SpeculationObserveResult{Accepted: true}, nil
}
func (c *speculationTestClient) ClaimSpeculation(context.Context, protocol.SpeculationClaimParams) (protocol.SpeculationClaimResult, error) {
	return protocol.SpeculationClaimResult{}, nil
}
func (c *speculationTestClient) CompleteSpeculation(ctx context.Context, params protocol.SpeculationCompleteParams) (protocol.SpeculationCompleteResult, error) {
	if c.completions != nil {
		select {
		case c.completions <- params:
		case <-ctx.Done():
			return protocol.SpeculationCompleteResult{}, ctx.Err()
		}
	}
	return protocol.SpeculationCompleteResult{Accepted: true}, nil
}
func (c *speculationTestClient) EndSpeculation(context.Context, protocol.SpeculationEndParams) (protocol.SpeculationEndResult, error) {
	return protocol.SpeculationEndResult{}, nil
}
func (c *speculationTestClient) SetSpeculationHost(host extension.SpeculationHost) { c.host = host }
func (*speculationTestClient) Intercept(context.Context, protocol.InterceptEvent, json.RawMessage, time.Duration) (protocol.InterceptResult, error) {
	return protocol.InterceptResult{Decision: protocol.DecisionContinue}, nil
}
func (*speculationTestClient) TryNotifyEvent(protocol.InterceptEvent, json.RawMessage) error {
	return nil
}

type speculationTestTool struct {
	started            chan struct{}
	release            chan struct{}
	calls              atomic.Int32
	deterministic      bool
	ignoreCancellation bool
	policyStarted      chan struct{}
	policyRelease      chan struct{}
}

func (*speculationTestTool) Name() string            { return "pure_test" }
func (*speculationTestTool) Description() string     { return "test" }
func (*speculationTestTool) Schema() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }
func (*speculationTestTool) ReadOnly() bool          { return true }
func (t *speculationTestTool) SpeculationPolicy(json.RawMessage) (tool.SpeculationPolicy, bool) {
	if t.policyStarted != nil {
		close(t.policyStarted)
		<-t.policyRelease
	}
	return tool.SpeculationPolicy{Pure: true, Deterministic: t.deterministic}, true
}
func (t *speculationTestTool) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	if t.calls.Add(1) == 1 {
		close(t.started)
	}
	if t.ignoreCancellation {
		<-t.release
		return "speculated", nil
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-t.release:
		return "speculated", nil
	}
}

func TestHostSpeculationAdoptsCanonicalReusableCall(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{}), deterministic: true}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "attempt"}
	if !a.speculation.begin(context.Background(), scope, &turnRuntime{}, client) {
		t.Fatal("begin host speculation scope")
	}
	observed := protocol.SpeculationCall{ID: "first", Name: candidate.Name(), Arguments: json.RawMessage(`{"b":2,"a":1}`)}
	if !a.speculation.allow(scope, observed) {
		t.Fatal("allow observed call")
	}
	started, err := a.StartSpeculation(context.Background(), protocol.HostSpeculationStartParams{
		Scope: scope, Call: observed, Reusable: true,
	})
	if err != nil || !started.Accepted || started.Handle == "" {
		t.Fatalf("start = %+v, %v", started, err)
	}
	select {
	case <-candidate.started:
	case <-time.After(time.Second):
		t.Fatal("speculative execution did not start")
	}
	close(candidate.release)
	select {
	case completed := <-client.completions:
		if completed.Handle != started.Handle || completed.Completion != protocol.SpeculationReady {
			t.Fatalf("completion = %+v", completed)
		}
	case <-time.After(time.Second):
		t.Fatal("speculative execution did not complete")
	}
	result, adoptErr, ok := a.speculation.adopt(context.Background(), scope, started.Handle, protocol.SpeculationCall{
		ID: "second", Name: candidate.Name(), Arguments: json.RawMessage(`{"a":1,"b":2}`),
	})
	if !ok || adoptErr != nil || result != "speculated" {
		t.Fatalf("adopt = %q, %v, %v", result, adoptErr, ok)
	}
	if got := candidate.calls.Load(); got != 1 {
		t.Fatalf("Execute calls = %d, want 1", got)
	}
	a.speculation.end(scope)
}

func TestHostSpeculationRejectsUnobservedAndPolicyMismatch(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{}), deterministic: true}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "attempt"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	if result := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call, Reusable: true}); result.Accepted {
		t.Fatal("unobserved call was accepted")
	}
	if !a.speculation.allow(scope, call) {
		t.Fatal("allow observed call")
	}
	if result := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call, Reusable: false}); result.Accepted {
		t.Fatal("determinism policy mismatch was accepted")
	}
	a.speculation.close(scope)
	if result := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call, Reusable: true}); result.Accepted {
		t.Fatal("closing scope accepted a new start")
	}
	a.speculation.end(scope)
}

func TestHostSpeculationBlockedPolicyDoesNotHoldRuntimeLock(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{
		started: make(chan struct{}), release: make(chan struct{}),
		policyStarted: make(chan struct{}), policyRelease: make(chan struct{}),
	}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "blocked-policy"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)
	result := make(chan protocol.HostSpeculationStartResult, 1)
	go func() {
		result <- a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	}()
	<-candidate.policyStarted
	closed := make(chan struct{})
	go func() {
		a.speculation.close(scope)
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("blocked speculation policy held the global runtime mutex")
	}
	close(candidate.policyRelease)
	select {
	case started := <-result:
		if started.Accepted {
			t.Fatal("start was accepted after its scope closed during policy evaluation")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked policy did not resume")
	}
	a.speculation.end(scope)
}

func TestHostSpeculationStartsObservedCallAtMostOnce(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "duplicate"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)

	const attempts = 16
	results := make(chan protocol.HostSpeculationStartResult, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
		}()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for result := range results {
		if result.Accepted {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted starts = %d, want 1", accepted)
	}
	if result := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call}); result.Accepted {
		t.Fatal("sequential duplicate start was accepted")
	}
	if a.speculation.allow(scope, call) {
		t.Fatal("re-observation reopened an already-started call ID")
	}
	close(candidate.release)
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("accepted speculative execution did not complete")
	}
	if got := candidate.calls.Load(); got != 1 {
		t.Fatalf("Execute calls = %d, want 1", got)
	}
	a.speculation.end(scope)
}

func TestHostSpeculationDeliversEveryCompletionWhenNotificationsBackUp(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "completion-backup"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	const count = 16
	for i := range count {
		call := protocol.SpeculationCall{
			ID: fmt.Sprintf("call-%d", i), Name: candidate.Name(),
			Arguments: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		}
		if !a.speculation.allow(scope, call) {
			t.Fatalf("allow call %d", i)
		}
		if started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call}); !started.Accepted {
			t.Fatalf("start call %d = %+v", i, started)
		}
	}
	deadline := time.Now().Add(time.Second)
	for candidate.calls.Load() != count {
		if time.Now().After(deadline) {
			t.Fatalf("Execute calls = %d, want %d", candidate.calls.Load(), count)
		}
		time.Sleep(time.Millisecond)
	}
	close(candidate.release)
	seen := make(map[string]bool, count)
	for range count {
		select {
		case completion := <-client.completions:
			if seen[completion.Handle] {
				t.Fatalf("duplicate completion for %q", completion.Handle)
			}
			seen[completion.Handle] = true
		case <-time.After(time.Second):
			t.Fatalf("received %d/%d completions", len(seen), count)
		}
	}
	a.speculation.end(scope)
}

func TestHostSpeculationHandlesAreUniqueAcrossAgents(t *testing.T) {
	newAgent := func() (*Agent, *speculationTestTool, *speculationTestClient) {
		registry := tool.NewRegistry()
		candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
		registry.Add(candidate)
		return &Agent{svc: agentServices{tools: registry}}, candidate,
			&speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	}
	a, toolA, clientA := newAgent()
	b, toolB, clientB := newAgent()
	scopeA := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "a"}
	scopeB := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "b"}
	call := protocol.SpeculationCall{ID: "call", Name: "pure_test", Arguments: json.RawMessage(`{}`)}
	for _, item := range []struct {
		agent  *Agent
		scope  protocol.SpeculationScope
		client *speculationTestClient
	}{{a, scopeA, clientA}, {b, scopeB, clientB}} {
		item.agent.speculation.begin(context.Background(), item.scope, &turnRuntime{}, item.client)
		item.agent.speculation.allow(item.scope, call)
	}
	startedA := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scopeA, Call: call})
	startedB := b.speculation.start(b, protocol.HostSpeculationStartParams{Scope: scopeB, Call: call})
	if !startedA.Accepted || !startedB.Accepted || startedA.Handle == startedB.Handle {
		t.Fatalf("cross-agent handles = %+v / %+v", startedA, startedB)
	}
	close(toolA.release)
	close(toolB.release)
	<-clientA.completions
	<-clientB.completions
	a.speculation.end(scopeA)
	b.speculation.end(scopeB)
}

func TestHostSpeculativeReadDoesNotMutateAuthoritativeObservations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.txt")
	if err := os.WriteFile(path, []byte("content\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	for _, candidate := range (builtin.Workspace{Dir: dir}).Tools("read_file") {
		registry.Add(candidate)
	}
	observations := fileops.NewStore()
	a := &Agent{svc: agentServices{tools: registry}, fileObservations: observations}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "observations"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: "read_file", Arguments: json.RawMessage(`{"path":"source.txt"}`)}
	a.speculation.allow(scope, call)
	started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if !started.Accepted {
		t.Fatalf("start = %+v", started)
	}
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("speculative read did not complete")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	target, _ := fileops.DiskSnapshot(path, info)
	if got := observations.Get(target); got.Kind != fileops.Unseen {
		t.Fatalf("unclaimed speculative read committed observation %+v", got)
	}
	adoption, ok := a.speculation.adoptDetailed(context.Background(), scope, started.Handle, call)
	if !ok || adoption.err != nil {
		t.Fatalf("adopt = %+v, %v", adoption, ok)
	}
	candidate, _ := registry.Get("read_file")
	plan := &toolCallPlan{runTool: candidate, runArgs: call.Arguments}
	if !a.validateSpeculativeRead(context.Background(), plan, adoption) {
		t.Fatal("fresh speculative read was rejected")
	}
	if got := observations.Get(target); got.Kind != fileops.Present {
		t.Fatalf("adopted speculative read did not commit observation: %+v", got)
	}
	a.speculation.end(scope)
}

func TestStaleSpeculativeReadDoesNotMutateAuthoritativeObservations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.txt")
	if err := os.WriteFile(path, []byte("before\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	for _, candidate := range (builtin.Workspace{Dir: dir}).Tools("read_file") {
		registry.Add(candidate)
	}
	observations := fileops.NewStore()
	a := &Agent{svc: agentServices{tools: registry}, fileObservations: observations}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "stale-observations"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: "read_file", Arguments: json.RawMessage(`{"path":"source.txt"}`)}
	a.speculation.allow(scope, call)
	started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if !started.Accepted {
		t.Fatalf("start = %+v", started)
	}
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("speculative read did not complete")
	}
	adoption, ok := a.speculation.adoptDetailed(context.Background(), scope, started.Handle, call)
	if !ok || adoption.err != nil {
		t.Fatalf("adopt = %+v, %v", adoption, ok)
	}
	replacement := filepath.Join(dir, "replacement.txt")
	if err := os.WriteFile(replacement, []byte("after\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	candidate, _ := registry.Get("read_file")
	plan := &toolCallPlan{runTool: candidate, runArgs: call.Arguments}
	if a.validateSpeculativeRead(context.Background(), plan, adoption) {
		t.Fatal("stale speculative read was accepted")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	target, _ := fileops.DiskSnapshot(path, info)
	if got := observations.Get(target); got.Kind != fileops.Unseen {
		t.Fatalf("stale speculative read committed observation %+v", got)
	}
	a.speculation.end(scope)
}

type speculationPermissionGate struct {
	allow bool
}

func (g speculationPermissionGate) Check(context.Context, string, json.RawMessage, bool) (bool, string, error) {
	return g.allow, "denied", nil
}
func (g speculationPermissionGate) SpeculationAllowed(string, json.RawMessage, bool) bool {
	return g.allow
}

func TestHostSpeculationRequiresPermissionPreauthorization(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	a.SetGate(speculationPermissionGate{allow: false})
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "permission"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)
	result := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if result.Accepted || candidate.calls.Load() != 0 {
		t.Fatalf("denied speculative start = %+v, calls=%d", result, candidate.calls.Load())
	}
	a.speculation.end(scope)
}

func TestHostSpeculationRejectsPermissionReplacementStrategy(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	extensions := dispatch.New(nil, map[extension.Slot]extension.ContributionSource{
		extension.SlotPermission: {PluginID: "permission-owner"},
	}, nil, nil, dispatch.Options{})
	a := &Agent{svc: agentServices{tools: registry, extensions: extensions}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "permission-slot"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)
	result := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if result.Accepted || candidate.calls.Load() != 0 {
		t.Fatalf("permission replacement speculative start = %+v, calls=%d", result, candidate.calls.Load())
	}
	a.speculation.end(scope)
}

func TestHostSpeculationHoldsPermissionGateThroughExecution(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	a.SetGate(speculationPermissionGate{allow: true})
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "gate-lease"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)
	started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if !started.Accepted {
		t.Fatalf("start = %+v", started)
	}
	select {
	case <-candidate.started:
	case <-time.After(time.Second):
		t.Fatal("speculative execution did not start")
	}
	replaced := make(chan struct{})
	go func() {
		a.SetGate(speculationPermissionGate{allow: false})
		close(replaced)
	}()
	select {
	case <-replaced:
		t.Fatal("permission gate was replaced while speculative execution was active")
	case <-time.After(30 * time.Millisecond):
	}
	close(candidate.release)
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("speculative execution did not complete")
	}
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("permission gate lease was not released after execution")
	}
	a.speculation.end(scope)
}

func TestHostSpeculationReleasesPermissionGateBeforeCompletionNotification(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	a.SetGate(speculationPermissionGate{allow: true})
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "notification"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)
	started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if !started.Accepted {
		t.Fatalf("start = %+v", started)
	}
	<-candidate.started
	replaced := make(chan struct{})
	go func() {
		a.SetGate(speculationPermissionGate{allow: false})
		close(replaced)
	}()
	close(candidate.release)
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("blocked completion notification retained permission gate lease")
	}
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("completion notification was not sent")
	}
	a.speculation.end(scope)
}

func TestHostSpeculationCancellationPinsPermissionLeaseUntilBlockedToolReturns(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{
		started: make(chan struct{}), release: make(chan struct{}), ignoreCancellation: true,
	}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	a.SetGate(speculationPermissionGate{allow: true})
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 1)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "blocked-cancel"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	call := protocol.SpeculationCall{ID: "call", Name: candidate.Name(), Arguments: json.RawMessage(`{}`)}
	a.speculation.allow(scope, call)
	started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: call})
	if !started.Accepted {
		t.Fatalf("start = %+v", started)
	}
	<-candidate.started
	if !a.speculation.cancel(scope, started.Handle) {
		t.Fatal("cancel rejected active speculative execution")
	}
	replaced := make(chan struct{})
	go func() {
		a.SetGate(speculationPermissionGate{allow: false})
		close(replaced)
	}()
	select {
	case <-replaced:
		t.Fatal("cancelled execution released its permission lease before Execute returned")
	case <-time.After(30 * time.Millisecond):
	}
	close(candidate.release)
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("permission gate lease was not released after blocked Execute returned")
	}
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("detached speculative execution did not clean up")
	}
	a.speculation.end(scope)
}

func TestHostSpeculationCancellationWinsReadyToken(t *testing.T) {
	runtime := hostSpeculationRuntime{scopes: make(map[protocol.SpeculationScope]*hostSpeculationScope)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "cancelled"}
	call := protocol.SpeculationCall{ID: "call", Name: "read_file", Arguments: json.RawMessage(`{}`)}
	done := make(chan struct{})
	close(done)
	runtime.scopes[scope] = &hostSpeculationScope{tokens: map[string]*hostSpeculationToken{
		"handle": {call: call, result: "stale success", done: done},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adoption, ok := runtime.adoptDetailed(ctx, scope, "handle", call)
	if !ok || adoption.err != context.Canceled || adoption.result != "" {
		t.Fatalf("cancelled adoption = %+v, %v", adoption, ok)
	}
}

func TestValidateSpeculativeExecutionErrorFallsBack(t *testing.T) {
	candidate := &speculationTestTool{}
	plan := &toolCallPlan{runTool: candidate}
	if (&Agent{}).validateSpeculativeRead(context.Background(), plan, speculationAdoption{err: errors.New("failed")}) {
		t.Fatal("failed speculative execution was accepted instead of falling back")
	}
}

func TestHostSpeculationRejectsWrongNondeterministicHandle(t *testing.T) {
	registry := tool.NewRegistry()
	candidate := &speculationTestTool{started: make(chan struct{}), release: make(chan struct{})}
	registry.Add(candidate)
	a := &Agent{svc: agentServices{tools: registry}}
	client := &speculationTestClient{completions: make(chan protocol.SpeculationCompleteParams, 2)}
	scope := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "identity"}
	a.speculation.begin(context.Background(), scope, &turnRuntime{}, client)
	first := protocol.SpeculationCall{ID: "first", Name: candidate.Name(), Arguments: json.RawMessage(`{"x":1}`)}
	second := protocol.SpeculationCall{ID: "second", Name: candidate.Name(), Arguments: json.RawMessage(`{"x":1}`)}
	a.speculation.allow(scope, first)
	a.speculation.allow(scope, second)
	started := a.speculation.start(a, protocol.HostSpeculationStartParams{Scope: scope, Call: first})
	if !started.Accepted {
		t.Fatalf("start = %+v", started)
	}
	if _, _, ok := a.speculation.adopt(context.Background(), scope, started.Handle, second); ok {
		t.Fatal("nondeterministic handle was adopted for a different call ID")
	}
	close(candidate.release)
	select {
	case <-client.completions:
	case <-time.After(time.Second):
		t.Fatal("speculative execution did not stop")
	}
	a.speculation.end(scope)
}

type stagedSpeculationProvider struct {
	release <-chan struct{}
}

func (*stagedSpeculationProvider) Name() string { return "speculation-test" }
func (p *stagedSpeculationProvider) Stream(ctx context.Context, _ provider.Request) (<-chan provider.Chunk, error) {
	chunks := make(chan provider.Chunk)
	go func() {
		defer close(chunks)
		select {
		case chunks <- provider.Chunk{Type: provider.ChunkToolCall, ToolCall: &provider.ToolCall{
			ID: "early", Name: "pure_test", Arguments: `{"value":1}`,
		}}:
		case <-ctx.Done():
			return
		}
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	}()
	return chunks, nil
}

func TestStreamObservesCompleteToolCallBeforeProviderFinishes(t *testing.T) {
	release := make(chan struct{})
	client := &speculationTestClient{observed: make(chan protocol.SpeculationObserveParams, 1)}
	dispatcher := dispatch.New(nil, map[extension.Slot]extension.ContributionSource{
		extension.SlotSpeculation: {PluginID: "spec-ptc"},
	}, func(string) dispatch.Client { return client }, nil, dispatch.Options{})
	registry := tool.NewRegistry()
	registry.Add(&speculationTestTool{started: make(chan struct{}), release: make(chan struct{})})
	a := &Agent{svc: agentServices{
		prov:       &stagedSpeculationProvider{release: release},
		sink:       event.Discard,
		tools:      registry,
		extensions: dispatcher,
	}}
	resultCh := make(chan streamedTurn, 1)
	go func() {
		resultCh <- a.streamWithFrozen(context.Background(), 1, event.Discard, &samplingRequest{}, "attempt")
	}()
	select {
	case observed := <-client.observed:
		if observed.Seq != 1 || observed.Call.ID != "early" {
			t.Fatalf("observation = %+v", observed)
		}
	case <-time.After(time.Second):
		t.Fatal("tool call was not observed while provider stream remained open")
	}
	close(release)
	select {
	case result := <-resultCh:
		if result.err != nil || len(result.calls) != 1 || result.speculation == nil {
			t.Fatalf("stream result = %+v", result)
		}
		a.endSpeculationRound(result.speculation)
	case <-time.After(time.Second):
		t.Fatal("stream did not finish")
	}
}
