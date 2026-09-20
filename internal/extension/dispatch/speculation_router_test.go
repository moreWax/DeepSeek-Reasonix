package dispatch

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"reasonix/internal/extension"
	"reasonix/internal/extension/protocol"
)

type routerSpeculationClient struct {
	host extension.SpeculationHost
}

func (*routerSpeculationClient) Intercept(context.Context, protocol.InterceptEvent, json.RawMessage, time.Duration) (protocol.InterceptResult, error) {
	return protocol.InterceptResult{Decision: protocol.DecisionContinue}, nil
}
func (*routerSpeculationClient) TryNotifyEvent(protocol.InterceptEvent, json.RawMessage) error {
	return nil
}
func (*routerSpeculationClient) SpeculationContext() protocol.SessionContext {
	return protocol.SessionContext{Generation: 1, SessionID: "session"}
}
func (*routerSpeculationClient) BeginSpeculation(context.Context, protocol.SpeculationBeginParams) (protocol.SpeculationBeginResult, error) {
	return protocol.SpeculationBeginResult{Accepted: true}, nil
}
func (*routerSpeculationClient) ObserveSpeculation(context.Context, protocol.SpeculationObserveParams) (protocol.SpeculationObserveResult, error) {
	return protocol.SpeculationObserveResult{Accepted: true}, nil
}
func (*routerSpeculationClient) ClaimSpeculation(context.Context, protocol.SpeculationClaimParams) (protocol.SpeculationClaimResult, error) {
	return protocol.SpeculationClaimResult{}, nil
}
func (*routerSpeculationClient) CompleteSpeculation(context.Context, protocol.SpeculationCompleteParams) (protocol.SpeculationCompleteResult, error) {
	return protocol.SpeculationCompleteResult{Accepted: true}, nil
}
func (*routerSpeculationClient) EndSpeculation(context.Context, protocol.SpeculationEndParams) (protocol.SpeculationEndResult, error) {
	return protocol.SpeculationEndResult{}, nil
}
func (c *routerSpeculationClient) SetSpeculationHost(host extension.SpeculationHost) { c.host = host }

type routerHost struct {
	starts atomic.Int32
}

func (h *routerHost) StartSpeculation(context.Context, protocol.HostSpeculationStartParams) (protocol.HostSpeculationStartResult, error) {
	h.starts.Add(1)
	return protocol.HostSpeculationStartResult{Accepted: true, Handle: "h"}, nil
}
func (*routerHost) CancelSpeculation(context.Context, protocol.HostSpeculationCancelParams) (protocol.HostSpeculationCancelResult, error) {
	return protocol.HostSpeculationCancelResult{Cancelled: true}, nil
}

func TestSpeculationRoutesConcurrentAgentScopes(t *testing.T) {
	client := &routerSpeculationClient{}
	d := New(nil, map[extension.Slot]extension.ContributionSource{
		extension.SlotSpeculation: {PluginID: "spec"},
	}, func(string) Client { return client }, nil, Options{})
	if client.host != d {
		t.Fatal("dispatcher was not bound as the sidecar speculation router")
	}
	scopeA := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "a"}
	scopeB := protocol.SpeculationScope{Generation: 1, SessionID: "session", TurnID: "1", AttemptID: "b"}
	hostA, hostB := &routerHost{}, &routerHost{}
	if !d.RegisterSpeculationScope(scopeA, hostA) || !d.RegisterSpeculationScope(scopeB, hostB) {
		t.Fatal("register distinct scopes")
	}
	var done atomic.Int32
	for _, scope := range []protocol.SpeculationScope{scopeA, scopeB} {
		scope := scope
		go func() {
			_, _ = client.host.StartSpeculation(context.Background(), protocol.HostSpeculationStartParams{Scope: scope})
			done.Add(1)
		}()
	}
	deadline := time.Now().Add(time.Second)
	for done.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if hostA.starts.Load() != 1 || hostB.starts.Load() != 1 {
		t.Fatalf("routed starts = %d/%d", hostA.starts.Load(), hostB.starts.Load())
	}
	d.UnregisterSpeculationScope(scopeA)
	result, err := client.host.StartSpeculation(context.Background(), protocol.HostSpeculationStartParams{Scope: scopeA})
	if err != nil || result.Accepted {
		t.Fatalf("stale route = %+v, %v", result, err)
	}
}
