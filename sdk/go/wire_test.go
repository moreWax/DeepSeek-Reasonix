package extension

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestSpeculationCompletionIsAcknowledgedAfterHandlerSaturation proves a
// lifecycle-critical completion is rejected explicitly under overload and can
// be retried without silent notification loss.
func TestSpeculationCompletionIsAcknowledgedAfterHandlerSaturation(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, maxConcurrentHandlers)
	completed := make(chan SpeculationCompleteParams, 1)
	host, _ := startFakeHost(t, basicHandler(), Options{
		Interceptors: map[string]InterceptorFunc{
			"tool.before": func(ctx context.Context, _ string, _ json.RawMessage) (*InterceptResult, error) {
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
				return Continue(), nil
			},
		},
		Speculation: SpeculationHandler{
			Complete: func(_ context.Context, params SpeculationCompleteParams) (SpeculationCompleteResult, error) {
				completed <- params
				return SpeculationCompleteResult{Accepted: true}, nil
			},
		},
	})
	host.handshake(t)

	parked := make([]chan hostResponse, 0, maxConcurrentHandlers)
	for i := range maxConcurrentHandlers {
		_, response := host.startRequest(MethodExtensionIntercept, InterceptParams{
			Event: EventToolBefore, Seq: uint64(i + 1), Payload: json.RawMessage(`{}`),
		})
		parked = append(parked, response)
	}
	for range maxConcurrentHandlers {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("handler pool did not saturate")
		}
	}
	params := SpeculationCompleteParams{
		Scope:  SpeculationScope{Generation: 1, SessionID: "s", TurnID: "t", AttemptID: "a"},
		Handle: "h", Completion: SpeculationReady,
	}
	busy := host.request(MethodExtensionSpeculationComplete, params)
	if busy.Err == nil || busy.Err.Code != CodeServerBusy {
		t.Fatalf("saturated completion response = %+v, want server busy", busy)
	}
	select {
	case got := <-completed:
		t.Fatalf("server-busy completion reached callback: %+v", got)
	default:
	}

	close(release)
	for i, response := range parked {
		select {
		case result := <-response:
			if result.Err != nil {
				t.Fatalf("parked request %d: %+v", i, result.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("parked request %d did not finish", i)
		}
	}
	ack := host.request(MethodExtensionSpeculationComplete, params)
	if ack.Err != nil {
		t.Fatalf("retried completion failed: %+v", ack.Err)
	}
	var result SpeculationCompleteResult
	if err := json.Unmarshal(ack.Result, &result); err != nil || !result.Accepted {
		t.Fatalf("completion ack = %s, %v", ack.Result, err)
	}
	select {
	case got := <-completed:
		if got.Handle != params.Handle || got.Scope != params.Scope {
			t.Fatalf("completion callback = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retried completion did not reach callback")
	}
}

// TestHandlerPoolSaturated verifies the bounded inbound concurrency: with all
// 32 handler slots occupied, the next request is answered -32099 (server
// busy) while the parked requests still complete afterwards.
func TestHandlerPoolSaturated(t *testing.T) {
	release := make(chan struct{})
	interceptors := map[string]InterceptorFunc{
		"tool.before": func(ctx context.Context, _ string, _ json.RawMessage) (*InterceptResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Continue(), nil
		},
	}
	host, _ := startFakeHost(t, basicHandler(), Options{Interceptors: interceptors})
	host.handshake(t)

	channels := make([]chan hostResponse, 0, maxConcurrentHandlers+1)
	for i := 0; i < maxConcurrentHandlers+1; i++ {
		_, ch := host.startRequest(MethodExtensionIntercept, InterceptParams{
			Event: EventToolBefore, Seq: uint64(i + 1), Payload: json.RawMessage(`{}`),
		})
		channels = append(channels, ch)
	}

	// Exactly one request — the one finding no handler slot — is rejected
	// immediately; the rest stay parked on release.
	busyIdx := -1
	deadline := time.Now().Add(5 * time.Second)
	for busyIdx < 0 && time.Now().Before(deadline) {
		for i, ch := range channels {
			select {
			case resp := <-ch:
				if resp.Err == nil || resp.Err.Code != CodeServerBusy {
					t.Fatalf("request %d: unexpected early response %+v", i, resp)
				}
				busyIdx = i
			default:
			}
		}
		if busyIdx < 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if busyIdx < 0 {
		t.Fatal("no request was answered server-busy")
	}
	for i, ch := range channels {
		if i == busyIdx {
			continue
		}
		select {
		case resp := <-ch:
			t.Fatalf("request %d: expected parked, got %+v", i, resp)
		default:
		}
	}

	close(release)
	for i, ch := range channels {
		if i == busyIdx {
			continue
		}
		select {
		case resp := <-ch:
			if resp.Err != nil {
				t.Fatalf("parked request %d errored: %+v", i, resp.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("parked request %d did not complete after release", i)
		}
	}
}
