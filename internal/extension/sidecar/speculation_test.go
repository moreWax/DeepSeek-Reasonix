package sidecar

import (
	"context"
	"encoding/json"
	"testing"

	"reasonix/internal/extension/protocol"
)

type recordingSpeculationHost struct {
	start  protocol.HostSpeculationStartParams
	cancel protocol.HostSpeculationCancelParams
}

func (h *recordingSpeculationHost) StartSpeculation(_ context.Context, params protocol.HostSpeculationStartParams) (protocol.HostSpeculationStartResult, error) {
	h.start = params
	return protocol.HostSpeculationStartResult{Accepted: true, Handle: "host-handle"}, nil
}
func (h *recordingSpeculationHost) CancelSpeculation(_ context.Context, params protocol.HostSpeculationCancelParams) (protocol.HostSpeculationCancelResult, error) {
	h.cancel = params
	return protocol.HostSpeculationCancelResult{Cancelled: true}, nil
}

func TestSpeculationReverseHandlersDecodeAndDelegate(t *testing.T) {
	host := &recordingSpeculationHost{}
	client := &Client{}
	client.SetSpeculationHost(host)
	scope := protocol.SpeculationScope{Generation: 2, SessionID: "s", TurnID: "t", AttemptID: "a"}
	start := protocol.HostSpeculationStartParams{
		Scope: scope,
		Call:  protocol.SpeculationCall{ID: "c", Name: "read_file", Arguments: json.RawMessage(`{"path":"x"}`)},
	}
	raw, err := json.Marshal(start)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.handleSpeculationStart(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	started := result.(protocol.HostSpeculationStartResult)
	if !started.Accepted || started.Handle != "host-handle" || host.start.Call.ID != "c" {
		t.Fatalf("start result/params = %+v / %+v", started, host.start)
	}
	cancel := protocol.HostSpeculationCancelParams{Scope: scope, Handle: "host-handle"}
	raw, _ = json.Marshal(cancel)
	result, err = client.handleSpeculationCancel(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	cancelled := result.(protocol.HostSpeculationCancelResult)
	if !cancelled.Cancelled || host.cancel.Handle != "host-handle" {
		t.Fatalf("cancel result/params = %+v / %+v", cancelled, host.cancel)
	}
}

func TestSpeculationReverseHandlerRejectsInvalidParams(t *testing.T) {
	client := &Client{}
	client.SetSpeculationHost(&recordingSpeculationHost{})
	if _, err := client.handleSpeculationStart(context.Background(), json.RawMessage(`{"scope":{}}`)); err == nil {
		t.Fatal("invalid start params were accepted")
	}
}
