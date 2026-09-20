package extension

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// SpeculationHandler carries the high-concurrency scheduler callbacks. Begin,
// Observe, Claim, End, and Complete may overlap across scopes; implementations
// must fence state by SpeculationScope and honor callback cancellation.
type SpeculationHandler struct {
	Begin    func(context.Context, SpeculationBeginParams) (SpeculationBeginResult, error)
	Observe  func(context.Context, SpeculationObserveParams) (SpeculationObserveResult, error)
	Claim    func(context.Context, SpeculationClaimParams) (SpeculationClaimResult, error)
	Complete func(context.Context, SpeculationCompleteParams) (SpeculationCompleteResult, error)
	End      func(context.Context, SpeculationEndParams) (SpeculationEndResult, error)
}

// SpeculationHost is a concurrency-safe client for host-owned speculative tool
// execution. Capture it from any SDK callback context; unlike HostUI's
// context-bound helpers, the captured client may be retained by background
// workers until Serve returns.
type SpeculationHost struct{ server *server }

// CaptureSpeculationHost binds a durable host client to this sidecar
// connection. The callback context itself is not retained.
func CaptureSpeculationHost(ctx context.Context) (SpeculationHost, error) {
	s := serverFrom(ctx)
	if s == nil {
		return SpeculationHost{}, ErrNoConnection
	}
	return SpeculationHost{server: s}, nil
}

// Start asks Reasonix to admit and asynchronously execute one opt-in pure tool
// call. Accepted means Handle is non-empty; execution completion is delivered
// through SpeculationHandler.Complete.
func (h SpeculationHost) Start(ctx context.Context, p HostSpeculationStartParams) (HostSpeculationStartResult, error) {
	if h.server == nil {
		return HostSpeculationStartResult{}, ErrNoConnection
	}
	if !validSpeculationScope(p.Scope) || !validSpeculationCall(p.Call) {
		return HostSpeculationStartResult{}, errors.New("extension: invalid speculation start")
	}
	raw, err := h.server.callHost(ctx, MethodHostSpeculationStart, p)
	if err != nil {
		return HostSpeculationStartResult{}, err
	}
	var result HostSpeculationStartResult
	if err := strictDecode(raw, &result); err != nil || result.Accepted != (strings.TrimSpace(result.Handle) != "") {
		return HostSpeculationStartResult{}, &ProtocolError{Reason: ErrProtocolError, Message: "invalid host/speculation/start result"}
	}
	return result, nil
}

// Cancel retracts one unclaimed speculative execution.
func (h SpeculationHost) Cancel(ctx context.Context, p HostSpeculationCancelParams) (bool, error) {
	if h.server == nil {
		return false, ErrNoConnection
	}
	if !validSpeculationScope(p.Scope) || strings.TrimSpace(p.Handle) == "" {
		return false, errors.New("extension: invalid speculation cancel")
	}
	raw, err := h.server.callHost(ctx, MethodHostSpeculationCancel, p)
	if err != nil {
		return false, err
	}
	var result HostSpeculationCancelResult
	if err := strictDecode(raw, &result); err != nil {
		return false, &ProtocolError{Reason: ErrProtocolError, Message: "invalid host/speculation/cancel result"}
	}
	return result.Cancelled, nil
}

func (s *server) handleSpeculationBegin(ctx context.Context, raw json.RawMessage) (any, error) {
	if s.opts.Speculation.Begin == nil {
		return nil, MustProtocolError(ErrUnknownMethod)
	}
	var p SpeculationBeginParams
	if err := strictDecode(raw, &p); err != nil || !validSpeculationScope(p.Scope) {
		return nil, MustProtocolError(ErrInvalidParams)
	}
	return s.opts.Speculation.Begin(ctx, p)
}

func (s *server) handleSpeculationObserve(ctx context.Context, raw json.RawMessage) (any, error) {
	if s.opts.Speculation.Observe == nil {
		return nil, MustProtocolError(ErrUnknownMethod)
	}
	var p SpeculationObserveParams
	if err := strictDecode(raw, &p); err != nil || p.Seq == 0 || !validSpeculationScope(p.Scope) || !validSpeculationCall(p.Call) {
		return nil, MustProtocolError(ErrInvalidParams)
	}
	return s.opts.Speculation.Observe(ctx, p)
}

func (s *server) handleSpeculationClaim(ctx context.Context, raw json.RawMessage) (any, error) {
	if s.opts.Speculation.Claim == nil {
		return nil, MustProtocolError(ErrUnknownMethod)
	}
	var p SpeculationClaimParams
	if err := strictDecode(raw, &p); err != nil || !validSpeculationScope(p.Scope) || !validSpeculationCall(p.Call) {
		return nil, MustProtocolError(ErrInvalidParams)
	}
	result, err := s.opts.Speculation.Claim(ctx, p)
	if err != nil {
		return nil, err
	}
	if result.Hit != (strings.TrimSpace(result.Handle) != "") {
		return nil, MustProtocolError(ErrInvalidParams)
	}
	return result, nil
}

func (s *server) handleSpeculationEnd(ctx context.Context, raw json.RawMessage) (any, error) {
	if s.opts.Speculation.End == nil {
		return nil, MustProtocolError(ErrUnknownMethod)
	}
	var p SpeculationEndParams
	if err := strictDecode(raw, &p); err != nil || !validSpeculationScope(p.Scope) {
		return nil, MustProtocolError(ErrInvalidParams)
	}
	return s.opts.Speculation.End(ctx, p)
}

func (s *server) handleSpeculationComplete(ctx context.Context, raw json.RawMessage) (any, error) {
	if s.opts.Speculation.Complete == nil {
		return nil, MustProtocolError(ErrUnknownMethod)
	}
	var p SpeculationCompleteParams
	if err := strictDecode(raw, &p); err != nil || !validSpeculationScope(p.Scope) || strings.TrimSpace(p.Handle) == "" ||
		p.Completion != SpeculationReady && p.Completion != SpeculationFailed {
		return nil, MustProtocolError(ErrInvalidParams)
	}
	return s.opts.Speculation.Complete(ctx, p)
}

func validSpeculationScope(scope SpeculationScope) bool {
	return scope.Generation > 0 && strings.TrimSpace(scope.SessionID) != "" &&
		strings.TrimSpace(scope.TurnID) != "" && strings.TrimSpace(scope.AttemptID) != ""
}

func validSpeculationCall(call SpeculationCall) bool {
	if strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Name) == "" || !json.Valid(call.Arguments) {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(call.Arguments, &object) == nil && object != nil
}
