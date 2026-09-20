package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"reasonix/internal/extension"
	"reasonix/internal/extension/protocol"
	"reasonix/internal/extension/rpcwire"
)

type unavailableSpeculationHost struct{}

func (unavailableSpeculationHost) StartSpeculation(context.Context, protocol.HostSpeculationStartParams) (protocol.HostSpeculationStartResult, error) {
	return protocol.HostSpeculationStartResult{Accepted: false, Reason: "speculative execution is not bound to this host"}, nil
}

func (unavailableSpeculationHost) CancelSpeculation(context.Context, protocol.HostSpeculationCancelParams) (protocol.HostSpeculationCancelResult, error) {
	return protocol.HostSpeculationCancelResult{}, nil
}

// SetSpeculationHost atomically binds the active agent's host-owned execution
// broker. Nil restores a fail-open unavailable handler.
func (c *Client) SetSpeculationHost(host extension.SpeculationHost) {
	if host == nil {
		host = unavailableSpeculationHost{}
	}
	c.speculationMu.Lock()
	c.speculation = host
	c.speculationMu.Unlock()
}

func (c *Client) speculationHost() extension.SpeculationHost {
	c.speculationMu.RLock()
	host := c.speculation
	c.speculationMu.RUnlock()
	return host
}

func (c *Client) handleSpeculationStart(ctx context.Context, raw json.RawMessage) (any, error) {
	decoded, err := protocol.DecodeExtensionRequestParams(protocol.MethodHostSpeculationStart, raw)
	if err != nil {
		return nil, protocol.MustProtocolError(protocol.ErrInvalidParams).RPCError()
	}
	return c.speculationHost().StartSpeculation(ctx, decoded.(protocol.HostSpeculationStartParams))
}

func (c *Client) handleSpeculationCancel(ctx context.Context, raw json.RawMessage) (any, error) {
	decoded, err := protocol.DecodeExtensionRequestParams(protocol.MethodHostSpeculationCancel, raw)
	if err != nil {
		return nil, protocol.MustProtocolError(protocol.ErrInvalidParams).RPCError()
	}
	return c.speculationHost().CancelSpeculation(ctx, decoded.(protocol.HostSpeculationCancelParams))
}

// SpeculationContext returns the immutable session identity used at handshake.
func (c *Client) SpeculationContext() protocol.SessionContext { return c.session }

// BeginSpeculation opens one exact turn scope in the scheduler extension.
func (c *Client) BeginSpeculation(ctx context.Context, params protocol.SpeculationBeginParams) (protocol.SpeculationBeginResult, error) {
	var result protocol.SpeculationBeginResult
	err := c.speculationRequest(ctx, protocol.MethodExtensionSpeculationBegin, params, &result)
	return result, err
}

// ObserveSpeculation submits one complete streamed call for early dispatch.
func (c *Client) ObserveSpeculation(ctx context.Context, params protocol.SpeculationObserveParams) (protocol.SpeculationObserveResult, error) {
	var result protocol.SpeculationObserveResult
	err := c.speculationRequest(ctx, protocol.MethodExtensionSpeculationObserve, params, &result)
	return result, err
}

// ClaimSpeculation asks for the oldest matching host execution token.
func (c *Client) ClaimSpeculation(ctx context.Context, params protocol.SpeculationClaimParams) (protocol.SpeculationClaimResult, error) {
	var result protocol.SpeculationClaimResult
	err := c.speculationRequest(ctx, protocol.MethodExtensionSpeculationClaim, params, &result)
	return result, err
}

// CompleteSpeculation durably reports that a host token reached terminal
// execution state. Server-busy responses are retried within ctx because losing
// a completion would retain extension-side admission until scope teardown.
func (c *Client) CompleteSpeculation(ctx context.Context, params protocol.SpeculationCompleteParams) (protocol.SpeculationCompleteResult, error) {
	for {
		var result protocol.SpeculationCompleteResult
		err := c.speculationRequest(ctx, protocol.MethodExtensionSpeculationComplete, params, &result)
		if err == nil {
			return result, nil
		}
		var responseErr *rpcwire.ResponseError
		if !errors.As(err, &responseErr) || responseErr.Code != rpcwire.ErrServerBusy {
			return protocol.SpeculationCompleteResult{}, err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return protocol.SpeculationCompleteResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// EndSpeculation closes one turn scope and returns its metrics.
func (c *Client) EndSpeculation(ctx context.Context, params protocol.SpeculationEndParams) (protocol.SpeculationEndResult, error) {
	var result protocol.SpeculationEndResult
	err := c.speculationRequest(ctx, protocol.MethodExtensionSpeculationEnd, params, &result)
	return result, err
}

func (c *Client) speculationRequest(ctx context.Context, method protocol.Method, params, result any) error {
	if err := c.readyErr(); err != nil {
		return err
	}
	raw, err := c.conn.Request(ctx, string(method), params)
	if err != nil {
		return mapRequestError(err)
	}
	decoded, err := protocol.DecodeHostRequestResult(method, raw)
	if err != nil {
		return &protocol.ProtocolError{Reason: protocol.ErrProtocolError, Message: "invalid speculation result: " + err.Error()}
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, result); err != nil {
		return errors.New("sidecar: speculation result type mismatch")
	}
	return nil
}
