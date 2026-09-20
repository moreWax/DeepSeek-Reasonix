package protocol

import (
	"encoding/json"
	"errors"
	"strings"
)

// SpeculationScope fences speculative work to one runtime generation and model
// attempt. Every method carries the complete scope so stale frames fail closed.
type SpeculationScope struct {
	Generation uint64 `json:"generation" validate:"min=1"`
	SessionID  string `json:"sessionId" validate:"nonempty"`
	TurnID     string `json:"turnId" validate:"nonempty"`
	AttemptID  string `json:"attemptId" validate:"nonempty"`
}

func (s SpeculationScope) Validate() error {
	if s.Generation == 0 || strings.TrimSpace(s.SessionID) == "" ||
		strings.TrimSpace(s.TurnID) == "" || strings.TrimSpace(s.AttemptID) == "" {
		return validationError("speculation scope requires generation, sessionId, turnId, and attemptId")
	}
	return nil
}

// SpeculationCall is one complete provider tool call observed before the model
// stream ends. Arguments is the exact provider JSON object used for both start
// and later claim identity.
type SpeculationCall struct {
	ID        string          `json:"id" validate:"nonempty"`
	Name      string          `json:"name" validate:"nonempty"`
	Arguments json.RawMessage `json:"arguments"`
}

func (c SpeculationCall) Validate() error {
	if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Name) == "" {
		return validationError("speculation call requires id and name")
	}
	if len(c.Arguments) == 0 || !json.Valid(c.Arguments) {
		return validationError("speculation call arguments must be valid JSON")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(c.Arguments, &object); err != nil || object == nil {
		return validationError("speculation call arguments must be a JSON object")
	}
	return nil
}

// SpeculationBeginParams opens one turn scope in the extension.
type SpeculationBeginParams struct {
	Scope SpeculationScope `json:"scope"`
}

func (p SpeculationBeginParams) Validate() error { return p.Scope.Validate() }

// SpeculationBeginResult reports whether the extension accepted the scope.
type SpeculationBeginResult struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// SpeculationObserveParams delivers one stream candidate. Seq is monotonic
// within Scope and lets the extension reject drops or reordering.
type SpeculationObserveParams struct {
	Scope SpeculationScope `json:"scope"`
	Seq   uint64           `json:"seq" validate:"min=1"`
	Call  SpeculationCall  `json:"call"`
}

func (p SpeculationObserveParams) Validate() error {
	if err := p.Scope.Validate(); err != nil {
		return err
	}
	if p.Seq == 0 {
		return validationError("speculation observe seq must be >= 1")
	}
	return p.Call.Validate()
}

// SpeculationObserveResult acknowledges durable enqueue, not execution.
type SpeculationObserveResult struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// SpeculationClaimParams asks for the oldest matching execution. BarrierSeq is
// the final observe sequence the extension must have processed first.
type SpeculationClaimParams struct {
	Scope      SpeculationScope `json:"scope"`
	BarrierSeq uint64           `json:"barrierSeq" validate:"min=0"`
	Call       SpeculationCall  `json:"call"`
}

func (p SpeculationClaimParams) Validate() error {
	if err := p.Scope.Validate(); err != nil {
		return err
	}
	return p.Call.Validate()
}

// SpeculationClaimResult returns an opaque host token on a hit.
type SpeculationClaimResult struct {
	Hit    bool   `json:"hit"`
	Handle string `json:"handle,omitempty"`
}

func (r SpeculationClaimResult) Validate() error {
	if r.Hit != (strings.TrimSpace(r.Handle) != "") {
		return validationError("speculation claim hit requires exactly one non-empty handle")
	}
	return nil
}

// SpeculationEndParams closes one scope after real execution has consumed all
// claims. BarrierSeq fences every observation before cleanup.
type SpeculationEndParams struct {
	Scope      SpeculationScope `json:"scope"`
	BarrierSeq uint64           `json:"barrierSeq" validate:"min=0"`
}

func (p SpeculationEndParams) Validate() error { return p.Scope.Validate() }

// SpeculationMetrics mirrors the upstream dispatch/claim/eviction accounting.
type SpeculationMetrics struct {
	Dispatched    uint64 `json:"dispatched,omitempty"`
	Hits          uint64 `json:"hits,omitempty"`
	Misses        uint64 `json:"misses,omitempty"`
	Evictions     uint64 `json:"evictions,omitempty"`
	Rejected      uint64 `json:"rejected,omitempty"`
	StartFailures uint64 `json:"startFailures,omitempty"`
	Cancelled     uint64 `json:"cancelled,omitempty"`
}

// SpeculationEndResult returns the final turn metrics.
type SpeculationEndResult struct {
	Metrics SpeculationMetrics `json:"metrics"`
}

// SpeculationCompletion names the terminal state of a host-owned execution.
type SpeculationCompletion string

const (
	SpeculationReady  SpeculationCompletion = "ready"
	SpeculationFailed SpeculationCompletion = "failed"
)

// SpeculationCompleteParams releases extension-side admission for a host token.
type SpeculationCompleteParams struct {
	Scope      SpeculationScope      `json:"scope"`
	Handle     string                `json:"handle" validate:"nonempty"`
	Completion SpeculationCompletion `json:"completion"`
}

func (p SpeculationCompleteParams) Validate() error {
	if err := p.Scope.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(p.Handle) == "" {
		return errors.New("speculation completion handle is required")
	}
	return nil
}

// SpeculationCompleteResult acknowledges durable terminal-state enqueue.
type SpeculationCompleteResult struct {
	Accepted bool `json:"accepted"`
}

// HostSpeculationStartParams asks Reasonix to admit and start one opt-in pure
// tool execution. Reusable allows deterministic calls to claim one token more
// than once.
type HostSpeculationStartParams struct {
	Scope    SpeculationScope `json:"scope"`
	Call     SpeculationCall  `json:"call"`
	Reusable bool             `json:"reusable,omitempty"`
}

func (p HostSpeculationStartParams) Validate() error {
	if err := p.Scope.Validate(); err != nil {
		return err
	}
	return p.Call.Validate()
}

// HostSpeculationStartResult reports admission without blocking for execution.
type HostSpeculationStartResult struct {
	Accepted bool   `json:"accepted"`
	Handle   string `json:"handle,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

func (r HostSpeculationStartResult) Validate() error {
	if r.Accepted != (strings.TrimSpace(r.Handle) != "") {
		return validationError("accepted speculation start requires exactly one non-empty handle")
	}
	return nil
}

// HostSpeculationCancelParams retracts one unclaimed token.
type HostSpeculationCancelParams struct {
	Scope  SpeculationScope `json:"scope"`
	Handle string           `json:"handle" validate:"nonempty"`
}

func (p HostSpeculationCancelParams) Validate() error {
	if err := p.Scope.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(p.Handle) == "" {
		return validationError("speculation cancel handle is required")
	}
	return nil
}

// HostSpeculationCancelResult reports whether cancellation won the claim race.
type HostSpeculationCancelResult struct {
	Cancelled bool `json:"cancelled"`
}
